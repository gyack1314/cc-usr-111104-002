"""解析仓库自带的 schema.graphql 与 operations.graphql。

只支持本仓库使用的 GraphQL 子集：对象类型、非空标记、单层列表与带别名的
查询选择集。解析结果用于：

* 按响应里的别名与数组下标解释 errors.path；
* 依据字段非空性计算错误实际让哪个祖先节点变成 null（冒泡落点）；
* 判断 pageInfo、边与节点在一次响应中是否仍然可信。
"""
import re
from pathlib import Path

def parse_type_ref(token):
    """把 String! / [TeamEdge!]! 这类类型记号拆成结构化表示。"""
    rest = token
    outer_non_null = rest.endswith("!")
    if outer_non_null:
        rest = rest[:-1]
    if rest.startswith("["):
        if not rest.endswith("]"):
            raise ValueError(f"无法识别的列表类型记号: {token}")
        inner_token = rest[1:-1]
        elem_non_null = inner_token.endswith("!")
        if elem_non_null:
            inner_token = inner_token[:-1]
        return {
            "kind": "list",
            "inner": {"kind": "named", "name": inner_token, "non_null": elem_non_null},
            "non_null": outer_non_null,
        }
    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", rest):
        raise ValueError(f"无法识别的类型记号: {token}")
    return {"kind": "named", "name": rest, "non_null": outer_non_null}


def named_type(type_ref):
    ref = type_ref
    while ref["kind"] == "list":
        ref = ref["inner"]
    return ref["name"]


def element_type(type_ref):
    if type_ref["kind"] != "list":
        raise ValueError("只有列表类型才有元素类型")
    return type_ref["inner"]


def _strip_comments(text):
    return "\n".join(line.split("#", 1)[0] for line in text.splitlines())


class ObjectType:
    def __init__(self, name):
        self.name = name
        self.fields = {}

    def __repr__(self):
        return f"ObjectType({self.name!r}, fields={list(self.fields)})"


class Schema:
    """schema.graphql 的对象类型表。"""

    def __init__(self, text):
        self.types = {}
        for type_name, body in re.findall(r"type\s+([A-Za-z_]\w*)\s*\{(.*?)\}", _strip_comments(text), re.S):
            obj = ObjectType(type_name)
            for field_name, args, type_token in re.findall(r"([A-Za-z_]\w*)\s*(?:\(([^)]*)\))?\s*:\s*([^\s,]+)", body):
                obj.fields[field_name] = {
                    "name": field_name,
                    "args": args or "",
                    "type": parse_type_ref(type_token),
                }
            self.types[type_name] = obj

    @classmethod
    def load(cls, path):
        return cls(Path(path).read_text(encoding="utf8"))

    def field(self, type_name, field_name):
        return self.types[type_name].fields[field_name]


class Selection:
    """选择集中的一个字段（可能带别名），children 以响应键为键。"""

    def __init__(self, key, field_name):
        self.key = key
        self.field_name = field_name
        self.children = {}

    def __repr__(self):
        return f"Selection(key={self.key!r}, field={self.field_name!r})"


class Operation:
    def __init__(self, name, root, source):
        self.name = name
        self.root = root  # 虚拟根，children 即 Query 层选择
        self.source = source  # 原始 query 文本，请求体直接携带


def _tokenize_selection(text):
    tokens = []
    i = 0
    while i < len(text):
        char = text[i]
        if char.isspace():
            i += 1
        elif char in "{}():,!$":
            tokens.append(char)
            i += 1
        else:
            match = re.match(r"[A-Za-z_][A-Za-z0-9_]*", text[i:])
            if not match:
                raise ValueError(f"无法词法分析的查询片段: {text[i:i + 20]!r}")
            tokens.append(match.group(0))
            i += match.end()
    return tokens


def _parse_selection_set(tokens, pos):
    """tokens[pos] 必须是 '{'，返回 (选择字典, 新 pos)。"""
    assert tokens[pos] == "{"
    pos += 1
    selections = {}
    while tokens[pos] != "}":
        first = tokens[pos]
        pos += 1
        if tokens[pos] == ":":  # 别名
            pos += 1
            field_name = tokens[pos]
            pos += 1
            key = first
        else:
            field_name = first
            key = first
        if tokens[pos] == "(":  # 跳过参数，参数里没有选择集
            depth = 1
            pos += 1
            while depth:
                if tokens[pos] == "(":
                    depth += 1
                elif tokens[pos] == ")":
                    depth -= 1
                pos += 1
        node = Selection(key, field_name)
        if tokens[pos] == "{":
            node.children, pos = _parse_selection_set(tokens, pos)
        selections[key] = node
    return selections, pos + 1


class OperationSet:
    """operations.graphql 中全部操作的别名选择树。"""

    def __init__(self, text):
        self.operations = {}
        clean = _strip_comments(text)
        for header in re.finditer(r"query\s+([A-Za-z_]\w*)[^{]*", clean):
            name = header.group(1)
            depth = 0
            for pos in range(header.end(), len(clean)):
                if clean[pos] == "{":
                    depth += 1
                elif clean[pos] == "}":
                    depth -= 1
                    if depth == 0:
                        body = clean[header.end():pos + 1]
                        break
            else:  # pragma: no cover - 文档固定合法
                raise ValueError(f"操作 {name} 的选择集不完整")
            tokens = _tokenize_selection(body)
            root, _ = _parse_selection_set(tokens, 0)
            operation = Operation(name, Selection("$root", "$root"), body.strip())
            operation.root.children = root
            self.operations[name] = operation

    @classmethod
    def load(cls, path):
        return cls(Path(path).read_text(encoding="utf8"))


class PathInterpretation:
    """一条 errors.path 的解释结果。

    known=False 表示路径在任何已知操作的别名选择树与 schema 中都对不上，
    调用方必须把它当成不可忽略的未知错误。
    """

    def __init__(self, path, known, source_operation=None, chain=None, effective_path=None, leaf=None):
        self.path = list(path)
        self.known = known
        self.source_operation = source_operation
        self.chain = chain or []
        self.effective_path = list(effective_path) if effective_path is not None else None
        self.leaf = leaf

    @property
    def bubbled(self):
        return self.known and self.effective_path != self.path

    def to_dict(self):
        return {
            "path": self.path,
            "known": self.known,
            "source_operation": self.source_operation,
            "effective_path": self.effective_path,
            "bubbled": self.bubbled,
            "leaf": self.leaf,
        }


class SchemaModel:
    """把 schema 与操作文档组合起来，对外只暴露路径解释能力。"""

    def __init__(self, schema, operations):
        self.schema = schema
        self.operations = operations

    @classmethod
    def load(cls, schema_path, operations_path):
        return cls(Schema.load(schema_path), OperationSet.load(operations_path))

    def _resolve_in(self, operation, path):
        """在单个操作的别名树里沿 path 行走，返回字段链或 None。"""
        selection = operation.root
        type_name = "Query"
        chain = []
        for segment in path:
            if isinstance(segment, int):
                if not chain or chain[-1]["kind"] != "field" or chain[-1]["field"] != "edges":
                    return None
                chain.append({"kind": "index", "index": segment})
                continue
            if not isinstance(segment, str) or segment not in selection.children:
                return None
            child = selection.children[segment]
            try:
                field_def = self.schema.field(type_name, child.field_name)
            except KeyError:
                return None
            chain.append({
                "kind": "field",
                "key": segment,
                "field": child.field_name,
                "type": field_def["type"],
            })
            selection = child
            type_name = named_type(field_def["type"])
        return chain

    def interpret(self, operation_name, path):
        """解释错误路径；当前操作对不上时，再用其它操作的别名树尝试。"""
        ordered = [operation_name] + [name for name in self.operations.operations if name != operation_name]
        for name in ordered:
            operation = self.operations.operations.get(name)
            if operation is None:
                continue
            chain = self._resolve_in(operation, path)
            if chain is None:
                continue
            effective = self._effective_null(chain)
            leaf = next((item for item in reversed(chain) if item["kind"] == "field"), None)
            return PathInterpretation(
                path,
                known=True,
                source_operation=name,
                chain=chain,
                effective_path=path[:effective + 1] if effective >= 0 else [],
                leaf=leaf["field"] if leaf else None,
            )
        return PathInterpretation(path, known=False)

    @staticmethod
    def _effective_null(chain):
        """按 GraphQL 非空冒泡规则，返回真正变成 null 的字段链下标。

        返回 -1 表示连 data 根都保不住（对应响应 data=null）。
        """
        i = len(chain) - 1
        while i >= 0:
            item = chain[i]
            if item["kind"] == "field":
                if not item["type"]["non_null"]:
                    return i
                i -= 1
                continue
            # 数组下标处：先看列表元素本身能否为 null
            list_field = chain[i - 1]
            if not element_type(list_field["type"])["non_null"]:
                return i
            i -= 1  # 元素非空，冒泡到 edges 列表字段，下一轮判断列表字段
        return -1

    @staticmethod
    def is_prefix(ancestor, descendant):
        """ancestor 是否为 descendant 的路径前缀（含相等）。"""
        return len(ancestor) <= len(descendant) and descendant[:len(ancestor)] == ancestor
