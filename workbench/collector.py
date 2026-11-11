"""GraphQL 赛事资料的分连接采集引擎。

设计要点（与 docs/source-format.md、schema.graphql、operations.graphql 对齐）：

* rounds / 每轮 teams / 每队 submissions 是三层独立连接，游标只属于对应
  父节点、字段与 revision，绝不允许用一个全局游标推进所有队伍。
* 所有请求绑定第一次取得的 revision；明确收到 REVISION_EXPIRED 时立即
  停止本轮采集并保留证据，不合并该响应、不拼接新旧版本。
* errors.path 按别名与数组下标解释；可空字段的局部错误只产生缺口，
  沿非空约束冒泡的错误会让整条父分支失效，相关连接全部阻断。
* 只有结构与布尔位都可信的 pageInfo 才能推进连接；endCursor 重复出现
  记为翻页循环，不能当成新页继续走。
* 同一个 team 可属于多个 round：节点按类型与 id 单存，参赛关系单独保留；
* 同版本同 id 节点的字段冲突单独记录，不去重、不覆盖。
* 每个请求的变量、原始请求/响应字节摘要与字段路径都落盘，状态机每次
  请求后原子写盘，重启后只继续未完成或带开放缺口的连接，已完成的
  兄弟分支不会重扫。
"""
import json
import time
import urllib.error
import urllib.request
from datetime import datetime, timezone
from pathlib import Path

from .gqlschema import SchemaModel
from .raw_store import save_bytes

ROUNDS = "rounds"
TEAMS = "teams"
WORK = "work"

ROUNDS_ROOT = ["rounds"]
ROSTER_ROOT = ["round", "roster"]
REVISION_EXPIRED = "REVISION_EXPIRED"


def utc_now():
    return datetime.now(timezone.utc).isoformat()


class TransportError(RuntimeError):
    """传输层或非 GraphQL 协议错误，区别于 HTTP 200 内的 errors。"""

    def __init__(self, message, request_id=None):
        super().__init__(message)
        self.request_id = request_id


class HttpTransport:
    """最小 HTTP POST 传输，无第三方依赖。"""

    def __init__(self, endpoint, timeout=10):
        self.endpoint = endpoint
        self.timeout = timeout

    def post(self, payload):
        data = json.dumps(payload, ensure_ascii=False).encode("utf8")
        request = urllib.request.Request(
            self.endpoint,
            data=data,
            method="POST",
            headers={"Content-Type": "application/json", "Accept": "application/json"},
        )
        try:
            with urllib.request.urlopen(request, timeout=self.timeout) as response:
                return response.status, response.read()
        except urllib.error.HTTPError as exc:
            return exc.code, exc.read()


def _conn_key(kind, *parts):
    return "|".join([kind, *parts])


def _navigate(data, path):
    """按路径取响应片段，任一段缺失返回 (False, None)。"""
    current = data
    for segment in path:
        if isinstance(segment, int):
            if not isinstance(current, list) or segment >= len(current):
                return False, None
        elif not isinstance(current, dict) or segment not in current:
            return False, None
        current = current[segment]
    return True, current


def _is_prefix(prefix, path):
    return len(prefix) <= len(path) and path[:len(prefix)] == prefix


def _work_root(index):
    return ["round", "roster", "edges", index, "node", "work"]


class Collector:
    """绑定一个输出目录与一个 revision 的采集状态机。"""

    def __init__(self, model, transport, output_dir, transport_retries=3, field_retries=3,
                 sleep=time.sleep, initial_revision=None):
        self.model = model
        self.transport = transport
        self.output_dir = Path(output_dir)
        self.raw_dir = self.output_dir / "raw"
        self.state_path = self.output_dir / "state.json"
        self.report_path = self.output_dir / "report.json"
        self.transport_retries = transport_retries
        self.field_retries = field_retries
        self.sleep = sleep
        self.state = self._load_state()
        # 仅存在于当前进程：传输故障在本次运行内只自动重试一次，
        # 重启后该集合清空，transport_failed 才会被再次调度，避免单轮死循环
        self._transport_retried = set()
        if initial_revision and not self.state.get("revision"):
            self.state["revision"] = initial_revision

    # ------------------------------------------------------------------ 状态
    def _new_state(self):
        return {
            "revision": None,
            "status": "new",
            "abort": None,
            "connections": {},
            "nodes": {"round": {}, "team": {}, "submission": {}},
            "roster_memberships": [],
            "work_memberships": [],
            "gaps": [],
            "conflicts": [],
            "requests": [],
            "counters": {"request": 0, "gap": 0},
        }

    def _load_state(self):
        if self.state_path.exists():
            return json.loads(self.state_path.read_text(encoding="utf8"))
        return self._new_state()

    def _save_state(self):
        self.output_dir.mkdir(parents=True, exist_ok=True)
        tmp = self.state_path.with_suffix(".json.tmp")
        tmp.write_text(json.dumps(self.state, ensure_ascii=False, indent=2), encoding="utf8")
        tmp.replace(self.state_path)

    def _next_id(self, kind):
        number = self.state["counters"][kind] + 1
        self.state["counters"][kind] = number
        return f"{kind}-{number}"

    def _conn(self, key, kind, parent=None):
        conns = self.state["connections"]
        if key not in conns:
            conns[key] = {
                "key": key,
                "kind": kind,
                "parent": parent,
                "status": "pending",
                "cursor": None,
                "seen_cursors": [],
                "edge_cursors": [],
                "pages": 0,
                "refetch": False,
                "open_gap_ids": [],
                "blocked_reason": None,
                "last_request": None,
                "created_at": utc_now(),
                "updated_at": None,
            }
        return conns[key]

    # --------------------------------------------------------------- 请求证据
    def _add_gap(self, kind, request_id, operation, variables, connections, message, code=None,
                 path=None, interpretation=None, resolution=None, reuse=False):
        if reuse:
            # 同一连接、同一路径的局部字段错误在重采中复用缺口记录，
            # 只追加观测，不把一次持续故障膨胀成一串重复缺口。
            for existing in self.state["gaps"]:
                if (
                    existing["status"] == "open"
                    and existing["kind"] == kind
                    and existing["path"] == path
                    and existing["connections"] == connections
                ):
                    existing.setdefault("observations", []).append({
                        "request": request_id,
                        "message": message,
                        "code": code,
                        "at": utc_now(),
                    })
                    return existing
        gap = {
            "id": self._next_id("gap"),
            "status": "open",
            "kind": kind,
            "operation": operation,
            "variables": variables,
            "request": request_id,
            "connections": connections,
            "message": message,
            "code": code,
            "path": path,
            "effective_path": interpretation.get("effective_path") if interpretation else None,
            "interpretation": interpretation,
            "resolution": resolution,
            "opened_at": utc_now(),
        }
        self.state["gaps"].append(gap)
        for key in connections:
            conn = self.state["connections"].get(key)
            if conn is not None and gap["id"] not in conn["open_gap_ids"]:
                conn["open_gap_ids"].append(gap["id"])
        return gap

    def _resolve_gap(self, gap, request_id, note):
        gap["status"] = "resolved"
        gap["resolution"] = {"request": request_id, "note": note, "at": utc_now()}
        for key in gap["connections"]:
            conn = self.state["connections"].get(key)
            if conn is not None and gap["id"] in conn["open_gap_ids"]:
                conn["open_gap_ids"].remove(gap["id"])

    def _add_conflict(self, kind, node_id, field, existing, incoming, request_id):
        record = {
            "kind": kind,
            "node_type": kind,
            "node_id": node_id,
            "field": field,
            "existing": existing,
            "incoming": incoming,
            "request": request_id,
            "at": utc_now(),
        }
        self.state["conflicts"].append(record)
        return record

    # ----------------------------------------------------------------- 发请求
    def _post(self, operation_name, variables):
        """发送一次 GraphQL 请求，返回 (请求记录, 解析后的响应)。

        传输层错误与非 200 响应按策略重试，每次尝试的原始字节都登记为证据；
        最终失败抛 TransportError，由调用方登记为连接级缺口。
        REVISION_EXPIRED 在返回后由调用方判断。
        """
        operation = self.model.operations.operations[operation_name]
        payload = {
            "operationName": operation_name,
            "query": operation.source,
            "variables": variables,
        }
        request_bytes = json.dumps(payload, ensure_ascii=False).encode("utf8")
        attempts = []
        last_error = None
        for attempt in range(1, self.transport_retries + 1):
            try:
                status, body = self.transport.post(payload)
            except Exception as exc:  # 传输异常：连接拒绝、超时等
                last_error = exc
                attempts.append({"attempt": attempt, "status": None, "body": None, "error": str(exc)})
                if attempt < self.transport_retries:
                    self.sleep(min(attempt, 3) * 0.01)
                    continue
                record = self._record_attempts(operation_name, variables, request_bytes, attempts)
                raise TransportError(f"传输失败: {exc}", record["id"]) from exc
            attempts.append({"attempt": attempt, "status": status, "body": body, "error": None})
            if status == 200:
                break
            last_error = f"HTTP {status}"
            if attempt < self.transport_retries:
                self.sleep(min(attempt, 3) * 0.01)
        else:
            record = self._record_attempts(operation_name, variables, request_bytes, attempts)
            raise TransportError(f"连续 {self.transport_retries} 次得到 {last_error}", record["id"])
        try:
            parsed = json.loads(body.decode("utf8"))
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            record = self._record_attempts(operation_name, variables, request_bytes, attempts)
            raise TransportError(f"响应不是合法 JSON: {exc}", record["id"]) from exc
        errors = parsed.get("errors") if isinstance(parsed, dict) else None
        record = self._record_attempts(
            operation_name,
            variables,
            request_bytes,
            attempts,
            error_count=len(errors) if isinstance(errors, list) else 0,
        )
        return record, parsed

    def _record_attempts(self, operation_name, variables, request_bytes, attempts, error_count=0):
        request_key = save_bytes(self.raw_dir, request_bytes)
        raw_attempts = []
        for item in attempts:
            entry = {"attempt": item["attempt"], "status": item["status"], "error": item["error"]}
            if item["body"] is not None:
                entry["response_bytes_key"] = save_bytes(self.raw_dir, item["body"])
            raw_attempts.append(entry)
        record = {
            "id": self._next_id("request"),
            "operation": operation_name,
            "variables": variables,
            "request_bytes_key": request_key,
            "response_status": raw_attempts[-1]["status"],
            "response_bytes_key": raw_attempts[-1].get("response_bytes_key"),
            "response_sha256": raw_attempts[-1].get("response_bytes_key"),
            "attempts": raw_attempts,
            "error_count": error_count,
            "at": utc_now(),
        }
        self.state["requests"].append(record)
        return record

    def _is_revision_expired(self, parsed):
        errors = parsed.get("errors") if isinstance(parsed, dict) else None
        if not isinstance(errors, list):
            return False
        for error in errors:
            if isinstance(error, dict) and error.get("extensions", {}).get("code") == REVISION_EXPIRED:
                return True
        # data 整体为 null 且没有可用数据时，按版本失效的保守形态处理
        return isinstance(parsed, dict) and parsed.get("data") is None and bool(errors)

    def _abort(self, status, reason, record, parsed):
        self.state["status"] = status
        self.state["abort"] = {
            "reason": reason,
            "request": record["id"] if record else None,
            "errors": parsed.get("errors") if isinstance(parsed, dict) else None,
            "at": utc_now(),
        }
        self._save_state()

    # ------------------------------------------------------------- 错误解释
    def _interpret_errors(self, operation_name, parsed):
        errors = parsed.get("errors") or []
        results = []
        for error in errors:
            path = error.get("path")
            if not isinstance(path, list):
                interpretation = self.model.interpret(operation_name, [])
                interpretation.path = []
                interpretation.known = False
            else:
                interpretation = self.model.interpret(operation_name, path)
            results.append((error, interpretation))
        return results

    def _block(self, conn, reason, gap):
        conn["status"] = "blocked"
        conn["blocked_reason"] = reason
        conn["refetch"] = False
        if gap["id"] not in conn["open_gap_ids"]:
            conn["open_gap_ids"].append(gap["id"])

    @staticmethod
    def _page_info(connection_obj):
        """只承认结构完整、布尔位明确的 pageInfo；否则返回不可信原因。"""
        if not isinstance(connection_obj, dict):
            return None, "连接对象缺失或不是对象"
        page_info = connection_obj.get("pageInfo")
        if not isinstance(page_info, dict):
            return None, "pageInfo 缺失或不是对象"
        has_next = page_info.get("hasNextPage")
        if not isinstance(has_next, bool):
            return None, "hasNextPage 不是布尔值"
        end_cursor = page_info.get("endCursor")
        if end_cursor is not None and not isinstance(end_cursor, str):
            return None, "endCursor 不是字符串或 null"
        if has_next and end_cursor is None:
            return None, "hasNextPage 为 true 却没有 endCursor，无法推进"
        return page_info, None

    def _advance(self, conn, connection_obj, edge_path, record, loop_gap_connections):
        """用可信 pageInfo 推进连接，返回 True 表示还有下一页。"""
        page_info, reason = self._page_info(connection_obj)
        if page_info is None:
            gap = self._add_gap(
                "page_info",
                record["id"],
                record["operation"],
                record["variables"],
                loop_gap_connections,
                reason,
            )
            self._block(conn, reason, gap)
            return False
        found, edges = _navigate(connection_obj, ["edges"])
        if not found or not isinstance(edges, list):
            gap = self._add_gap(
                "page_info",
                record["id"],
                record["operation"],
                record["variables"],
                loop_gap_connections,
                "edges 缺失或不是数组",
            )
            self._block(conn, "edges 缺失或不是数组", gap)
            return False
        for edge in edges:
            if isinstance(edge, dict) and isinstance(edge.get("cursor"), str):
                # 重采会再次返回同一批边，游标集合保持幂等
                if edge["cursor"] not in conn["edge_cursors"]:
                    conn["edge_cursors"].append(edge["cursor"])
        if not conn.get("refetch_seen"):
            conn["pages"] += 1
        conn["last_request"] = record["id"]
        conn["updated_at"] = utc_now()
        if not page_info["hasNextPage"]:
            conn["cursor"] = page_info["endCursor"]
            conn["status"] = "complete"
            conn["refetch"] = False
            return False
        cursor = page_info["endCursor"]
        if cursor in conn["seen_cursors"]:
            gap = self._add_gap(
                "loop",
                record["id"],
                record["operation"],
                record["variables"],
                loop_gap_connections,
                f"endCursor 重复出现: {cursor!r}",
                path=edge_path + ["pageInfo", "endCursor"],
            )
            conn["status"] = "loop"
            conn["blocked_reason"] = "游标循环"
            if gap["id"] not in conn["open_gap_ids"]:
                conn["open_gap_ids"].append(gap["id"])
            return False
        conn["seen_cursors"].append(cursor)
        conn["cursor"] = cursor
        conn["status"] = "active"
        return True

    # ------------------------------------------------------------- 节点合并
    def _observe_field(self, node, field_name, value, trusted, request_id):
        """trusted=False 表示该 null 来自 errors，而不是 schema 允许的合法空值。

        冲突只在“两次都观测到非空值且取值不同”时记录；错误 null 之后恢复、
        合法 null 之后补齐都属于缺口修复，不算同版本冲突。
        """
        slot = node.setdefault(field_name, {})
        incoming = {
            "value": value,
            "status": "present" if value is not None else ("null:error" if not trusted else "null:legal"),
            "request": request_id,
        }
        if (
            slot
            and slot.get("status") == "present"
            and incoming["status"] == "present"
            and slot["value"] != value
        ):
            self._add_conflict(
                node["_type"], node["id"], field_name,
                {"value": slot["value"], "request": slot["request"]},
                {"value": value, "request": request_id},
                request_id,
            )
            return False
        # 已有的可信非空值不被后来的空值（任何形态）覆盖
        if slot.get("status") == "present":
            return True
        slot.update(incoming)
        return True

    def _ensure_node(self, node_type, node_id):
        bucket = self.state["nodes"][node_type]
        if node_id not in bucket:
            bucket[node_id] = {"_type": node_type, "id": node_id}
        return bucket[node_id]

    def _add_roster_membership(self, round_id, team_id, team_cursor, page_after):
        """记录参赛关系。

        team_cursor 是队伍边自己的游标（身份与边顺序证据）；page_after 是
        roster 连接翻到这一页所用的游标，也就是 Submissions 操作的 teamAfter，
        第一页为 None。两者语义不同，不能互相替代。
        """
        for item in self.state["roster_memberships"]:
            if item["round_id"] == round_id and item["team_id"] == team_id:
                item["team_cursor"] = team_cursor
                item["page_after"] = page_after
                return item
        item = {
            "round_id": round_id,
            "team_id": team_id,
            "team_cursor": team_cursor,
            "page_after": page_after,
        }
        self.state["roster_memberships"].append(item)
        return item

    def _add_work_membership(self, round_id, team_id, submission_id, cursor):
        for item in self.state["work_memberships"]:
            if item["round_id"] == round_id and item["team_id"] == team_id and item["submission_id"] == submission_id:
                return item
        item = {"round_id": round_id, "team_id": team_id, "submission_id": submission_id, "cursor": cursor}
        self.state["work_memberships"].append(item)
        return item

    # --------------------------------------------------------------- 主流程
    def run(self):
        if self.state["status"] in ("aborted_revision", "aborted_fatal"):
            return self.build_report()
        self.state["status"] = "running"
        self._save_state()
        if not self._ensure_revision():
            return self.build_report()
        rounds_key = _conn_key(ROUNDS)
        self._conn(rounds_key, ROUNDS)
        while True:
            self._save_state()
            progressed = False
            # rounds 未完整结束（含 loop/blocked/failed）前绝不下沉，
            # 避免在不可信或未采完的轮次上建立子连接
            if self._eligible(rounds_key):
                if self.state["connections"][rounds_key]["status"] == "transport_failed":
                    self._transport_retried.add(rounds_key)
                self._drive_rounds(self.state["connections"][rounds_key])
                progressed = True
            elif self.state["connections"][rounds_key]["status"] == "complete":
                candidate = self._next_work_candidate()
                if candidate is not None:
                    kind, key, payload = candidate
                    if self.state["connections"][key]["status"] == "transport_failed":
                        self._transport_retried.add(key)
                    if kind == TEAMS:
                        self._drive_teams(self.state["connections"][key], payload)
                    else:
                        membership = payload
                        self._drive_work(self.state["connections"][key], membership)
                    progressed = True
            if self.state["status"] in ("aborted_revision", "aborted_fatal"):
                return self.build_report()
            if not progressed:
                break
        open_gaps = [gap for gap in self.state["gaps"] if gap["status"] == "open"]
        if not open_gaps and all(
            conn["status"] == "complete" for conn in self.state["connections"].values()
        ) and self.state["connections"]:
            self.state["status"] = "complete_with_conflicts" if self.state["conflicts"] else "complete"
        else:
            self.state["status"] = "incomplete"
        self._save_state()
        return self.build_report()

    def _eligible(self, key):
        conn = self.state["connections"].get(key)
        if conn is None:
            return False
        # blocked/loop/exhausted 需要外部条件变化或人工介入，不自动重跑
        if conn["status"] in ("blocked", "loop", "exhausted"):
            return False
        # transport_failed 每次进程运行只自动重试一次，防止持续故障下空转
        if conn["status"] == "transport_failed":
            return key not in self._transport_retried
        return conn["status"] in ("pending", "active") or conn["refetch"]

    def _next_work_candidate(self):
        """选择下一条要推进的 teams/work 连接，已完成的兄弟连接不会重扫。"""
        round_nodes = self.state["nodes"]["round"]
        if not round_nodes:  # rounds 未采到可信节点前不下沉
            return None
        # 轮次节点来自完整的 rounds 连接，顺序以采集到的 id 为准
        for round_id in sorted(round_nodes):
            teams_key = _conn_key(TEAMS, round_id)
            teams_conn = self._conn(teams_key, TEAMS, parent=_conn_key(ROUNDS))
            if self._eligible(teams_key):
                return TEAMS, teams_key, round_id
        for membership in self.state["roster_memberships"]:
            work_key = _conn_key(WORK, membership["round_id"], membership["team_id"])
            work_conn = self._conn(work_key, WORK, parent=_conn_key(TEAMS, membership["round_id"]))
            if self._eligible(work_key):
                return WORK, work_key, membership
        return None

    def _ensure_revision(self):
        if self.state["revision"]:
            return True
        try:
            record, parsed = self._post("Revision", {})
        except TransportError as exc:
            gap = self._add_gap("transport", None, "Revision", {}, [], str(exc))
            self.state["status"] = "incomplete"
            self._save_state()
            return False
        if self._is_revision_expired(parsed):
            self._add_gap(
                "revision_expired", record["id"], "Revision", {}, [],
                "取得 revision 前即版本失效，不能绑定任何版本",
                code=REVISION_EXPIRED,
            )
            self._abort("aborted_revision", "取得 revision 前即版本失效", record, parsed)
            return False
        data = parsed.get("data") if isinstance(parsed, dict) else None
        revision = data.get("revision") if isinstance(data, dict) else None
        if not isinstance(revision, str) or not revision:
            # 任何无法取得可信 revision 的情况都不能安全绑定版本；
            # 无法识别的错误路径在 abort 原因里单独标明，绝不静默成功。
            unknown = any(
                not interpretation.known
                for _error, interpretation in self._interpret_errors("Revision", parsed)
            )
            reason = ("revision 响应出现无法识别的错误路径，不能安全绑定版本"
                      if unknown else "revision 响应没有可用的 revision 字段")
            self._abort("aborted_fatal", reason, record, parsed)
            return False
        self.state["revision"] = revision
        self._save_state()
        return True

    # --------------------------------------------------------------- rounds
    def _drive_rounds(self, conn):
        if conn["refetch"]:
            conn["cursor"] = None
            conn["seen_cursors"] = []
            conn["refetch"] = False
        variables = {"revision": self.state["revision"], "after": conn["cursor"]}
        try:
            record, parsed = self._post("Rounds", variables)
        except TransportError as exc:
            self._fail_conn(conn, exc, variables)
            return
        if self._expired_or_fatal(conn, record, parsed):
            return
        blocking = self._collect_rounds_errors(conn, record, parsed)
        if conn["status"] == "blocked":
            return
        rounds_obj = (parsed.get("data") or {}).get("rounds")
        if not isinstance(rounds_obj, dict):
            gap = self._add_gap("bubbled", record["id"], "Rounds", variables, [conn["key"]],
                                "rounds 连接对象不可用", path=ROUNDS_ROOT)
            self._block(conn, "rounds 连接对象不可用", gap)
            return
        edges = rounds_obj.get("edges") if isinstance(rounds_obj.get("edges"), list) else []
        for index, edge in enumerate(edges):
            node = edge.get("node") if isinstance(edge, dict) else None
            if not isinstance(node, dict) or not isinstance(node.get("id"), str):
                gap = self._add_gap(
                    "bubbled", record["id"], "Rounds", variables, [conn["key"]],
                    "round 边缺少可信节点", path=ROUNDS_ROOT + ["edges", index],
                )
                self._block(conn, "round 边缺少可信节点", gap)
                return
            round_id = node["id"]
            self._ensure_node("round", round_id)
            # teams 子连接等 rounds 完整结束后由 _next_work_candidate 惰性建立，
            # 这样 rounds 若在后续页遇到循环/冒泡，不会留下半可信的子连接
        self._advance(conn, rounds_obj, ROUNDS_ROOT, record, [conn["key"]])
        if not blocking and conn["status"] == "complete":
            self._resolve_conn_gaps(conn, record, "rounds 重采成功")

    def _collect_rounds_errors(self, conn, record, parsed):
        blocking = False
        for error, interpretation in self._interpret_errors("Rounds", parsed):
            variables = record["variables"]
            if not interpretation.known:
                gap = self._add_gap(
                    "unknown_path", record["id"], "Rounds", variables, [conn["key"]],
                    error.get("message", "未知错误路径"), code=error.get("extensions", {}).get("code"),
                    path=error.get("path"), interpretation=interpretation.to_dict(),
                )
                self._block(conn, "无法识别的错误路径", gap)
                blocking = True
                continue
            effective = interpretation.effective_path
            if not effective or not _is_prefix(ROUNDS_ROOT, effective):
                gap = self._add_gap(
                    "bubbled", record["id"], "Rounds", variables, [conn["key"]],
                    error.get("message", "错误冒泡到 data 根"), code=error.get("extensions", {}).get("code"),
                    path=error.get("path"), interpretation=interpretation.to_dict(),
                )
                self._block(conn, "错误冒泡到 data 根", gap)
                blocking = True
                continue
            gap = self._add_gap(
                "bubbled" if interpretation.bubbled else "field_error",
                record["id"], "Rounds", variables, [conn["key"]],
                error.get("message", ""), code=error.get("extensions", {}).get("code"),
                path=error.get("path"), interpretation=interpretation.to_dict(),
            )
            self._block(conn, "rounds 分支出现错误", gap)
            blocking = True
        return blocking

    # ---------------------------------------------------------------- teams
    def _drive_teams(self, conn, round_id):
        if conn["refetch"]:
            conn["cursor"] = None
            conn["seen_cursors"] = []
            conn["refetch"] = False
        variables = {"id": round_id, "revision": self.state["revision"], "after": conn["cursor"]}
        try:
            record, parsed = self._post("Teams", variables)
        except TransportError as exc:
            self._fail_conn(conn, exc, variables)
            return
        if self._expired_or_fatal(conn, record, parsed):
            return
        if self._collect_roster_errors(conn, record, parsed, "Teams", round_id):
            return
        data = parsed.get("data") or {}
        round_obj = data.get("round")
        if not isinstance(round_obj, dict):
            gap = self._add_gap("bubbled", record["id"], "Teams", variables, [conn["key"]],
                                "round 分支为 null，整轮队伍关系不可信", path=["round"])
            self._block(conn, "round 分支为 null", gap)
            self._block_descendants(round_id, reason="父轮次冒泡为空", record=record)
            return
        roster = round_obj.get("roster")
        if not isinstance(roster, dict):
            gap = self._add_gap("bubbled", record["id"], "Teams", variables, [conn["key"]],
                                "roster 连接对象不可用", path=ROSTER_ROOT)
            self._block(conn, "roster 连接对象不可用", gap)
            return
        edges = roster.get("edges") if isinstance(roster.get("edges"), list) else []
        for index, edge in enumerate(edges):
            node = edge.get("node") if isinstance(edge, dict) else None
            team_id = node.get("id") if isinstance(node, dict) else None
            if not isinstance(team_id, str) or not isinstance(edge.get("cursor"), str):
                gap = self._add_gap(
                    "bubbled", record["id"], "Teams", variables, [conn["key"]],
                    "team 边缺少可信节点或游标", path=ROSTER_ROOT + ["edges", index],
                )
                self._block(conn, "team 边缺少可信节点或游标", gap)
                return
            team_node = self._ensure_node("team", team_id)
            name = node.get("name")
            # Teams 操作里 name! 若出错会冒泡到 round=null，走到这里说明 name 可信
            self._observe_field(team_node, "name", name, trusted=True, request_id=record["id"])
            # conn["cursor"] 此刻还是请求本页时用的 roster 游标，即 teamAfter
            self._add_roster_membership(round_id, team_id, edge["cursor"], conn["cursor"])
            work_key = _conn_key(WORK, round_id, team_id)
            self._conn(work_key, WORK, parent=conn["key"])
        self._advance(conn, roster, ROSTER_ROOT, record, [conn["key"]])
        if conn["status"] == "complete":
            self._resolve_conn_gaps(conn, record, "teams 重采成功")

    def _collect_roster_errors(self, conn, record, parsed, operation_name, round_id):
        blocked = False
        for error, interpretation in self._interpret_errors(operation_name, parsed):
            variables = record["variables"]
            if not interpretation.known:
                gap = self._add_gap(
                    "unknown_path", record["id"], operation_name, variables, [conn["key"]],
                    error.get("message", "未知错误路径"), code=error.get("extensions", {}).get("code"),
                    path=error.get("path"), interpretation=interpretation.to_dict(),
                )
                self._block(conn, "无法识别的错误路径", gap)
                blocked = True
                continue
            effective = interpretation.effective_path
            if not effective:
                gap = self._add_gap(
                    "bubbled", record["id"], operation_name, variables, [conn["key"]],
                    error.get("message", "错误冒泡到 data 根"),
                    code=error.get("extensions", {}).get("code"),
                    path=error.get("path"), interpretation=interpretation.to_dict(),
                )
                self._block(conn, "错误冒泡到 data 根", gap)
                blocked = True
                continue
            if _is_prefix(ROSTER_ROOT, effective) or effective == ["round"]:
                gap = self._add_gap(
                    "bubbled" if interpretation.bubbled else "field_error",
                    record["id"], operation_name, variables, [conn["key"]],
                    error.get("message", ""), code=error.get("extensions", {}).get("code"),
                    path=error.get("path"), interpretation=interpretation.to_dict(),
                )
                self._block(conn, "roster 父分支冒泡为空", gap)
                self._block_descendants(round_id, "父分支冒泡为空", record)
                blocked = True
                continue
            gap = self._add_gap(
                "field_error", record["id"], operation_name, variables, [conn["key"]],
                error.get("message", ""), code=error.get("extensions", {}).get("code"),
                path=error.get("path"), interpretation=interpretation.to_dict(),
            )
            self._block(conn, "轮次级字段错误", gap)
            blocked = True
        return blocked

    def _block_descendants(self, round_id, reason, record):
        for membership in self.state["roster_memberships"]:
            if membership["round_id"] != round_id:
                continue
            work_key = _conn_key(WORK, round_id, membership["team_id"])
            work_conn = self._conn(work_key, WORK, parent=_conn_key(TEAMS, round_id))
            if work_conn["status"] not in ("blocked", "loop", "exhausted", "complete"):
                gap = self._add_gap(
                    "bubbled", record["id"], record["operation"], record["variables"],
                    [work_key], f"祖先连接阻断: {reason}",
                )
                self._block(work_conn, reason, gap)
            elif work_conn["status"] == "complete":
                # 已完成的子连接也要在报告里挂出祖先失效证据
                self._add_gap(
                    "bubbled", record["id"], record["operation"], record["variables"],
                    [work_key], f"祖先连接阻断: {reason}",
                )

    # ---------------------------------------------------------- submissions
    def _drive_work(self, conn, membership):
        round_id = membership["round_id"]
        team_id = membership["team_id"]
        refetching = conn["refetch"]
        if refetching:
            conn["cursor"] = None
            conn["seen_cursors"] = []
            conn["refetch"] = False
            conn["refetch_seen"] = True
        variables = {
            "id": round_id,
            "revision": self.state["revision"],
            "teamAfter": membership["page_after"],
            "after": conn["cursor"],
        }
        try:
            record, parsed = self._post("Submissions", variables)
        except TransportError as exc:
            self._fail_conn(conn, exc, variables)
            return
        if self._expired_or_fatal(conn, record, parsed):
            return
        local_errors, blocked = self._collect_work_errors(conn, record, parsed, round_id)
        if conn["status"] == "blocked":
            return
        data = parsed.get("data") or {}
        round_obj = data.get("round")
        if not isinstance(round_obj, dict):
            gap = self._add_gap("bubbled", record["id"], "Submissions", variables, [conn["key"]],
                                "round 分支为 null", path=["round"])
            self._block(conn, "round 分支为 null", gap)
            return
        roster = round_obj.get("roster")
        if not isinstance(roster, dict) or not isinstance(roster.get("edges"), list):
            gap = self._add_gap("bubbled", record["id"], "Submissions", variables, [conn["key"]],
                                "roster 不可用，无法定位本队 work 连接", path=ROSTER_ROOT)
            self._block(conn, "roster 不可用", gap)
            return
        team_edge = self._find_team_edge(roster["edges"], team_id)
        if team_edge is None:
            gap = self._add_gap(
                "page_info", record["id"], "Submissions", variables, [conn["key"]],
                f"响应中找不到队伍 {team_id}，work 连接无法定位",
            )
            self._block(conn, "响应中找不到目标队伍", gap)
            return
        team_index, team_edge = team_edge
        team_node = self._ensure_node("team", team_id)
        if isinstance(team_edge.get("node"), dict) and isinstance(team_edge["node"].get("name"), str):
            self._observe_field(team_node, "name", team_edge["node"]["name"], True, record["id"])
        work = team_edge.get("node", {}).get("work")
        if not isinstance(work, dict):
            gap = self._add_gap(
                "bubbled", record["id"], "Submissions", variables, [conn["key"]],
                "work 连接对象不可用", path=_work_root(team_index),
            )
            self._block(conn, "work 连接对象不可用", gap)
            return
        edges = work.get("edges") if isinstance(work.get("edges"), list) else []
        attachment_error_paths = {tuple(item[1].path) for item in local_errors if item[1].leaf == "attachment"}
        for sub_index, edge in enumerate(edges):
            node = edge.get("node") if isinstance(edge, dict) else None
            sub_id = node.get("id") if isinstance(node, dict) else None
            if not isinstance(sub_id, str) or not isinstance(edge.get("cursor"), str):
                gap = self._add_gap(
                    "bubbled", record["id"], "Submissions", variables, [conn["key"]],
                    "submission 边缺少可信节点或游标",
                    path=_work_root(team_index) + ["edges", sub_index],
                )
                self._block(conn, "submission 边缺少可信节点或游标", gap)
                return
            sub_node = self._ensure_node("submission", sub_id)
            title = node.get("title")
            if isinstance(title, str):
                self._observe_field(sub_node, "title", title, True, record["id"])
            attachment_path = _work_root(team_index) + ["edges", sub_index, "node", "attachment"]
            attachment_trusted = tuple(attachment_path) not in attachment_error_paths
            self._observe_field(sub_node, "attachment", node.get("attachment"), attachment_trusted, record["id"])
            self._add_work_membership(round_id, team_id, sub_id, edge["cursor"])
        has_more = self._advance(conn, work, _work_root(team_index), record, [conn["key"]])
        if conn["status"] == "blocked":
            return
        if local_errors:
            # 可空叶子局部错误：连接、边与 pageInfo 仍可信，只安排这条 work
            # 连接（含必要的 roster 祖先请求）重走，兄弟连接不重扫。
            attempts = conn.get("refetch_attempts", 0)
            if attempts >= self.field_retries:
                conn["status"] = "exhausted"
                conn["blocked_reason"] = "局部字段重采超过重试上限"
                conn["refetch"] = False
                return
            conn["refetch_attempts"] = attempts + 1
            conn["refetch"] = True
            # 保持已采集到的页位置；重走从该连接起点重新取一遍以替换错误 null
            conn["status"] = "active"
            return
        if not has_more and conn["status"] == "complete":
            self._resolve_conn_gaps(conn, record, "work 重采后字段恢复")
            conn.pop("refetch_attempts", None)
            conn.pop("refetch_seen", None)

    def _find_team_edge(self, edges, team_id):
        for index, edge in enumerate(edges):
            node = edge.get("node") if isinstance(edge, dict) else None
            if isinstance(node, dict) and node.get("id") == team_id:
                return index, edge
        return None

    def _collect_work_errors(self, conn, record, parsed, round_id):
        local_errors = []
        blocked = False
        for error, interpretation in self._interpret_errors("Submissions", parsed):
            variables = record["variables"]
            if not interpretation.known:
                gap = self._add_gap(
                    "unknown_path", record["id"], "Submissions", variables, [conn["key"]],
                    error.get("message", "未知错误路径"), code=error.get("extensions", {}).get("code"),
                    path=error.get("path"), interpretation=interpretation.to_dict(),
                )
                self._block(conn, "无法识别的错误路径", gap)
                blocked = True
                continue
            effective = interpretation.effective_path
            if not effective:
                gap = self._add_gap(
                    "bubbled", record["id"], "Submissions", variables, [conn["key"]],
                    error.get("message", "错误冒泡到 data 根"),
                    code=error.get("extensions", {}).get("code"),
                    path=error.get("path"), interpretation=interpretation.to_dict(),
                )
                self._block(conn, "错误冒泡到 data 根", gap)
                blocked = True
                continue
            work_match = self._match_work_root(effective)
            if work_match is not None and not interpretation.bubbled and interpretation.leaf == "attachment":
                # 唯一的可空叶子：连接、边与 pageInfo 仍可信，只登记字段缺口；
                # 重采期间同路径错误复用同一条缺口记录
                gap = self._add_gap(
                    "field_error", record["id"], "Submissions", variables, [conn["key"]],
                    error.get("message", ""), code=error.get("extensions", {}).get("code"),
                    path=error.get("path"), interpretation=interpretation.to_dict(),
                    reuse=True,
                )
                local_errors.append((error, interpretation))
                continue
            if work_match is not None:
                gap = self._add_gap(
                    "bubbled", record["id"], "Submissions", variables, [conn["key"]],
                    error.get("message", ""), code=error.get("extensions", {}).get("code"),
                    path=error.get("path"), interpretation=interpretation.to_dict(),
                )
                self._block(conn, "work 父分支冒泡为空", gap)
                blocked = True
                continue
            if _is_prefix(ROSTER_ROOT, effective) or effective == ["round"]:
                teams_key = _conn_key(TEAMS, round_id)
                gap = self._add_gap(
                    "bubbled", record["id"], "Submissions", variables, [conn["key"], teams_key],
                    error.get("message", ""), code=error.get("extensions", {}).get("code"),
                    path=error.get("path"), interpretation=interpretation.to_dict(),
                )
                self._block(conn, "roster 父分支冒泡为空", gap)
                blocked = True
                continue
            gap = self._add_gap(
                "field_error", record["id"], "Submissions", variables, [conn["key"]],
                error.get("message", ""), code=error.get("extensions", {}).get("code"),
                path=error.get("path"), interpretation=interpretation.to_dict(),
            )
            self._block(conn, "未归类的错误路径", gap)
            blocked = True
        return local_errors, blocked

    @staticmethod
    def _match_work_root(path):
        """路径是否落在某个 work 连接内，返回队伍边下标，否则 None。

        work 根的固定形状为
        round / roster / edges / <i:int> / node / work
        其后才是 edges / <j> / node / ... 等连接内部段。
        """
        if len(path) < 6 or not isinstance(path[3], int):
            return None
        if path[:3] != ["round", "roster", "edges"]:
            return None
        if path[4:6] != ["node", "work"]:
            return None
        return path[3]
    # ------------------------------------------------------------ 公共处理
    def _expired_or_fatal(self, conn, record, parsed):
        if self._is_revision_expired(parsed):
            self._add_gap(
                "revision_expired", record["id"], record["operation"], record["variables"],
                [conn["key"]], "revision 已失效，停止本轮采集且不合并该响应",
                code=REVISION_EXPIRED,
            )
            self._abort("aborted_revision", "接口明确返回版本失效", record, parsed)
            return True
        return False

    def _fail_conn(self, conn, exc, variables):
        gap = self._add_gap(
            "transport", getattr(exc, "request_id", None), conn["kind"], variables, [conn["key"]], str(exc),
        )
        # 瞬时传输故障：保留游标与已采证据，重启后可从断点继续
        conn["status"] = "transport_failed"
        conn["blocked_reason"] = str(exc)
        if gap["id"] not in conn["open_gap_ids"]:
            conn["open_gap_ids"].append(gap["id"])
        self._save_state()

    def _resolve_conn_gaps(self, conn, record, note):
        # 连接走到完整成功页时，其名下的开放缺口（含传输故障、可空字段错误）都已消除
        for gap in self.state["gaps"]:
            if gap["status"] == "open" and conn["key"] in gap["connections"]:
                self._resolve_gap(gap, record["id"], note)

    # ---------------------------------------------------------------- 报告
    def build_report(self):
        conns = []
        for key in sorted(self.state["connections"]):
            conn = self.state["connections"][key]
            conns.append({
                "key": conn["key"],
                "kind": conn["kind"],
                "parent": conn["parent"],
                "status": conn["status"],
                "pages": conn["pages"],
                "cursor": conn["cursor"],
                "seen_cursors": list(conn["seen_cursors"]),
                "edge_cursors": list(conn["edge_cursors"]),
                "refetch": conn["refetch"],
                "open_gap_ids": list(conn["open_gap_ids"]),
                "blocked_reason": conn["blocked_reason"],
                "last_request": conn["last_request"],
            })
        report = {
            "revision": self.state["revision"],
            "status": self.state["status"],
            "abort": self.state["abort"],
            "summary": {
                "rounds": len(self.state["nodes"]["round"]),
                "teams": len(self.state["nodes"]["team"]),
                "submissions": len(self.state["nodes"]["submission"]),
                "connections": len(conns),
                "open_gaps": sum(1 for gap in self.state["gaps"] if gap["status"] == "open"),
                "resolved_gaps": sum(1 for gap in self.state["gaps"] if gap["status"] == "resolved"),
                "conflicts": len(self.state["conflicts"]),
                "requests": len(self.state["requests"]),
            },
            "connections": conns,
            "nodes": self.state["nodes"],
            "roster_memberships": self.state["roster_memberships"],
            "work_memberships": self.state["work_memberships"],
            "gaps": self.state["gaps"],
            "conflicts": self.state["conflicts"],
            "requests": self.state["requests"],
            "generated_at": utc_now(),
        }
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.report_path.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf8")
        return report


def collect(endpoint, output_dir, schema_path, operations_path, **kwargs):
    """命令与测试共用的便捷入口。"""
    model = SchemaModel.load(schema_path, operations_path)
    transport = HttpTransport(endpoint, timeout=kwargs.pop("timeout", 10))
    collector = Collector(model, transport, output_dir, **kwargs)
    return collector.run()
