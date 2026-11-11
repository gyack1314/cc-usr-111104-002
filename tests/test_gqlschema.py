"""schema/操作文档解析与非空冒泡规则的单元测试。"""
import unittest
from pathlib import Path

from workbench.gqlschema import SchemaModel, parse_type_ref

ROOT = Path(__file__).resolve().parent.parent


class TypeRefTests(unittest.TestCase):
    def test_parse_type_ref(self):
        self.assertEqual(parse_type_ref("String"),
                         {"kind": "named", "name": "String", "non_null": False})
        self.assertEqual(parse_type_ref("String!"),
                         {"kind": "named", "name": "String", "non_null": True})
        self.assertEqual(parse_type_ref("[TeamEdge!]!"),
                         {"kind": "list",
                          "inner": {"kind": "named", "name": "TeamEdge", "non_null": True},
                          "non_null": True})
        self.assertEqual(parse_type_ref("[TeamEdge!]"),
                         {"kind": "list",
                          "inner": {"kind": "named", "name": "TeamEdge", "non_null": True},
                          "non_null": False})
        with self.assertRaises(ValueError):
            parse_type_ref("[Broken")


class SchemaModelTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.model = SchemaModel.load(ROOT / "sources" / "schema.graphql",
                                     ROOT / "sources" / "operations.graphql")

    def test_operations_keep_alias_trees_and_source(self):
        operations = self.model.operations.operations
        self.assertEqual(set(operations), {"Revision", "Rounds", "Teams", "Submissions"})
        teams = operations["Teams"].root.children
        self.assertIn("round", teams)
        roster = teams["round"].children["roster"]
        self.assertEqual(roster.field_name, "teams")  # 别名 roster -> 字段 teams
        sub_team_node = (operations["Submissions"].root.children["round"]
                         .children["roster"].children["edges"].children["node"])
        self.assertIn("work", sub_team_node.children)  # 别名 work -> 字段 submissions
        self.assertTrue(operations["Rounds"].source.startswith("{"))

    def test_nullable_leaf_does_not_bubble(self):
        result = self.model.interpret(
            "Submissions",
            ["round", "roster", "edges", 0, "node", "work", "edges", 0, "node", "attachment"],
        )
        self.assertTrue(result.known)
        self.assertFalse(result.bubbled)
        self.assertEqual(result.leaf, "attachment")
        self.assertEqual(result.effective_path, result.path)

    def test_non_null_leaf_bubbles_to_nullable_round(self):
        # name: String!，沿 node(可空) -> 边元素(非空) -> edges!(非空) ->
        # roster(可空) 一路冒泡，最终停在可空的 round
        result = self.model.interpret(
            "Teams", ["round", "roster", "edges", 0, "node", "name"])
        self.assertTrue(result.known)
        self.assertTrue(result.bubbled)
        self.assertEqual(result.effective_path, ["round"])
        self.assertEqual(result.leaf, "name")

    def test_non_null_round_id_bubbles_to_data_root(self):
        result = self.model.interpret(
            "Rounds", ["rounds", "edges", 0, "node", "id"])
        self.assertTrue(result.known)
        self.assertEqual(result.effective_path, [])  # rounds! 非空，直抵 data 根

    def test_unknown_path_is_reported_not_ignored(self):
        result = self.model.interpret("Teams", ["missing", 9])
        self.assertFalse(result.known)
        self.assertIsNone(result.effective_path)

    def test_cross_operation_path_still_resolves(self):
        # 某操作响应里出现按另一操作别名书写的路径时，仍应被识别
        result = self.model.interpret(
            "Rounds", ["round", "roster", "edges", 0, "node", "name"])
        self.assertTrue(result.known)
        self.assertEqual(result.source_operation, "Teams")

    def test_schema_non_null_layout(self):
        schema = self.model.schema
        self.assertTrue(schema.field("Team", "name")["type"]["non_null"])
        self.assertFalse(schema.field("Submission", "attachment")["type"]["non_null"])
        self.assertFalse(schema.field("PageInfo", "endCursor")["type"]["non_null"])
        self.assertTrue(schema.field("PageInfo", "hasNextPage")["type"]["non_null"])


if __name__ == "__main__":
    unittest.main()
