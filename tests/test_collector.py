"""采集引擎的场景测试。

覆盖任务约定的核对面：同队跨轮身份与参赛关系、单个附件局部失败与重采、
非空冒泡让整条父分支为空、游标循环、revision 过期、未知错误路径、
合法 null 与错误 null 的区分、同版本节点冲突、逐连接结论与断点续采。
"""
import json
import tempfile
import unittest
from pathlib import Path

from workbench.collector import Collector
from workbench.gqlschema import SchemaModel
from workbench.raw_store import read_bytes

ROOT = Path(__file__).resolve().parent.parent
SCHEMA = ROOT / "sources" / "schema.graphql"
OPERATIONS = ROOT / "sources" / "operations.graphql"


def load_fixture(name):
    return json.loads((ROOT / "sources" / name).read_text(encoding="utf8"))


class FakeTransport:
    """按 sources/*.json 的路由规则在进程内回放响应，不监听端口。"""

    def __init__(self, scenario_name, overrides=None):
        config = load_fixture(scenario_name)
        self.base = config.get("routes", [])
        self.overrides = overrides or []
        self.counts = {}
        self.calls = []

    @staticmethod
    def _subset(actual, wanted):
        if not isinstance(actual, dict):
            return False
        for key, value in wanted.items():
            if key not in actual:
                return False
            if isinstance(value, dict):
                if not FakeTransport._subset(actual[key], value):
                    return False
            elif actual[key] != value:
                return False
        return True

    def _match(self, payload):
        rules = self.overrides + self.base
        for index, rule in enumerate(rules):
            if rule.get("method", "GET") != "POST" or rule["path"] != "/graphql":
                continue
            if self._subset(payload, rule.get("body", {})):
                sequence = rule["responses"]
                count = self.counts.get(index, 0)
                self.counts[index] = count + 1
                return sequence[min(count, len(sequence) - 1)]
        raise AssertionError(f"没有路由匹配请求: {payload['operationName']} {payload['variables']}")

    def post(self, payload):
        self.calls.append(payload)
        found = self._match(payload)
        if "file" in found:
            body = (ROOT / "sources" / found["file"]).read_bytes()
        else:
            body = found.get("text", "").encode("utf8")
        return found.get("status", 200), body


class ScriptedTransport:
    """按 (operationName, variables) 精确匹配的脚本化响应，用于构造专用夹具。"""

    def __init__(self, rules):
        # rules: list of {"match": {...subset...}, "responses": [dict, ...]}
        self.rules = rules
        self.counts = {}
        self.calls = []

    def post(self, payload):
        for index, rule in enumerate(self.rules):
            wanted = rule["match"]
            if ScriptedTransport._subset(payload, wanted):
                count = self.counts.get(index, 0)
                self.counts[index] = count + 1
                sequence = rule["responses"]
                response = sequence[min(count, len(sequence) - 1)]
                self.calls.append(payload)
                return 200, json.dumps(response, ensure_ascii=False).encode("utf8")
        raise AssertionError(f"脚本未覆盖的请求: {payload['operationName']} {payload['variables']}")

    @staticmethod
    def _subset(actual, wanted):
        if not isinstance(actual, dict):
            return False
        for key, value in wanted.items():
            if key not in actual:
                return False
            if isinstance(value, dict):
                if not ScriptedTransport._subset(actual[key], value):
                    return False
            elif actual[key] != value:
                return False
        return True


def make_collector(transport, directory, **kwargs):
    model = SchemaModel.load(SCHEMA, OPERATIONS)
    return Collector(model, transport, directory, sleep=lambda _seconds: None, **kwargs)


def connection(report, key):
    return next(conn for conn in report["connections"] if conn["key"] == key)


def open_gaps(report):
    return [gap for gap in report["gaps"] if gap["status"] == "open"]


def resolved_gaps(report):
    return [gap for gap in report["gaps"] if gap["status"] == "resolved"]


class NormalScenarioTests(unittest.TestCase):
    def test_full_collection_preserves_identity_and_recovers_attachment(self):
        with tempfile.TemporaryDirectory() as directory:
            collector = make_collector(FakeTransport("normal.json"), directory)
            report = collector.run()
            self.assertEqual(report["status"], "complete")
            self.assertEqual(report["revision"], "v1")
            self.assertEqual(report["summary"], {
                "rounds": 2,
                "teams": 1,
                "submissions": 2,
                "connections": 5,
                "open_gaps": 0,
                "resolved_gaps": 2,
                "conflicts": 0,
                "requests": 8,
            })
            # T1 只有一个节点对象，却分别属于 R1 与 R2
            self.assertEqual(set(report["nodes"]["team"]), {"T1"})
            roster = {(item["round_id"], item["team_id"]) for item in report["roster_memberships"]}
            self.assertEqual(roster, {("R1", "T1"), ("R2", "T1")})
            work = {(item["round_id"], item["team_id"], item["submission_id"])
                    for item in report["work_memberships"]}
            self.assertEqual(work, {("R1", "T1", "R1S"), ("R2", "T1", "R2S")})
            # 每个连接单独给出结论，而不是只报总条数
            for conn in report["connections"]:
                self.assertEqual(conn["status"], "complete", conn["key"])
                self.assertEqual(conn["open_gap_ids"], [])
            # 附件先 null:error，重采后恢复为 present
            for sub_id in ("R1S", "R2S"):
                attachment = report["nodes"]["submission"][sub_id]["attachment"]
                self.assertEqual(attachment["status"], "present")
                self.assertEqual(attachment["value"], "/files/a")
            self.assertEqual({gap["kind"] for gap in resolved_gaps(report)}, {"field_error"})
            self.assertTrue(all(gap["code"] == "TEMPORARY" for gap in resolved_gaps(report)))
            # 证据可追溯到请求变量、原始字节与字段路径
            for request in report["requests"]:
                self.assertIn("variables", request)
                self.assertTrue(read_bytes(Path(directory) / "raw", request["request_bytes_key"]))
                self.assertTrue(read_bytes(Path(directory) / "raw", request["response_bytes_key"]))
            for gap in resolved_gaps(report):
                self.assertEqual(gap["path"][-1], "attachment")
                self.assertFalse(gap["interpretation"]["bubbled"])
                self.assertEqual(gap["effective_path"], gap["path"])

    def test_restart_resumes_without_rescanning_finished_branches(self):
        with tempfile.TemporaryDirectory() as directory:
            first = make_collector(FakeTransport("normal.json"), directory)
            first.run()
            requests_after_first = len(first.state["requests"])
            second = make_collector(FakeTransport("normal.json"), directory)
            report = second.run()
            self.assertEqual(report["status"], "complete")
            self.assertEqual(len(report["requests"]), requests_after_first)
            # 续跑使用的 revision 仍然是首次绑定的版本
            self.assertEqual(second.state["revision"], "v1")


class BubbleScenarioTests(unittest.TestCase):
    def test_non_null_error_bubbles_whole_round_branch(self):
        with tempfile.TemporaryDirectory() as directory:
            transport = FakeTransport(
                "normal.json",
                overrides=[{
                    "path": "/graphql",
                    "method": "POST",
                    "body": {"operationName": "Teams", "variables": {"id": "R1"}},
                    "responses": [{"file": "bubble-response.json"}],
                }],
            )
            collector = make_collector(transport, directory)
            report = collector.run()
            self.assertEqual(report["status"], "incomplete")
            teams_r1 = connection(report, "teams|R1")
            self.assertEqual(teams_r1["status"], "blocked")
            gap = next(g for g in open_gaps(report) if "teams|R1" in g["connections"])
            self.assertEqual(gap["kind"], "bubbled")
            # 原始错误落在 name!，实际 null 冒泡到可空的 round
            self.assertEqual(gap["path"][-1], "name")
            self.assertEqual(gap["effective_path"], ["round"])
            self.assertTrue(gap["interpretation"]["bubbled"])
            # 冒泡让整支父分支不可信：teams 连接阻断，缺口覆盖其 work 子树；
            # 在从未可信观测到 T1 关系的情况下不伪造 work|R1|T1 连接
            self.assertEqual(teams_r1["status"], "blocked")
            self.assertTrue(any("teams|R1" in gap["connections"] for gap in open_gaps(report)))
            self.assertNotIn("work|R1|T1", {conn["key"] for conn in report["connections"]})
            # 兄弟轮次 R2 不应该被牵连
            self.assertEqual(connection(report, "teams|R2")["status"], "complete")
            # round 节点在冒泡响应中没有可信数据，不新增队伍关系
            self.assertNotIn(("R1", "T1"),
                             {(item["round_id"], item["team_id"]) for item in report["roster_memberships"]})

    def test_bubble_after_membership_known_blocks_descendant_work(self):
        # 先正常拿到 R1  roster，再让 Submissions 上的 name! 冒泡到 round=null
        with tempfile.TemporaryDirectory() as directory:
            transport = FakeTransport(
                "normal.json",
                overrides=[{
                    "path": "/graphql",
                    "method": "POST",
                    "body": {"operationName": "Submissions", "variables": {"id": "R1"}},
                    "responses": [{"file": "bubble-response.json"}],
                }],
            )
            collector = make_collector(transport, directory)
            report = collector.run()
            self.assertEqual(connection(report, "work|R1|T1")["status"], "blocked")
            gap = next(g for g in open_gaps(report) if "work|R1|T1" in g["connections"])
            self.assertEqual(gap["effective_path"], ["round"])
            # R2 是独立轮次连接，不被 R1 的冒泡牵连
            self.assertEqual(connection(report, "work|R2|T1")["status"], "complete")


class ExpiredScenarioTests(unittest.TestCase):
    def test_revision_expired_aborts_and_keeps_evidence(self):
        with tempfile.TemporaryDirectory() as directory:
            collector = make_collector(FakeTransport("expired.json"), directory)
            report = collector.run()
            self.assertEqual(report["status"], "aborted_revision")
            self.assertIsNone(report["revision"])
            self.assertIsNotNone(report["abort"])
            self.assertIn("版本失效", report["abort"]["reason"])
            gap = open_gaps(report)[0]
            self.assertEqual(gap["kind"], "revision_expired")
            self.assertEqual(gap["code"], "REVISION_EXPIRED")
            request = report["requests"][0]
            # 失效响应的原始字节必须保留，但任何数据都不得并入节点表
            self.assertTrue(read_bytes(Path(directory) / "raw", request["response_bytes_key"]))
            self.assertEqual(report["summary"]["rounds"], 0)
            self.assertEqual(report["summary"]["connections"], 0)
            # 重启后保持停止，不会偷偷换版本继续
            second = make_collector(FakeTransport("expired.json"), directory)
            again = second.run()
            self.assertEqual(again["status"], "aborted_revision")
            self.assertEqual(len(again["requests"]), len(report["requests"]))

    def test_expired_mid_run_pins_first_revision_and_stops(self):
        with tempfile.TemporaryDirectory() as directory:
            transport = FakeTransport(
                "expired.json",
                overrides=[
                    {
                        "path": "/graphql",
                        "method": "POST",
                        "body": {"operationName": "Revision"},
                        "responses": [{"file": "Revision-0-0.json"}],
                    },
                    {
                        "path": "/graphql",
                        "method": "POST",
                        "body": {"operationName": "Rounds"},
                        "responses": [{"file": "expired-response.json"}],
                    },
                ],
            )
            collector = make_collector(transport, directory)
            report = collector.run()
            self.assertEqual(report["status"], "aborted_revision")
            self.assertEqual(report["revision"], "v1")
            self.assertEqual(open_gaps(report)[0]["kind"], "revision_expired")
            operations = [request["operation"] for request in report["requests"]]
            self.assertEqual(operations, ["Revision", "Rounds"])
            self.assertNotIn("Teams", operations)


class UnknownPathScenarioTests(unittest.TestCase):
    def test_unknown_error_path_is_never_treated_as_success(self):
        with tempfile.TemporaryDirectory() as directory:
            transport = FakeTransport(
                "unknown-path.json",
                overrides=[{
                    "path": "/graphql",
                    "method": "POST",
                    "body": {"operationName": "Submissions", "variables": {"id": "R1"}},
                    "responses": [{"file": "unknown-path-response.json"}],
                }],
            )
            collector = make_collector(transport, directory, initial_revision="v1")
            # 先让 rounds/teams 走正常夹具，再在 work 上遇到未知路径
            collector.state["nodes"]["round"] = {"R1": {"_type": "round", "id": "R1"}}
            report = collector.run()
            self.assertEqual(report["status"], "incomplete")
            gap = next(g for g in open_gaps(report) if g["kind"] == "unknown_path")
            self.assertEqual(gap["path"], ["missing", 9])
            self.assertFalse(gap["interpretation"]["known"])
            work_conn = connection(report, gap["connections"][0])
            self.assertEqual(work_conn["status"], "blocked")
            self.assertEqual(work_conn["blocked_reason"], "无法识别的错误路径")


class LoopScenarioTests(unittest.TestCase):
    def test_repeated_end_cursor_is_flagged_per_connection(self):
        with tempfile.TemporaryDirectory() as directory:
            collector = make_collector(FakeTransport("loop.json"), directory, initial_revision="v1")
            report = collector.run()
            rounds_conn = connection(report, "rounds")
            self.assertEqual(rounds_conn["status"], "loop")
            gap = next(g for g in open_gaps(report) if g["kind"] == "loop")
            self.assertIn("again", gap["message"])
            self.assertEqual(gap["connections"], ["rounds"])
            # 不可信推进下不允许展开子连接
            self.assertEqual(report["summary"]["connections"], 1)


class NullSemanticsTests(unittest.TestCase):
    def test_legal_null_attachment_distinguished_from_error_null(self):
        response = {
            "data": {
                "round": {
                    "id": "R1",
                    "roster": {
                        "edges": [{
                            "cursor": "cT1",
                            "node": {
                                "id": "T1",
                                "name": "合成甲队",
                                "work": {
                                    "edges": [{
                                        "cursor": "cR1S",
                                        "node": {"id": "R1S", "title": "展示资料", "attachment": None},
                                    }],
                                    "pageInfo": {"endCursor": None, "hasNextPage": False},
                                },
                            },
                        }],
                        "pageInfo": {"endCursor": None, "hasNextPage": False},
                    },
                }
            }
        }
        rules = [
            {"match": {"operationName": "Rounds"}, "responses": [rounds_single_response()]},
            {"match": {"operationName": "Teams", "variables": {"id": "R1"}},
             "responses": [load_fixture_response("Teams-2-0.json")]},
            {"match": {"operationName": "Submissions"}, "responses": [response]},
        ]
        with tempfile.TemporaryDirectory() as directory:
            collector = make_collector(ScriptedTransport(rules), directory, initial_revision="v1")
            report = collector.run()
            self.assertEqual(report["status"], "complete")
            self.assertEqual(open_gaps(report), [])
            attachment = report["nodes"]["submission"]["R1S"]["attachment"]
            self.assertEqual(attachment["status"], "null:legal")
            self.assertIsNone(attachment["value"])


class PaginationIsolationTests(unittest.TestCase):
    def test_each_team_uses_its_own_work_cursor(self):
        # 两个队伍各自两页 work；若使用全局游标，第二队会错误地从第二页开始
        teams_page = {
            "data": {"round": {"id": "R1", "roster": {
                "edges": [
                    {"cursor": "cT1", "node": {"id": "T1", "name": "甲队"}},
                    {"cursor": "cT2", "node": {"id": "T2", "name": "乙队"}},
                ],
                "pageInfo": {"endCursor": None, "hasNextPage": False},
            }}}
        }

        def work_page(team_id, submissions, end_cursor, has_next):
            return {"data": {"round": {"id": "R1", "roster": {"edges": [{
                "cursor": "cT1" if team_id == "T1" else "cT2",
                "node": {"id": team_id, "name": "甲队" if team_id == "T1" else "乙队", "work": {
                    "edges": [{"cursor": f"c-{submissions[0]}",
                               "node": {"id": submissions[0], "title": submissions[0], "attachment": None}}],
                    "pageInfo": {"endCursor": end_cursor, "hasNextPage": has_next},
                }},
            }], "pageInfo": {"endCursor": None, "hasNextPage": False}}}}}

        rules = [
            {"match": {"operationName": "Rounds"}, "responses": [rounds_single_response()]},
            {"match": {"operationName": "Teams"}, "responses": [teams_page]},
            {
                "match": {"operationName": "Submissions", "variables": {"after": None, "teamAfter": None}},
                "responses": [
                    work_page("T1", ["S1"], "w1", True),
                    work_page("T2", ["S3"], "w3", True),
                ],
            },
            {
                "match": {"operationName": "Submissions", "variables": {"after": "w1"}},
                "responses": [work_page("T1", ["S2"], None, False)],
            },
            {
                "match": {"operationName": "Submissions", "variables": {"after": "w3"}},
                "responses": [work_page("T2", ["S4"], None, False)],
            },
        ]
        with tempfile.TemporaryDirectory() as directory:
            transport = ScriptedTransport(rules)
            collector = make_collector(transport, directory, initial_revision="v1")
            report = collector.run()
            self.assertEqual(report["status"], "complete")
            t1 = connection(report, "work|R1|T1")
            t2 = connection(report, "work|R1|T2")
            self.assertEqual(t1["edge_cursors"], ["c-S1", "c-S2"])
            self.assertEqual(t2["edge_cursors"], ["c-S3", "c-S4"])
            # 深度优先：先走完 T1 的两页，再从 T2 自己的起点开始；
            # 每队第一页 after 都是 None，不存在全局游标串用
            first_after = [call["variables"]["after"] for call in transport.calls
                           if call["operationName"] == "Submissions"]
            self.assertEqual(first_after, [None, "w1", None, "w3"])
            team_after = [call["variables"]["teamAfter"] for call in transport.calls
                          if call["operationName"] == "Submissions"]
            self.assertEqual(team_after, [None, None, None, None])


class ConflictTests(unittest.TestCase):
    def test_same_version_node_conflict_is_recorded_not_overwritten(self):
        teams_response = {
            "data": {"round": {"id": "R1", "roster": {
                "edges": [{"cursor": "cT1", "node": {"id": "T1", "name": "旧队名"}}],
                "pageInfo": {"endCursor": None, "hasNextPage": False},
            }}}
        }
        submissions_response = {
            "data": {"round": {"id": "R1", "roster": {
                "edges": [{"cursor": "cT1", "node": {"id": "T1", "name": "新队名", "work": {
                    "edges": [{"cursor": "cS",
                               "node": {"id": "S1", "title": "资料", "attachment": None}}],
                    "pageInfo": {"endCursor": None, "hasNextPage": False},
                }}}],
                "pageInfo": {"endCursor": None, "hasNextPage": False},
            }}}
        }
        rules = [
            {"match": {"operationName": "Rounds"}, "responses": [rounds_single_response()]},
            {"match": {"operationName": "Teams"}, "responses": [teams_response]},
            {"match": {"operationName": "Submissions"}, "responses": [submissions_response]},
        ]
        with tempfile.TemporaryDirectory() as directory:
            collector = make_collector(ScriptedTransport(rules), directory, initial_revision="v1")
            report = collector.run()
            self.assertEqual(report["summary"]["conflicts"], 1)
            conflict = report["conflicts"][0]
            self.assertEqual(conflict["node_type"], "team")
            self.assertEqual(conflict["node_id"], "T1")
            self.assertEqual(conflict["field"], "name")
            self.assertEqual(conflict["existing"]["value"], "旧队名")
            self.assertEqual(conflict["incoming"]["value"], "新队名")
            # 先到的值得以保留，不去重、不覆盖；冲突双方都能追溯到请求
            self.assertEqual(report["nodes"]["team"]["T1"]["name"]["value"], "旧队名")
            self.assertTrue(conflict["existing"]["request"] and conflict["incoming"]["request"])
            self.assertEqual(report["status"], "complete_with_conflicts")


def load_fixture_response(name):
    return json.loads((ROOT / "sources" / name).read_text(encoding="utf8"))


def rounds_single_response():
    return {
        "data": {"rounds": {
            "edges": [{"cursor": "cR1", "node": {"id": "R1"}}],
            "pageInfo": {"endCursor": None, "hasNextPage": False},
        }}
    }


class PersistentFieldFailureTests(unittest.TestCase):
    def _rules(self, failing):
        return [
            {"match": {"operationName": "Rounds"}, "responses": [rounds_single_response()]},
            {"match": {"operationName": "Teams"}, "responses": [load_fixture_response("Teams-2-0.json")]},
            {"match": {"operationName": "Submissions"}, "responses": [failing]},
        ]

    def test_retry_budget_exhausted_marks_failed_and_keeps_single_gap(self):
        failing = load_fixture_response("Submissions-3-0.json")  # 附件 TEMPORARY 错误且一直不恢复
        with tempfile.TemporaryDirectory() as directory:
            transport = ScriptedTransport(self._rules(failing))
            collector = make_collector(transport, directory, initial_revision="v1", field_retries=2)
            report = collector.run()
            self.assertEqual(report["status"], "incomplete")
            work_conn = connection(report, "work|R1|T1")
            self.assertEqual(work_conn["status"], "exhausted")
            self.assertIn("重试上限", work_conn["blocked_reason"])
            field_gaps = [gap for gap in open_gaps(report) if gap["kind"] == "field_error"]
            # 持续故障只保留一条缺口，首次建档后两次重采复用并追加观测
            self.assertEqual(len(field_gaps), 1)
            self.assertEqual(len(field_gaps[0]["observations"]), 2)
            # 错误 null 不能被当成合法空值
            attachment = report["nodes"]["submission"]["R1S"]["attachment"]
            self.assertEqual(attachment["status"], "null:error")
            # 只重走出错的 work 连接，teams 等兄弟连接各只请求一次
            submissions_calls = [c for c in transport.calls if c["operationName"] == "Submissions"]
            self.assertEqual(len(submissions_calls), 3)  # 首次 + 2 次重采
            self.assertEqual(len([c for c in transport.calls if c["operationName"] == "Teams"]), 1)

    def test_resume_after_retry_budget_then_recovery(self):
        failing = load_fixture_response("Submissions-3-0.json")
        recovered = load_fixture_response("Submissions-3-1.json")
        with tempfile.TemporaryDirectory() as directory:
            first = make_collector(
                ScriptedTransport(self._rules(failing)), directory,
                initial_revision="v1", field_retries=1)
            first.run()
            self.assertEqual(connection(first.build_report(), "work|R1|T1")["status"], "exhausted")
            # 字段重试上限属于需人工确认的状态：显式重置后重启即可继续
            second = make_collector(
                ScriptedTransport(self._rules(recovered)), directory,
                initial_revision="v1", field_retries=1)
            failed = second.state["connections"]["work|R1|T1"]
            failed["status"] = "active"
            failed["refetch"] = True
            report = second.run()
            self.assertEqual(connection(report, "work|R1|T1")["status"], "complete")
            self.assertEqual(open_gaps(report), [])
            self.assertEqual(
                report["nodes"]["submission"]["R1S"]["attachment"]["status"], "present")


class TransportResumeTests(unittest.TestCase):
    def test_transport_failure_resumes_from_cursor_after_restart(self):
        class FlakyTransport(ScriptedTransport):
            def __init__(self, rules, fail_on, armed=True):
                super().__init__(rules)
                self.fail_on = fail_on
                self.armed = armed  # 第一次采集期间持续故障，重启后置为恢复

            def post(self, payload):
                if self.armed and payload["operationName"] == self.fail_on:
                    raise ConnectionRefusedError("模拟连接拒绝")
                return super().post(payload)

        rules = [
            {"match": {"operationName": "Rounds"}, "responses": [rounds_single_response()]},
            {"match": {"operationName": "Teams"}, "responses": [load_fixture_response("Teams-2-0.json")]},
            {"match": {"operationName": "Submissions"},
             "responses": [load_fixture_response("Submissions-3-1.json")]},
        ]
        with tempfile.TemporaryDirectory() as directory:
            first = make_collector(FlakyTransport(rules, "Submissions"), directory,
                                   initial_revision="v1", transport_retries=2)
            report = first.run()
            work_conn = connection(report, "work|R1|T1")
            self.assertEqual(work_conn["status"], "transport_failed")
            self.assertTrue(any(g["kind"] == "transport" for g in open_gaps(report)))
            # 重启：传输恢复后 transport_failed 自动重新调度，已完成的兄弟连接不重扫
            recovered = FlakyTransport(rules, "Submissions", armed=False)
            second = make_collector(recovered, directory, initial_revision="v1")
            resumed = second.run()
            self.assertEqual(connection(resumed, "work|R1|T1")["status"], "complete")
            # teams 已在第一次采集中完成，续跑不再请求它
            self.assertFalse(
                any(call["operationName"] == "Teams" for call in recovered.calls))


if __name__ == "__main__":
    unittest.main()
