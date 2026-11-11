"""本地资料工作台命令入口。

子命令：
  inspect  查看本地资料文件及其字节指纹（原有能力）
  collect  针对 GraphQL 资料服务执行分连接采集，状态与证据落盘
  report   读取采集输出目录并打印完整性报告（逐连接结论）
"""
import argparse
import hashlib
import json
from pathlib import Path

from .collector import collect


def cmd_inspect(args):
    root = Path(args.directory)
    if not root.is_dir():
        raise SystemExit("资料目录不存在")
    rows = []
    for path in sorted(root.rglob("*")):
        if path.is_file():
            data = path.read_bytes()
            rows.append({
                "path": path.relative_to(root).as_posix(),
                "size": len(data),
                "sha256": hashlib.sha256(data).hexdigest(),
            })
    print(json.dumps(rows, ensure_ascii=False, indent=2))


def cmd_collect(args):
    report = collect(
        args.endpoint,
        args.out,
        args.schema,
        args.operations,
        transport_retries=args.transport_retries,
        field_retries=args.field_retries,
        timeout=args.timeout,
        initial_revision=args.revision,
    )
    print(json.dumps({
        "status": report["status"],
        "revision": report["revision"],
        "summary": report["summary"],
        "abort": report["abort"],
        "report": str(Path(args.out) / "report.json"),
    }, ensure_ascii=False, indent=2))
    if report["status"] in ("aborted_revision", "aborted_fatal", "incomplete"):
        raise SystemExit(2)


def cmd_report(args):
    path = Path(args.out) / "report.json"
    if not path.exists():
        raise SystemExit(f"找不到报告: {path}")
    report = json.loads(path.read_text(encoding="utf8"))
    print(json.dumps(report, ensure_ascii=False, indent=2))


def build_parser():
    parser = argparse.ArgumentParser(description="赛事资料本地工作台")
    sub = parser.add_subparsers(dest="command", required=True)

    p_inspect = sub.add_parser("inspect", help="查看资料目录的字节指纹")
    p_inspect.add_argument("directory")
    p_inspect.set_defaults(func=cmd_inspect)

    default_root = Path(__file__).resolve().parent.parent
    p_collect = sub.add_parser("collect", help="采集 GraphQL 资料服务")
    p_collect.add_argument("endpoint", help="GraphQL 服务地址，例如 http://127.0.0.1:8765/graphql")
    p_collect.add_argument("--out", required=True, help="采集输出目录（独立于原始样本）")
    p_collect.add_argument("--schema", default=str(default_root / "sources" / "schema.graphql"))
    p_collect.add_argument("--operations", default=str(default_root / "sources" / "operations.graphql"))
    p_collect.add_argument("--revision", default=None, help="预置 revision；缺省时先请求 Revision 操作")
    p_collect.add_argument("--transport-retries", type=int, default=3)
    p_collect.add_argument("--field-retries", type=int, default=3)
    p_collect.add_argument("--timeout", type=int, default=10)
    p_collect.set_defaults(func=cmd_collect)

    p_report = sub.add_parser("report", help="打印已保存的完整性报告")
    p_report.add_argument("out", help="采集输出目录")
    p_report.set_defaults(func=cmd_report)
    return parser


def main(argv=None):
    args = build_parser().parse_args(argv)
    args.func(args)


if __name__ == "__main__":
    main()
