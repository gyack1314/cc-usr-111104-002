# 多轮赛事 GraphQL 资料的局部错误补采

此项目维护本地合成来源、协议资料及原始字节工具，用于复查机器人赛事与课程资料。所有名称、数据和地址都不代表真实组织或设备。

`source_lab` 提供资料服务，`sources` 保存业务样本，`workbench.raw_store` 按摘要存取原始字节。HTTP来源的每条路由可依次返回多个响应，最后一个响应会重复；服务仅绑定环回地址。详细数据格式见 [资料约定](docs/source-format.md)。

`workbench.gqlschema` 解析 `sources` 下的 schema 与操作文档，按别名与数组下标解释 `errors.path`，并依据字段非空性计算错误冒泡的实际落点。`workbench.collector` 在其上实现分连接采集：rounds、每轮 teams、每队 submissions 各自维护独立游标；只有结构与布尔位都可信的 pageInfo 才会推进连接；可空叶子的局部错误只登记字段缺口并重走受影响连接，沿非空约束冒泡的错误会阻断整条父分支；未知错误路径不会被当成成功。所有请求绑定首次取得的 revision，收到 `REVISION_EXPIRED` 立即停止并保留证据。请求变量、原始字节摘要与字段路径随 `state.json`/`report.json` 落盘，重启后只继续未完成或带开放缺口的连接。

使用 Python 3.11 或以上版本，无第三方Python依赖。在项目根目录执行：

```sh
python -m unittest discover -s tests -v
python -m compileall -q workbench source_lab tests
python -m workbench.cli inspect sources
```

采集一个资料服务并输出逐连接完整性报告（输出目录独立于原始样本）：

```sh
python -m workbench.cli collect http://127.0.0.1:8765/graphql --out out/run-1
python -m workbench.cli report out/run-1
```

单路由合成场景对所有操作返回同一份响应，可改用 `--revision v1` 预置版本后直接观察冒泡、循环、版本失效或未知路径的处理结果。

启动本地HTTP资料服务：

```sh
python -m source_lab.http sources/normal.json --port 8765
```

容器方式可执行 `docker build -t resource-source .`，随后 `docker run --rm resource-source python -m unittest discover -s tests -v`。服务地址只对本次运行有效，客户端应从配置取得地址。输出资料写入单独目录，原始样本保持只读。
