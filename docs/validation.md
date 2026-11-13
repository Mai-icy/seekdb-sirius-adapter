# 首次拆分验证

2026-11-13，在独立仓库目录内完成以下检查。

| 检查 | 结果 |
| --- | --- |
| 原生计划编码单测 | 11/11 通过 |
| Decimal 边界与 Parquet 统计缓存单测 | 14/14 通过 |
| CLI 加载及 `--help` | 12 个入口通过 |
| 生产代码与提取来源 SHA-256 | 一致 |
| Python 语法及 Git diff 空白检查 | 通过 |

本地使用已有的 macOS Python 3.14.5 测试环境，依赖版本为 PyArrow 26.0.0、substrait-protobuf 0.78.0、protobuf 6.33.6、PyMySQL 1.2.3。执行命令为：

```sh
python -m unittest discover -s tests/unit -v
```

CI 配置在 Linux / Python 3.12 下运行同一套 CPU 单测，实际状态以仓库 Actions 为准。

本次未重新构建 GPU 桥接库，未启动远端 GPU 查询，也未重新运行 SF500。桥接与服务运行代码是已有集成工作区的原样提取；历史验证结果不能替代独立仓库后续的三方固定提交验收。后续需发布匹配的 seekdb 功能提交，再对固定版本组合执行集成检查。
