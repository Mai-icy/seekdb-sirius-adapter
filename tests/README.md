# 验证入口

在仓库根目录激活虚拟环境并安装 `requirements.txt`。

## 无 GPU 单测

```sh
python -m unittest discover -s tests/unit -v
```

覆盖原生计划到 Substrait、列绑定、参数、Decimal 精度边界、反连接及不可变输入统计缓存。不连接数据库或加载 Sirius。

## 集成检查

使用隔离的测试实例；脚本可能创建、写入、删除测试数据库和用户。环境与命令行参数必须与该 seekdb 进程启动时的 endpoint、TLS、导出目录相匹配。以下脚本不由默认 CPU 单测自动触发。

| 入口 | 用途 |
| --- | --- |
| `native_export_integration.py` | 实际 seekdb 原生计划导出和编码 |
| `parquet_gpu_integration.py` | 公开 FFI、Parquet 列绑定及 GPU 计算 |
| `decimal_anti_gpu_integration.py` | Decimal 与反连接实际 GPU 语义 |
| `query_path_integration.py` | Unix socket、SQL 快照、事务及结果回传 |
| `network_query_integration.py` | 双向 TLS、上传、命名快照、权限与生命周期 |
| `two_host_query_integration.py` | 两台机器间 SQL 往返，服务需预先启动 |
| `decimal_query_path_integration.py` | SQL 路径的 Decimal 边界 |
| `tpch_plan_audit.py` | TPC-H 原生计划和 Substrait 编码检查 |
| `tpch_query_path_integration.py` | Q1–Q22 CPU/GPU 结果值与 MySQL 元数据校验 |

有命令行接口的脚本可通过 `--help` 查看参数；直接读取环境的脚本需检查文件中的连接配置。`query_path_integration.py` 及其复用者支持 `SEEKDB_HOST`、`SEEKDB_PORT`、`SEEKDB_USER`、`SEEKDB_PASSWORD`。TPC-H runner 当前连接本地端口、root 无密码测试实例，远端访问可使用专用 SSH 转发；不要将它直接用于生产环境。

服务已启动、数据已导入测试库后，例如：

```sh
mkdir -p artifacts
python tests/integration/tpch_query_path_integration.py \
  --port 2881 --database tpch_sf1 --scale-factor 1 \
  --gpu-snapshot validation-v1 --cpu-dop 1 --timeout 900 \
  --output artifacts/tpch-sf1
```

输出目录应为尚不存在的新目录。Q11 阈值按规模因子调整，并规范化 SQL 未规定的等值行顺序；Decimal 使用精确值比较。记录首次准备与快照复用的差别、CPU DOP、GPU 配置及三个项目的提交。结果不是正式审计的 TPC-H 成绩。

原仓库的机器专用日志、证书、Parquet、二进制和历史大体积结果未迁入。本次拆分的 CPU 验证记录见 `docs/validation.md`；历史 GPU 验证不等于本 checkout 已重新执行 GPU 验收。
