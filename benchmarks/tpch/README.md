# seekdb + Sirius TPC-H benchmark

通过 seekdb MySQL 入口运行 Q1–Q22：seekdb CPU 作为结果对照，GPU 请求走本仓库的 Parquet / Substrait / Sirius 路径。每轮都对比结果值、Decimal 类型、行数和 MySQL 列元数据，随后汇总测量轮次的耗时。没有在适配器里重新规划 SQL，也没有直接绕过 seekdb 调用 GPU。

## 准备条件

1. 按根目录 README 构建、部署 seekdb 和 Sirius 服务，配置 endpoint、双向 TLS、私有导出目录及磁盘配额，关闭 Sirius CPU fallback。
2. 在 seekdb 中准备已经导入的 TPC-H 八表：`region`、`nation`、`supplier`、`customer`、`part`、`partsupp`、`orders`、`lineitem`。使用实际规模因子，例如 SF1 或 SF500；脚本不生成数据、不自动导入数据，也不自动启动服务。
3. 测量期间保持源数据和 schema 不变。CPU 每次读源库，命名快照固定旧版本；二者不是跨连接共享的数据库事务快照。已有命名快照必须确认对应同一批源数据。
4. 使用固定的 CPU DOP、数据库内存预算、GPU 数量、Sirius 配置和服务并发度。单条查询可使用多 GPU，但本入口按顺序执行，不测并发吞吐量。
5. 在客户端安装根目录 `requirements.txt`。SQL 与 GPU 必须在实际部署环境验证；客户端可运行在第三台机器。

建议先完成 SF1 全套验证，再运行 SF500。`--scale-factor` 不会生成或检查表规模；它用于报告和 Q11 阈值调整，必须与实际数据匹配。容量规划依据 Parquet 实际大小和 GPU/主机内存配置，不能直接把 SF 数字当作显存需求。

## 连接及部署记录

```sh
export SEEKDB_HOST=127.0.0.1
export SEEKDB_PORT=2881
export SEEKDB_USER=root
# 如需密码，从现有凭据机制设置 SEEKDB_PASSWORD；不要写进命令参数或报告。
```

也可通过 `--host`、`--port`、`--user` 覆盖连接参数。密码只从 `SEEKDB_PASSWORD` 读取。这些是客户端到 seekdb 的连接参数，与 seekdb 到 adapter 的双向 TLS 配置不同；跨机器 MySQL 访问可通过既有安全连接或 SSH 转发。

复制 [environment.example.json](environment.example.json) 到本地并填写部署信息，通过 `--environment` 传入。至少记录真实运行的三方提交、seekdb/桥接库/Sirius 二进制 SHA-256、GPU 型号与数量、内存预算、Sirius 配置哈希、数据来源及网络拓扑。文件会原样写入本地结果，不能包含密码、私钥或 token。

脚本自动记录的是**客户端 checkout** 的提交、dirty 状态及相关源码哈希，不会据此宣称远端进程使用相同二进制。客户端平台也不等于 GPU 机器配置。Sirius 禁用 fallback、实际上传量和缓存命中需结合服务日志确认；runner 的路径标签不是引擎运行时审计。

## 先预览，再执行

在仓库根目录运行；每次输出目录必须是尚不存在的新目录。

```sh
python benchmarks/tpch/run.py \
  --database tpch_sf1 --scale-factor 1 \
  --mode snapshot --cpu-dop 4 --warmups 1 --rounds 3 \
  --output artifacts/sf1-plan --dry-run
```

`--dry-run` 只写 `run.json`，不会连接数据库或启动 GPU。检查其中的连接、查询集、快照名和命令后，使用新目录执行：

```sh
python benchmarks/tpch/run.py \
  --database tpch_sf1 --scale-factor 1 \
  --mode snapshot --cpu-dop 4 --warmups 1 --rounds 3 \
  --environment /absolute/local-benchmark-environment.json \
  --output artifacts/sf1-snapshot
```

默认每轮运行完整 Q1–Q22，默认先 CPU 后 GPU。`--gpu-first` 可反转每条查询的顺序；两种顺序可能影响操作系统页缓存，因此不同顺序的结果不能直接合并。快速检查可加 `--queries 1,6`，报告会标记为查询子集，不算完整 TPC-H。

### 命名快照复用

`--mode snapshot` 默认生成唯一的 `bench-…` 版本名，至少执行一轮完整预热套件，再运行测量套件。首次 GPU 查询可能包含全库 Parquet 准备；它在预热记录中，不计入测量轮次。复用已有不可变版本可加 `--gpu-snapshot analytics-v1`，但仍需预热。

预热首条查询的耗时包含准备和查询，**不是独立的 Parquet 导出耗时**。导出、上传、统计收集的细分耗时从 seekdb/adapter 日志或独立导出记录获取。脚本不会自动清操作系统页缓存，也不声称预热后所有数据驻留 GPU。每次查询仍有独立 worker 和 Context 的成本。

### 当前语句输入

```sh
python benchmarks/tpch/run.py \
  --database tpch_sf1 --scale-factor 1 \
  --mode statement --cpu-dop 4 --warmups 0 --rounds 3 \
  --output artifacts/sf1-statement
```

该模式固定 `gpu_snapshot=''`，每条 GPU SQL 重新导出所需列并传输，计时包含这些步骤。它不是强制冷磁盘缓存模式，也不能与快照复用耗时混为一谈。SF500 下反复导出的成本可能很高，先用少量查询确认资源和超时配置。

### SF500 快照复用示例

```sh
python benchmarks/tpch/run.py \
  --database tpch_sf500 --scale-factor 500 \
  --mode snapshot --gpu-snapshot sf500-validated-v1 \
  --cpu-dop 4 --timeout 14400 --warmups 1 --rounds 3 \
  --environment /absolute/local-benchmark-environment.json \
  --output artifacts/sf500-snapshot
```

版本名、超时和 DOP 是示例，不自动推断最优配置。默认 1 次预热加 3 次测量会执行 4 套 CPU 与 GPU 对照，共 176 条查询，另有 GPU 计划导出。使用 `configs/dual-gpu.yaml` 前核对机器资源；它是两张 48 GiB GPU、大内存单 NUMA 主机的专用配置。

## 结果与计时口径

```text
artifacts/sf1-snapshot/
├── run.json                  参数、版本、源码哈希、执行状态
├── warmup-01.log             原始预热日志
├── warmup-01/results.json    预热正确性和耗时，不进入汇总
├── measurement-01.log       测量日志（后续轮次依此类推）
├── measurement-01/          SQL、原生计划、CPU/GPU 行结果及元数据
├── summary.json             每条查询的全部样本、中位数、最小/最大值
└── summary.md               可阅读的结果表
```

- 每条查询时间是客户端 `execute + fetchall`，包含 seekdb、适配服务、Sirius、结果传输等开销；不是纯 GPU 内核时间。
- 会话配置、额外保存的 `EXPLAIN FORMAT=SIRIUS_JSON`、结果文件写入和外部比较不计入查询时间。`run.json` 中每轮 `wall_seconds` 则包含整轮脚本开销。
- 每条查询汇总测量轮次中位数；套件先在每一轮内求查询耗时之和，再对轮次总和取中位数。套件中位数不等于逐查询中位数之和。
- `CPU/GPU` 是 CPU 中位耗时除以 GPU 中位耗时。失败、缺失查询、配置不匹配、元数据不一致或不完整轮次均不生成整体加速比。
- Q11 阈值按 SF 调整，并验证 value 降序后规范化等值行顺序；Decimal 精确比较。所有模式均是实验性正确性/性能测量，不是经审计的 TPC-H 成绩。

运行失败返回非零退出码并停止后续套件，保留日志与部分产物。修改配置或补测应使用新目录；不要删除失败查询后把剩余值当成完整套件。重新生成汇总不连接数据库：

```sh
python benchmarks/tpch/summarize.py artifacts/sf1-snapshot
```

命名快照不会在完成或失败后自动删除，以便复查或复用。确认不再使用后，在计算端用 `adapter/snapshot_store.py --root …` 找到对应 ID，再通过 `--drop ID` 释放。不要直接删除正在使用的快照目录。

## 数据准备与历史记录

如果需要显式预导出，数据库子仓库提供 `deps/seekdb/tools/parquet_export.py`。它按主键范围协调原生 `INTO OUTFILE`，多表需明确使用同一个有效 SCN。该工具产生的 manifest **不能直接当作** adapter 的命名快照目录；验证、身份绑定及原子发布仍需单独流程。本 benchmark 默认使用服务现有的自动快照准备或已经正确发布的版本。

[2026-11-13 SF500 历史说明](results/2026-11-13-sf500.md) 保留两轮既有实验的背景、逐查询数据和限制；不代表当前 checkout 已重新执行 SF500。仓库只保存小型脱敏摘要，原始数据、机器专用日志和大体积结果留在本地或测试机器。
