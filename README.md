# seekdb-sirius-adapter

连接 seekdb 与 Sirius GPU 执行引擎的独立适配服务。数据仍由 seekdb 管理，Parquet 是计算端的中间输入；Sirius 使用未修改的上游公开接口。

```text
客户端 SQL
    → seekdb：解析、权限检查、优化、原生计划和一致性快照
    → TCP + 双向 TLS：计划与 Parquet 分片上传
    → adapter：输入校验、快照管理、Substrait Protobuf 编码
    → Sirius：原生读取 Parquet，执行 GPU 查询
    → Arrow C Stream（进程内）→ 分批结果协议（TLS）
    → seekdb → MySQL 结果 → 客户端
```

## 仓库边界与依赖

| 项目 | 职责 | 上游 |
| --- | --- | --- |
| seekdb | SQL、计划、权限、读取快照、原生 Parquet 导出、传输客户端及 MySQL 回包 | [Mai-icy/seekdb](https://github.com/Mai-icy/seekdb)，集成功能分支 |
| 本仓库 | Python 服务、快照管理、计划编码、Decimal 适配、公开 FFI 桥接及验证 | 本仓库 |
| Sirius | Parquet 扫描、执行计划落地、GPU 计算 | [sirius-db/sirius](https://github.com/sirius-db/sirius)，不修改源码 |

**这是集成开发版本，不能直接搭配普通 seekdb main 使用。** 两个上游通过 Git submodule 关联，仓库固定其提交，不会随上游分支自动升级：

- `deps/seekdb` → 你的 fork 的 `feat/sirius-parquet` 分支，固定 `193ecbf93c940bd9d94cf24740798abda1cb7d35`。
- `deps/sirius` → 未修改的 Sirius 上游，固定 `40106bf1c79cbbaddf64cba88a1f24287ffc1621`。

所需接口和更新方法见 [依赖约定](docs/upstreams.md)。[来源清单](docs/source-provenance.json) 保留首次拆分时的历史信息，当前部署版本以 submodule 指针为准。

当前输入路径为 **Parquet、无 pin**。Arrow 输入是另行维护、等待 Sirius 上游支持的方向，本仓库初始版本不提供 Arrow 输入执行路径；已有的 Arrow C Stream 是结果输出接口。

## 安装与构建

首次下载配套源码：

```sh
git clone --recurse-submodules https://github.com/Mai-icy/seekdb-sirius-adapter.git
cd seekdb-sirius-adapter
```

已有 checkout 更新后执行 `git submodule update --init --recursive`。该命令检出仓库记录的固定提交；不要在部署时使用 `--remote`。

Sirius 自身还关联了 DuckDB、Substrait reader 及其他实验集成的仓库。只需查看两个主仓库时，可先普通 clone，再执行 `git submodule update --init deps/seekdb deps/sirius`；构建所需的嵌套依赖按 Sirius 的构建说明初始化。

计算端使用具备匹配 CUDA 环境的 Linux GPU 机器。先按 Sirius 上游说明构建固定版本；下面的头文件和 `libsirius.so` 必须来自同一构建。Python 编码单测可以在无 GPU 的机器运行。

```sh
python3.12 -m venv .venv
. .venv/bin/activate
python -m pip install -r requirements.txt

cmake -S native -B build \
  -DSIRIUS_INCLUDE_DIR=/absolute/sirius/include \
  -DSIRIUS_LIBRARY=/absolute/sirius-build/libsirius.so
cmake --build build --parallel
```

运行时保证 Sirius 及其依赖库可被动态加载器找到，例如设置相应的 `LD_LIBRARY_PATH`。两个引擎需按各自构建说明编译；适配器的 CMake 只构建桥接库，不自动构建引擎，也不分发引擎二进制。

## 两台机器运行

准备可信 CA 签发的客户端和服务端证书，服务端证书 SAN 应覆盖所用 IP 或 `TLS_SERVER_NAME`。以下均为示例路径，需替换为实际配置。

在 **seekdb 机器**上，于启动具有集成功能的数据库进程前设置：

```sh
install -d -m 700 /absolute/source-inputs
export SEEKDB_SIRIUS_EXPORT_DIR=/absolute/source-inputs
export SEEKDB_SIRIUS_EXPORT_WORKERS=4
export SEEKDB_SIRIUS_ENDPOINT=192.0.2.20:9410
export SEEKDB_SIRIUS_SOURCE_ID=analytics-source-01
export SEEKDB_SIRIUS_TLS_CA=/absolute/tls/ca.crt
export SEEKDB_SIRIUS_TLS_CERT=/absolute/tls/client.crt
export SEEKDB_SIRIUS_TLS_KEY=/absolute/tls/client.key
export SEEKDB_SIRIUS_TLS_SERVER_NAME=gpu.example.internal
```

`SOURCE_ID` 在不同实例间必须唯一，在同一实例重启后保持稳定。endpoint 使用数字 IPv4 地址。两端无须共享文件系统。

在 **Sirius 机器**上，从仓库根目录启动服务：

```sh
install -d -m 700 /absolute/compute-inputs
CUDA_VISIBLE_DEVICES=0 python adapter/query_service.py \
  --listen 0.0.0.0:9410 \
  --cache-root /absolute/compute-inputs \
  --tls-ca /absolute/tls/ca.crt \
  --tls-cert /absolute/tls/server.crt \
  --tls-key /absolute/tls/server.key \
  --library "$PWD/build/libsirius_compute_bridge.so" \
  --config /absolute/sirius.yaml \
  --workers 1
```

使用与硬件匹配的 Sirius 配置并关闭 CPU fallback。`configs/dual-gpu.yaml` 是两张 48 GiB GPU、单 NUMA 大内存机器的专用配置，不是通用默认值；使用它时设置 `CUDA_VISIBLE_DEVICES=0,1`。默认单次输入配额为 16 GiB、缓存配额为 64 GiB，较大数据集需显式调高 `--snapshot-bytes` 和 `--cache-bytes`，并为并发上传留出磁盘空间。

客户端仍连接 seekdb 的 MySQL 端口：

```sql
SET parallel_degree_policy = 'MANUAL';
SET _force_parallel_query_dop = 1;
SET execution_backend = 'gpu';
SET gpu_snapshot = '';
SELECT COUNT(*) FROM your_table;
```

`gpu_snapshot=''` 每次按当前语句快照导出计划所需列，查询结束后回收计算端输入。设置非空版本名，例如 `analytics-v1`，首次会导出当前数据库的全部普通持久表及可见列，完整发布后复用。命名版本要求 autocommit 且不在显式事务内；后续 DML 不改变该版本，刷新需换新版本名。它不是增量同步，也不是 GPU 常驻 pin。

同机调试可使用同 UID 的 Unix socket：源端设置 `SEEKDB_SIRIUS_QUERY_SOCKET`，服务端用 `--socket` 替换 `--listen` 并省略 TLS 参数。若同时设置 endpoint，源端优先选择 TCP。

查看及删除未使用的命名快照：

```sh
python adapter/snapshot_store.py --root /absolute/compute-inputs
python adapter/snapshot_store.py --root /absolute/compute-inputs --drop SNAPSHOT_ID
```

## 验证

```sh
python -m unittest discover -s tests/unit -v
```

真实 SQL/TLS/GPU 检查位于 `tests/integration/`，需独立测试数据库、匹配的 seekdb 二进制和远端 GPU 环境；部分脚本会创建、写入并删除测试数据库或用户。运行条件见 [测试说明](tests/README.md)。本仓库首次拆分只重新执行 CPU 单测，不把历史 SF500 数据当作新仓库的重新验收。

## 设计边界

- seekdb 导出 `SIRIUS_JSON` 原生 IR，适配层编码为 Substrait Protobuf；IR 不是标准 Substrait JSON。
- Parquet 分片完成后才上传，全部输入就绪后才调用 Sirius。上传可与源端编码重叠，尚不支持边接收输入边执行 GPU 查询。
- Sirius 完成执行并持有结果后暴露 Arrow C Stream，适配层分批读取、通过 TLS 回传；这不等于端到端流式计算。
- Decimal 依据不可变输入的统计与关系约束证明可安全执行；无法证明则报错，不降级为 DOUBLE。非支持计划明确失败，不自动回退 CPU。
- 每个请求独立子进程和 Context；命名快照复用磁盘 Parquet，不复用跨查询 GPU 驻留数据。

更多消息格式见 [协议说明](docs/protocol.md)。许可证为 [Apache-2.0](LICENSE)。
