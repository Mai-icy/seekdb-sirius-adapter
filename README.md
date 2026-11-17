# seekdb-sirius-adapter

An independent adapter service connecting seekdb to the Sirius GPU query engine. seekdb remains the system of record; Parquet provides intermediate input on the compute side. The adapter uses unmodified public Sirius interfaces.

```text
Client SQL
    → seekdb: parsing, authorization, optimization, native plans and consistent snapshots
    → TCP + mutual TLS: plan and Parquet fragment uploads
    → adapter: input validation, snapshot management, Substrait Protobuf encoding
    → Sirius: native Parquet reads and GPU execution
    → Arrow C Stream (in-process) → batched result protocol (TLS)
    → seekdb → MySQL results → client
```

## Repository boundaries and dependencies

| Project | Responsibilities | Upstream |
| --- | --- | --- |
| seekdb | SQL, planning, authorization, read snapshots, native Parquet export, transport client and MySQL responses | Integration branch in [Mai-icy/seekdb](https://github.com/Mai-icy/seekdb) |
| This repository | Python service, snapshot management, plan encoding, Decimal adaptation, public FFI bridge and validation | This repository |
| Sirius | Parquet scans, execution-plan lowering and GPU computation | Unmodified [sirius-db/sirius](https://github.com/sirius-db/sirius) |

**This is an integration development version; it requires the seekdb feature branch rather than a stock seekdb main checkout.** Both upstreams are Git submodules pinned to specific commits. They do not advance automatically with upstream branches:

- `deps/seekdb` → the fork's `feat/sirius-parquet` branch, pinned to `193ecbf93c940bd9d94cf24740798abda1cb7d35`.
- `deps/sirius` → unmodified upstream Sirius, pinned to `40106bf1c79cbbaddf64cba88a1f24287ffc1621`.

See [upstream dependencies](docs/upstreams.md) for required interfaces and update procedures. The [source manifest](docs/source-provenance.json) preserves the initial extraction history; submodule pointers determine the current deployment versions.

The current input path uses **Parquet without pinning**. Arrow input is maintained as a separate direction pending upstream Sirius support; this repository's initial version does not provide an Arrow input execution path. The existing Arrow C Stream interface carries results.

## Installation and build

Clone the matching source trees:

```sh
git clone --recurse-submodules https://github.com/Mai-icy/seekdb-sirius-adapter.git
cd seekdb-sirius-adapter
```

After updating an existing checkout, run `git submodule update --init --recursive`. This checks out the pinned commits. Do not use `--remote` for deployment.

Sirius also references DuckDB, the Substrait reader and other experimental integrations. To inspect only the two main repositories, use a regular clone followed by `git submodule update --init deps/seekdb deps/sirius`. Initialize nested build dependencies according to the Sirius build instructions.

The compute side requires a Linux GPU machine with a matching CUDA environment. Build the pinned Sirius version according to its upstream instructions. The headers and `libsirius.so` below must come from the same build. Python encoder unit tests can run without a GPU.

```sh
python3.12 -m venv .venv
. .venv/bin/activate
python -m pip install -r requirements.txt

cmake -S native -B build \
  -DSIRIUS_INCLUDE_DIR=/absolute/sirius/include \
  -DSIRIUS_LIBRARY=/absolute/sirius-build/libsirius.so
cmake --build build --parallel
```

Ensure the dynamic loader can locate Sirius and its dependencies at runtime, for example through `LD_LIBRARY_PATH`. Build each engine using its own instructions. The adapter's CMake builds only the bridge; it does not build the engines automatically or distribute engine binaries.

## Running on two machines

Prepare client and server certificates signed by a trusted CA. The server certificate SAN must cover the endpoint IP or `TLS_SERVER_NAME`. Replace the example paths below with actual deployment paths.

On the **seekdb machine**, set these variables before starting the database process built with the integration features:

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

`SOURCE_ID` must be unique across instances and stable across restarts of the same instance. The endpoint uses a numeric IPv4 address. The two machines do not need a shared filesystem.

On the **Sirius machine**, start the service from the repository root:

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

Use a Sirius configuration appropriate for the hardware and disable CPU fallback. `configs/dual-gpu.yaml` targets two 48 GiB GPUs and a large-memory host with one NUMA node; it is not a general default. Set `CUDA_VISIBLE_DEVICES=0,1` when using it. The default input quota is 16 GiB per request and the cache quota is 64 GiB. For larger datasets, increase `--snapshot-bytes` and `--cache-bytes` explicitly and reserve disk space for concurrent uploads.

Clients continue to connect to seekdb's MySQL port:

```sql
SET parallel_degree_policy = 'MANUAL';
SET _force_parallel_query_dop = 1;
SET execution_backend = 'gpu';
SET gpu_snapshot = '';
SELECT COUNT(*) FROM your_table;
```

With `gpu_snapshot=''`, each statement exports the columns required by its plan at the statement's read snapshot. Compute-side inputs are reclaimed after the query. A nonempty version name, such as `analytics-v1`, initially exports all ordinary persistent tables and their visible columns in the current database, then reuses the fully published version. Named versions require autocommit outside an explicit transaction. Subsequent DML does not change the version; refresh by choosing a new name. This is neither incremental synchronization nor persistent GPU pinning.

For local debugging, use a Unix socket with the same UID on both sides: set `SEEKDB_SIRIUS_QUERY_SOCKET` on the source and replace `--listen` with `--socket` on the service, omitting TLS options. If an endpoint is also configured, the source prefers TCP.

List and remove unused named snapshots:

```sh
python adapter/snapshot_store.py --root /absolute/compute-inputs
python adapter/snapshot_store.py --root /absolute/compute-inputs --drop SNAPSHOT_ID
```

## Validation

The reusable [TPC-H benchmark](benchmarks/tpch/README.md) provides SF1/SF500 examples, warmup and repeated CPU/GPU comparisons, JSON/Markdown summaries and historical SF500 notes. It distinguishes named Parquet snapshot reuse from preparing input for every SQL statement. Incomplete or failed suites do not produce an aggregate speedup.

```sh
python -m unittest discover -s tests/unit -v
```

Real SQL/TLS/GPU checks are in `tests/integration/`. They require an isolated test database, a matching seekdb binary and a remote GPU environment. Some scripts create, populate and delete test databases or users. See the [test instructions](tests/README.md). The initial repository extraction reran only CPU unit tests; historical SF500 results are not a new acceptance run for this repository.

## Design boundaries

- seekdb exports native `SIRIUS_JSON` IR; the adapter encodes Substrait Protobuf. The IR is not standard Substrait JSON.
- Parquet fragments are uploaded after completion, and Sirius is invoked only after all inputs are ready. Uploads can overlap source-side encoding, but GPU execution does not consume inputs while they are still arriving.
- Sirius finishes execution and holds the results before exposing an Arrow C Stream. The adapter reads and returns batches over TLS; this is not end-to-end streaming computation.
- Decimal execution requires a safety proof based on immutable input statistics and relational constraints. If the proof fails, the query is rejected without conversion to DOUBLE. Unsupported plans fail explicitly without automatic CPU fallback.
- Each request uses its own child process and Context. Named snapshots reuse Parquet on disk, not GPU-resident data across queries.

See the [protocol description](docs/protocol.md) for message formats. Licensed under [Apache-2.0](LICENSE).
