# seekdb + Sirius TPC-H benchmark

Run Q1–Q22 through seekdb's MySQL endpoint. seekdb CPU execution provides the reference results; GPU requests follow this repository's Parquet / Substrait / Sirius path. Each round compares result values, Decimal types, row counts and MySQL column metadata before measured timings are summarized. The adapter does not replan SQL, and the runner does not bypass seekdb to invoke the GPU directly.

## Prerequisites

1. Build and deploy seekdb and the Sirius service as described in the root README. Configure the endpoint, mutual TLS, private export directory and disk quotas, and disable Sirius CPU fallback.
2. Load all eight TPC-H tables into seekdb: `region`, `nation`, `supplier`, `customer`, `part`, `partsupp`, `orders` and `lineitem`. Use the actual scale factor, such as SF1 or SF500. The scripts do not generate or import data, or start services automatically.
3. Keep source data and schemas unchanged during measurement. CPU queries read the source database each time, while a named snapshot remains fixed. These are not a shared database transaction snapshot across connections. Confirm that any existing named snapshot represents the same source data.
4. Keep CPU DOP, database memory budget, GPU count, Sirius configuration and service concurrency fixed. A query may use multiple GPUs, but this runner executes sequentially and does not measure concurrent throughput.
5. Install the root `requirements.txt` on the client. SQL and GPU execution must be validated in the actual deployment environment; the client may run on a third machine.

Complete an SF1 suite before running SF500. `--scale-factor` neither generates data nor checks table sizes; it controls reporting and the Q11 threshold and must match the actual dataset. Plan capacity using the actual Parquet size and GPU/host memory configuration, not the SF number alone.

## Connection and deployment records

```sh
export SEEKDB_HOST=127.0.0.1
export SEEKDB_PORT=2881
export SEEKDB_USER=root
# If needed, set SEEKDB_PASSWORD through your credential mechanism; do not put it in arguments or reports.
```

Override connection parameters with `--host`, `--port` and `--user`. Passwords are read only from `SEEKDB_PASSWORD`. These settings control the client-to-seekdb connection and are separate from mutual TLS between seekdb and the adapter. Cross-machine MySQL access can use an existing secure connection or SSH forwarding.

Copy [environment.example.json](environment.example.json), fill in the deployment details and pass it with `--environment`. Record at least the actual deployed commits of all three projects; SHA-256 hashes of the seekdb, bridge and Sirius binaries; GPU model and count; memory budgets; Sirius configuration hash; dataset provenance; and network topology. The file is copied verbatim into local results and must not contain passwords, private keys or tokens.

The runner automatically records commits, dirty states and relevant source hashes from the **client checkout**. These do not establish which binaries remote processes are using. The client platform is also not the GPU machine's configuration. Verify disabled fallback, actual upload volume and cache hits against service logs; the runner's path labels are not an engine runtime audit.

## Preview before execution

Run from the repository root. Every output directory must be new and must not already exist.

```sh
python benchmarks/tpch/run.py \
  --database tpch_sf1 --scale-factor 1 \
  --mode snapshot --cpu-dop 4 --warmups 1 --rounds 3 \
  --output artifacts/sf1-plan --dry-run
```

`--dry-run` writes only `run.json`, without connecting to the database or starting GPU execution. Review its connection settings, query selection, snapshot name and commands, then execute with a new output directory:

```sh
python benchmarks/tpch/run.py \
  --database tpch_sf1 --scale-factor 1 \
  --mode snapshot --cpu-dop 4 --warmups 1 --rounds 3 \
  --environment /absolute/local-benchmark-environment.json \
  --output artifacts/sf1-snapshot
```

Each round runs all Q1–Q22 by default, executing CPU before GPU for each query. `--gpu-first` reverses that order. Execution order can affect the OS page cache, so results from different orders must not be combined directly. For a quick check, add `--queries 1,6`; the report marks this as a query subset rather than a full TPC-H suite.

### Named snapshot reuse

`--mode snapshot` generates a unique `bench-…` version name by default and runs at least one complete warmup suite before the measurement suites. The first GPU query may include Parquet preparation for the entire database. It is retained in warmup records and excluded from measurement rounds. Use `--gpu-snapshot analytics-v1` to reuse an existing immutable version; warmup is still required.

The first warmup query includes both preparation and execution; **it is not a standalone Parquet export time**. Obtain export, upload and statistics collection breakdowns from seekdb/adapter logs or separate export records. The runner does not clear the OS page cache or claim that warmup leaves all data resident on the GPU. Each query still incurs its own worker and Context costs.

### Current-statement input

```sh
python benchmarks/tpch/run.py \
  --database tpch_sf1 --scale-factor 1 \
  --mode statement --cpu-dop 4 --warmups 0 --rounds 3 \
  --output artifacts/sf1-statement
```

This mode fixes `gpu_snapshot=''`. Each GPU SQL statement exports and transfers its required columns again, and those steps are included in the timing. It does not force cold disk caches and must not be conflated with snapshot reuse measurements. Repeated export at SF500 can be expensive; first check resource and timeout settings with a small query subset.

### SF500 snapshot reuse example

```sh
python benchmarks/tpch/run.py \
  --database tpch_sf500 --scale-factor 500 \
  --mode snapshot --gpu-snapshot sf500-validated-v1 \
  --cpu-dop 4 --timeout 14400 --warmups 1 --rounds 3 \
  --environment /absolute/local-benchmark-environment.json \
  --output artifacts/sf500-snapshot
```

The version name, timeout and DOP are examples, not automatically selected optimal settings. The default 1 warmup plus 3 measurement rounds execute 4 CPU/GPU comparison suites: 176 queries, plus GPU plan exports. Check hardware capacity before using `configs/dual-gpu.yaml`; it targets two 48 GiB GPUs and a large-memory host with one NUMA node.

## Outputs and timing definitions

```text
artifacts/sf1-snapshot/
├── run.json                  Parameters, versions, source hashes and execution status
├── warmup-01.log             Raw warmup log
├── warmup-01/results.json    Warmup correctness and timings, excluded from aggregates
├── measurement-01.log       Measurement log; subsequent rounds follow the same pattern
├── measurement-01/          SQL, native plans, CPU/GPU rows and metadata
├── summary.json             All samples, medians and min/max values for each query
└── summary.md               Readable result table
```

- Per-query time is client-side `execute + fetchall`. It includes seekdb, the adapter, Sirius and result transfer; it is not pure GPU kernel time.
- Session configuration, the separately saved `EXPLAIN FORMAT=SIRIUS_JSON`, result file writes and external comparison are excluded from query timings. Each round's `wall_seconds` in `run.json` includes the full script overhead.
- Per-query summaries use the median across measurement rounds. Suite totals sum query times within each round, then take the median of those totals. The suite median is not the sum of per-query medians.
- `CPU/GPU` is median CPU time divided by median GPU time. Failures, missing queries, mismatched settings or metadata, and incomplete rounds suppress the aggregate speedup.
- The Q11 threshold is scaled by SF. Its value ordering is checked as descending before tie ordering is normalized; Decimal comparisons are exact. All modes are experimental correctness/performance measurements, not audited TPC-H scores.

A failed run returns a nonzero exit code and stops subsequent suites while retaining logs and partial artifacts. Use a new directory for configuration changes or retries. Do not remove failed queries and present the remaining values as a complete suite. Regenerate summaries without connecting to the database:

```sh
python benchmarks/tpch/summarize.py artifacts/sf1-snapshot
```

Named snapshots are not automatically deleted after success or failure, allowing inspection or reuse. When a version is no longer needed, locate its ID on the compute host with `adapter/snapshot_store.py --root …`, then release it with `--drop ID`. Do not delete a snapshot directory while it is in use.

## Data preparation and historical results

For explicit pre-export, the database submodule provides `deps/seekdb/tools/parquet_export.py`. It coordinates native `INTO OUTFILE` over primary-key ranges. Multiple tables must explicitly use the same valid SCN. Its manifest **cannot be used directly as** an adapter named snapshot directory: validation, identity binding and atomic publication require a separate workflow. This benchmark uses the service's existing automatic snapshot preparation or an already correctly published version.

The [2026-11-13 SF500 historical notes](results/2026-11-13-sf500.md) preserve the context, per-query data and limitations of two earlier experiments. They do not indicate that the current checkout has rerun SF500. The repository stores only small sanitized summaries; raw data, machine-specific logs and large result sets remain locally or on the test machines.
