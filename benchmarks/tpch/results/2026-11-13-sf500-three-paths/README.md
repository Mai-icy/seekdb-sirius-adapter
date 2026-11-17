# SF500: CPU, legacy native pin and Parquet (2026-11-13)

This archive publishes an existing experiment, not a new benchmark run. All three paths completed Q1–Q22 successfully. Each query ran twice consecutively: trial 0 was warmup, and trial 1 supplied the reported timing. All 132 executions were checked against saved CPU reference values.

| Execution path | Measured Q1–Q22 total | Queries passed |
| --- | ---: | ---: |
| seekdb CPU | 192.805 s | 22/22 |
| Legacy native pin, high-resource configuration | 31.307 s | 22/22 |
| Non-pin Parquet, dual GPU | 791.319 s | 22/22 |

For this experiment, CPU time divided by pin time was 6.159, Parquet time divided by pin time was 25.276, and Parquet took 4.104 times the CPU time. These compare complete execution paths with different versions and lifecycles, not the isolated effect of enabling pinning. They are single measured samples after adjacent warmup, not medians or audited TPC-H scores.

The pin preparation step, including loading all eight tables and an eight-table COUNT join, took **656.720 seconds** and is excluded from the query total. The COUNT join returned 3,000,028,242; that is not the sum of all table row counts. All 44 pin executions reused prepared input with zero input rows and bytes reported by the pin reuse events. All 44 Parquet executions hit the existing snapshot and uploaded zero bytes.

These reuse counters describe source input and upload activity, not an absence of disk reads, host-memory access or transfers within the compute engine.

## Machine and resource settings

The source database, adapter, execution engine and retained inputs were on the same Linux machine. Queries entered through seekdb's MySQL interface; adapter traffic used a local Unix socket. Cross-host transfer was not measured.

| Hardware | Recorded value |
| --- | --- |
| CPU | Intel Xeon w7-3565X, x86_64 |
| CPU topology | 1 socket, 32 physical cores, 2 threads/core, 64 logical CPUs |
| NUMA | 1 node |
| Host RAM | 540,462,571,520 bytes, approximately 503 GiB |
| GPUs | 2 × NVIDIA RTX 5880 Ada Generation |
| GPU memory | 48 GiB class; 49,140 MiB reported per device |

The hardware evidence comes from the preceding query-performance capture on the same host and date. Kernel, distribution, NVIDIA driver, CUDA runtime and storage model were not established by that capture; they are explicitly unknown in [environment.json](environment.json). No current machine values were substituted for missing historical evidence.

| Setting | CPU phase | Parquet phase | Legacy pin phase |
| --- | --- | --- | --- |
| seekdb plan DOP | 64 | 1 | 1 |
| seekdb memory budget | 416 GiB | 48 GiB | 24 GiB |
| seekdb KV cache limit | 352 GiB | 24 GiB | 8 GiB |
| Sirius GPU pool | N/A | 46 GiB per GPU | 46 GiB per GPU |
| Sirius host pool | N/A | 416 GiB per NUMA node | 416 GiB per NUMA node |
| Scan threads | N/A | 64 | 64 |
| Pipeline / task creator threads | N/A | 16 / 16 | 16 / 16 |
| Native storage decoder workers | N/A | N/A | 16 |
| Scan batch | N/A | 512 MiB | 512 MiB |
| Host block size | N/A | 1 MiB | 8 MiB |
| Hash partition size | N/A | 256 MiB | 1 GiB |
| Maximum build hash table | N/A | 1 GiB | 2 GiB |

CPU and GPU budgets were used in separate phases. The CPU process exited before GPU testing; these limits were not all allocated concurrently. Configuration limits do not establish measured resident memory. Parquet used one service worker, a 320 GiB input quota and a 384 GiB cache quota. The legacy pin configuration included 128 GiB of spill capacity; this does not establish actual spill usage.

Exact settings are retained in [CPU effective parameters](configs/cpu-effective-parameters.json), [Parquet configuration](configs/parquet.yaml) and [legacy pin configuration](configs/legacy-pin.yaml). Only the legacy spill directory was replaced with a placeholder. The legacy configuration belongs to its historical engine stack and is not a preset for the current public-FFI service.

## Versions and execution boundaries

CPU and Parquet used the then-current seekdb integration binary. Parquet used the public Sirius Substrait FFI stack and created and released a worker and Context for every query. Native pin used its original compatible seekdb, adapter, native storage decoder and private-interface Sirius library, with a persistent Context and host pinning.

The current public Substrait FFI does not expose the legacy native pin interface. No Sirius kernel or production adapter code was changed to obtain this experiment, but that does **not** make the legacy stack an unmodified public-upstream implementation. The versions, scan implementations, memory layout, operator settings and process lifecycle differ between the two GPU paths.

[environment.json](environment.json) records the retained legacy binary hashes, earlier public-FFI build provenance and related dataset-preparation hashes. Their capture dates and limits are explicit. Exact deployed seekdb and adapter commits were not recorded as release commits at that time; current submodule commits must not be retroactively treated as proof of the deployed source state. This archive does not distribute the legacy binaries.

## Dataset, timing and correctness

- The experiment reused isolated SF500 database clones. The Parquet input contained 154 files, totaling 229,908,148,390 bytes and 4,330,028,272 rows across all eight tables. Business table data was not written by the benchmark.
- All modes used the same [22 SQL files](sql/). Their SHA-256 hashes match the per-query records in all three modes. Q11 already contains the SF500 threshold; do not scale these archived SQL files again.
- Each timer covered client `execute` through complete `fetchall`. Preparation, pin loading, external result comparison and result-file writes were excluded. The OS page cache was not cleared, and modes ran sequentially to avoid resource contention.
- Every execution compared values and row counts against previously validated CPU output for the same data. Numeric columns identified by the CPU metadata were compared using exact Decimal values, without approximate floating-point tolerances. Q11 descending order was checked before tie ordering was normalized.
- MySQL metadata was recorded per query, but the legacy and current versions were not required to match every metadata field. Do not describe this as fresh, identical-metadata acceptance across all three stacks.
- Raw JSON result hashes may differ for Q11 because its tied rows may appear in different valid orders. Byte identity is not the comparison criterion for those ties.

The current `benchmarks/tpch/run.py` warms an entire suite and measures repeated CPU/Parquet suites. This historical runner used adjacent warmup per query and a separate legacy pin stack. The current runner cannot reproduce all three paths unchanged.

## Per-query measured times

| Query | CPU seconds | Legacy pin seconds | Parquet seconds | Returned rows | Value check |
| --- | ---: | ---: | ---: | ---: | --- |
| Q1 | 12.928 | 2.608 | 36.981 | 4 | Passed |
| Q2 | 0.803 | 0.600 | 9.961 | 100 | Passed |
| Q3 | 10.510 | 1.242 | 37.125 | 10 | Passed |
| Q4 | 5.429 | 0.832 | 33.214 | 5 | Passed |
| Q5 | 6.174 | 1.121 | 40.554 | 5 | Passed |
| Q6 | 3.670 | 0.795 | 37.026 | 1 | Passed |
| Q7 | 12.331 | 1.027 | 45.794 | 4 | Passed |
| Q8 | 8.102 | 1.458 | 45.504 | 2 | Passed |
| Q9 | 19.508 | 2.761 | 47.741 | 175 | Passed |
| Q10 | 7.560 | 1.831 | 37.530 | 20 | Passed |
| Q11 | 2.340 | 1.616 | 9.778 | 467,405 | Passed |
| Q12 | 6.614 | 1.487 | 40.067 | 2 | Passed |
| Q13 | 9.642 | 1.358 | 13.992 | 41 | Passed |
| Q14 | 4.029 | 1.036 | 42.019 | 1 | Passed |
| Q15 | 3.663 | 1.148 | 57.030 | 1 | Passed |
| Q16 | 2.836 | 0.680 | 6.788 | 27,840 | Passed |
| Q17 | 3.506 | 1.254 | 47.439 | 1 | Passed |
| Q18 | 18.238 | 2.227 | 44.077 | 100 | Passed |
| Q19 | 9.027 | 2.345 | 43.772 | 1 | Passed |
| Q20 | 6.791 | 1.179 | 41.350 | 72,343 | Passed |
| Q21 | 36.907 | 2.358 | 63.706 | 100 | Passed |
| Q22 | 2.198 | 0.345 | 9.871 | 7 | Passed |
| Total | 192.805 | 31.307 | 791.319 | — | 22/22 per path |

[query-times.csv](query-times.csv) includes both warmup and measured timings. [comparison.json](comparison.json) retains full-precision timings, all trial outcomes, SQL/reference hashes, per-query MySQL metadata, pin preparation and measured Parquet stage totals. This file is an unchanged copy of the original final summary.

## Initial failure and legacy pin recovery

The first attempt completed CPU and Parquet but failed to start the pin source. Its status remained failed; the final successful pin results came from a separate retry. These outcomes are distinguished in [recovery.json](recovery.json).

The historical pin data directory had already been removed. A new isolated clone was made with reflink from a stopped source instance. To let the old binary read the clone, the test restored the old system-variable name (`gpu_snapshot` to `sirius_snapshot`), legacy flags of 66, and the metadata compression alias (`zstd` to `zstd_1.3.8`). Affected system-table row counts are preserved in the recovery record. Startup rejection and unsupported table-query failures before recovery are also recorded.

These adjustments were confined to the isolated test clone. Business table data was not rewritten, and the original source was not modified. This is historical test recovery, not a supported product upgrade/downgrade procedure or a reproduction command to run on a live database.

At the recorded final audit, all benchmark source and service processes had exited. Parquet files, statistics caches and the isolated pin-compatible clone were retained. This describes the historical cleanup state, not current process or disk state.

## Related experiments and archive contents

The verified 2026-11-09 native pin total was 28.006818 seconds. This run's 31.306837 seconds was approximately 11.8% slower and did not exactly reproduce the earlier result. See [historical-pin-reference.json](historical-pin-reference.json); the older record used saved CPU references and did not provide fresh CPU timings.

The separate [GPU configuration comparison](../2026-11-13-sf500.md) reported 782.043 seconds after tuning versus 3122.206 seconds before tuning. It is a different run with a different comparison baseline; do not substitute those numbers for this three-path table.

| Artifact | Contents |
| --- | --- |
| [comparison.json](comparison.json) | Original final three-path summary, including 132 trial outcomes and metadata |
| [query-times.csv](query-times.csv) | Warmup and measured timings for all 22 queries and three paths |
| [environment.json](environment.json) | Hardware, phase-specific settings, dataset, topology and qualified software provenance |
| [result-sha256.json](result-sha256.json) | Sizes and hashes of retained raw row-result files; those large files are not included |
| [recovery.json](recovery.json) | Initial pin failure, isolated clone adjustments, successful retry and cleanup status |
| [provenance.json](provenance.json) | Original artifact hashes and any public-copy transformations |
| [sql/](sql/) | The exact 22 SQL files used by all three modes |

This publication retains compact evidence and removes private machine paths, IP addresses and process IDs. Original logs, full row outputs and binaries remain outside the public archive. All figures here are historical observations; publication did not start any benchmark or GPU process.
