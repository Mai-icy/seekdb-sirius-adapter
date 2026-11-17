# Historical benchmark results

These are archives of completed experiments, not new executions of the current checkout. Each report states its timing method, correctness checks, hardware and limitations.

| Experiment | Main result | Interpretation |
| --- | --- | --- |
| [SF500 CPU / legacy native pin / Parquet](2026-11-13-sf500-three-paths/README.md) | CPU 192.805 s; legacy native pin 31.307 s; Parquet 791.319 s; all three paths passed Q1–Q22 | Second of two adjacent executions per query; preparation excluded; different engine stacks and lifecycles |
| [SF500 Parquet GPU configuration comparison](2026-11-13-sf500.md) | GPU total decreased from 3122.206 s to 782.043 s | Different GPU configurations and statistics processing; not a CPU/GPU speedup |

The three-path archive includes machine and phase-specific resource settings, exact SQL, warmup and measured times, result hashes and the legacy pin failure/recovery record. Legacy native pin used an older private-interface stack; it is not a feature exposed by this repository's current public-FFI path.

For new CPU/Parquet measurements, use the [benchmark runner](../README.md). Do not merge timings from these different historical experiments into a single suite.
