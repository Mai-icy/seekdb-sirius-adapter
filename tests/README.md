# Validation entry points

Activate a virtual environment and install `requirements.txt` from the repository root.

## Unit tests without a GPU

```sh
python -m unittest discover -s tests/unit -v
```

Coverage includes native plan to Substrait encoding, column binding, parameters, Decimal precision bounds, anti joins and immutable input statistics caching. These tests do not connect to a database or load Sirius.

## Integration checks

Use isolated test instances. Scripts may create, populate and delete test databases and users. Environment variables and CLI arguments must match the endpoint, TLS configuration and export directory configured when seekdb started. The default CPU unit suite does not run these scripts automatically.

| Entry point | Purpose |
| --- | --- |
| `native_export_integration.py` | Native plan export and encoding against a real seekdb instance |
| `parquet_gpu_integration.py` | Public FFI, Parquet column binding and GPU computation |
| `decimal_anti_gpu_integration.py` | Actual GPU semantics for Decimal and anti joins |
| `query_path_integration.py` | Unix socket, SQL snapshots, transactions and result return |
| `network_query_integration.py` | Mutual TLS, uploads, named snapshots, permissions and lifecycle |
| `two_host_query_integration.py` | SQL round trips across two machines; the service must already be running |
| `decimal_query_path_integration.py` | Decimal edge cases through the SQL path |
| `tpch_plan_audit.py` | TPC-H native plans and Substrait encoding checks |
| `tpch_query_path_integration.py` | Q1–Q22 CPU/GPU result values and MySQL metadata parity |

Use `--help` for scripts with a CLI; inspect connection settings in scripts that read their environment directly. `query_path_integration.py`, scripts reusing it and the TPC-H runner support `SEEKDB_HOST`, `SEEKDB_PORT`, `SEEKDB_USER` and `SEEKDB_PASSWORD`. The TPC-H runner also accepts `--host`, `--port` and `--user`; it reads passwords only from the environment. Remote access can use a dedicated SSH tunnel. Do not run these test scripts against production instances.

Use the entry point below for a single correctness run. For warmup, repeated measurements and summaries, see the [benchmark instructions](../benchmarks/tpch/README.md). Both reuse the same SQL templates and result comparison logic.

After starting the service and loading the test database, for example:

```sh
mkdir -p artifacts
python tests/integration/tpch_query_path_integration.py \
  --port 2881 --database tpch_sf1 --scale-factor 1 \
  --gpu-snapshot validation-v1 --cpu-dop 1 --timeout 900 \
  --output artifacts/tpch-sf1
```

The output directory must not already exist. The runner adjusts the Q11 threshold by scale factor and normalizes tie ordering not specified by the SQL. Decimal values are compared exactly. Record initial preparation versus snapshot reuse, CPU DOP, GPU configuration and the commits of all three projects. These results are not audited TPC-H scores.

Machine-specific logs, certificates, Parquet files, binaries and large historical result sets from the original repository were not migrated. CPU validation for the extraction is recorded in `docs/validation.md`; historical GPU validation does not mean this checkout has rerun GPU acceptance tests.
