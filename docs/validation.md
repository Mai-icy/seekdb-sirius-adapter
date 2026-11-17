# Initial extraction validation

The following checks ran in the independent repository directory on 2026-11-13.

| Check | Result |
| --- | --- |
| Native plan encoding unit tests | 11/11 passed |
| Decimal bounds and Parquet statistics cache unit tests | 14/14 passed |
| CLI loading and `--help` | 12 entry points passed |
| Production code SHA-256 against extraction sources | Matched |
| Python syntax and Git diff whitespace checks | Passed |

Local validation used an existing macOS Python 3.14.5 environment with PyArrow 26.0.0, substrait-protobuf 0.78.0, protobuf 6.33.6 and PyMySQL 1.2.3. The command was:

```sh
python -m unittest discover -s tests/unit -v
```

CI runs the same CPU unit tests on Linux / Python 3.12. See the repository's Actions runs for their actual status.

The initial extraction did not rebuild the GPU bridge, start remote GPU queries or rerun SF500. The bridge and service runtime code were copied unchanged from the existing integration worktree. Historical validation does not replace subsequent acceptance testing of the three pinned projects in this independent repository.

## Pinning upstream commits

On the same day, the seekdb features were published as `193ecbf93c940bd9d94cf24740798abda1cb7d35` and linked to unmodified Sirius `40106bf1c79cbbaddf64cba88a1f24287ffc1621`.

- The native upload component was rebuilt and checked locally: pipelining, backpressure, deletion after acknowledgment, cancellation, remote errors, deadlines and callback thread isolation passed.
- Remote Linux validation ran `cargo test --locked --offline -p parquet-export`: 15 tests passed, with 1 performance benchmark ignored by default. Before execution, SHA-256 hashes of the writer source, tests, Cargo manifests and lock file were checked against the committed content and matched.
- The export coordinator's CLI loaded successfully, and source diff whitespace checks passed.

Pinning these dependencies did not change adapter runtime code or Sirius, and did not repeat the full seekdb build, GPU queries or SF500 acceptance testing.
