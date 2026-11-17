# Upstream versions and ownership

This repository depends on two upstreams through Git submodules. `.gitmodules` records their URLs, while gitlinks in the Git tree pin the actual commits. Engine source code and history remain in their respective repositories.

## seekdb feature branch

- Repository: https://github.com/Mai-icy/seekdb
- Submodule: `deps/seekdb`.
- Published feature branch: [`feat/sirius-parquet`](https://github.com/Mai-icy/seekdb/tree/feat/sirius-parquet).
- Pinned feature commit: `193ecbf93c940bd9d94cf24740798abda1cb7d35`.
- Separate storage fix: `2d0caef9`, the parent of the feature commit. It fixes macro-block iteration by selecting the payload format after reading the next item's type.
- Development baseline: `a8b56bbc69e669dc2ac277c3ec885b8c6d8abfaa`. Do not deploy this baseline alone.

Required features include native plan export through `EXPLAIN FORMAT=SIRIUS_JSON`, the `execution_backend` / `gpu_snapshot` session variables, the native Parquet writer, the version 2 query transport client and result conversion. Database source, the Rust/CXX writer and database regression coverage remain in the seekdb repository. The initial extraction preserved the original worktree files without deleting or replacing changes from other tasks.

Release records should identify the seekdb, adapter and Sirius commits together with IR and transport versions. The seekdb `branch` field in `.gitmodules` indicates the development update source; normal initialization and checkout still use the pinned commit.

## Sirius upstream

- Repository: https://github.com/sirius-db/sirius
- Submodule: `deps/sirius`.
- Execution library commit used in prior integration validation: `40106bf1c79cbbaddf64cba88a1f24287ffc1621`.
- Matching Substrait reader: `922489d3c8c77140b189ad3847554de141378b04`.
- Uses the public C++ `sirius::ffi::Context::execute_substrait()` interface without private patches.

These are previously validated versions, not a claim about the latest upstream main or compatibility with arbitrary main revisions. Rebuild the bridge using public headers matching the execution library. Sirius still performs plan binding and execution-plan generation internally; the adapter does not replace those steps.

`native/compute_bridge.cpp` belongs to the adapter. It is a thin bridge between Python ctypes and the public C++ interface, not a Sirius source modification or part of seekdb's Rust/CXX writer.

## Initialization and updates

```sh
git submodule update --init --recursive
git submodule status
```

A detached HEAD is normal for a submodule pinned to a specific version. Develop, validate and push seekdb features on the fork's branch first, then explicitly update the adapter repository's pointer, for example:

```sh
git -C deps/seekdb fetch origin feat/sirius-parquet
git -C deps/seekdb checkout --detach <validated-feature-commit>
git add deps/seekdb
git commit -m 'Update seekdb integration dependency'
```

Use the same process for Sirius. Before updating, check the public FFI, reader and execution semantics, and run the matching integration validation. Commit upstream pointer updates without adding private patches inside the Sirius submodule. Update the versions in the README and this document when changing pointers. Do not use `git submodule update --remote` on a deployment checkout.

## Arrow path

The current release contains Parquet input and Arrow result output only. Arrow input is maintained separately pending public Sirius input interfaces and execution semantics; future integration requires separate validation. Do not present historical private patches as capabilities of unmodified upstream Sirius.

## Extraction provenance

`source-provenance.json` records the initial extraction sources and their SHA-256 hashes. Production runtime files were copied byte for byte; test changes only relocated repository paths. The manifest's uncommitted status describes the historical state at extraction. The current seekdb dependency has been published; use the submodule pointer for deployment.
