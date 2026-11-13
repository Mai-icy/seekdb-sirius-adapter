# 上游版本与所有权

本仓库通过 Git submodule 依赖两个上游。`.gitmodules` 记录地址，Git tree 中的 gitlink 固定实际提交；引擎源码和修改历史留在各自仓库。

## seekdb 功能分支

- 仓库：https://github.com/Mai-icy/seekdb
- submodule：`deps/seekdb`。
- 已发布功能分支：[`feat/sirius-parquet`](https://github.com/Mai-icy/seekdb/tree/feat/sirius-parquet)。
- 固定功能提交：`193ecbf93c940bd9d94cf24740798abda1cb7d35`。
- 独立存储修复：`2d0caef9`，为上述提交的父提交，修正宏块迭代器读取新条目后再选择载荷格式。
- 功能开发基线：`a8b56bbc69e669dc2ac277c3ec885b8c6d8abfaa`，不应单独用于部署。

所需功能包括 `EXPLAIN FORMAT=SIRIUS_JSON` 原生计划导出、`execution_backend` / `gpu_snapshot` 会话变量、原生 Parquet writer、版本 2 查询传输客户端和结果转换。数据库源码、Rust/CXX writer、数据库自身回归仍在 seekdb 仓库维护。首次拆分保留原工作区文件，未删除或替换其他任务的修改。

发布记录应同时记录 seekdb、adapter、Sirius 的提交及 IR/传输版本。`.gitmodules` 的 seekdb `branch` 字段只提示开发更新来源；普通初始化和 checkout 仍使用固定提交。

## Sirius 上游

- 仓库：https://github.com/sirius-db/sirius
- submodule：`deps/sirius`。
- 既有集成验证所用执行库提交：`40106bf1c79cbbaddf64cba88a1f24287ffc1621`。
- 对应 Substrait reader：`922489d3c8c77140b189ad3847554de141378b04`。
- 调用公开 C++ `sirius::ffi::Context::execute_substrait()`，不依赖私有补丁。

这些是历史验证版本，不表示上游最新 main，也不表示任意 main 都兼容。桥接库必须使用与执行库匹配的公开头文件重新构建。Sirius 内部仍会完成计划绑定和执行计划生成；适配层不会替代这些步骤。

`native/compute_bridge.cpp` 属于适配器，是 Python ctypes 与公开 C++ 接口之间的薄桥接；它不是 Sirius 源码修改，也不是 seekdb Rust/CXX writer 的一部分。

## 初始化与更新

```sh
git submodule update --init --recursive
git submodule status
```

submodule 默认处于 detached HEAD，这是固定版本的正常状态。seekdb 功能先在 fork 分支开发、验证并推送，再由适配仓库显式更新指针，例如：

```sh
git -C deps/seekdb fetch origin feat/sirius-parquet
git -C deps/seekdb checkout --detach <已验证的功能提交>
git add deps/seekdb
git commit -m 'Update seekdb integration dependency'
```

Sirius 同理，更新前需重新核对公开 FFI、reader 和执行语义，并执行配套集成验证。只提交上游指针变化，不在 Sirius submodule 中追加私有补丁。修改指针时同步更新 README 和本文记录的版本；不要对部署 checkout 使用 `git submodule update --remote`。

## Arrow 路径

当前发布内容仅包含 Parquet 输入与 Arrow 结果输出。Arrow 输入方向另行维护，等待 Sirius 公开输入接口及执行语义支持，未来接入需单独验证。不得把历史私有补丁作为未修改上游的能力发布。

## 拆分来源

`source-provenance.json` 记录首次提取文件的来源及 SHA-256。生产运行代码保持原字节不变；测试仅调整仓库路径。清单中的“未提交”状态是首次拆分时的历史事实，当前 seekdb 依赖已经发布，以 submodule 指针为准。
