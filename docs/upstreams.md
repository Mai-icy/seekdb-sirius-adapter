# 上游版本与所有权

本仓库依赖两个上游，不把两个引擎作为源码副本或 submodule 纳入仓库。

## seekdb 功能分支

- 仓库：https://github.com/Mai-icy/seekdb
- 拆分来源工作分支：`feat/sirius-parquet`。
- 来源基线：`a8b56bbc69e669dc2ac277c3ec885b8c6d8abfaa`。
- **兼容功能提交尚未发布。** 上述基线不包含工作区中的集成功能，不能用它单独复现部署。

所需功能包括 `EXPLAIN FORMAT=SIRIUS_JSON` 原生计划导出、`execution_backend` / `gpu_snapshot` 会话变量、原生 Parquet writer、版本 2 查询传输客户端和结果转换。数据库源码、Rust/CXX writer、数据库自身回归仍在 seekdb 仓库维护。首次拆分保留原工作区文件，未删除或替换其他任务的修改。

正式发布前，必须把 seekdb 功能改动提交到 fork 的可获取分支，并将此处更新为实际功能提交。发布记录应同时记录 seekdb、adapter、Sirius 的提交及 IR/传输版本。不要仅用移动中的分支名表达可复现依赖。

## Sirius 上游

- 仓库：https://github.com/sirius-db/sirius
- 既有集成验证所用执行库提交：`40106bf1c79cbbaddf64cba88a1f24287ffc1621`。
- 对应 Substrait reader：`922489d3c8c77140b189ad3847554de141378b04`。
- 调用公开 C++ `sirius::ffi::Context::execute_substrait()`，不依赖私有补丁。

这些是历史验证版本，不表示上游最新 main，也不表示任意 main 都兼容。桥接库必须使用与执行库匹配的公开头文件重新构建。Sirius 内部仍会完成计划绑定和执行计划生成；适配层不会替代这些步骤。

`native/compute_bridge.cpp` 属于适配器，是 Python ctypes 与公开 C++ 接口之间的薄桥接；它不是 Sirius 源码修改，也不是 seekdb Rust/CXX writer 的一部分。

## Arrow 路径

当前发布内容仅包含 Parquet 输入与 Arrow 结果输出。Arrow 输入方向另行维护，等待 Sirius 公开输入接口及执行语义支持，未来接入需单独验证。不得把历史私有补丁作为未修改上游的能力发布。

## 拆分来源

`source-provenance.json` 记录提取文件的来源及 SHA-256。生产运行代码保持原字节不变；测试仅调整仓库路径。原工作区可能继续迭代，这份清单描述的是提取时的快照。
