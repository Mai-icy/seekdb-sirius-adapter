# 查询与数据协议

当前原生 IR 版本为 1，查询传输版本为 2。所有控制、输入及结果消息使用同一个连接：跨机器为双向认证 TLS，同机可用同 UID Unix socket。

帧结构是 `type:u8 | payload_length:u32 big-endian | payload`，单帧最多 16 MiB。字符串为 UTF-8 字节，前置大端 u32 长度。结果 NULL 用长度 `0xffffffff` 表示。

| 消息 | 方向 | 含义 |
| --- | --- | --- |
| Q | seekdb → adapter | 版本、超时、原生计划 JSON、预期结果类型、快照名、源身份、读取版本、表目录 |
| I | adapter → seekdb | 请求数据库、表及列，在原连接上反向请求输入 |
| B | seekdb → adapter | 开始一个完整 Parquet 文件的上传 |
| D | seekdb → adapter | 文件字节，每块最多 1 MiB |
| E | seekdb → adapter | 文件结束及 SHA-256 校验信息 |
| A | adapter → seekdb | 校验 schema、摘要并同步文件后的确认；源端可以删除该分片 |
| F | seekdb → adapter | 当前表的所有分片上传完毕 |
| M | adapter → seekdb | 结果列数，源端保留原 SQL 的 MySQL 元数据 |
| R | adapter → seekdb | 行批次及长度分隔的值，最多 8,192 行、约 8 MiB |
| Z | adapter → seekdb | 成功结束，已释放本次 Context 和结果资源 |
| X | adapter → seekdb | 错误文本，查询失败 |

精确字段编码以 `adapter/query_service.py` 和匹配的 seekdb 传输实现为准；表格用于描述交互，不是独立实现协议的完整规范。

服务不通过第二条 MySQL 连接读取源表。seekdb 在原查询读取快照内导出输入，上传文件内容而不是源端路径。文件完整并校验通过后才能纳入输入清单；命名快照完整后原子发布，部分上传不会成为可见版本。

命名快照按客户端证书身份、源实例、用户、数据库及版本名隔离。表 ID 和 schema 版本变化会拒绝旧版本；后续 DML 不刷新已经发布的版本。使用期间持有锁，删除需要排他锁；容量不足返回错误，不静默淘汰后重建。

跨机器结果是类型约定下的 UTF-8 值及独立 NULL 标记，不是 Arrow IPC 或 Arrow 指针。Arrow C Stream 只用于 Python 与 Sirius 的进程内结果交接。
