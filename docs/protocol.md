# Query and data protocol

The current native IR version is 1 and the query transport version is 2. Control, input and result messages share one connection: mutual TLS across machines, or a same-UID Unix socket on one machine.

Frames use `type:u8 | payload_length:u32 big-endian | payload`, with a maximum size of 16 MiB per frame. Strings are UTF-8 bytes prefixed by a big-endian u32 length. A result length of `0xffffffff` represents NULL.

| Message | Direction | Meaning |
| --- | --- | --- |
| Q | seekdb → adapter | Version, timeout, native plan JSON, expected result types, snapshot name, source identity, read version and table catalog |
| I | adapter → seekdb | Request a database, table and columns over the original connection |
| B | seekdb → adapter | Begin uploading a complete Parquet file |
| D | seekdb → adapter | File bytes, at most 1 MiB per chunk |
| E | seekdb → adapter | End of file and SHA-256 checksum information |
| A | adapter → seekdb | Acknowledge after schema and checksum validation and file synchronization; the source may delete the fragment |
| F | seekdb → adapter | All fragments for the current table have been uploaded |
| M | adapter → seekdb | Result column count; the source retains the original SQL's MySQL metadata |
| R | adapter → seekdb | Row batch with length-delimited values, at most 8,192 rows and approximately 8 MiB |
| Z | adapter → seekdb | Successful completion after releasing result resources and the snapshot lease; the Context may be reused |
| X | adapter → seekdb | Error text indicating query failure |

For exact field encodings, refer to `adapter/query_service.py` and the matching seekdb transport implementation. This table describes the interaction; it is not a complete specification for an independent protocol implementation.

The service does not open a second MySQL connection to read source tables. seekdb exports inputs within the original query's read snapshot and uploads file contents rather than source-side paths. Files enter the input manifest only after completion and validation. Named snapshots are published atomically when complete; partial uploads never become visible versions.

Named snapshots are isolated by client certificate identity, source instance, user, database and version name. Changes to table IDs or schema versions reject an old version; subsequent DML does not refresh a published version. Queries hold locks while using a snapshot, and deletion requires an exclusive lock. Insufficient capacity returns an error rather than silently evicting and rebuilding a version.

Cross-machine results carry UTF-8 values under the agreed type contract and a separate NULL marker, not Arrow IPC or Arrow pointers. Arrow C Stream is used only for in-process result transfer between Python and Sirius.

Context reuse does not change the wire version or frame format. Each query still uses its own connection. Internally, a private Unix packet socket passes accepted connection descriptors to persistent workers. Each Context executes serially, with its previous result released before the next query. Any failed request, disconnect or deadline retires the worker. `--no-context-reuse` retains per-query Context lifetimes.
