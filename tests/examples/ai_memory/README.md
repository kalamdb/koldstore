# AI memory example

Workspace-scoped agent session history with large prompt/response payloads.

## What the test covers

- Parallel writers across many workspaces
- Multi-wave batch flushing and force flush
- Parquet file size bounds + manifest.json verification
- Indexes on session and user timelines
- Concurrent hot UPDATE/DELETE
- Compliance delete: cold flush → rematerialize hot → DELETE → flush tombstone (cold Parquet remains)
- Cold audit window reads via `KoldMergeScan`
