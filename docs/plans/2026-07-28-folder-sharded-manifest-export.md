# Folder-Sharded Object Manifest Export

**Goal:** Object-store export is folder-sharded only: thin root + per-folder shards. No monolithic root segment list; Postgres remains query authority.

**Status:** Implemented. Legacy monolith load/write paths are rejected. Root
references include shard content hashes; unchanged completed folders are not
rewritten.
