# Strict Mirror Capture (Historical)

> **Removed.** Strict / trigger capture is no longer supported (#71).
> KoldStore uses WAL-only committed-WAL capture exclusively.
> See [mirror-capture-modes.md](mirror-capture-modes.md).

This document is retained only as a historical reference for development
installations that still have strict-managed tables. Those tables must be
explicitly unmanaged and re-managed; do not reinterpret trigger-generated
historical `seq` values as commit-ordered `changes_since` cursors.

## Former behavior

Strict mode installed `AFTER ... FOR EACH STATEMENT` INSERT/UPDATE/DELETE
triggers so heap mutation, mirror mutation, and row-counter deltas shared the
application transaction. That path allocated `seq` before PostgreSQL commit and
could not serve as a commit-ordered changes cursor without a second mechanism.

WAL-only capture moves mirror maintenance out of the application transaction
and allocates authoritative `seq` only in the serialized WAL applier.
