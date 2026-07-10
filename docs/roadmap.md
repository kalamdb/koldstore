# Roadmap

KoldStore 0.1 focuses on reliable hot/cold table management, sequence-ordered
flushes, and correct `KoldMergeScan` reads. The following features are deferred
until after that baseline is stable.

## Storage layout and pruning

- Operator-configurable `pruning_columns` and `bloom_filter_columns`.
- Segment compaction and small-file combining.
- Size-aware segment writing based on `target_file_size_mb`.
- Configurable `flush_order_by`; flush selection is always ordered by mirror
  `seq` today.

## Table management and flush policy

- `koldstore.alter_table` for changing managed-table settings after
  registration.
- Time- or age-based flush triggers such as `max_flush_interval`.
- Background scheduling, richer retry controls, and operational policy tuning.

## Query execution

- Remaining streaming `KoldMergeScan` polish, including bounded-memory
  execution, rescans, and broader planner integration where needed.
- User-scoped cold-segment loading and parallel custom-scan execution.
- Additional predicate, projection, and ordering pushdown.

## Other post-0.1 work

- Segment lifecycle tooling, validation, and repair automation.
- Export/import and coordinated PostgreSQL/object-storage backup workflows.
- Public change-cursor and explicit cold-row DML APIs.
- Broader schema evolution and primary-key change support.
- Production hardening, observability, and performance tuning.
