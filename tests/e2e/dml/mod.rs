//! DML and mirror-capture E2E category.

mod aaa_changes_since_latency_diagnostics;
mod async_change_log_mirror;
mod async_mirror_worker;
mod change_feed;
mod change_log_mirror;
mod changes_since_latency;
mod cold_dml_matrix;
mod persistent_wal_applier;
mod pgoutput_old_row_cow;
mod wal_only_seq_cursor;
