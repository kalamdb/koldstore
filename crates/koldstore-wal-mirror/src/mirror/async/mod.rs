//! Async mirror helpers (PostgreSQL-free).
//!
//! Owns the `pgoutput` decoder, tuple→JSON helpers, and apply-batch flush
//! policy. SPI/WAL orchestration stays in `pg_koldstore::mirror`.

pub mod apply_row;
pub mod batch;
pub mod pgoutput;

pub use apply_row::{
    parse_pk_bool, parse_pk_ints, pg_value_json, pg_value_text, pk_identity, primary_key_json,
};
pub use batch::{must_flush_before_push, BatchFlushReason, APPLY_BATCH_ROWS};
pub use pgoutput::{
    decode_message, PgOutputColumn, PgOutputDecodeError, PgOutputMessage, PgOutputRelation,
    PgOutputTuple, PgOutputValue,
};
