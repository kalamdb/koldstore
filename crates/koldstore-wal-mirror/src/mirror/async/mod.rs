//! Async mirror helpers (PostgreSQL-free).
//!
//! Owns the `pgoutput` decoder, typed PK identity / bind columns, and
//! apply-batch flush policy. SPI/WAL orchestration stays in `pg_koldstore::mirror`.

pub mod apply_row;
pub mod batch;
pub mod pgoutput;

pub use apply_row::{
    order_column_text, parse_pk_bool, parse_pk_ints, pg_value_text, pk_column_indexes, pk_identity,
    pk_type_oids, primary_key_cells, take_pk_cells_and_order_text, PkBindColumn, PkCell,
    PkIdentity, BOOLOID, INT2OID, INT4OID, INT8OID,
};
pub use batch::{must_flush_before_push, BatchFlushReason, APPLY_BATCH_ROWS};
pub use pgoutput::{
    decode_message, PgOutputColumn, PgOutputDecodeError, PgOutputMessage, PgOutputRelation,
    PgOutputTuple, PgOutputValue,
};
