//! KoldStore Sort Key V1 — order-preserving binary bounds for cold segment indexes.
//!
//! Wraps [`storekey`] behind a pinned codec version so flush writers and query
//! planners share one encoding path. PostgreSQL compares the resulting `bytea`
//! values with ordinary B-tree operators.
//!
//! Must not depend on `pgrx` or any other `koldstore-*` crate.

mod encode;
mod error;
mod types;

pub use encode::{decode_sort_key, encode_sort_key, encode_sort_key_json};
pub use error::SortKeyError;
pub use types::{
    SortKeyType, SortKeyValue, CODEC_VERSION, PG_EPOCH_DAYS_FROM_UNIX, PG_EPOCH_MICROS_FROM_UNIX,
};
