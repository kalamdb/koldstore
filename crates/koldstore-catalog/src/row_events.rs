//! Row-event catalog model.

use serde::{Deserialize, Serialize};

use koldstore_core::RowOperation;

/// Serializable catalog row event.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CatalogRowEvent {
    pub table_oid: u32,
    pub scope_key: Option<String>,
    pub pk_hash: String,
    pub op: RowOperation,
    pub seq: i64,
    pub commit_seq: i64,
    pub deleted: bool,
}
