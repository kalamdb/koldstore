//! Object storage client trait and `object_store`-backed implementation.

mod api;
mod error;
mod keys;
mod map_error;
mod object_store_client;

pub use api::{PutOutcome, PutPrecondition, StorageClient};
pub use error::{StorageClientError, StorageResult};
pub use object_store_client::ObjectStoreClient;
