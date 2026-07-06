//! Mirror storage errors.

use thiserror::Error;

/// Mirror storage API result.
pub type MirrorResult<T> = Result<T, MirrorError>;

/// Error emitted by pg-free mirror storage planning.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum MirrorError {
    /// A source table without a primary key cannot have a latest-state mirror.
    #[error("managed tables require a primary key before mirror storage is created")]
    MissingPrimaryKey,
    /// Mirror relation names must remain safe generated identifiers.
    #[error("invalid mirror relation `{0}`")]
    InvalidMirrorName(String),
    /// Primary-key columns in the source catalog should always be non-null.
    #[error("primary-key column `{0}` must be not null")]
    NullablePrimaryKey(String),
    /// Column names supplied to mirror storage must be valid SQL identifiers.
    #[error("invalid mirror column `{0}`")]
    InvalidColumn(String),
}
