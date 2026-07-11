//! Versioned schema access owned by the catalog crate.
//!
//! Stub for feature `003-column-id-lifecycle` (US1). Will provide
//! `active_schema` / `schema_at` / `next_column_id` allocation helpers so
//! migrate, flush, and the extension call one catalog API for schema versions
//! (with `koldstore-schema` remaining a type/evolution leaf only).
//!
//! No runtime behavior yet — Phase 1 setup stub only.

/// Placeholder so the module compiles and stays referenced until US1 lands.
#[derive(Debug, Default, Clone, Copy)]
#[allow(dead_code)] // Phase 1 stub; wired in US1
pub struct SchemaVersionsStub;
