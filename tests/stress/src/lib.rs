//! Timed chat penetration soak for PostgreSQL + `pg_koldstore`.
//!
//! Manual / CI harness (not part of the default workspace build). Runs a
//! same-process baseline then a parallel soak with optional feature packs.
//! See `docs/plans/2026-07-19-chat-penetration-stress-design.md`.

#![allow(dead_code)]

#[path = "../../e2e/common/mod.rs"]
pub mod e2e;

pub mod baseline;
pub mod config;
pub mod control;
pub mod metrics;
pub mod packs;
pub mod report;
pub mod scenario;
pub mod schema;
pub mod support;
pub mod watchdog;
pub mod workload;
