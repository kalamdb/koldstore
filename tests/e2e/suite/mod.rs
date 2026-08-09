//! Cross-cutting lifecycle / contract E2E category.

mod async_load_soak;
mod cms_wordpress_journey;
mod cms_wordpress_load;
mod endurance;
mod extension_lifecycle;
mod failure_injection;
mod first_time_user_journey;
mod flush_memory_spike;
mod full_lifecycle;
mod jobs_and_recovery;
mod memory_leak;
mod multi_database_stress;
mod query_cancel;
mod quickstart_matrix;
mod schema_evolution;
mod snowflake_concurrency;
mod tiered_coverage;
