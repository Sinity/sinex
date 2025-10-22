//! `sinex-core` integration fixtures
//!
//! Consolidates the legacy `tests/integration` suites that focus on repository
//! and schema behaviour provided directly by `sinex-core`.

pub mod checkpoint_consistency_test;
pub mod distributed_locking_test;
pub mod event_ordering_test;
pub mod ingest_service_test;
pub mod pipeline_integration_test;
pub mod provenance_test;
pub mod resource_management_test;
pub mod schema_integration_test;
pub mod single_writer_enforcement_test;
pub mod state_management_test;
pub mod subscription_service_test;
pub mod test_automation_integration_test;
pub mod timestamp_test;
pub mod type_safety_test;
pub mod validation_cache_test;
pub mod work_queue_test;
