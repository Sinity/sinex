pub use sea_orm_migration::prelude::*;

mod m20240101_000001_initial_schema;
mod m20240102_000002_add_validation_functions;
mod m20240103_000003_create_analytics_views;
mod m20240104_000004_create_helper_functions;
mod m20240105_000005_create_test_helper_functions;
mod m20240106_000006_create_coordination_tables;
mod m20240107_000007_create_llm_infrastructure;
mod m20240108_000008_add_schema_content_hash;
mod m20240109_000009_add_payload_validation_function;
mod m20240110_000010_add_event_payload_check_constraint;
mod m20250103_000001_source_material_refactor;
mod m20250810_000001_create_outbox_table;
mod m20250810_000002_add_constraints_and_archives;
mod m20250810_000003_create_sensd_tables;
mod m20250810_000004_create_operations_log;
mod m20250810_000006_add_archive_trigger;
mod m20250810_000007_add_recommended_indexes;
mod m20250810_132050_drop_obsolete_artifact_tables;
pub mod schema;

pub struct Migrator;

#[async_trait::async_trait]
impl MigratorTrait for Migrator {
    fn migrations() -> Vec<Box<dyn MigrationTrait>> {
        vec![
            Box::new(m20240101_000001_initial_schema::Migration),
            Box::new(m20240102_000002_add_validation_functions::Migration),
            Box::new(m20240103_000003_create_analytics_views::Migration),
            Box::new(m20240104_000004_create_helper_functions::Migration),
            Box::new(m20240105_000005_create_test_helper_functions::Migration),
            Box::new(m20240106_000006_create_coordination_tables::Migration),
            Box::new(m20240107_000007_create_llm_infrastructure::Migration),
            Box::new(m20240108_000008_add_schema_content_hash::Migration),
            Box::new(m20240109_000009_add_payload_validation_function::Migration),
            Box::new(m20240110_000010_add_event_payload_check_constraint::Migration),
            Box::new(m20250103_000001_source_material_refactor::Migration),
            Box::new(m20250810_000001_create_outbox_table::Migration),
            Box::new(m20250810_000002_add_constraints_and_archives::Migration),
            Box::new(m20250810_000003_create_sensd_tables::Migration),
            Box::new(m20250810_000004_create_operations_log::Migration),
            Box::new(m20250810_000006_add_archive_trigger::Migration),
            Box::new(m20250810_000007_add_recommended_indexes::Migration),
            Box::new(m20250810_132050_drop_obsolete_artifact_tables::Migration),
        ]
    }
}
