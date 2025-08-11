pub use sea_orm_migration::prelude::*;

/// Macro to create migration vector with less boilerplate
macro_rules! migrations {
    ($($migration:ident),* $(,)?) => {
        vec![
            $(Box::new($migration::Migration) as Box<dyn MigrationTrait>,)*
        ]
    };
}

mod m20240101_000001_initial_schema;
mod m20240102_000002_add_validation_functions;
mod m20240103_000003_create_analytics_views;
mod m20240104_000004_create_helper_functions;
mod m20240105_000005_create_test_helper_functions;
mod m20240106_000006_create_coordination_tables;
mod m20240109_000009_add_payload_validation_function;
mod m20240110_000010_add_event_payload_check_constraint;
mod m20250111_000001_schema_alignment;
mod m20250810_000001_create_outbox_table;
mod m20250810_000002_add_constraints_and_archives;
mod m20250810_000004_create_operations_log;
mod m20250810_000006_add_archive_trigger;
mod m20250810_000007_add_recommended_indexes;
mod m20250810_132050_drop_obsolete_artifact_tables;
mod m20250811_000002_add_path_validation_functions;
mod m20250811_230000_standardize_source_material_primary_key;
pub mod schema;

pub struct Migrator;

#[async_trait::async_trait]
impl MigratorTrait for Migrator {
    fn migrations() -> Vec<Box<dyn MigrationTrait>> {
        migrations![
            m20240101_000001_initial_schema,
            m20240102_000002_add_validation_functions,
            m20240103_000003_create_analytics_views,
            m20240104_000004_create_helper_functions,
            m20240105_000005_create_test_helper_functions,
            m20240106_000006_create_coordination_tables,
            m20240109_000009_add_payload_validation_function,
            m20240110_000010_add_event_payload_check_constraint,
            m20250810_000001_create_outbox_table,
            m20250810_000002_add_constraints_and_archives,
            m20250810_000004_create_operations_log,
            m20250810_000006_add_archive_trigger,
            m20250810_000007_add_recommended_indexes,
            m20250810_132050_drop_obsolete_artifact_tables,
            m20250811_000002_add_path_validation_functions,
            m20250811_230000_standardize_source_material_primary_key,
            m20250111_000001_schema_alignment,
        ]
    }
}
