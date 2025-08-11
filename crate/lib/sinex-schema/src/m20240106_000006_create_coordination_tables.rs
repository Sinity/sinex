use sea_orm_migration::prelude::*;

#[derive(DeriveMigrationName)]
pub struct Migration;

#[async_trait::async_trait]
impl MigrationTrait for Migration {
    async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        // Table for tracking all satellite instances
        manager
            .create_table(
                Table::create()
                    .table(Alias::new("satellite_instances"))
                    .if_not_exists()
                    .col(
                        ColumnDef::new(Alias::new("instance_id"))
                            .uuid()
                            .not_null()
                            .primary_key(),
                    )
                    .col(ColumnDef::new(Alias::new("service_name")).text().not_null())
                    .col(ColumnDef::new(Alias::new("version")).text().not_null())
                    .col(
                        ColumnDef::new(Alias::new("start_time"))
                            .timestamp_with_time_zone()
                            .not_null(),
                    )
                    .col(
                        ColumnDef::new(Alias::new("last_heartbeat"))
                            .timestamp_with_time_zone()
                            .not_null(),
                    )
                    .col(ColumnDef::new(Alias::new("host_name")).text().not_null())
                    .col(
                        ColumnDef::new(Alias::new("metadata"))
                            .json()
                            .not_null()
                            .default("{}"),
                    )
                    .col(
                        ColumnDef::new(Alias::new("created_at"))
                            .timestamp_with_time_zone()
                            .not_null()
                            .default("NOW()"),
                    )
                    .col(
                        ColumnDef::new(Alias::new("updated_at"))
                            .timestamp_with_time_zone()
                            .not_null()
                            .default("NOW()"),
                    )
                    .to_owned(),
            )
            .await?;

        // Table for inter-satellite signaling
        manager
            .create_table(
                Table::create()
                    .table(Alias::new("satellite_signals"))
                    .if_not_exists()
                    .col(
                        ColumnDef::new(Alias::new("id"))
                            .integer()
                            .not_null()
                            .auto_increment()
                            .primary_key(),
                    )
                    .col(
                        ColumnDef::new(Alias::new("target_instance"))
                            .text()
                            .not_null(),
                    )
                    .col(ColumnDef::new(Alias::new("signal_type")).text().not_null())
                    .col(ColumnDef::new(Alias::new("message")).text())
                    .col(
                        ColumnDef::new(Alias::new("payload"))
                            .json()
                            .not_null()
                            .default("{}"),
                    )
                    .col(
                        ColumnDef::new(Alias::new("created_at"))
                            .timestamp_with_time_zone()
                            .not_null()
                            .default("NOW()"),
                    )
                    .col(ColumnDef::new(Alias::new("processed_at")).timestamp_with_time_zone())
                    .col(ColumnDef::new(Alias::new("processed_by")).uuid())
                    .foreign_key(
                        ForeignKey::create()
                            .name("fk_processed_by_instance")
                            .from(Alias::new("satellite_signals"), Alias::new("processed_by"))
                            .to(Alias::new("satellite_instances"), Alias::new("instance_id"))
                            .on_delete(ForeignKeyAction::SetNull),
                    )
                    .to_owned(),
            )
            .await?;

        // Table for tracking current service leadership
        manager
            .create_table(
                Table::create()
                    .table(Alias::new("service_leadership"))
                    .if_not_exists()
                    .col(
                        ColumnDef::new(Alias::new("service_name"))
                            .text()
                            .not_null()
                            .primary_key(),
                    )
                    .col(ColumnDef::new(Alias::new("instance_id")).uuid().not_null())
                    .col(
                        ColumnDef::new(Alias::new("acquired_at"))
                            .timestamp_with_time_zone()
                            .not_null(),
                    )
                    .col(
                        ColumnDef::new(Alias::new("last_heartbeat"))
                            .timestamp_with_time_zone()
                            .not_null(),
                    )
                    .col(ColumnDef::new(Alias::new("version")).text().not_null())
                    .col(
                        ColumnDef::new(Alias::new("metadata"))
                            .json()
                            .not_null()
                            .default("{}"),
                    )
                    .foreign_key(
                        ForeignKey::create()
                            .name("fk_leader_instance")
                            .from(Alias::new("service_leadership"), Alias::new("instance_id"))
                            .to(Alias::new("satellite_instances"), Alias::new("instance_id")),
                    )
                    .to_owned(),
            )
            .await?;

        // Create indexes
        // Note: SeaQuery doesn't support partial indexes directly, so we'll create it with raw SQL
        manager
            .get_connection()
            .execute_unprepared(
                r#"
                CREATE INDEX IF NOT EXISTS idx_satellite_signals_target_unprocessed 
                    ON satellite_signals(target_instance, created_at) 
                    WHERE processed_at IS NULL;
                "#,
            )
            .await?;

        manager
            .create_index(
                Index::create()
                    .table(Alias::new("satellite_instances"))
                    .name("idx_satellite_instances_service_version")
                    .col(Alias::new("service_name"))
                    .col(Alias::new("version"))
                    .col(Alias::new("start_time"))
                    .to_owned(),
            )
            .await?;

        manager
            .create_index(
                Index::create()
                    .table(Alias::new("service_leadership"))
                    .name("idx_service_leadership_heartbeat")
                    .col(Alias::new("last_heartbeat"))
                    .to_owned(),
            )
            .await?;

        // Move tables to core schema
        manager
            .get_connection()
            .execute_unprepared(
                r#"
                ALTER TABLE satellite_instances SET SCHEMA core;
                ALTER TABLE satellite_signals SET SCHEMA core;
                ALTER TABLE service_leadership SET SCHEMA core;
                "#,
            )
            .await?;

        // Add check constraint for signal types
        manager
            .get_connection()
            .execute_unprepared(
                r#"
                ALTER TABLE core.satellite_signals
                ADD CONSTRAINT check_signal_type 
                CHECK (signal_type IN ('handoff_request', 'leader_failure', 'handoff_ready', 'shutdown', 'restart'));
                "#
            )
            .await?;

        // Create cleanup functions
        manager
            .get_connection()
            .execute_unprepared(
                r#"
                -- Function to cleanup old satellite instances (older than 24 hours)
                CREATE OR REPLACE FUNCTION core.cleanup_old_satellite_instances()
                RETURNS INTEGER AS $$
                DECLARE
                    deleted_count INTEGER;
                BEGIN
                    DELETE FROM core.satellite_instances 
                    WHERE last_heartbeat < NOW() - INTERVAL '24 hours';
                    
                    GET DIAGNOSTICS deleted_count = ROW_COUNT;
                    RETURN deleted_count;
                END;
                $$ LANGUAGE plpgsql;

                -- Function to cleanup processed signals (older than 1 hour)
                CREATE OR REPLACE FUNCTION core.cleanup_processed_signals()
                RETURNS INTEGER AS $$
                DECLARE
                    deleted_count INTEGER;
                BEGIN
                    DELETE FROM core.satellite_signals 
                    WHERE processed_at IS NOT NULL 
                      AND processed_at < NOW() - INTERVAL '1 hour';
                    
                    GET DIAGNOSTICS deleted_count = ROW_COUNT;
                    RETURN deleted_count;
                END;
                $$ LANGUAGE plpgsql;
                "#,
            )
            .await?;

        Ok(())
    }

    async fn down(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        // Drop functions
        manager
            .get_connection()
            .execute_unprepared(
                r#"
                DROP FUNCTION IF EXISTS core.cleanup_old_satellite_instances();
                DROP FUNCTION IF EXISTS core.cleanup_processed_signals();
                "#,
            )
            .await?;

        // Drop tables in correct order
        manager
            .drop_table(
                Table::drop()
                    .table(Alias::new("core.service_leadership"))
                    .to_owned(),
            )
            .await?;
        manager
            .drop_table(
                Table::drop()
                    .table(Alias::new("core.satellite_signals"))
                    .to_owned(),
            )
            .await?;
        manager
            .drop_table(
                Table::drop()
                    .table(Alias::new("core.satellite_instances"))
                    .to_owned(),
            )
            .await?;

        Ok(())
    }
}
