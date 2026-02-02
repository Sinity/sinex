//! Add trigram indexes for entity name search.

use crate::schema::Entities;
use sea_orm_migration::prelude::*;

#[derive(DeriveMigrationName)]
pub(crate) struct Migration;

#[async_trait::async_trait]
impl MigrationTrait for Migration {
    async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        manager
            .get_connection()
            .execute_unprepared(r#"CREATE EXTENSION IF NOT EXISTS "pg_trgm";"#)
            .await?;

        for index_sql in Entities::create_trigram_indexes_sql() {
            manager
                .get_connection()
                .execute_unprepared(&index_sql)
                .await?;
        }

        Ok(())
    }

    async fn down(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        for index in ["ix_entities_name_trgm", "ix_entities_canonical_name_trgm"] {
            let sql = format!("DROP INDEX IF EXISTS core.{index}");
            manager.get_connection().execute_unprepared(&sql).await?;
        }
        Ok(())
    }
}
