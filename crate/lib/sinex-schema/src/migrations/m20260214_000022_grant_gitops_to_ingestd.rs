//! Grant gitops_schema_sources permissions to ingestd and gateway roles
//!
//! This migration updates the RBAC grants for `sinex_schemas.gitops_schema_sources`:
//! - **sinex_ingestd**: SELECT, UPDATE (reads sources, updates sync state)
//! - **sinex_gateway**: SELECT, INSERT, DELETE (management API)
//!
//! The previous migration (000020) granted only SELECT to gateway and readonly.
//! Now that the gitops sync service is implemented, ingestd needs read+update
//! access and gateway needs full CRUD for the management API.

use sea_orm_migration::prelude::*;

#[derive(DeriveMigrationName)]
pub(crate) struct Migration;

#[async_trait::async_trait]
impl MigrationTrait for Migration {
    async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        manager
            .get_connection()
            .execute_unprepared(GITOPS_GRANTS_UP)
            .await?;

        Ok(())
    }

    async fn down(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        manager
            .get_connection()
            .execute_unprepared(GITOPS_GRANTS_DOWN)
            .await?;

        Ok(())
    }
}

const GITOPS_GRANTS_UP: &str = r"
-- Update gitops_schema_sources grants for sync service and management API
REVOKE ALL ON sinex_schemas.gitops_schema_sources FROM sinex_ingestd, sinex_gateway, sinex_readonly;
GRANT SELECT, UPDATE ON sinex_schemas.gitops_schema_sources TO sinex_ingestd;
GRANT SELECT, INSERT, DELETE ON sinex_schemas.gitops_schema_sources TO sinex_gateway;
GRANT SELECT ON sinex_schemas.gitops_schema_sources TO sinex_readonly;
";

const GITOPS_GRANTS_DOWN: &str = r"
-- Revert to pre-sync grants (SELECT-only for gateway and readonly)
REVOKE ALL ON sinex_schemas.gitops_schema_sources FROM sinex_ingestd, sinex_gateway, sinex_readonly;
GRANT SELECT ON sinex_schemas.gitops_schema_sources TO sinex_gateway;
GRANT SELECT ON sinex_schemas.gitops_schema_sources TO sinex_readonly;
";
