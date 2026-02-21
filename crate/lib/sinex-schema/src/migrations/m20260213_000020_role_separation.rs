//! Database role separation with per-table GRANT/REVOKE
//!
//! This migration implements role-based access control (RBAC) for the Sinex database.
//! Three roles are created with distinct permissions:
//!
//! - **sinex_ingestd**: Write events, read schemas, manage source materials
//! - **sinex_gateway**: Full API access, read/write events, manage derived data
//! - **sinex_readonly**: Read-only access for monitoring and auditing
//!
//! ## Architecture
//!
//! ```text
//! sinex_ingestd          sinex_gateway           sinex_readonly
//!   │                      │                        │
//!   ├─ INSERT events       ├─ SELECT/DELETE events ├─ SELECT events
//!   ├─ INSERT schemas      ├─ INSERT/UPDATE blobs  ├─ SELECT blobs
//!   ├─ UPDATE source_mat   ├─ FULL entities mgmt   ├─ SELECT entities
//!   └─ EXECUTE functions   ├─ FULL annotations     └─ SELECT everything
//!                          └─ FULL lifecycle ops
//! ```
//!
//! ## Design Philosophy
//!
//! This follows PostgreSQL principle of least privilege:
//! - Explicit REVOKE ALL before granting specific permissions
//! - Each role gets only what it needs for its operational role
//! - Ingestd is write-only for raw events, gateway orchestrates lifecycle
//! - Readonly role cannot affect any state

use sea_orm_migration::prelude::*;

#[derive(DeriveMigrationName)]
pub(crate) struct Migration;

#[async_trait::async_trait]
impl MigrationTrait for Migration {
    async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        manager
            .get_connection()
            .execute_unprepared(ROLE_SEPARATION_UP)
            .await?;

        Ok(())
    }

    async fn down(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        manager
            .get_connection()
            .execute_unprepared(ROLE_SEPARATION_DOWN)
            .await?;

        Ok(())
    }
}

/// Create roles and grant schema/table permissions.
///
/// This migration:
/// 1. Creates three NOLOGIN roles (cannot be used for interactive login)
/// 2. Grants USAGE on all schemas
/// 3. Explicitly REVOKE ALL then grant specific permissions per table
/// 4. Grants function execution permissions for critical operations
///
/// All operations use IF NOT EXISTS where possible to ensure idempotency.
const ROLE_SEPARATION_UP: &str = r"
-- Create roles (IF NOT EXISTS via DO block)
DO $$ BEGIN
  IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'sinex_ingestd') THEN
    CREATE ROLE sinex_ingestd NOLOGIN;
  END IF;
  IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'sinex_gateway') THEN
    CREATE ROLE sinex_gateway NOLOGIN;
  END IF;
  IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'sinex_readonly') THEN
    CREATE ROLE sinex_readonly NOLOGIN;
  END IF;
END $$;

-- Grant schema USAGE to all roles
GRANT USAGE ON SCHEMA core, raw, sinex_schemas, audit TO sinex_ingestd, sinex_gateway, sinex_readonly;

-- ============================================================
-- core schema tables
-- ============================================================

REVOKE ALL ON core.events FROM sinex_ingestd, sinex_gateway, sinex_readonly;
GRANT SELECT, INSERT ON core.events TO sinex_ingestd;
GRANT SELECT, DELETE ON core.events TO sinex_gateway;
GRANT SELECT ON core.events TO sinex_readonly;

REVOKE ALL ON core.blobs FROM sinex_ingestd, sinex_gateway, sinex_readonly;
GRANT SELECT, INSERT ON core.blobs TO sinex_ingestd;
GRANT SELECT, INSERT ON core.blobs TO sinex_gateway;
GRANT SELECT ON core.blobs TO sinex_readonly;

REVOKE ALL ON core.entities FROM sinex_ingestd, sinex_gateway, sinex_readonly;
GRANT SELECT, INSERT, UPDATE ON core.entities TO sinex_gateway;
GRANT SELECT ON core.entities TO sinex_readonly;

REVOKE ALL ON core.entity_relations FROM sinex_ingestd, sinex_gateway, sinex_readonly;
GRANT SELECT, INSERT, UPDATE, DELETE ON core.entity_relations TO sinex_gateway;
GRANT SELECT ON core.entity_relations TO sinex_readonly;

REVOKE ALL ON core.event_annotations FROM sinex_ingestd, sinex_gateway, sinex_readonly;
GRANT SELECT, INSERT, UPDATE, DELETE ON core.event_annotations TO sinex_gateway;
GRANT SELECT ON core.event_annotations TO sinex_readonly;

REVOKE ALL ON core.tags FROM sinex_ingestd, sinex_gateway, sinex_readonly;
GRANT SELECT, INSERT, UPDATE, DELETE ON core.tags TO sinex_gateway;
GRANT SELECT ON core.tags TO sinex_readonly;

REVOKE ALL ON core.tagged_items FROM sinex_ingestd, sinex_gateway, sinex_readonly;
GRANT SELECT, INSERT, DELETE ON core.tagged_items TO sinex_gateway;
GRANT SELECT ON core.tagged_items TO sinex_readonly;

REVOKE ALL ON core.operations_log FROM sinex_ingestd, sinex_gateway, sinex_readonly;
GRANT SELECT, INSERT, UPDATE ON core.operations_log TO sinex_gateway;
GRANT SELECT ON core.operations_log TO sinex_readonly;

REVOKE ALL ON core.event_tombstones FROM sinex_ingestd, sinex_gateway, sinex_readonly;
GRANT SELECT, INSERT ON core.event_tombstones TO sinex_gateway;
GRANT SELECT ON core.event_tombstones TO sinex_readonly;

REVOKE ALL ON core.node_manifests FROM sinex_ingestd, sinex_gateway, sinex_readonly;
GRANT SELECT, INSERT, UPDATE ON core.node_manifests TO sinex_ingestd;
GRANT SELECT ON core.node_manifests TO sinex_gateway;
GRANT SELECT ON core.node_manifests TO sinex_readonly;

REVOKE ALL ON core.embedding_models FROM sinex_ingestd, sinex_gateway, sinex_readonly;
GRANT SELECT, INSERT ON core.embedding_models TO sinex_gateway;
GRANT SELECT ON core.embedding_models TO sinex_readonly;

REVOKE ALL ON core.embedding_cache FROM sinex_ingestd, sinex_gateway, sinex_readonly;
GRANT SELECT, INSERT, UPDATE ON core.embedding_cache TO sinex_gateway;
GRANT SELECT ON core.embedding_cache TO sinex_readonly;

REVOKE ALL ON core.event_embeddings FROM sinex_ingestd, sinex_gateway, sinex_readonly;
GRANT SELECT, INSERT, DELETE ON core.event_embeddings TO sinex_gateway;
GRANT SELECT ON core.event_embeddings TO sinex_readonly;

REVOKE ALL ON core.event_clusters FROM sinex_ingestd, sinex_gateway, sinex_readonly;
GRANT SELECT, INSERT, UPDATE, DELETE ON core.event_clusters TO sinex_gateway;
GRANT SELECT ON core.event_clusters TO sinex_readonly;

REVOKE ALL ON core.event_cluster_members FROM sinex_ingestd, sinex_gateway, sinex_readonly;
GRANT SELECT, INSERT, DELETE ON core.event_cluster_members TO sinex_gateway;
GRANT SELECT ON core.event_cluster_members TO sinex_readonly;

-- ============================================================
-- raw schema tables
-- ============================================================

REVOKE ALL ON raw.source_material_registry FROM sinex_ingestd, sinex_gateway, sinex_readonly;
GRANT SELECT, INSERT, UPDATE ON raw.source_material_registry TO sinex_ingestd;
GRANT SELECT, INSERT, UPDATE ON raw.source_material_registry TO sinex_gateway;
GRANT SELECT ON raw.source_material_registry TO sinex_readonly;

REVOKE ALL ON raw.temporal_ledger FROM sinex_ingestd, sinex_gateway, sinex_readonly;
GRANT SELECT, INSERT ON raw.temporal_ledger TO sinex_ingestd;
GRANT SELECT ON raw.temporal_ledger TO sinex_gateway;
GRANT SELECT ON raw.temporal_ledger TO sinex_readonly;

-- ============================================================
-- sinex_schemas schema tables
-- ============================================================

REVOKE ALL ON sinex_schemas.event_payload_schemas FROM sinex_ingestd, sinex_gateway, sinex_readonly;
GRANT SELECT, INSERT, UPDATE ON sinex_schemas.event_payload_schemas TO sinex_ingestd;
GRANT SELECT ON sinex_schemas.event_payload_schemas TO sinex_gateway;
GRANT SELECT ON sinex_schemas.event_payload_schemas TO sinex_readonly;

REVOKE ALL ON sinex_schemas.validation_cache FROM sinex_ingestd, sinex_gateway, sinex_readonly;
GRANT SELECT, INSERT, UPDATE ON sinex_schemas.validation_cache TO sinex_ingestd;
GRANT SELECT ON sinex_schemas.validation_cache TO sinex_gateway;
GRANT SELECT ON sinex_schemas.validation_cache TO sinex_readonly;

REVOKE ALL ON sinex_schemas.gitops_schema_sources FROM sinex_ingestd, sinex_gateway, sinex_readonly;
GRANT SELECT ON sinex_schemas.gitops_schema_sources TO sinex_gateway;
GRANT SELECT ON sinex_schemas.gitops_schema_sources TO sinex_readonly;

-- ============================================================
-- audit schema tables
-- ============================================================

REVOKE ALL ON audit.archived_events FROM sinex_ingestd, sinex_gateway, sinex_readonly;
GRANT SELECT, INSERT ON audit.archived_events TO sinex_gateway;
GRANT SELECT ON audit.archived_events TO sinex_readonly;

-- ============================================================
-- Function execution grants
-- ============================================================

GRANT EXECUTE ON FUNCTION core.start_operation TO sinex_gateway;
GRANT EXECUTE ON FUNCTION core.complete_operation TO sinex_gateway;
GRANT EXECUTE ON FUNCTION core.fail_operation TO sinex_gateway;
GRANT EXECUTE ON FUNCTION core.execute_cascade_tombstone TO sinex_gateway;
GRANT EXECUTE ON FUNCTION core.execute_cascade_restore TO sinex_gateway;
GRANT EXECUTE ON FUNCTION core.lifecycle_tier_status TO sinex_gateway, sinex_readonly;
GRANT EXECUTE ON FUNCTION core.jsonb_merge_deep TO sinex_ingestd, sinex_gateway;
";

/// Revoke all grants and drop roles.
///
/// This migration:
/// 1. Revokes all table permissions from all roles in all schemas
/// 2. Revokes schema USAGE
/// 3. Drops the three roles
///
/// Note: Uses CASCADE drop to handle role dependencies.
const ROLE_SEPARATION_DOWN: &str = r"
-- Revoke all grants from all schemas
REVOKE ALL ON ALL TABLES IN SCHEMA core, raw, sinex_schemas, audit FROM sinex_ingestd, sinex_gateway, sinex_readonly;
REVOKE ALL ON ALL FUNCTIONS IN SCHEMA core FROM sinex_ingestd, sinex_gateway, sinex_readonly;
REVOKE USAGE ON SCHEMA core, raw, sinex_schemas, audit FROM sinex_ingestd, sinex_gateway, sinex_readonly;

-- Drop roles
DROP ROLE IF EXISTS sinex_readonly;
DROP ROLE IF EXISTS sinex_gateway;
DROP ROLE IF EXISTS sinex_ingestd;
";
