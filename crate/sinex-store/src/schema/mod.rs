//! The PostgreSQL schema: table declarations, bootstrap of an empty database,
//! checks of an installed one against its recorded catalog, and ordered upgrades.
/// Builds a `Column`; defined before the submodules so they can all use it.
macro_rules! col {
    ($name:literal, $ty:literal) => {
        Column {
            name: $name,
            ty: $ty,
            nullable: false,
            default: None,
            generated: None,
        }
    };
    ($name:literal, $ty:literal, null) => {
        Column {
            nullable: true,
            ..col!($name, $ty)
        }
    };
    ($name:literal, $ty:literal, default $expr:literal) => {
        Column {
            default: Some($expr),
            ..col!($name, $ty)
        }
    };
    ($name:literal, $ty:literal, generated $expr:literal) => {
        Column {
            nullable: true,
            generated: Some($expr),
            ..col!($name, $ty)
        }
    };
}

mod catalog;
mod declarations;
mod storage;
mod tables;
mod upgrade;

use catalog::capture_catalog;
pub(crate) use catalog::inspect;
use declarations::{configure_events, declarations, fingerprint, first_release_shell_payload};
use upgrade::apply_change_entry;
pub use upgrade::{apply_changes, schema_changes};

use crate::{Error, Result};
use sqlx::{Connection, Executor, PgConnection};

pub const WRITER_ROLE: &str = "sinex_writer";
pub const READER_ROLE: &str = "sinex_reader";
pub const READ_MODEL_ROLE: &str = "sinex_read_model";

pub const TIMESCALEDB_VERSION: &str = "2.27.2";
pub use sinex_model::events::CAPTURED_FUTURE_SKEW_SECONDS;

#[derive(Debug, Clone, serde::Serialize)]
pub struct SchemaStatus {
    pub version: i32,
    pub fingerprint: String,
    pub table_count: usize,
    pub pending_changes: Vec<SchemaChange>,
    pub checked_changes: Vec<SchemaChangeCheck>,
}
#[derive(Debug, Clone, serde::Serialize)]
pub struct SchemaChangeCheck {
    pub number: i32,
    pub outcome: &'static str,
}

#[derive(Debug, Clone, serde::Serialize)]
pub struct SchemaChange {
    pub number: i32,
    pub description: &'static str,
}

/// Creates the schema in an empty database; an existing schema is only checked, never
/// reset. The `sinex_writer` and `sinex_reader` LOGIN roles must already exist.
pub async fn bootstrap(admin_url: &str) -> Result<SchemaStatus> {
    let mut conn = PgConnection::connect(admin_url).await?;
    require_pg18(&mut conn).await?;
    let mut tx = conn.begin().await?;
    sqlx::query("SELECT pg_advisory_xact_lock(73018924011)")
        .execute(&mut *tx)
        .await?;
    let count: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM pg_namespace WHERE nspname IN ('core','raw','sinex_schemas','reflection','audit','derivation')",
    )
    .fetch_one(&mut *tx)
    .await?;
    let mut checked_changes = Vec::new();
    if count == 0 {
        // On an empty database each change only reports that its objects are absent.
        for entry in schema_changes() {
            checked_changes.push(apply_change_entry(&mut tx, &entry).await?);
        }
        let extension_sql = [
            "CREATE EXTENSION IF NOT EXISTS timescaledb VERSION '",
            TIMESCALEDB_VERSION,
            "'",
        ]
        .concat();
        tx.execute(extension_sql.as_str()).await?;
        tx.execute("CREATE EXTENSION IF NOT EXISTS pg_trgm").await?;
        let installed: String =
            sqlx::query_scalar("SELECT extversion FROM pg_extension WHERE extname='timescaledb'")
                .fetch_one(&mut *tx)
                .await?;
        if installed != TIMESCALEDB_VERSION {
            return Err(Error::Schema("TimescaleDB 2.27.2 is required".into()));
        }
        sqlx::raw_sql("CREATE SCHEMA core; CREATE SCHEMA raw; CREATE SCHEMA sinex_schemas; CREATE SCHEMA reflection; CREATE SCHEMA audit; CREATE SCHEMA derivation; REVOKE ALL ON SCHEMA core, raw, sinex_schemas,reflection,audit,derivation FROM PUBLIC").execute(&mut *tx).await?;
        for ddl in declarations()? {
            tx.execute(ddl.as_str()).await?;
        }
        configure_events(&mut tx).await?;
        sqlx::query("INSERT INTO sinex_schemas.payload_schemas (id,event_type,version,schema) VALUES (2,'command.executed','atuin-v1',$1)")
            .bind(crate::atuin::payload_schema()).execute(&mut *tx).await?;
        sqlx::query("INSERT INTO sinex_schemas.payload_schemas(id,event_type,version,schema) VALUES(3,'command.canonical','shell-command-v1-single-source',$1)").bind(first_release_shell_payload()).execute(&mut *tx).await?;
        sqlx::query("INSERT INTO sinex_schemas.payload_schemas(id,event_type,version,schema) VALUES(5,'command.canonical','shell-command-v1-atuin-zsh',$1)").bind(crate::shell::payload_schema()).execute(&mut *tx).await?;
        sqlx::query("INSERT INTO sinex_schemas.payload_schemas(id,event_type,version,schema) VALUES(4,'command.imported','zsh-extended-v1',$1)").bind(crate::zsh::payload_schema()).execute(&mut *tx).await?;
        sqlx::query("INSERT INTO sinex_schemas.payload_schemas(id,event_type,version,schema) VALUES(6,'activity.session.boundary','shell-session-v1',$1)").bind(crate::session::payload_schema()).execute(&mut *tx).await?;
        for (id, kind, download) in [(7, "page.visited", false), (8, "download.observed", true)] {
            sqlx::query("INSERT INTO sinex_schemas.payload_schemas(id,event_type,version,schema) VALUES($1,$2,'chromium-history-v1',$3)").bind(id).bind(kind).bind(sinex_model::browser::payload_schema(download)).execute(&mut *tx).await?;
        }
        sqlx::query("INSERT INTO core.event_types (id,name) VALUES (1,'synthetic.observed'),(2,'command.executed'),(3,'command.canonical'),(4,'command.imported'),(5,'activity.session.boundary'),(6,'page.visited'),(7,'download.observed')")
            .execute(&mut *tx)
            .await?;
        sqlx::query("INSERT INTO core.commit_counter (one,seq) VALUES (true,0)")
            .execute(&mut *tx)
            .await?;
        sqlx::query("INSERT INTO derivation.session_progress(one) VALUES(true)")
            .execute(&mut *tx)
            .await?;
        sqlx::query("INSERT INTO derivation.shell_progress(one) VALUES(true)")
            .execute(&mut *tx)
            .await?;
        for entry in schema_changes() {
            sqlx::query(
                "INSERT INTO sinex_schemas.applied_changes(number,description) VALUES($1,$2)",
            )
            .bind(entry.number)
            .bind(entry.description)
            .execute(&mut *tx)
            .await?;
        }
        let catalog = capture_catalog(&mut tx).await?;
        let catalog = serde_json::to_value(catalog)
            .map_err(|_| Error::Schema("cannot encode canonical catalog".into()))?;
        sqlx::query(
            "INSERT INTO sinex_schemas.definition (version,fingerprint,catalog) VALUES (1,$1,$2)",
        )
        .bind(fingerprint()?)
        .bind(catalog)
        .execute(&mut *tx)
        .await?;
        sqlx::raw_sql("DO $$ BEGIN IF NOT EXISTS(SELECT 1 FROM pg_roles WHERE rolname='sinex_read_model') THEN CREATE ROLE sinex_read_model LOGIN; END IF; END $$; GRANT USAGE ON SCHEMA core,raw,audit,sinex_schemas,derivation,reflection TO sinex_read_model; GRANT SELECT ON ALL TABLES IN SCHEMA core,raw,audit,sinex_schemas,derivation,reflection TO sinex_read_model; GRANT INSERT,UPDATE,DELETE ON core.search_text,core.event_project,derivation.read_model_position TO sinex_read_model").execute(&mut *tx).await?;
        sqlx::raw_sql("GRANT USAGE ON SCHEMA core,raw,sinex_schemas TO sinex_writer,sinex_reader; GRANT USAGE ON SCHEMA reflection,audit,derivation TO sinex_writer,sinex_reader; GRANT SELECT ON ALL TABLES IN SCHEMA core,raw,sinex_schemas,reflection,audit,derivation TO sinex_writer,sinex_reader; GRANT INSERT ON core.deleted_occurrences,core.producer_versions,derivation.shell_dirty,derivation.session_dirty,core.operation_effects,core.events,reflection.events,core.live_coordinates,core.live_equivalence,audit.archived,core.commit_log,raw.source_material_registry,core.manifests,raw.inputs,raw.record_failures TO sinex_writer; GRANT INSERT,UPDATE ON core.operations_log,raw.source_cursors,core.blobs,core.runs,raw.observations TO sinex_writer; GRANT DELETE ON core.deleted_occurrences,core.live_equivalence,derivation.shell_dirty,derivation.session_dirty,core.live_coordinates TO sinex_writer; GRANT UPDATE ON core.commit_counter,core.live_equivalence,core.live_coordinates,derivation.shell_dirty,derivation.shell_progress,derivation.session_dirty,derivation.session_progress TO sinex_writer; GRANT UPDATE(parsed_event_count,blob_id,status,total_bytes,durable_bytes) ON raw.source_material_registry TO sinex_writer").execute(&mut *tx).await?;
    }
    let mut status = inspect(&mut tx).await?;
    tx.commit().await?;
    status.checked_changes = checked_changes;
    Ok(status)
}

pub async fn check_schema(admin_url: &str) -> Result<SchemaStatus> {
    let mut conn = PgConnection::connect(admin_url).await?;
    require_pg18(&mut conn).await?;
    inspect(&mut conn).await
}

async fn require_pg18(conn: &mut PgConnection) -> Result<()> {
    let version: String = sqlx::query_scalar("SHOW server_version_num")
        .fetch_one(conn)
        .await?;
    if version
        .parse::<u32>()
        .map_err(|_| Error::Schema("unreadable PostgreSQL version".into()))?
        < 180000
    {
        return Err(Error::Schema("PostgreSQL 18 or newer is required".into()));
    }
    Ok(())
}

#[cfg(test)]
mod tests;
