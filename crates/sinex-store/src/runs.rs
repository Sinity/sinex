//! Runs and manifests: every event names the run that produced it, and every
//! run names what it is (name, kind, declared version, exact build).
use crate::Result;
use sinex_model::RunId;
use sqlx::PgConnection;

pub struct Manifest<'a> {
    pub name: &'a str,
    pub kind: &'a str,
    pub version: &'a str,
    pub build: &'a str,
    pub emits: &'a [&'a str],
}

/// Register the manifest (once per name, version and build) and start a run.
pub async fn start(conn: &mut PgConnection, manifest: &Manifest<'_>, host: &str) -> Result<RunId> {
    let emits: Vec<String> = manifest.emits.iter().map(|s| s.to_string()).collect();
    let manifest_id = sqlx::query_scalar!(
        r#"INSERT INTO core.manifests (id, name, kind, version, build, emits) VALUES ($1, $2, $3, $4, $5, $6)
           ON CONFLICT (name, version, build) DO UPDATE SET emits = EXCLUDED.emits RETURNING id"#,
        uuid::Uuid::now_v7(),
        manifest.name,
        manifest.kind,
        manifest.version,
        manifest.build,
        &emits,
    )
    .fetch_one(&mut *conn)
    .await?;
    let run = RunId::new();
    sqlx::query!(
        "INSERT INTO core.runs (id, manifest_id, name, host) VALUES ($1, $2, $3, $4)",
        run.0,
        manifest_id,
        manifest.name,
        host
    )
    .execute(&mut *conn)
    .await?;
    Ok(run)
}

pub async fn end(conn: &mut PgConnection, run: RunId) -> Result<()> {
    sqlx::query!("UPDATE core.runs SET ended_at = now() WHERE id = $1", run.0)
        .execute(&mut *conn)
        .await?;
    Ok(())
}
