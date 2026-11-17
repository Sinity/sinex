//! Diagnostics that work while the daemon is down: `doctor`, `status
//! --offline`, `store verify --offline`. Read-only: they open the database
//! directly in read-only transactions and never write.
use crate::{Error, Result};
use serde_json::{Value, json};
use sinex_model::EventPayload;
use sinex_store::Verified;
use sinex_store::diagnostics;
use std::path::Path;

/// Count retained located contents with no current recorded file available here.
/// This checks paths, not the contents of external media; nothing is rehashed.
async fn missing_located_files(conn: &mut sqlx::PgConnection) -> Result<usize> {
    use std::collections::BTreeMap;
    let rows = diagnostics::located_file_paths(conn).await?;
    let mut available = BTreeMap::new();
    for row in rows {
        let found = available.entry(row.content).or_insert(false);
        if *found {
            continue;
        }
        let (Some(root), Some(path)) = (row.root, row.path) else {
            continue;
        };
        let root = recorded_path(root, row.native_root)?;
        let path = recorded_path(path, row.native_path)?;
        if !root.is_absolute()
            || path
                .components()
                .any(|part| !matches!(part, std::path::Component::Normal(_)))
        {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "invalid recorded file location",
            )
            .into());
        }
        match std::fs::metadata(root.join(path)) {
            Ok(metadata) => *found = metadata.is_file(),
            Err(error)
                if matches!(
                    error.kind(),
                    std::io::ErrorKind::NotFound | std::io::ErrorKind::NotADirectory
                ) => {}
            Err(error) => return Err(error.into()),
        }
    }
    Ok(available.values().filter(|found| !**found).count())
}

fn recorded_path(display: String, native: Option<String>) -> std::io::Result<std::path::PathBuf> {
    use base64::Engine;
    use std::os::unix::ffi::OsStringExt;
    match native {
        None => Ok(display.into()),
        Some(encoded) => {
            let bytes = base64::engine::general_purpose::STANDARD
                .decode(encoded)
                .map_err(|_| {
                    std::io::Error::new(
                        std::io::ErrorKind::InvalidData,
                        "invalid recorded path encoding",
                    )
                })?;
            Ok(std::ffi::OsString::from_vec(bytes).into())
        }
    }
}

fn utc_context() -> sinex_query::Context {
    sinex_query::Context::new(
        jiff::Timestamp::now(),
        jiff::tz::TimeZone::UTC,
        jiff::civil::Time::constant(0, 0, 0, 0),
    )
}

/// Every source row through the query's source-status relation.
pub(crate) async fn source_rows(
    conn: &mut sqlx::PgConnection,
    context: &sinex_query::Context,
) -> Result<Vec<Value>> {
    let mut rows = Vec::new();
    let mut page = sinex_store::query::Page::default();
    loop {
        let answer =
            sinex_store::query::run(conn, "sources | sort by binding", None, context, &page)
                .await?;
        rows.extend(answer.rows);
        match answer.page.cursor {
            Some(cursor) => page.cursor = Some(cursor),
            None => return Ok(rows),
        }
    }
}

/// What the database holds, without the daemon.
async fn status(database: &str) -> Result<Value> {
    let mut conn = diagnostics::connect(database).await?;
    let s = diagnostics::snapshot(&mut conn).await?;
    let context = utc_context();
    let (sources, samples) = if s.schema_version.is_some() {
        (
            source_rows(&mut conn, &context).await?,
            sinex_store::telemetry::recent(
                &mut conn,
                sinex_model::event_types::telemetry::SinexTelemetry::EVENT_TYPE,
                crate::api::HISTORY_WINDOW,
            )
            .await?,
        )
    } else {
        (Vec::new(), Vec::new())
    };
    let mut status = crate::api::status_fields(&samples, &sources, &context)?;
    status.as_object_mut().expect("status object").extend(
        json!({
            "commit_seq": s.commit_seq, "schema_version": s.schema_version,
            "failures_24h": s.failures_24h, "blobs": s.blobs,
            "zone": "UTC",
        })
        .as_object()
        .expect("status details")
        .clone(),
    );
    Ok(status)
}

/// Check the database is reachable and at this build's schema version, the
/// content store is readable, and sources report their expected states.
async fn doctor(database: &str, store: &Path) -> Value {
    let mut checks = Vec::new();
    let mut check = |name: &str, ok: bool, detail: String| {
        checks.push(json!({ "check": name, "ok": ok, "detail": detail }));
    };
    match diagnostics::connect(database).await {
        Err(e) => check("database", false, format!("cannot connect: {e}")),
        Ok(mut conn) => {
            check("database", true, "reachable".into());
            match diagnostics::snapshot(&mut conn).await {
                Err(e) => check("schema", false, e.to_string()),
                Ok(s) => {
                    match s.schema_version {
                        None => check(
                            "schema",
                            false,
                            "no Sinex schema; `sinexd admin schema` creates it".into(),
                        ),
                        Some(v) if v == sinex_schema::VERSION => {
                            check("schema", true, format!("version {v}"))
                        }
                        Some(v) => check(
                            "schema",
                            false,
                            format!(
                                "database at version {v}, this build declares {}",
                                sinex_schema::VERSION
                            ),
                        ),
                    }
                    let behind = s.commit_seq - s.search_position.unwrap_or(0);
                    check(
                        "search index",
                        true,
                        format!("{behind} commits behind; the daemon catches it up"),
                    );
                    check(
                        "failures",
                        true,
                        format!(
                            "{} records failed in the last 24 hours; `sinexctl query 'failures where since:24h'` lists them",
                            s.failures_24h
                        ),
                    );
                    if s.schema_version.is_some() {
                        match missing_located_files(&mut conn).await {
                            Ok(missing) => check(
                                "located_files",
                                true,
                                format!(
                                    "{missing} located files are in none of their recorded places on this machine; Sinex keeps their hashes and last locations"
                                ),
                            ),
                            Err(error) => check("located_files", false, error.to_string()),
                        }
                    }
                    match source_rows(&mut conn, &utc_context()).await {
                        Ok(rows) => {
                            let stalled: Vec<_> = rows
                                .iter()
                                .filter(|source| source["state"] == "stalled")
                                .map(|source| {
                                    format!(
                                        "{}: {}",
                                        source["binding"].as_str().unwrap_or("source"),
                                        source["reason"].as_str().unwrap_or("stalled")
                                    )
                                })
                                .collect();
                            let detail = if stalled.is_empty() {
                                format!("{} sources; none stalled", rows.len())
                            } else {
                                stalled.join("; ")
                            };
                            check("sources", stalled.is_empty(), detail);
                        }
                        Err(error) => check("sources", false, error.to_string()),
                    }
                }
            }
        }
    }
    match std::fs::read_dir(store) {
        Ok(_) => check(
            "content store",
            true,
            format!("{} is readable", store.display()),
        ),
        Err(e) => check("content store", false, format!("{}: {e}", store.display())),
    }
    let ok = checks.iter().all(|c| c["ok"] == json!(true));
    json!({ "ok": ok, "checks": checks })
}

/// Read every stored blob back and check it against its name.
async fn verify_store(database: &str, store: &Path) -> Result<Value> {
    let mut conn = diagnostics::connect(database).await?;
    let names = diagnostics::blob_names(&mut conn).await?;
    let blobs = sinex_store::Blobs::open(store)?;
    blobs.load(&mut conn).await?;
    let store_path = store.to_path_buf();
    // Verification reads every byte; keep it off the async threads.
    tokio::task::spawn_blocking(move || {
        let (mut intact, mut corrupt, mut missing) = (0u64, Vec::new(), Vec::new());
        for name in &names {
            match blobs.verify(name) {
                Ok(Verified::Intact | Verified::CiphertextIntact) => intact += 1,
                Ok(Verified::Corrupt(why)) => corrupt.push(json!({ "blob": name, "why": why })),
                Ok(Verified::Missing) => missing.push(json!(name)),
                Err(e) => corrupt.push(json!({ "blob": name, "why": e.to_string() })),
            }
        }
        json!({
            "store": store_path.display().to_string(),
            "checked": names.len(),
            "intact": intact,
            "corrupt": corrupt,
            "missing": missing,
            "ok": corrupt.is_empty() && missing.is_empty(),
        })
    })
    .await
    .map_err(Error::from)
}

#[derive(clap::Subcommand)]
pub(crate) enum Command {
    /// Database and content-store checks while the daemon is stopped.
    Doctor {
        #[arg(long)]
        database: String,
        #[arg(long)]
        store: std::path::PathBuf,
    },
    /// Read the database's current status without the running daemon.
    Status {
        #[arg(long)]
        database: String,
    },
    /// Read every stored blob and check it against its name.
    VerifyStore {
        #[arg(long)]
        database: String,
        #[arg(long)]
        store: std::path::PathBuf,
    },
}
pub(crate) async fn run(command: Command) -> Result<()> {
    let value = match command {
        Command::Doctor { database, store } => doctor(&database, &store).await,
        Command::Status { database } => status(&database).await?,
        Command::VerifyStore { database, store } => verify_store(&database, &store).await?,
    };
    crate::admin::emit(&format!("{value}\n"));
    Ok(())
}

#[cfg(test)]
mod tests;
