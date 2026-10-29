//! Diagnostics that work while the daemon is down: `doctor`, `status
//! --offline`, `store verify --offline`. Read-only: they open the database
//! directly in read-only transactions and never write.
use serde_json::{Value, json};
use sinex_store::Verified;
use sinex_store::diagnostics::{self, Snapshot};
use std::path::Path;

fn time(t: Option<time::OffsetDateTime>) -> Value {
    t.and_then(|t| {
        t.format(&time::format_description::well_known::Rfc3339)
            .ok()
    })
    .map_or(Value::Null, Value::String)
}

fn snapshot_json(s: &Snapshot) -> Value {
    json!({
        "commit_seq": s.commit_seq,
        "schema_version": s.schema_version,
        "search_index": {
            "position": s.search_position,
            "behind": s.commit_seq - s.search_position.unwrap_or(0),
        },
        "failures_24h": s.failures_24h,
        "blobs": s.blobs,
        "sources": s.sources.iter().map(|src| json!({
            "source": src.source,
            "events": src.events,
            "last_event": time(src.last_event),
            "last_seen": time(Some(src.last_seen)),
        })).collect::<Vec<_>>(),
    })
}

/// What the database holds, without the daemon.
async fn status(database: &str) -> Result<Value, String> {
    let mut conn = diagnostics::connect(database)
        .await
        .map_err(|e| e.to_string())?;
    let s = diagnostics::snapshot(&mut conn)
        .await
        .map_err(|e| e.to_string())?;
    Ok(snapshot_json(&s))
}

/// Check the database is reachable and at this build's schema version, the
/// content store is readable, and the search index is not behind.
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
                        behind == 0,
                        if behind == 0 {
                            "current".into()
                        } else {
                            format!("{behind} commits behind; the daemon catches it up")
                        },
                    );
                    check(
                        "failures",
                        s.failures_24h == 0,
                        format!("{} records failed in the last 24 hours", s.failures_24h),
                    );
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
async fn verify_store(database: &str, store: &Path) -> Result<Value, String> {
    let mut conn = diagnostics::connect(database)
        .await
        .map_err(|e| e.to_string())?;
    let names = diagnostics::blob_names(&mut conn)
        .await
        .map_err(|e| e.to_string())?;
    let blobs = sinex_store::Blobs::open(store).map_err(|e| e.to_string())?;
    blobs.load(&mut conn).await.map_err(|e| e.to_string())?;
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
    .map_err(|e| e.to_string())
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
pub(crate) async fn run(command: Command) -> Result<(), String> {
    let value = match command {
        Command::Doctor { database, store } => doctor(&database, &store).await,
        Command::Status { database } => status(&database).await?,
        Command::VerifyStore { database, store } => verify_store(&database, &store).await?,
    };
    println!("{value}");
    Ok(())
}
