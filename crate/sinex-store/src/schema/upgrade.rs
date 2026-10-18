//! Ordered schema changes and `apply_changes`, which dumps the database before
//! applying any of them.
use super::declarations::{
    LEGACY_UNAVAILABLE_INDEX, atuin_payload_schema_at, declarations_at, fingerprint_at,
    installed_tables,
};
use super::tables::EVENT_CONSTRAINTS;
use super::{
    SchemaChange, SchemaChangeCheck, SchemaStatus, capture_catalog, inspect, require_pg18,
};
use crate::{Error, Result};
use sqlx::{Connection, Executor, PgConnection};

/// Descriptions are stored in `sinex_schemas.applied_changes` and compared on every
/// check, so they must not change.
pub fn schema_changes() -> Vec<SchemaChange> {
    vec![
        SchemaChange {
            number: 1,
            description: "register retained zsh and two-parent shell contracts",
        },
        SchemaChange {
            number: 2,
            description: "retain shell dirty frontier and initialize grouping progress",
        },
        SchemaChange {
            number: 3,
            description: "register shell sessions and durable logical-day reconciliation",
        },
        SchemaChange {
            number: 4,
            description: "register retained Chromium visits and persisted download states",
        },
        SchemaChange {
            number: 5,
            description: "retain exact metadata when full-text representation is unavailable",
        },
        SchemaChange {
            number: 6,
            description: "preserve signed Atuin provider exit statuses",
        },
        SchemaChange {
            number: 7,
            description: "replace bounded vectors with complete normalized search token text",
        },
    ]
}

// pg_dump gets the checked SQLx connection as libpq environment variables (libpq
// does not expand a URI in PGDATABASE), which keeps the password out of argv.
pub(super) fn dump_environment(admin_url: &str) -> Result<Vec<(&'static str, String)>> {
    use sqlx::{
        ConnectOptions,
        postgres::{PgConnectOptions, PgSslMode},
    };
    let options: PgConnectOptions = admin_url.parse()?;
    // SQLx has no password getter; this public representation includes its resolved
    // password (including defaults/pgpass), not just the URI's explicit password.
    let resolved = options.to_url_lossy();
    if percent_encoding::percent_decode_str(resolved.username())
        .decode_utf8()
        .map_err(|_| Error::Invalid("invalid administrative user encoding"))?
        .as_ref()
        != options.get_username()
    {
        return Err(Error::Invalid(
            "administrative user cannot be represented for a verified dump",
        ));
    }
    let mut env = vec![
        (
            "PGHOST",
            options
                .get_socket()
                .map(|p| p.to_string_lossy().into_owned())
                .unwrap_or_else(|| options.get_host().to_owned()),
        ),
        ("PGPORT", options.get_port().to_string()),
        (
            "PGDATABASE",
            options
                .get_database()
                .unwrap_or(options.get_username())
                .to_owned(),
        ),
        ("PGUSER", options.get_username().to_owned()),
        ("PGPASSFILE", "/dev/null".to_owned()),
        (
            "PGSSLMODE",
            match options.get_ssl_mode() {
                PgSslMode::Disable => "disable",
                PgSslMode::Allow => "allow",
                PgSslMode::Prefer => "prefer",
                PgSslMode::Require => "require",
                PgSslMode::VerifyCa => "verify-ca",
                PgSslMode::VerifyFull => "verify-full",
            }
            .to_owned(),
        ),
    ];
    if let Some(password) = resolved.password() {
        env.push((
            "PGPASSWORD",
            percent_encoding::percent_decode_str(password)
                .decode_utf8()
                .map_err(|_| Error::Invalid("invalid administrative password encoding"))?
                .into_owned(),
        ));
    }
    if let Some(value) = options.get_options() {
        env.push(("PGOPTIONS", value.to_owned()));
    }
    if let Some(value) = options.get_application_name() {
        env.push(("PGAPPNAME", value.to_owned()));
    }
    for (key, value) in resolved.query_pairs() {
        let target = match key.as_ref() {
            "sslrootcert" => "PGSSLROOTCERT",
            "sslcert" => "PGSSLCERT",
            "sslkey" => "PGSSLKEY",
            _ => continue,
        };
        let path = value.strip_prefix("file: ").ok_or(Error::Invalid(
            "inline TLS material is unsupported for the guarded dump; use certificate/key files",
        ))?;
        env.push((target, path.to_owned()));
    }
    Ok(env)
}

pub(super) fn dump_command(environment: Vec<(&'static str, String)>) -> std::process::Command {
    let mut command = std::process::Command::new("pg_dump");
    // libpq service files, hostaddr and future PG defaults must not redirect the
    // backup away from the SQLx connection whose catalog we have verified.
    for (key, _) in std::env::vars_os() {
        if key.to_string_lossy().starts_with("PG") {
            command.env_remove(key);
        }
    }
    command
        .args(["--format=custom", "--no-password"])
        .envs(environment);
    command
}

pub(super) async fn apply_change_entry(
    conn: &mut PgConnection,
    entry: &SchemaChange,
) -> Result<SchemaChangeCheck> {
    // Each change alters one object that exists in every installed version it
    // applies to. When that object is absent (a fresh database during bootstrap)
    // the change has nothing to alter.
    let probe = match entry.number {
        1 | 3 | 4 => "core.events",
        2 => "derivation.shell_dirty",
        5 | 7 => "core.search_text",
        6 => "sinex_schemas.payload_schemas",
        _ => return Err(Error::Schema("unknown schema change".into())),
    };
    let exists: bool = sqlx::query_scalar("SELECT to_regclass($1) IS NOT NULL")
        .bind(probe)
        .fetch_one(&mut *conn)
        .await?;
    if !exists {
        return Ok(SchemaChangeCheck {
            number: entry.number,
            outcome: "old_objects_absent",
        });
    }
    match entry.number {
        1 => {
            let ddl = declarations_at(1)?;
            let metadata = installed_tables(1)
                .iter()
                .position(|d| d.name == "applied_changes")
                .unwrap();
            conn.execute(ddl[metadata].as_str()).await?;
            for table in ["core.events", "reflection.events"] {
                conn.execute(
                    format!("ALTER TABLE {table} DROP CONSTRAINT registered_shell_payload")
                        .as_str(),
                )
                .await?;
                for name in ["registered_shell_payload", "registered_zsh_payload"] {
                    let body = EVENT_CONSTRAINTS
                        .iter()
                        .find(|(n, _)| *n == name)
                        .unwrap()
                        .1;
                    conn.execute(
                        format!("ALTER TABLE {table} ADD CONSTRAINT {name} {body}").as_str(),
                    )
                    .await?;
                }
            }
            sqlx::raw_sql("ALTER TABLE raw.inputs DROP CONSTRAINT input_kind; ALTER TABLE raw.inputs ADD CONSTRAINT input_kind CHECK(kind IN ('sqlite','file')); GRANT SELECT ON sinex_schemas.applied_changes TO sinex_reader,sinex_writer,sinex_read_model; INSERT INTO core.event_types(id,name) VALUES(4,'command.imported')").execute(&mut *conn).await?;
            sqlx::query("INSERT INTO sinex_schemas.payload_schemas(id,event_type,version,schema) VALUES(5,'command.canonical','shell-command-v1-atuin-zsh',$1)").bind(crate::shell::payload_schema()).execute(&mut *conn).await?;
            sqlx::query("INSERT INTO sinex_schemas.payload_schemas(id,event_type,version,schema) VALUES(4,'command.imported','zsh-extended-v1',$1)").bind(crate::zsh::payload_schema()).execute(&mut *conn).await?;
        }
        2 => {
            sqlx::raw_sql("ALTER TABLE derivation.shell_dirty ADD COLUMN dirty_seq bigint; UPDATE derivation.shell_dirty SET dirty_seq=(SELECT seq FROM core.commit_counter WHERE one); ALTER TABLE derivation.shell_dirty ALTER COLUMN dirty_seq SET NOT NULL").execute(&mut *conn).await?;
            let ddl = declarations_at(2)?;
            let progress = installed_tables(2)
                .iter()
                .position(|d| d.name == "shell_progress")
                .unwrap();
            conn.execute(ddl[progress].as_str()).await?;
            sqlx::raw_sql("INSERT INTO derivation.shell_progress(one) VALUES(true); GRANT SELECT ON derivation.shell_progress TO sinex_reader,sinex_writer,sinex_read_model; GRANT UPDATE ON derivation.shell_dirty,derivation.shell_progress TO sinex_writer; INSERT INTO derivation.shell_dirty(parent_id,dirty_seq) SELECT e.id,c.seq FROM core.live_events e CROSS JOIN core.commit_counter c WHERE c.one AND e.source='terminal.atuin-history' AND e.event_type='command.executed' AND e.record_state='present' ON CONFLICT(parent_id) DO UPDATE SET dirty_seq=greatest(shell_dirty.dirty_seq,EXCLUDED.dirty_seq)").execute(&mut *conn).await?;
        }
        3 => {
            let ddl = declarations_at(3)?;
            for (i, d) in installed_tables(3).iter().enumerate() {
                if d.name.starts_with("session_") {
                    conn.execute(ddl[i].as_str()).await?;
                }
            }
            sqlx::raw_sql("INSERT INTO derivation.session_progress(one) VALUES(true); INSERT INTO core.event_types(id,name) VALUES(5,'activity.session.boundary'); GRANT SELECT ON derivation.session_dirty,derivation.session_progress TO sinex_reader,sinex_writer,sinex_read_model; GRANT INSERT,UPDATE,DELETE ON derivation.session_dirty TO sinex_writer; GRANT UPDATE ON derivation.session_progress TO sinex_writer").execute(&mut *conn).await?;
            sqlx::query("INSERT INTO sinex_schemas.payload_schemas(id,event_type,version,schema) VALUES(6,'activity.session.boundary','shell-session-v1',$1)").bind(crate::session::payload_schema()).execute(&mut *conn).await?;
            sqlx::query(crate::session::DIRTY_SPANS)
                .bind(Option::<uuid::Uuid>::None)
                .execute(&mut *conn)
                .await?;
        }
        4 => {
            for table in ["core.events", "reflection.events"] {
                let body = EVENT_CONSTRAINTS
                    .iter()
                    .find(|(n, _)| *n == "registered_browser_payload")
                    .expect("declared browser constraint")
                    .1;
                conn.execute(
                    format!("ALTER TABLE {table} ADD CONSTRAINT registered_browser_payload {body}")
                        .as_str(),
                )
                .await?;
            }
            sqlx::raw_sql("INSERT INTO core.event_types(id,name) VALUES(6,'page.visited'),(7,'download.observed')").execute(&mut *conn).await?;
            for (id, kind, download) in [(7, "page.visited", false), (8, "download.observed", true)]
            {
                sqlx::query("INSERT INTO sinex_schemas.payload_schemas(id,event_type,version,schema) VALUES($1,$2,'chromium-history-v1',$3)").bind(id).bind(kind).bind(sinex_model::browser::payload_schema(download)).execute(&mut *conn).await?;
            }
        }
        5 => {
            conn.execute("ALTER TABLE core.search_text ALTER COLUMN tsv DROP NOT NULL")
                .await?;
            conn.execute(LEGACY_UNAVAILABLE_INDEX).await?;
        }
        6 => {
            let changed = sqlx::query("UPDATE sinex_schemas.payload_schemas SET schema=$1 WHERE id=2 AND event_type='command.executed' AND version='atuin-v1'")
                .bind(atuin_payload_schema_at(6))
                .execute(&mut *conn)
                .await?;
            if changed.rows_affected() != 1 {
                return Err(Error::Schema(
                    "registered Atuin payload schema missing".into(),
                ));
            }
        }
        7 => {
            // Only the derived token column is discarded. Search stops serving until
            // the read model is rebuilt from the full log.
            sqlx::raw_sql("DROP INDEX core.search_text_tsv; DROP INDEX core.search_text_unavailable; ALTER TABLE core.search_text RENAME COLUMN tsv TO tokens; ALTER TABLE core.search_text ALTER COLUMN tokens TYPE text USING NULL::text; UPDATE derivation.read_model_position SET serving=false WHERE name='shell-search'")
                .execute(&mut *conn).await?;
            for sql in super::storage::declarations().into_iter().filter(|s| {
                s.starts_with("CREATE INDEX search_text_tokens ")
                    || s == super::storage::FULL_TEXT_UNAVAILABLE_INDEX
            }) {
                conn.execute(sql.as_str()).await?;
            }
        }
        _ => unreachable!("checked above"),
    }
    Ok(SchemaChangeCheck {
        number: entry.number,
        outcome: "applied",
    })
}

/// Writes a complete `pg_dump` to `dump_file`, then applies each pending change in
/// its own transaction. Nothing changes if the dump fails. Every committed change
/// records its catalog, so an interrupted upgrade resumes from a checked state.
pub async fn apply_changes(admin_url: &str, dump_file: &std::path::Path) -> Result<SchemaStatus> {
    use std::os::unix::fs::OpenOptionsExt;
    let mut conn = PgConnection::connect(admin_url).await?;
    require_pg18(&mut conn).await?;
    let locked: bool = sqlx::query_scalar(
        "SELECT pg_try_advisory_lock(73018924011) AND pg_try_advisory_lock(73018924012)",
    )
    .fetch_one(&mut conn)
    .await?;
    if !locked {
        return Err(Error::Schema("schema or writer is already owned".into()));
    }
    let status = inspect(&mut conn).await?;
    if status.pending_changes.is_empty() {
        return Ok(status);
    }
    if !dump_file.is_absolute() {
        return Err(Error::Invalid("absolute dump path required"));
    }
    let dump = std::fs::OpenOptions::new()
        .create_new(true)
        .write(true)
        .mode(0o600)
        .open(dump_file)?;
    let mut diagnostic_path = dump_file.as_os_str().to_os_string();
    diagnostic_path.push(".stderr");
    let diagnostic = std::fs::OpenOptions::new()
        .create_new(true)
        .write(true)
        .mode(0o600)
        .open(std::path::Path::new(&diagnostic_path))?;
    let parent = dump_file
        .parent()
        .expect("absolute dump path has parent")
        .to_owned();
    let dump_environment = dump_environment(admin_url)?;
    let (complete, exit_code) =
        tokio::task::spawn_blocking(move || -> std::io::Result<(bool, Option<i32>)> {
            let output = dump.try_clone()?;
            let result = dump_command(dump_environment)
                .stdout(std::process::Stdio::from(output))
                .stderr(std::process::Stdio::from(diagnostic.try_clone()?))
                .status()?;
            dump.sync_all()?;
            diagnostic.sync_all()?;
            std::fs::File::open(parent)?.sync_all()?;
            Ok((
                result.success() && dump.metadata()?.len() > 0,
                result.code(),
            ))
        })
        .await
        .map_err(|_| Error::Schema("dump worker failed; no schema change applied".into()))??;
    if !complete {
        return Err(Error::Schema(format!(
            "pg_dump failed (exit {exit_code:?}); no schema change applied (partial dump and raw diagnostics retained)"
        )));
    }
    let mut checked_changes = Vec::new();
    for entry in status.pending_changes {
        // Re-check the live schema before each change.
        let current = inspect(&mut conn).await?;
        if current.pending_changes.first().map(|c| c.number) != Some(entry.number) {
            return Err(Error::Schema("schema changes are out of order".into()));
        }
        let mut tx = conn.begin().await?;
        let checked = apply_change_entry(&mut tx, &entry).await?;
        sqlx::query("INSERT INTO sinex_schemas.applied_changes(number,description) VALUES($1,$2)")
            .bind(entry.number)
            .bind(entry.description)
            .execute(&mut *tx)
            .await?;
        let catalog = serde_json::to_value(capture_catalog(&mut tx).await?)
            .map_err(|_| Error::Schema("cannot encode upgraded catalog".into()))?;
        sqlx::query(
            "UPDATE sinex_schemas.definition SET fingerprint=$1,catalog=$2 WHERE version=1",
        )
        .bind(fingerprint_at(entry.number as usize)?)
        .bind(catalog)
        .execute(&mut *tx)
        .await?;
        inspect(&mut tx).await?;
        tx.commit().await?;
        checked_changes.push(checked);
    }
    let mut status = inspect(&mut conn).await?;
    status.checked_changes = checked_changes;
    Ok(status)
}
