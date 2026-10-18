//! Sinex's schema on devkit's PostgreSQL: the roles, the schema bootstrap
//! (`sinexd admin schema`), and the environment database tests and
//! scenarios read.
use crate::Result;
use devkit::pg::{ClusterOptions, Schema, SchemaDatabases, Server};
use std::path::{Path, PathBuf};
use std::process::Command;

pub const ROLES: &[&str] = &["sinex_writer", "sinex_reader"];

/// TimescaleDB settings for a throwaway server; the per-user service is
/// configured the same way by Sinnix.
pub fn cluster_options() -> ClusterOptions {
    ClusterOptions {
        preload: vec!["timescaledb".into()],
        settings: vec![
            ("timescaledb.telemetry_level".into(), "off".into()),
            ("timescaledb.enable_chunk_skipping".into(), "on".into()),
        ],
    }
}

/// The schema is declared in `crate/sinex-store/src/schema`; the server
/// version is part of its identity because it decides the catalog.
pub fn schema_hash(root: &Path) -> Result<String> {
    let version = Command::new("postgres")
        .arg("--version")
        .output()
        .map(|o| String::from_utf8_lossy(&o.stdout).trim().to_owned())
        .map_err(|e| format!("postgres --version: {e}"))?;
    Ok(devkit::pg::schema_hash(
        &[root.join("crate/sinex-store/src/schema")],
        &[&version],
    )?)
}

/// Applies the schema with the admin-only daemon at `admin`, building it
/// first when no binary is given.
pub fn bootstrap(server: &Server, database: &str, admin: Option<&Path>) -> devkit::pg::Result {
    server.ensure_roles(ROLES)?;
    let admin = match admin {
        Some(path) => path.to_owned(),
        None => {
            let status = Command::new("cargo")
                .args(["build", "--locked", "-p", "sinexd", "--no-default-features"])
                .status()
                .map_err(|e| format!("cargo build: {e}"))?;
            if !status.success() {
                return Err(format!("building the admin-only sinexd failed: {status}"));
            }
            crate::target_dir().join("debug/sinexd")
        }
    };
    let file = std::env::temp_dir().join(format!("sinex-admin-{}", std::process::id()));
    std::fs::write(&file, server.url(database, None)).map_err(|e| e.to_string())?;
    let output = Command::new(&admin)
        .args(["admin", "schema", "--connection-file"])
        .arg(&file)
        .output()
        .map_err(|e| format!("{}: {e}", admin.display()));
    let _ = std::fs::remove_file(&file);
    let output = output?;
    if output.status.success() {
        Ok(())
    } else {
        Err(format!(
            "sinexd admin schema failed: {}\n{}",
            output.status,
            String::from_utf8_lossy(&output.stderr)
        ))
    }
}

/// The server and this schema version's databases, built if needed.
pub fn prepare(root: &Path, admin: Option<&Path>) -> Result<(Server, SchemaDatabases)> {
    let server = Server::service_or_start(&cluster_options())?;
    server.ensure_roles(ROLES)?;
    let hash = schema_hash(root)?;
    let apply = |server: &Server, database: &str| bootstrap(server, database, admin);
    let databases = server.ensure_schema(&Schema {
        project: "sinex",
        hash,
        bootstrap: &apply,
    })?;
    Ok((server, databases))
}

/// What a database test or scenario reads for the database `name`: the
/// admin, writer and reader URLs, a store directory under `scratch`,
/// `DATABASE_URL` for compile-time checks against `schema`, and `PGHOST` and
/// `PGPORT` for the command-line tools.
pub fn env(server: &Server, schema: &str, name: &str, scratch: &Path) -> Vec<(String, String)> {
    let store: PathBuf = scratch.join(format!("{name}-store"));
    let mut env = vec![
        ("DATABASE_URL".into(), server.url(schema, None)),
        ("SINEX_TEST_ADMIN_URL".into(), server.url(name, None)),
        (
            "SINEX_TEST_WRITER_URL".into(),
            server.url(name, Some("sinex_writer")),
        ),
        (
            "SINEX_TEST_READER_URL".into(),
            server.url(name, Some("sinex_reader")),
        ),
        ("SINEX_TEST_STORE_DIR".into(), store.display().to_string()),
    ];
    env.extend(server.pg_env());
    env
}
