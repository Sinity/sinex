//! Sinex's schema on devkit's PostgreSQL: the roles, the schema bootstrap
//! (the query-free `sinex-schema apply`), and the environment compile-time
//! checks and tests read.
use crate::Result;
use devkit::pg::{ClusterOptions, Schema, SchemaDatabases, Server};
use std::path::Path;
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

/// The schema is declared in `crates/sinex-schema`, with its CHECK lists and
/// typed views generated from `crates/sinex-model`; the server version is part
/// of its identity because it decides the catalog.
pub fn schema_hash(root: &Path) -> Result<String> {
    let version = Command::new("postgres")
        .arg("--version")
        .output()
        .map(|o| String::from_utf8_lossy(&o.stdout).trim().to_owned())
        .map_err(|e| format!("postgres --version: {e}"))?;
    Ok(devkit::pg::schema_hash(
        &[
            root.join("crates/sinex-schema/src"),
            root.join("crates/sinex-model/src"),
            root.join("crates/sinex-macros/src"),
        ],
        &[&version],
    )?)
}

/// Build the query-free schema tool and apply the schema to `database`.
pub fn bootstrap(server: &Server, database: &str) -> devkit::pg::Result {
    server.ensure_roles(ROLES)?;
    let status = devkit::cargo_command()
        .args(["build", "--locked", "-p", "sinex-schema"])
        .status()
        .map_err(|e| format!("cargo build: {e}"))?;
    if !status.success() {
        return Err(format!("building sinex-schema failed: {status}"));
    }
    let output = Command::new(crate::target_dir().join("debug/sinex-schema"))
        .arg("apply")
        .arg(server.url(database, None))
        .output()
        .map_err(|e| format!("sinex-schema: {e}"))?;
    if output.status.success() {
        Ok(())
    } else {
        Err(format!(
            "sinex-schema apply failed: {}\n{}",
            output.status,
            String::from_utf8_lossy(&output.stderr)
        ))
    }
}

pub fn schema(root: &Path) -> Result<(String, impl Fn(&Server, &str) -> devkit::pg::Result)> {
    Ok((schema_hash(root)?, bootstrap))
}

/// The server and this schema version's databases, built if needed.
pub fn prepare(root: &Path) -> Result<(Server, SchemaDatabases)> {
    let server = Server::service_or_start(&cluster_options())?;
    server.ensure_roles(ROLES)?;
    let (hash, apply) = schema(root)?;
    let databases = server.ensure_schema(&Schema {
        project: "sinex",
        hash,
        bootstrap: &apply,
    })?;
    Ok((server, databases))
}

/// `DATABASE_URL` for compile-time checks against the schema database, and
/// what tests need to clone the template (`devkit::pg::test_database`).
pub fn env(server: &Server, databases: &SchemaDatabases) -> Vec<(String, String)> {
    let mut env = vec![("DATABASE_URL".into(), server.url(&databases.schema, None))];
    env.extend(server.test_env(&databases.template));
    env.extend(server.pg_env());
    env
}
