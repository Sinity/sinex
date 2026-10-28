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
        .map_err(|e| format!("postgres --version: {e}"))?;
    if !version.status.success() {
        return Err(format!("postgres --version failed: {}", version.status));
    }
    let version = String::from_utf8(version.stdout).map_err(|e| e.to_string())?;
    let output = schema_tool(root, &["hash"])?;
    let declarations = String::from_utf8(output.stdout).map_err(|e| e.to_string())?;
    devkit::pg::schema_hash(&[], &[version.trim(), declarations.trim()])
}

/// Run the query-free schema tool from this checkout.
fn schema_tool(root: &Path, args: &[&str]) -> Result<std::process::Output> {
    let output = devkit::cargo_command()
        .args(["run", "--locked", "--quiet", "-p", "sinex-schema", "--"])
        .args(args)
        .current_dir(root)
        .output()
        .map_err(|e| format!("sinex-schema: {e}"))?;
    if !output.status.success() {
        return Err(format!(
            "sinex-schema failed: {}\n{}",
            output.status,
            String::from_utf8_lossy(&output.stderr)
        ));
    }
    Ok(output)
}

pub fn schema(root: &Path) -> Result<(String, impl Fn(&Server, &str) -> devkit::pg::Result)> {
    let hash = schema_hash(root)?;
    let root = root.canonicalize().map_err(|e| e.to_string())?;
    Ok((hash, move |server: &Server, database: &str| {
        server.ensure_roles(ROLES)?;
        schema_tool(&root, &["apply", &server.url(database, None)])?;
        Ok(())
    }))
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
