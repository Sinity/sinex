//! Test helpers shared by the workspace's tests.
//!
//! [`database`] gives each test its own PostgreSQL database with the schema
//! applied, cloned from a template so tests can run in parallel. The cluster
//! comes from `xtask test`, which sets `PGHOST` and `PGPORT`.
use sqlx::{Connection, Executor, PgConnection};
use std::path::{Path, PathBuf};

const TEMPLATE: &str = "sinex_template";
/// Serializes template creation across the test processes nextest runs in parallel.
const TEMPLATE_LOCK: i64 = 0x5e1e_7e57;

/// A fresh database with the schema applied, its connection URLs and a blob
/// directory. Both are dropped with the test process's cluster, not here.
pub struct TestDb {
    pub name: String,
    pub admin: String,
    pub writer: String,
    pub reader: String,
    store: tempfile::TempDir,
}

impl TestDb {
    /// An empty directory for the store's blobs, removed when the `TestDb` drops.
    pub fn store_dir(&self) -> PathBuf {
        self.store.path().to_path_buf()
    }

    /// URL for `role` (`sinex_writer`, `sinex_reader`, `sinex_read_model`, or "" for the admin).
    pub fn url(&self, role: &str) -> String {
        url(role, &self.name)
    }
}

/// A new database cloned from the schema template.
pub async fn database() -> TestDb {
    let name = format!("t_{}", uuid::Uuid::new_v4().simple());
    let mut postgres = connect("postgres").await;
    ensure_template(&mut postgres).await;
    retry_while_busy(
        &mut postgres,
        &format!("CREATE DATABASE {name} TEMPLATE {TEMPLATE}"),
    )
    .await;
    TestDb {
        admin: url("", &name),
        writer: url("sinex_writer", &name),
        reader: url("sinex_reader", &name),
        store: temp_dir(),
        name,
    }
}

/// [`database`] for synchronous tests.
pub fn database_blocking() -> TestDb {
    tokio::runtime::Runtime::new()
        .expect("tokio runtime")
        .block_on(database())
}

/// A temporary directory, on tmpfs when there is one: the daemon fsyncs its
/// state, which costs seconds per call on a busy disk.
pub fn temp_dir() -> tempfile::TempDir {
    let base = Path::new("/dev/shm");
    let builder = &mut tempfile::Builder::new();
    builder.prefix("sinex-test-");
    if base.is_dir() {
        builder.tempdir_in(base)
    } else {
        builder.tempdir()
    }
    .expect("create a temporary directory")
}

async fn ensure_template(postgres: &mut PgConnection) {
    postgres
        .execute(format!("SELECT pg_advisory_lock({TEMPLATE_LOCK})").as_str())
        .await
        .expect("template lock");
    let exists: bool =
        sqlx::query_scalar("SELECT EXISTS (SELECT 1 FROM pg_database WHERE datname = $1)")
            .bind(TEMPLATE)
            .fetch_one(&mut *postgres)
            .await
            .expect("look up the template");
    if !exists {
        // Build under another name and rename, so a half-built template is never cloned.
        let building = format!("{TEMPLATE}_building");
        postgres
            .execute(format!("DROP DATABASE IF EXISTS {building}").as_str())
            .await
            .expect("drop a leftover template build");
        postgres
            .execute(format!("CREATE DATABASE {building}").as_str())
            .await
            .expect("create the template");
        sinex_store::bootstrap(&url("", &building))
            .await
            .expect("apply the schema to the template");
        retry_while_busy(
            postgres,
            &format!("ALTER DATABASE {building} RENAME TO {TEMPLATE}"),
        )
        .await;
    }
    postgres
        .execute(format!("SELECT pg_advisory_unlock({TEMPLATE_LOCK})").as_str())
        .await
        .expect("template unlock");
}

/// Run a statement that needs the source database free of sessions. A closed
/// connection can linger on the server for a moment, so retry briefly.
async fn retry_while_busy(postgres: &mut PgConnection, statement: &str) {
    for attempt in 0.. {
        match postgres.execute(statement).await {
            Ok(_) => return,
            Err(error)
                if attempt < 100 && error.to_string().contains("being accessed by other users") =>
            {
                tokio::time::sleep(std::time::Duration::from_millis(50)).await;
            }
            Err(error) => panic!("{statement}: {error}"),
        }
    }
}

async fn connect(database: &str) -> PgConnection {
    PgConnection::connect(&url("", database))
        .await
        .expect("connect to the test cluster; run the tests through `xtask test`")
}

fn url(role: &str, database: &str) -> String {
    let host =
        std::env::var("PGHOST").expect("PGHOST is unset; run the tests through `xtask test`");
    let port =
        std::env::var("PGPORT").expect("PGPORT is unset; run the tests through `xtask test`");
    let user = if role.is_empty() {
        String::new()
    } else {
        format!("{role}@localhost")
    };
    format!("postgresql://{user}/{database}?host={host}&port={port}")
}
