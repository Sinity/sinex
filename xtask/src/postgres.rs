//! A throwaway PostgreSQL cluster for tests and scenarios. It lives in a
//! temporary directory and is stopped and removed on drop. Tests create their
//! own databases in it (see sinex-testkit). It never fsyncs: on `/realm` a
//! synced initdb alone takes a minute, and the tests kill processes, not machines.
use crate::Result;
use std::path::{Path, PathBuf};
use std::process::Command;

pub struct Cluster {
    dir: PathBuf,
    /// Short tmpfs path for the socket (Unix socket paths are limited to about
    /// 100 bytes) and for executables under test (the daemon hashes its own
    /// executable at startup, which is slow from a busy disk).
    fast: PathBuf,
    port: u16,
}

impl Cluster {
    /// Start a cluster with the `sinex_writer` and `sinex_reader` roles.
    pub fn start() -> Result<Self> {
        let unique = format!(
            "{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map_or(0, |d| d.subsec_nanos())
        );
        let dir = std::env::temp_dir().join(format!("sinex-pg-{unique}"));
        let tmpfs = if Path::new("/dev/shm").is_dir() {
            "/dev/shm"
        } else {
            "/tmp"
        };
        let fast = PathBuf::from(format!("{tmpfs}/sinex-pg-{unique}"));
        for path in [&dir, &fast] {
            std::fs::create_dir_all(path).map_err(|e| format!("{}: {e}", path.display()))?;
        }
        let port = std::net::TcpListener::bind("127.0.0.1:0")
            .and_then(|l| l.local_addr())
            .map_err(|e| format!("pick a port: {e}"))?
            .port();
        let cluster = Self { dir, fast, port };
        let data = cluster.dir.join("data");
        cluster.pg(Command::new("initdb")
            .args([
                "--no-sync",
                "--no-locale",
                "--auth=trust",
                "--data-checksums",
                "-D",
            ])
            .arg(&data))?;
        // No TimescaleDB background workers: they hold sessions on every
        // database, and a database cannot be cloned while it has sessions.
        let options = format!(
            "-k {} -c listen_addresses= -c shared_preload_libraries=timescaledb \
             -c timescaledb.telemetry_level=off -c timescaledb.enable_chunk_skipping=on \
             -c timescaledb.max_background_workers=0 -c max_connections=500 \
             -c fsync=off -c synchronous_commit=off -c full_page_writes=off -p {}",
            cluster.fast.display(),
            port
        );
        cluster.pg(Command::new("pg_ctl")
            .arg("-D")
            .arg(&data)
            .arg("-l")
            .arg(cluster.dir.join("postgres.log"))
            .args(["-o", &options, "-w", "start"]))?;
        for role in ["sinex_writer", "sinex_reader"] {
            cluster.pg(Command::new("createuser").arg(role))?;
        }
        Ok(cluster)
    }

    /// A tmpfs directory removed with the cluster, for executables under test.
    pub fn fast_dir(&self) -> &Path {
        &self.fast
    }

    /// Environment that points libpq, psql and sinex-testkit at this cluster.
    pub fn env(&self) -> Vec<(String, String)> {
        vec![
            ("PGHOST".into(), self.fast.display().to_string()),
            ("PGPORT".into(), self.port.to_string()),
        ]
    }

    /// Create database `name` and return its admin URL.
    pub fn create_database(&self, name: &str) -> Result<String> {
        self.pg(Command::new("createdb").arg(name))?;
        Ok(format!(
            "postgresql:///{name}?host={}&port={}",
            self.fast.display(),
            self.port
        ))
    }

    fn pg(&self, command: &mut Command) -> Result {
        let shown = format!("{command:?}");
        let output = command
            .envs(self.env())
            .env_remove("PGDATABASE")
            .env_remove("PGUSER")
            .output()
            .map_err(|e| format!("could not run {shown}: {e}"))?;
        if output.status.success() {
            Ok(())
        } else {
            Err(format!(
                "{shown} failed: {}\n{}",
                output.status,
                String::from_utf8_lossy(&output.stderr)
            ))
        }
    }
}

impl Drop for Cluster {
    fn drop(&mut self) {
        let _ = Command::new("pg_ctl")
            .arg("-D")
            .arg(self.dir.join("data"))
            .args(["-m", "immediate", "-w", "stop"])
            .output();
        let _ = std::fs::remove_dir_all(&self.dir);
        let _ = std::fs::remove_dir_all(&self.fast);
    }
}
