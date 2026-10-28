//! The daemon samples its own state every minute (how far the publisher, the
//! search index and each derivation are behind, how many keys wait, each
//! source's counts) and spools one line per part. The `sinex.telemetry`
//! binding reads the spool like any live capture. Samples use ordinary event
//! admission, visibility, replay and the commit log.
use sinex_model::Timestamp;
use sinex_source_core::drivers::live_stream::{Rotation, SpoolWriter};
use sinex_sources::sources::telemetry::Sample;
use sqlx::{Connection, PgConnection};
use std::path::{Path, PathBuf};
use std::time::Duration;
use tokio::sync::watch;

/// How often the daemon samples itself.
pub(crate) const EVERY: Duration = Duration::from_secs(60);

/// The spool the daemon writes its samples to when none is configured: one
/// beside the content store's own directories.
pub(crate) fn spool(store: &Path) -> PathBuf {
    store.join("telemetry")
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum Error {
    #[error("telemetry read: {0}")]
    Read(#[from] sinex_store::Error),
    #[error("telemetry spool: {0}")]
    Spool(#[from] std::io::Error),
}

/// Write one sample of every part to `writer`.
pub(crate) async fn sample(
    conn: &mut PgConnection,
    writer: &mut SpoolWriter,
) -> Result<usize, Error> {
    let readings = sinex_store::telemetry::read(conn).await?;
    let at = Timestamp::now();
    for r in &readings {
        let line = serde_json::to_vec(&Sample {
            component: r.component.to_owned(),
            name: r.name.clone(),
            measures: r.measures.clone(),
        })
        .expect("samples serialize");
        writer.append(at, &line)?;
    }
    Ok(readings.len())
}

/// A failed read has written no spool lines. Replace a dead connection before
/// retrying so a database restart does not disable all later samples.
async fn sample_reconnecting(
    conn: &mut PgConnection,
    database: &str,
    writer: &mut SpoolWriter,
) -> Result<usize, Error> {
    match sample(conn, writer).await {
        Err(Error::Read(error)) => {
            eprintln!("sinexd: telemetry read: {error}; reconnecting");
            *conn = PgConnection::connect(database)
                .await
                .map_err(sinex_store::Error::from)?;
            sample(conn, writer).await
        }
        result => result,
    }
}

/// Sample every [`EVERY`] until `stop` is set. A failed sample is reported
/// and the next one tried; telemetry never stops the daemon.
pub(crate) async fn run(
    connection: (PgConnection, String),
    spool: PathBuf,
    mut stop: watch::Receiver<bool>,
) -> Result<(), String> {
    let (mut conn, database) = connection;
    let mut writer = SpoolWriter::open(&spool, Rotation::default())
        .map_err(|e| format!("{}: {e}", spool.display()))?;
    loop {
        if let Err(error) = sample_reconnecting(&mut conn, &database, &mut writer).await {
            eprintln!("sinexd: telemetry: {error}");
        }
        tokio::select! {
            () = tokio::time::sleep(EVERY) => {}
            _ = stop.wait_for(|stopped| *stopped) => return Ok(()),
        }
    }
}

#[cfg(test)]
mod tests;
