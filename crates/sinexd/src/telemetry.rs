//! The daemon samples its own state every minute (how far the publisher, the
//! search index and each derivation are behind, how many keys wait, each
//! source's counts) and spools one line per part. The `sinex.telemetry`
//! binding reads the spool like any live capture, and admission keeps those
//! events in `telemetry.events`, apart from the activity record.
use sinex_model::Timestamp;
use sinex_source_core::drivers::live_stream::{Rotation, SpoolWriter};
use sinex_sources::sources::telemetry::Sample;
use sqlx::PgConnection;
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

/// Write one sample of every part to `writer`.
pub(crate) async fn sample(
    conn: &mut PgConnection,
    writer: &mut SpoolWriter,
) -> Result<usize, String> {
    let readings = sinex_store::telemetry::read(conn)
        .await
        .map_err(|e| e.to_string())?;
    let at = Timestamp::now();
    for r in &readings {
        let line = serde_json::to_vec(&Sample {
            component: r.component.to_owned(),
            name: r.name.clone(),
            measures: r.measures.clone(),
        })
        .expect("samples serialize");
        writer
            .append(at, &line)
            .map_err(|e| format!("telemetry spool: {e}"))?;
    }
    Ok(readings.len())
}

/// Sample every [`EVERY`] until `stop` is set. A failed sample is reported
/// and the next one tried; telemetry never stops the daemon.
pub(crate) async fn run(
    mut conn: PgConnection,
    spool: PathBuf,
    mut stop: watch::Receiver<bool>,
) -> Result<(), String> {
    let mut writer = SpoolWriter::open(&spool, Rotation::default())
        .map_err(|e| format!("{}: {e}", spool.display()))?;
    loop {
        if let Err(error) = sample(&mut conn, &mut writer).await {
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
