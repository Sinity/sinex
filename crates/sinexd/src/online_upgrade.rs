//! Upgrade work that runs after start (`sinex_schema::online`): bounded steps
//! on their own connection, between which the daemon serves as usual. A stop
//! or crash mid-step leaves a state the next start resumes from.
use sinex_schema::online::{self, Progress};
use sqlx::PgConnection;
use std::time::Duration;
use tokio::sync::watch;

/// How long a failed step waits before it is tried again. An index build
/// that failed restarts from the beginning, so retries are not immediate.
const RETRY: Duration = Duration::from_secs(60);

/// Run the remaining steps, then wait for `stop`.
pub(crate) async fn run(
    mut conn: PgConnection,
    mut stop: watch::Receiver<bool>,
) -> Result<(), String> {
    let mut rewritten = 0u64;
    let mut worked = false;
    loop {
        if *stop.borrow() {
            return Ok(());
        }
        match online::step(&mut conn, online::BATCH).await {
            Ok(Progress::Advanced { rewritten: rows }) => {
                rewritten += rows;
                worked = true;
                tokio::task::yield_now().await;
            }
            Ok(Progress::Done) => break,
            Err(error) => {
                eprintln!("sinexd: upgrade work after start failed, retrying: {error}");
                tokio::select! {
                    _ = tokio::time::sleep(RETRY) => {}
                    _ = stop.changed() => {}
                }
            }
        }
    }
    if worked {
        eprintln!(
            "sinexd: upgrade work after start finished: {rewritten} search rows left the trigram index, which was rebuilt"
        );
    }
    let _ = stop.wait_for(|s| *s).await;
    Ok(())
}
