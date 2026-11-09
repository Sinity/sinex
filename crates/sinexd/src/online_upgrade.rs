//! Upgrade work that runs after start (`sinex_schema::online`): bounded steps
//! on their own connection, between which the daemon serves as usual. A stop
//! or crash mid-step leaves a state the next start resumes from.
use crate::Result;
use sinex_schema::online::{self, Progress};
use sqlx::PgConnection;
use std::time::Duration;
use tokio::sync::watch;

/// How long a failed step, or one waiting for another session's work, waits
/// before it is tried again. A failed index build restarts from the
/// beginning, so retries are not immediate.
const RETRY: Duration = Duration::from_secs(60);

/// Run the remaining steps, then wait for `stop`.
pub(crate) async fn run(mut conn: PgConnection, mut stop: watch::Receiver<bool>) -> Result<()> {
    loop {
        if *stop.borrow() {
            return Ok(());
        }
        match online::step(&mut conn, online::BATCH).await {
            Ok(Progress::Advanced { .. }) => tokio::task::yield_now().await,
            Ok(Progress::Finished { task }) => {
                eprintln!("sinexd: upgrade work after start: {} done", task.name());
            }
            Ok(Progress::Waiting) => {
                tokio::select! {
                    _ = tokio::time::sleep(RETRY) => {}
                    _ = stop.changed() => {}
                }
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
    let _ = stop.wait_for(|s| *s).await;
    Ok(())
}
