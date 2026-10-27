//! The commit-log reader for read models: one task that reads committed events
//! in commit order from `core.commit_log` and feeds every read-model maintainer
//! (never database triggers). Each maintainer updates its tables and its
//! position (`core.consumer_positions`) in one transaction, so a restart
//! resumes exactly where it stopped: nothing is skipped, and nothing is applied
//! twice.
//!
//! One page is read for all consumers, from the lowest position among them;
//! each consumer gets the part of the page after its own position.
//! Per-event derivations are run by the derivation runner (`derive.rs`).
use futures::future::BoxFuture;
use sinex_store::commit_log::{self, Committed};
use sqlx::{Connection, PgConnection};
use std::time::Duration;
use tokio::sync::watch;

/// Immediately available commits per maintenance batch. The commit reader's
/// payload byte budget also applies; an oversized first event travels alone.
const PAGE: i64 = 10_000;
/// How long a failed consumer waits before it is offered the same events again.
const RETRY: Duration = Duration::from_secs(1);

/// Something fed committed events in commit order.
pub(crate) trait Consumer: Send + Sync {
    fn name(&self) -> &str;

    /// Whether the consumer reads events of this type. Events it does not
    /// read still advance its position.
    fn wants(&self, event_type: &str) -> bool;

    /// The saved position: the last commit position already handled.
    fn position<'a>(&'a self, conn: &'a mut PgConnection) -> BoxFuture<'a, Result<i64, String>>;

    /// Handle `events` (those it wants, in commit order) and durably record
    /// `up_to` as the new position together with the effect.
    fn handle<'a>(
        &'a mut self,
        events: &'a [Committed],
        up_to: i64,
        conn: &'a mut PgConnection,
    ) -> BoxFuture<'a, Result<(), String>>;
}

/// Run the reader until `stop` is set. `commits` changes whenever the writer
/// commits a batch.
pub(crate) async fn run(
    mut conn: PgConnection,
    mut consumers: Vec<Box<dyn Consumer>>,
    mut commits: watch::Receiver<u64>,
    mut stop: watch::Receiver<bool>,
) -> Result<(), String> {
    if consumers.is_empty() {
        let _ = stop.wait_for(|s| *s).await;
        return Ok(());
    }
    let measure = std::env::var_os("SINEX_BENCH_TIMINGS").is_some();
    let mut positions = Vec::with_capacity(consumers.len());
    for consumer in &consumers {
        positions.push(consumer.position(&mut conn).await?);
    }
    loop {
        if *stop.borrow() {
            return Ok(());
        }
        commits.borrow_and_update();
        let from = positions.iter().copied().min().unwrap_or(0);
        let read_started = std::time::Instant::now();
        let page = commit_log::after(&mut conn, from, PAGE, sinex_store::PAGE_BYTES)
            .await
            .map_err(|e| e.to_string())?;
        let Some(up_to) = page.up_to else {
            tokio::select! {
                _ = commits.changed() => {}
                _ = stop.changed() => {}
            }
            continue;
        };
        let read_time = read_started.elapsed();
        let mut failed = false;
        for (consumer, position) in consumers.iter_mut().zip(positions.iter_mut()) {
            if *position >= up_to {
                continue;
            }
            let mine: Vec<Committed> = page
                .events
                .iter()
                .filter(|e| e.commit_seq > *position && consumer.wants(&e.event_type))
                .cloned()
                .collect();
            let started = std::time::Instant::now();
            match consumer.handle(&mine, up_to, &mut conn).await {
                Ok(()) => {
                    if measure {
                        eprintln!(
                            "{}",
                            serde_json::json!({"search_batch": {
                                "consumer": consumer.name(), "events": mine.len(),
                                "from": *position, "up_to": up_to,
                                "timings_ms": {
                                    "read": read_time.as_secs_f64() * 1000.0,
                                    "apply": started.elapsed().as_secs_f64() * 1000.0,
                                }
                            }})
                        );
                    }
                    *position = up_to;
                }
                Err(error) => {
                    eprintln!(
                        "sinexd: {} failed after commit position {}: {error}",
                        consumer.name(),
                        *position
                    );
                    failed = true;
                }
            }
        }
        if failed {
            tokio::select! {
                _ = tokio::time::sleep(RETRY) => {}
                _ = stop.changed() => {}
            }
        }
    }
}

/// A table kept only to make reads fast, maintained from committed events:
/// never by database triggers.
pub(crate) trait ReadModel: Send + Sync {
    fn name(&self) -> &'static str;
    fn wants(&self, event_type: &str) -> bool;
    /// Apply `events` inside the open transaction `tx`.
    fn apply<'a>(
        &'a self,
        events: &'a [Committed],
        tx: &'a mut PgConnection,
    ) -> BoxFuture<'a, Result<(), String>>;
}

/// Every read model, in one list.
pub(crate) fn read_models() -> Vec<Box<dyn ReadModel>> {
    vec![Box::new(SearchText)]
}

/// The full-text read model `core.search_text`: every event's payload text,
/// normalized, for the query language's words.
struct SearchText;

impl ReadModel for SearchText {
    fn name(&self) -> &'static str {
        sinex_store::read_models::SEARCH_TEXT
    }

    fn wants(&self, _event_type: &str) -> bool {
        true
    }

    fn apply<'a>(
        &'a self,
        events: &'a [Committed],
        tx: &'a mut PgConnection,
    ) -> BoxFuture<'a, Result<(), String>> {
        Box::pin(async move {
            sinex_store::read_models::apply_search_text(tx, events)
                .await
                .map_err(|e| e.to_string())
        })
    }
}

/// A read model fed by the reader: its tables and its position change in one
/// transaction.
pub(crate) struct ReadModelRunner(pub Box<dyn ReadModel>);

impl Consumer for ReadModelRunner {
    fn name(&self) -> &str {
        self.0.name()
    }

    fn wants(&self, event_type: &str) -> bool {
        self.0.wants(event_type)
    }

    fn position<'a>(&'a self, conn: &'a mut PgConnection) -> BoxFuture<'a, Result<i64, String>> {
        Box::pin(async move {
            commit_log::consumer_position(conn, self.0.name())
                .await
                .map(|(seq, _)| seq)
                .map_err(|e| e.to_string())
        })
    }

    fn handle<'a>(
        &'a mut self,
        events: &'a [Committed],
        up_to: i64,
        conn: &'a mut PgConnection,
    ) -> BoxFuture<'a, Result<(), String>> {
        Box::pin(async move {
            let mut tx = conn.begin().await.map_err(|e| e.to_string())?;
            self.0.apply(events, &mut tx).await?;
            commit_log::set_consumer_position(&mut tx, self.0.name(), up_to, 0)
                .await
                .map_err(|e| e.to_string())?;
            tx.commit().await.map_err(|e| e.to_string())
        })
    }
}

#[cfg(test)]
mod tests;
