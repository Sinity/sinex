//! Lifecycle operations through the one writer: archive, restore, tombstone
//! and undo, their comparisons, and resuming one that stopped early. Each
//! batch is one writer job, so live admission runs between batches; the batch
//! size follows how long the previous batch held the writer.
use crate::Result;
use crate::writer::WriterHandle;
use futures::{StreamExt, TryStreamExt};
use serde_json::{Value, json};
use sinex_model::lifecycle::{Act, Scope};
use sinex_model::{DbCheck, OperationId};
use sinex_store::lifecycle::{self, Comparison, Counts, Operation, Progress, Refusal, Step};
use sqlx::PgConnection;
use std::time::Duration;

/// How long one batch should hold the writer. Live batches wait at most
/// about this long behind an operation.
const BATCH_HOLD: Duration = Duration::from_millis(250);
const FIRST_ROOTS: i64 = 256;

/// How an operation ended, as a caller is told.
#[derive(Debug)]
pub(crate) enum Answer {
    /// Ended `ok`, or a comparison: the cascade.
    Done(Value),
    /// Refused before anything moved; nothing was recorded.
    Refused(String),
    /// Moved some batches, then met a refusal; resumable.
    StoppedEarly(String, Value),
}

impl Answer {
    /// The reply every surface gives: ok, refused (kind `refused`, nothing
    /// moved), or stopped early with what moved.
    pub(crate) fn reply(self) -> sinex_protocol::Reply {
        use command_surface::{Failure, FailureKind};
        match self {
            Self::Done(result) => sinex_protocol::Reply::Ok { result },
            Self::Refused(reason) => {
                sinex_protocol::Reply::failed(Failure::new(FailureKind::Refused, reason))
            }
            Self::StoppedEarly(reason, result) => sinex_protocol::Reply::StoppedEarly {
                failure: sinex_protocol::stopped_early(Failure::new(FailureKind::Refused, reason)),
                result,
            },
        }
    }
}

/// Start `act` over `scope`, or with `compare` report what it would move.
pub(crate) async fn run(
    read: &mut PgConnection,
    writer: &WriterHandle,
    act: Act,
    actor: &str,
    scope: Scope,
    compare: bool,
) -> Result<Answer> {
    let op = lifecycle::begin(read, act, actor, scope).await?;
    if compare {
        let Comparison { counts, refusals } =
            lifecycle::compare(read, &op, FIRST_ROOTS * 16).await?;
        return Ok(Answer::Done(json!({
            "compare_only": true,
            "act": act.as_db(),
            "scope": op.scope,
            "cascade": counts,
            "refusals": refusals,
            "would_refuse": !refusals.is_empty(),
        })));
    }
    let source = if matches!(op.scope, Scope::Source { .. }) {
        Some(lifecycle::source_roots(read, &op, None)?)
    } else {
        None
    };
    drive(writer, &op, Progress::default(), source).await
}

/// Automatic retirement continues its existing source operation after restart.
pub(crate) async fn retire_source(
    read: &mut PgConnection,
    writer: &WriterHandle,
    source: String,
) -> Result<Answer> {
    let scope = Scope::Source {
        source,
        after: None,
        before: None,
    };
    let value = serde_json::to_value(&scope)?;
    let existing = sqlx::query_scalar!(
        "SELECT id FROM core.operations WHERE actor='sinex' AND kind='archive' AND scope=$1 AND result IS DISTINCT FROM 'ok' ORDER BY at DESC LIMIT 1",
        value
    ).fetch_optional(&mut *read).await?;
    if let Some(id) = existing {
        resume(read, writer, OperationId(id)).await
    } else {
        run(read, writer, Act::Archive, "sinex", scope, false).await
    }
}

/// Continue an operation from its saved position.
pub(crate) async fn resume(
    read: &mut PgConnection,
    writer: &WriterHandle,
    operation: OperationId,
) -> Result<Answer> {
    let (op, progress) = lifecycle::stored(read, operation).await?;
    let source = if matches!(op.scope, Scope::Source { .. }) && progress.phase == 0 {
        Some(lifecycle::source_roots(read, &op, progress.after)?)
    } else {
        None
    };
    drive(writer, &op, progress, source).await
}

async fn drive(
    writer: &WriterHandle,
    op: &Operation,
    mut progress: Progress,
    source: Option<futures::stream::BoxStream<'_, sinex_store::Result<uuid::Uuid>>>,
) -> Result<Answer> {
    let mut source = source.map(StreamExt::fuse);
    let mut pending = None;
    let mut roots = FIRST_ROOTS;
    let mut total = Counts::default();
    loop {
        let selected = if let Some(stream) = &mut source {
            let mut ids = Vec::new();
            if let Some(id) = pending.take() {
                ids.push(id);
            }
            while (ids.len() as i64) < roots {
                let Some(id) = stream.try_next().await? else {
                    break;
                };
                ids.push(id);
            }
            pending = stream.try_next().await?;
            lifecycle::Roots::Source {
                operation: op.id,
                ids,
                complete: pending.is_none(),
            }
        } else {
            lifecycle::Roots::Query(roots)
        };
        let completed = writer.lifecycle_step(op, progress, selected).await?;
        let held = completed.held;
        match completed.step {
            Step::Moved {
                progress: saved,
                counts,
                done,
                ..
            } => {
                total.add(&counts);
                progress = saved;
                if done {
                    return Ok(Answer::Done(report(op, &progress, &total, &[])));
                }
                if held < BATCH_HOLD / 2 {
                    roots = roots.saturating_mul(2);
                } else if held > BATCH_HOLD * 2 && roots > 1 {
                    roots /= 2;
                }
            }
            Step::Refused(refusals) => {
                if progress.batches == 0 {
                    return Ok(Answer::Refused(refusal_line(op, &refusals)));
                }
                writer.lifecycle_stopped(op.id, refusals.clone()).await?;
                return Ok(Answer::StoppedEarly(
                    format!(
                        "{}; operation {} stopped early after {} batches; `resume {}` continues it once the cause is cleared",
                        refusal_line(op, &refusals),
                        op.id.0,
                        progress.batches,
                        op.id.0
                    ),
                    report(op, &progress, &total, &refusals),
                ));
            }
        }
    }
}

fn report(op: &Operation, progress: &Progress, moved: &Counts, refusals: &[Refusal]) -> Value {
    json!({
        "operation": op.id,
        "act": op.act.as_db(),
        "actor": op.actor,
        "scope": op.scope,
        "batches": progress.batches,
        "cascade": moved,
        "refusals": refusals,
        "result": if refusals.is_empty() { "ok" } else { "stopped_early" },
    })
}

fn refusal_line(op: &Operation, refusals: &[Refusal]) -> String {
    let shown: Vec<String> = refusals
        .iter()
        .take(5)
        .map(|r| format!("{}: {}", r.member, r.reason))
        .collect();
    let more = refusals.len().saturating_sub(shown.len());
    format!(
        "{} refused: {}{}",
        op.act.as_db(),
        shown.join("; "),
        if more > 0 {
            format!(" (and {more} more)")
        } else {
            String::new()
        }
    )
}

#[cfg(test)]
mod tests;
