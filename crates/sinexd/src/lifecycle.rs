//! Lifecycle operations through the one writer: archive, restore, tombstone
//! and undo, their comparisons, and resuming one that stopped early. Each
//! batch is one writer job, so live admission runs between batches; the batch
//! size follows how long the previous batch held the writer.
use crate::Result;
use crate::writer::{WriteError, WriterHandle};
use futures::{StreamExt, TryStreamExt};
use serde_json::{Value, json};
use sinex_model::lifecycle::{Act, Scope};
use sinex_model::{DbCheck, OperationId};
use sinex_store::lifecycle::{self, Comparison, Counts, Operation, Progress, Refusal, Step};
use sqlx::PgConnection;
use std::collections::VecDeque;

type SourceRoots<'a> = futures::stream::BoxStream<'a, sinex_store::Result<uuid::Uuid>>;

const FIRST_ROOTS: i64 = 256;
const FIRST_MATERIAL_ONLY_ROOTS: i64 = 16;

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
    if matches!(scope, Scope::MaterialOnly { .. }) && actor != "operator" {
        return Err(crate::Error::refused(
            "only the operator can select material-only readings",
        ));
    }
    let op = lifecycle::begin(read, act, actor, scope).await?;
    if compare {
        return preview(read, &op).await;
    }
    crate::request_deadline::handoff_read(read).await?;
    let source = source_roots(read, &op, None)?;
    drive(writer, &op, Progress::default(), source).await
}

fn source_roots<'a>(
    read: &'a mut PgConnection,
    op: &'a Operation,
    after: Option<uuid::Uuid>,
) -> Result<Option<SourceRoots<'a>>> {
    Ok(match &op.scope {
        Scope::Source { .. } => Some(lifecycle::source_roots(read, op, after)?),
        Scope::MaterialOnly {
            source,
            parser_version,
            ..
        } => {
            let rule = sinex_sources::sources::sinnix_lake::lake_source(source)
                .filter(|source| source.spec.parser_version == *parser_version)
                .and_then(|source| source.material_only)
                .ok_or_else(|| {
                    crate::Error::refused("the operation's parser rule is unavailable")
                })?;
            Some(lifecycle::material_only_roots(
                read,
                op,
                after,
                rule.event_type,
                rule.record_field,
                rule.matches,
            )?)
        }
        _ => None,
    })
}

pub(crate) async fn preview(read: &mut PgConnection, op: &Operation) -> Result<Answer> {
    if matches!(op.scope, Scope::MaterialOnly { .. }) {
        let mut roots = source_roots(read, op, None)?.expect("material-only scope streams roots");
        let mut matching_readings = 0u64;
        while roots.try_next().await?.is_some() {
            matching_readings += 1;
        }
        return Ok(Answer::Done(json!({
            "compare_only": true,
            "act": op.act.as_db(),
            "scope": op.scope,
            "matching_readings": matching_readings,
        })));
    }
    let Comparison { counts, refusals } = lifecycle::compare(read, op, FIRST_ROOTS * 16).await?;
    Ok(Answer::Done(json!({
        "compare_only": true,
        "act": op.act.as_db(),
        "scope": op.scope,
        "cascade": counts,
        "refusals": refusals,
        "would_refuse": !refusals.is_empty(),
    })))
}

/// Archiving a removed producer's outputs continues its existing source operation after restart.
pub(crate) async fn archive_removed_source(
    read: &mut PgConnection,
    writer: &WriterHandle,
    source: String,
) -> Result<Answer> {
    let scope = Scope::Source {
        source,
        after: None,
        before: None,
    };
    let existing = lifecycle::unfinished(read, Act::Archive, "sinex", &scope).await?;
    if let Some(id) = existing {
        resume(read, writer, id).await
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
    crate::request_deadline::handoff_read(read).await?;
    let source = if progress.phase == 0 {
        source_roots(read, &op, progress.after)?
    } else {
        None
    };
    let answer = drive(writer, &op, progress, source).await?;
    if let Some(recorded) = lifecycle::recorded(read, op.id).await? {
        let receipt = report(
            &op,
            &recorded.progress,
            &recorded.counts,
            &recorded.refusals,
        );
        return Ok(match answer {
            Answer::Done(_) => Answer::Done(receipt),
            Answer::StoppedEarly(reason, _) => Answer::StoppedEarly(reason, receipt),
            refused => refused,
        });
    }
    Ok(answer)
}

/// A typed request owns one operation identity, including the interval after
/// its final batch commits and before the client receives its receipt.
pub(crate) async fn retry(
    read: &mut PgConnection,
    writer: &WriterHandle,
    op: &Operation,
) -> Result<Answer> {
    loop {
        let stored = lifecycle::recorded(read, op.id).await?;
        let progress = if let Some(stored) = stored {
            let progress = stored.progress;
            if let Some(result) = stored.result {
                let counts = stored.counts;
                let refusals = stored.refusals;
                let value = report(op, &progress, &counts, &refusals);
                return Ok(if result == "ok" {
                    Answer::Done(value)
                } else {
                    Answer::StoppedEarly(refusal_line(op, &refusals), value)
                });
            }
            progress
        } else {
            Progress::default()
        };
        crate::request_deadline::handoff_read(read).await?;
        let source = if progress.phase == 0 {
            source_roots(read, op, progress.after)?
        } else {
            None
        };
        let result = drive(writer, op, progress, source).await?;
        if matches!(result, Answer::Refused(_)) {
            return Ok(result);
        }
        // The operation owns cumulative counts, including batches from an earlier
        // attempt. Read those after the driver finishes rather than reconstructing
        // the receipt from only this attempt's batches.
    }
}

async fn drive(
    writer: &WriterHandle,
    op: &Operation,
    mut progress: Progress,
    source: Option<SourceRoots<'_>>,
) -> Result<Answer> {
    let mut source = source.map(StreamExt::fuse);
    let material_only = matches!(op.scope, Scope::MaterialOnly { .. });
    let mut pending = VecDeque::new();
    let mut roots = if material_only {
        FIRST_MATERIAL_ONLY_ROOTS
    } else {
        FIRST_ROOTS
    };
    let mut total = Counts::default();
    loop {
        let selected = if let Some(stream) = &mut source {
            while (pending.len() as i64) <= roots {
                let Some(id) = stream.try_next().await? else {
                    break;
                };
                pending.push_back(id);
            }
            lifecycle::Roots::Source {
                operation: op.id,
                ids: pending.iter().copied().take(roots as usize).collect(),
                complete: (pending.len() as i64) <= roots,
            }
        } else {
            lifecycle::Roots::Query(roots)
        };
        let completed = match writer.lifecycle_step(op, selected).await {
            Err(WriteError::LifecycleBatchTimeout) if material_only && roots > 1 => {
                roots /= 2;
                continue;
            }
            Err(WriteError::LifecycleBatchTimeout) if material_only && progress.batches > 0 => {
                let refusals = vec![Refusal {
                    member: pending.front().copied().unwrap_or(op.id.0),
                    reason: "one root exceeded the database statement timeout".into(),
                }];
                writer.lifecycle_stopped(op.id, refusals.clone()).await?;
                return Ok(Answer::StoppedEarly(
                    format!(
                        "{}; resume {} continues it",
                        refusal_line(op, &refusals),
                        op.id.0
                    ),
                    report(op, &progress, &total, &refusals),
                ));
            }
            result => result?,
        };
        pending.drain(..pending.len().min(roots as usize));
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
                roots = i64::try_from(crate::writer::next_bulk_size(
                    usize::try_from(roots).unwrap_or(1),
                    held,
                ))
                .unwrap_or(i64::MAX);
            }
            Step::Refused { progress, refusals } => {
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
    let mut report = json!({
        "operation": op.id,
        "act": op.act.as_db(),
        "actor": op.actor,
        "scope": op.scope,
        "batches": progress.batches,
        "cascade": moved,
        "refusals": refusals,
        "result": if refusals.is_empty() { "ok" } else { "stopped_early" },
    });
    if op.act != Act::Tombstone {
        report["undo"] = json!(format!("sinexctl op:{} undo", op.id));
    }
    report
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
