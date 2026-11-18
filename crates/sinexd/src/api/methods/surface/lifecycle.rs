//! Lifecycle steps retain their accepted operation before moving any batch.
use super::*;
use sinex_model::{
    EventId, OperationId,
    lifecycle::{Act, Scope},
};
use sinex_protocol::rows::{Rows, Step, StepOutcome, StepReply};
use sinex_store::{
    lifecycle::Operation,
    requests::{Accepted, CommittedStep},
};

impl Api {
    pub(super) async fn lifecycle_line(
        &self,
        request: &Rows,
        caller: &Caller,
        conn: &mut sqlx::PgConnection,
        question: Option<&sinex_store::query::Question>,
        accepted: Option<&Accepted>,
        index: usize,
    ) -> Result<StepOutcome> {
        let (act, options) = match &request.steps[index] {
            Step::Archive(options) => (Act::Archive, options),
            Step::Restore(options) => (Act::Restore, options),
            Step::Undo(options) => (Act::Undo, options),
            _ => unreachable!("lifecycle step"),
        };
        let writer = self.writer()?;
        if let Some(recorded) = accepted.and_then(|accepted| accepted.step(index)) {
            let recorded: CommittedStep = serde_json::from_value(recorded.clone())?;
            return Ok(lifecycle_outcome(recorded.reply));
        }
        let stored = accepted
            .and_then(|accepted| accepted.progress.get("lifecycle")?.get(index.to_string()));
        let operation: Operation = if let Some(stored) = stored {
            serde_json::from_value(stored.clone())?
        } else {
            let (scope, count, position) = if let Some(question) = question {
                let (scope, count) = question.lifecycle_scope(conn).await?;
                (scope, count, question.position)
            } else if let Some(text) = request
                .selection
                .as_deref()
                .filter(|text| text.starts_with("op:"))
            {
                let operation = text[3..]
                    .parse::<uuid::Uuid>()
                    .map_err(|_| Error::bad_request("op: needs an operation ID"))?;
                (
                    Scope::Operation {
                        operation: OperationId(operation),
                    },
                    1,
                    sinex_store::query::current_seq(conn).await?,
                )
            } else if let Some(named) = request.references.as_ref().cloned().or_else(|| {
                request
                    .selection
                    .as_ref()
                    .filter(|text| query::parse_reference(text).is_ok())
                    .map(|text| vec![text.clone()])
            }) {
                use sqlx::Connection;
                let mut snapshot = conn.begin().await?;
                sinex_store::query::read_one_snapshot(&mut snapshot).await?;
                let position = sinex_store::query::current_seq(&mut snapshot).await?;
                let mut events = Vec::with_capacity(named.len());
                for text in named {
                    let reference =
                        query::parse_reference(&text).map_err(|error| error.failure(&text))?;
                    events.push(EventId(
                        sinex_store::query::resolve(&mut snapshot, &reference).await?,
                    ));
                }
                snapshot.commit().await?;
                let count = events.len() as u64;
                (
                    Scope::Events {
                        events,
                        query: None,
                    },
                    count,
                    position,
                )
            } else {
                let text = request
                    .selection
                    .as_deref()
                    .expect("validated lifecycle selection");
                let question =
                    sinex_store::query::Question::accept(conn, text, &self.context()).await?;
                let (scope, count) = question.lifecycle_scope(conn).await?;
                (scope, count, question.position)
            };
            if count > 1 && !options.all && request.references.is_none() {
                return Ok(StepOutcome::Failed {
                    error: Error::refused(
                        "this query selects several rows; --all authorizes the complete selection",
                    )
                    .failure(),
                });
            }
            let mut operation =
                sinex_store::lifecycle::begin(conn, act, caller.actor, scope).await?;
            operation.start_seq = position;
            if let Some(accepted) = accepted.filter(|_| !options.dry_run) {
                sinex_store::requests::accept_lifecycle(conn, accepted.operation, index, operation)
                    .await?
            } else {
                operation
            }
        };
        let result = if options.dry_run {
            // The existing comparison shares scope, cascade and refusal checks;
            // it never records even an operation or request identity.
            crate::lifecycle::preview(conn, &operation).await?
        } else {
            crate::lifecycle::retry(conn, writer, &operation).await?
        };
        match result {
            crate::lifecycle::Answer::Refused(reason) => Ok(StepOutcome::Failed {
                error: Error::refused(reason).failure(),
            }),
            crate::lifecycle::Answer::Done(reply)
            | crate::lifecycle::Answer::StoppedEarly(_, reply) => {
                if let Some(accepted) = accepted.filter(|_| !options.dry_run) {
                    let receipt =
                        CommittedStep::new(reply.clone(), &sinex_store::Outcome::default());
                    let mut tx = sqlx::Connection::begin(&mut *conn).await?;
                    sinex_store::requests::save_step(
                        &mut tx,
                        accepted.operation,
                        index,
                        &serde_json::to_value(receipt)?,
                    )
                    .await?;
                    tx.commit().await?;
                }
                Ok(lifecycle_outcome(reply))
            }
        }
    }
}

pub(super) fn lifecycle_outcome(reply: Value) -> StepOutcome {
    if reply["result"] == "stopped_early" {
        let operation = reply["operation"].as_str().unwrap_or("unknown");
        StepOutcome::StoppedEarly {
            error: command_surface::Failure::new(
                command_surface::FailureKind::StoppedEarly,
                format!(
                    "operation {operation} stopped early; resume {operation} continues it once its refusals are cleared"
                ),
            ),
            reply,
        }
    } else {
        StepOutcome::Done {
            reply: StepReply::Written(reply),
        }
    }
}
