//! Mapping store errors and committed outcomes to RPC errors.
use serde_json::{Value, json};
use sinex_protocol::rpc::{ErrorKind, RpcError};

pub(super) fn read_error(error: sinex_store::Error) -> RpcError {
    match error {
        sinex_store::Error::Database(sqlx::Error::Database(error))
            if error.code().as_deref() == Some("57014") =>
        {
            statement_cancelled(None)
        }
        sinex_store::Error::SourceStatementCancelled {
            configured_statement_timeout_ms,
        } => statement_cancelled(Some(configured_statement_timeout_ms)),
        sinex_store::Error::ProjectionUnavailable(reason) => RpcError::new(
            ErrorKind::Partial,
            reason,
            json!({"reason":"projection_unavailable","complete":false}),
        ),
        sinex_store::Error::ReplayRefused(reason) => {
            RpcError::new(ErrorKind::Refused, reason, Value::Null)
        }
        sinex_store::Error::Invalid(reason) => {
            RpcError::new(ErrorKind::BadQuery, reason, Value::Null)
        }
        _ => RpcError::new(
            ErrorKind::Internal,
            "source or event read unavailable",
            Value::Null,
        ),
    }
}

/// `read_error`, except that the store's own deadline reports `timeout_ms`.
pub(super) fn timed_read_error(timeout_ms: u64) -> impl Fn(sinex_store::Error) -> RpcError {
    move |error| match error {
        sinex_store::Error::Deadline => deadline_error(timeout_ms),
        error => read_error(error),
    }
}

pub(super) fn deadline_error(timeout_ms: u64) -> RpcError {
    RpcError::new(
        ErrorKind::Budget,
        "query execution deadline exceeded",
        json!({
            "reason": "execution_deadline",
            "timeout_ms": timeout_ms,
            "estimate": null,
            "suggestion": "Narrow the supported text, time, project, source or ID scope, or retry after contention ends.",
        }),
    )
}

fn statement_cancelled(source_timeout_ms: Option<u64>) -> RpcError {
    let mut context = json!({"reason":"statement_cancelled"});
    if let Some(limit) = source_timeout_ms {
        context["operation"] = json!("sources.list");
        context["phase"] = json!("primary_source_rows");
        context["configured_statement_timeout_ms"] = json!(limit);
    }
    // SQLSTATE 57014 also covers user cancellation, so the configured limit is
    // reported as context, not as the cause or the elapsed time.
    RpcError::new(ErrorKind::Budget, "database statement cancelled", context)
}

pub(super) fn exclusion_outcome(mut value: Value) -> Result<Value, RpcError> {
    // The exclusion may already be committed, so a value that cannot be shown
    // must not read as a failed mutation.
    let identity = json!({"action_id":value["action_id"],"operation_id":value["operation_id"],"outcome":"retained_but_unpresentable"});
    if crate::output::admit(&mut value).is_err() {
        return Err(RpcError::new(
            ErrorKind::Partial,
            "mutation committed but its outcome cannot be presented; retry the same action_id to read it",
            identity,
        ));
    }
    match value["state"].as_str() {
        Some("refused") => Err(RpcError::new(
            ErrorKind::Refused,
            "exclusion action refused",
            value,
        )),
        Some("not_found") => Err(RpcError::new(
            ErrorKind::NotFound,
            "raw interpretation not found",
            value,
        )),
        _ => Ok(value),
    }
}

pub(super) fn replay_outcome(value: Value) -> Result<Value, RpcError> {
    if value["state"] == "refused" {
        Err(RpcError::new(
            ErrorKind::Refused,
            "replay refused; prior committed chunks remain",
            value,
        ))
    } else {
        Ok(value)
    }
}

#[cfg(test)]
#[path = "errors_tests.rs"]
mod tests;
