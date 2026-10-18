//! One handler per RPC method. Parameters were already checked by
//! `RpcRequest::validate`, so decoding them cannot fail.
use super::{
    App,
    errors::{deadline_error, exclusion_outcome, read_error, replay_outcome, timed_read_error},
};
use serde::de::DeserializeOwned;
use serde_json::{Value, json};
use sinex_protocol::rpc::{
    ErrorKind, Method, RpcError, RpcRequest, RpcResponse, SUPPORTED_METHODS, SchemaStatus, Status,
    VersionInfo, WriterOwnership,
};
use std::{sync::Arc, time::Duration};

/// Status, sources and single-event reads get a fixed 10 s deadline; queries
/// carry their own `timeout_ms`.
const READ_DEADLINE_MS: u64 = 10_000;

pub(super) async fn execute(state: Arc<App>, request: RpcRequest, method: Method) -> RpcResponse {
    let result = if matches!(
        method,
        Method::Status | Method::Sources | Method::Show | Method::Trace
    ) {
        tokio::time::timeout(
            Duration::from_millis(READ_DEADLINE_MS),
            dispatch(&state, &request.params, method),
        )
        .await
        .unwrap_or_else(|_| Err(deadline_error(READ_DEADLINE_MS)))
    } else {
        dispatch(&state, &request.params, method).await
    };
    match result {
        Ok(v) => RpcResponse::success(request.id, v),
        Err(e) => RpcResponse::failure(Some(request.id), e),
    }
}

async fn dispatch(state: &App, params: &Value, method: Method) -> Result<Value, RpcError> {
    match method {
        Method::Status => status(state).await,
        Method::Delete | Method::DeleteUndo => exclude(state, params, method).await,
        Method::ReplayPlan | Method::ReplayStart | Method::ReplayCancel => {
            replay(state, params, method).await
        }
        Method::ReplayStatus => replay_status(state, params).await,
        Method::Version => Ok(json!(VersionInfo {
            version: env!("CARGO_PKG_VERSION").into(),
            protocol: "2.0".into()
        })),
        Method::ConfigShow => {
            serde_json::to_value(&state.config).map_err(|_| internal("cannot encode configuration"))
        }
        Method::Catalog => Ok(json!(sinex_protocol::rpc::catalog())),
        Method::Sources => sources(state, params).await,
        Method::Query | Method::Search => query(state, params, method).await,
        Method::Show | Method::Trace => show(state, params, method).await,
    }
}

pub(super) fn decode<T: DeserializeOwned>(params: &Value) -> T {
    serde_json::from_value(params.clone()).expect("parameters validated by RpcRequest::validate")
}

pub(super) fn internal(message: &'static str) -> RpcError {
    RpcError::new(ErrorKind::Internal, message, Value::Null)
}

async fn status(state: &App) -> Result<Value, RpcError> {
    let reader = &state.reader;
    let (Ok(schema), Ok(status), Ok(replays)) = (
        reader.schema_status().await,
        reader.status().await,
        reader.replay_operations().await,
    ) else {
        return Err(internal("database schema or status unavailable"));
    };
    let mut value = serde_json::to_value(Status {
        schema: SchemaStatus {
            version: i64::from(schema.version),
            expected_version: 1,
            compatible: true,
            pending_changes: vec![],
            unavailable_paths: vec![],
        },
        writer: if state.admission.is_available() {
            WriterOwnership::Owned
        } else {
            WriterOwnership::Unavailable {
                reason: "admission owner has stopped".into(),
            }
        },
        commit_seq: status.commit_seq,
        replays,
        replay_authorized: state.operator_token.is_some(),
        indexed_retrieval: Value::Null,
        supported_sources: state.config.sources.clone(),
        supported_methods: SUPPORTED_METHODS.iter().map(|s| s.to_string()).collect(),
    })
    .map_err(|_| internal("cannot encode status"))?;
    value["indexed_retrieval"] = reader.projection_status().await.map_err(read_error)?;
    value["indexed_retrieval"]["configured"] = json!(state.indexed);
    Ok(value)
}

async fn exclude(state: &App, params: &Value, method: Method) -> Result<Value, RpcError> {
    let action = if method == Method::Delete {
        sinex_model::exclusion::Action::Exclude
    } else {
        sinex_model::exclusion::Action::Undo
    };
    state
        .admission
        .exclude(action, decode(params))
        .await
        .map_err(|error| match error {
            sinex_store::Error::Invalid(_) | sinex_store::Error::ReplayRefused(_) => {
                read_error(error)
            }
            _ => RpcError::new(
                ErrorKind::Partial,
                "mutation outcome unavailable; retry the same action_id",
                json!({"outcome":"indeterminate","action_id":params["action_id"]}),
            ),
        })
        .and_then(exclusion_outcome)
}

async fn replay(state: &App, params: &Value, method: Method) -> Result<Value, RpcError> {
    let command = match method {
        Method::ReplayPlan => sinex_store::ReplayCommand::Plan(decode(params), state.rules.clone()),
        Method::ReplayStart => sinex_store::ReplayCommand::Start(decode(params)),
        _ => sinex_store::ReplayCommand::Cancel(decode(params)),
    };
    let mutation = method != Method::ReplayPlan;
    state
        .admission
        .replay(command)
        .await
        .map_err(|error| match error {
            sinex_store::Error::Invalid(_) | sinex_store::Error::ReplayRefused(_) => {
                read_error(error)
            }
            _ if mutation => RpcError::new(
                ErrorKind::Partial,
                "mutation outcome unavailable; retry the same action_id or inspect the existing operation",
                json!({
                    "outcome": "indeterminate",
                    "action_id": params.get("action_id"),
                    "operation_id": params.get("operation_id"),
                }),
            ),
            _ => read_error(error),
        })
        .and_then(replay_outcome)
}

async fn replay_status(state: &App, params: &Value) -> Result<Value, RpcError> {
    let params: sinex_protocol::ReplayOperation = decode(params);
    state
        .reader
        .replay_status(params.operation_id)
        .await
        .map_err(read_error)?
        .ok_or_else(|| {
            RpcError::new(
                ErrorKind::NotFound,
                "replay operation not found",
                Value::Null,
            )
        })
        .and_then(replay_outcome)
}

async fn sources(state: &App, params: &Value) -> Result<Value, RpcError> {
    use sinex_model::acquisition::CollectorScope;
    let config = &state.config;
    let atuin = config.atuin.iter().map(|atuin| CollectorScope {
        source: sinex_store::ATUIN_SOURCE.into(),
        locator: atuin.path.to_string_lossy().into_owned(),
        run: state.run_id,
    });
    let browsers = config.chromium.iter().map(|browser| CollectorScope {
        source: sinex_model::browser::SOURCE.into(),
        locator: json!([browser.profile, browser.database_generation]).to_string(),
        run: state.run_id,
    });
    state
        .reader
        .inspect_sources(decode(params), atuin.chain(browsers).collect())
        .await
        .map_err(read_error)
}

async fn query(state: &App, params: &Value, method: Method) -> Result<Value, RpcError> {
    let params: sinex_protocol::QueryParams = decode(params);
    let timeout_ms = params.timeout_ms.unwrap_or(10_000);
    let timeout = Duration::from_millis(timeout_ms);
    let cursor = params.cursor.as_deref();
    let reader = &state.reader;
    if sinex_store::is_recall(&params.query) {
        let page = params.page.unwrap_or(100);
        return reader
            .recall_with_timeout(&params.query, &state.projects, cursor, page, timeout)
            .await
            .map_err(timed_read_error(timeout_ms));
    }
    if method == Method::Search || sinex_store::is_indexed_query(&params.query) {
        sinex_model::retrieval::bind(&params.query)
            .map_err(|message| RpcError::new(ErrorKind::BadQuery, message, Value::Null))?;
        if !state.indexed {
            return Err(read_error(sinex_store::Error::ProjectionUnavailable(
                "indexed retrieval is not configured",
            )));
        }
        let page = params.page.unwrap_or(20);
        return reader
            .search_with_timeout(&params.query, cursor, page, timeout)
            .await
            .map_err(timed_read_error(timeout_ms));
    }
    if params.page.is_some() {
        return Err(fixed_page_error());
    }
    sinex_store::validate_query(&params.query)
        .map_err(|e| RpcError::new(ErrorKind::BadQuery, e.0, Value::Null))?;
    reader
        .query_with_timeout(&params.query, cursor, timeout)
        .await
        .map_err(timed_read_error(timeout_ms))
}

pub(super) fn fixed_page_error() -> RpcError {
    RpcError::new(
        ErrorKind::BadQuery,
        "raw event queries use a fixed 100-row page and take no page size",
        Value::Null,
    )
}

async fn show(state: &App, params: &Value, method: Method) -> Result<Value, RpcError> {
    let params: sinex_protocol::ShowParams = decode(params);
    let id = event_id(&params.reference);
    let event = if method == Method::Trace {
        state.reader.trace(id, params.bytes, &state.rules).await
    } else {
        state.reader.show(id).await
    }
    .map_err(read_error)?
    .ok_or_else(|| RpcError::new(ErrorKind::NotFound, "live event not found", Value::Null))?;
    let removed = event.get("record_state") == Some(&json!("removed"))
        || event.get("source_state") == Some(&json!("removed at source"));
    let resolved = if removed {
        "removed at source"
    } else if event.get("archive").is_some_and(|a| !a.is_null()) {
        "archived"
    } else {
        "live"
    };
    Ok(shown(id, resolved, event))
}

/// Accept both `sinex:ev/<id>` and a bare id.
pub(super) fn event_id(reference: &str) -> &str {
    reference.strip_prefix("sinex:ev/").unwrap_or(reference)
}

pub(super) fn shown(id: &str, resolved: &str, row: Value) -> Value {
    json!({
        "notice": sinex_protocol::DATA_NOTICE,
        "ref": format!("sinex:ev/{id}"),
        "resolved": resolved,
        "row": row,
    })
}
