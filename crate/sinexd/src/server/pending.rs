//! Read-only mode used when the installed schema has changes waiting to be
//! applied: no writer, no sources, and only status, raw queries and
//! single-event reads against the installed schema.
use super::{
    BODY_LIMIT, accept,
    errors::{deadline_error, read_error},
    methods::{decode, event_id, fixed_page_error, internal, shown},
    present_response, shutdown_signal,
};
use crate::config::Config;
use axum::{
    Router,
    body::Bytes,
    extract::{DefaultBodyLimit, State},
    http::HeaderMap,
    response::{IntoResponse, Response},
    routing::post,
};
use serde_json::{Value, json};
use sinex_protocol::rpc::{
    ErrorKind, Method, RpcError, RpcResponse, SchemaStatus, Status, WriterOwnership,
};
use std::{sync::Arc, time::Duration};

struct PendingApp {
    rules: Vec<sinex_store::PrivacyRule>,
    reader: sinex_store::ReadPool,
    token: String,
    operator_token: Option<String>,
}

pub(super) async fn serve(
    listener: tokio::net::TcpListener,
    config: Config,
    token: String,
    operator_token: Option<String>,
    reader: String,
) -> Result<(), String> {
    let rules = config.rules();
    let reader = sinex_store::ReadPool::installed(&reader, config.store_dir)
        .await
        .map_err(crate::public_store_error)?;
    let app = Router::new()
        .route("/rpc", post(rpc))
        .layer(DefaultBodyLimit::max(BODY_LIMIT))
        .with_state(Arc::new(PendingApp {
            rules,
            reader,
            token,
            operator_token,
        }));
    axum::serve(listener, app)
        .with_graceful_shutdown(shutdown_signal())
        .await
        .map_err(|_| "pending schema HTTP server failed".to_owned())
}

async fn rpc(State(state): State<Arc<PendingApp>>, headers: HeaderMap, body: Bytes) -> Response {
    let (request, method, _) = match accept(
        &headers,
        &body,
        &state.token,
        state.operator_token.as_deref(),
    ) {
        Ok(accepted) => accepted,
        Err(response) => return *response,
    };
    let result = tokio::time::timeout(
        Duration::from_secs(10),
        dispatch(&state, &request.params, method),
    )
    .await
    .unwrap_or_else(|_| Err(deadline_error(10_000)));
    present_response(match result {
        Ok(v) => RpcResponse::success(request.id, v),
        Err(e) => RpcResponse::failure(Some(request.id), e),
    })
    .into_response()
}

async fn dispatch(state: &PendingApp, params: &Value, method: Method) -> Result<Value, RpcError> {
    let schema = state.reader.schema_status().await.map_err(read_error)?;
    let numbers = schema
        .pending_changes
        .iter()
        .map(|c| c.number)
        .collect::<Vec<_>>();
    let unavailable = vec![
        "writer/source acquisition".to_owned(),
        "replay/exclusion".to_owned(),
        "canonical/indexed queries".to_owned(),
    ];
    match method {
        Method::Status => {
            let status = state.reader.status().await.map_err(read_error)?;
            let index_state = if numbers.is_empty() {
                "restart_required"
            } else {
                "schema_changes_pending"
            };
            serde_json::to_value(Status {
                schema: SchemaStatus {
                    version: 1,
                    expected_version: 1,
                    compatible: numbers.is_empty(),
                    pending_changes: numbers,
                    unavailable_paths: unavailable,
                },
                writer: WriterOwnership::Unavailable {
                    reason: "installed-schema read mode; apply waiting changes with admin schema --apply --dump-file and restart".into(),
                },
                commit_seq: status.commit_seq,
                supported_sources: vec![],
                supported_methods: ["status", "query.run", "object.show", "object.trace"]
                    .map(String::from)
                    .to_vec(),
                replays: vec![],
                replay_authorized: false,
                indexed_retrieval: json!({"state":index_state,"mode":"installed_schema_read_only"}),
            })
            .map_err(|_| internal("cannot encode status"))
        }
        Method::Query => {
            let params: sinex_protocol::QueryParams = decode(params);
            if params.page.is_some() {
                return Err(fixed_page_error());
            }
            sinex_store::validate_installed_raw_query(&params.query)
                .map_err(|e| RpcError::new(ErrorKind::BadQuery, e.0, Value::Null))?;
            state
                .reader
                .query_with_timeout(
                    &params.query,
                    params.cursor.as_deref(),
                    Duration::from_millis(params.timeout_ms.unwrap_or(10_000)),
                )
                .await
                .map_err(read_error)
        }
        Method::Show | Method::Trace => {
            let params: sinex_protocol::ShowParams = decode(params);
            let id = event_id(&params.reference);
            let row = if method == Method::Trace {
                state.reader.trace(id, params.bytes, &state.rules).await
            } else {
                state.reader.show(id).await
            }
            .map_err(read_error)?
            .ok_or_else(|| RpcError::new(ErrorKind::NotFound, "event not found", Value::Null))?;
            let resolved = if row.get("archive").is_some_and(|v| !v.is_null()) {
                "archived"
            } else {
                "live"
            };
            Ok(shown(id, resolved, row))
        }
        _ => Err(RpcError::new(
            ErrorKind::Partial,
            "path unavailable until ordered schema changes are applied",
            json!({"pending_changes":numbers,"unavailable_paths":unavailable}),
        )),
    }
}
