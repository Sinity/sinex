use crate::config::{Config, secret};
use axum::{
    Router,
    body::{Body, Bytes},
    extract::{DefaultBodyLimit, State},
    http::{HeaderMap, StatusCode},
    response::IntoResponse,
    routing::post,
};
use fs2::FileExt;
use sinex_protocol::rpc::{
    ErrorKind, Method, RpcError, RpcRequest, RpcResponse, SUPPORTED_METHODS, SchemaStatus, Status,
    VersionInfo, WriterOwnership,
};
use std::sync::Arc;
struct App {
    run_id: uuid::Uuid,
    config: Config,
    token: String,
    operator_token: Option<String>,
    indexed: bool,
    reader: sinex_store::ReadPool,
    admission: sinex_store::Admission,
}
pub async fn serve(
    config: Config,
    qualification: Option<sinex_store::QualificationBoundary>,
    historical_fixture: bool,
) -> Result<(), String> {
    eprintln!(
        "{}",
        serde_json::json!({"component":"sinexd","phase":"starting"})
    );
    // Reserve endpoint before acquiring database writer ownership or creating state.
    let listener = tokio::net::TcpListener::bind(&config.listen)
        .await
        .map_err(|_| "configured endpoint is occupied or unavailable".to_string())?;
    eprintln!(
        "{}",
        serde_json::json!({"component":"sinexd","phase":"endpoint_reserved"})
    );
    let token = secret(&config.token_file)?;
    let operator_token = config
        .operator_token_file
        .as_deref()
        .map(secret)
        .transpose()?;
    if operator_token.as_ref() == Some(&token) {
        return Err("operator credential must differ from the read-only credential".into());
    }
    let writer = secret(&config.writer_connection_file)?;
    let reader = secret(&config.reader_connection_file)?;
    let installed = sinex_store::check_schema(&reader)
        .await
        .map_err(super::public_store_error)?;
    if !installed.pending_changes.is_empty() {
        return serve_pending(listener, config, token, operator_token, reader).await;
    }
    std::fs::create_dir_all(&config.state_dir)
        .map_err(|_| "cannot create configured state directory".to_string())?;
    let lock = std::fs::OpenOptions::new()
        .create(true)
        .truncate(false)
        .read(true)
        .write(true)
        .open(config.state_dir.join("owner.lock"))
        .map_err(|_| "cannot open state ownership lock".to_string())?;
    lock.try_lock_exclusive()
        .map_err(|_| "configured state already owned".to_string())?;
    eprintln!(
        "{}",
        serde_json::json!({"component":"sinexd","phase":"state_owned"})
    );
    let executable =
        std::env::current_exe().map_err(|_| "cannot identify running executable".to_string())?;
    let executable_blake3 = tokio::task::spawn_blocking(move || -> Result<String, String> {
        let mut file = std::fs::File::open(executable)
            .map_err(|_| "cannot read running executable".to_string())?;
        let mut hash = blake3::Hasher::new();
        hash.update_reader(&mut file)
            .map_err(|_| "cannot hash running executable".to_string())?;
        Ok(format!("blake3:{}", hash.finalize().to_hex()))
    })
    .await
    .map_err(|_| "executable identity worker failed".to_string())??;
    let host = std::fs::read_to_string("/proc/sys/kernel/hostname")
        .map_err(|_| "cannot capture runtime host identity".to_string())?
        .trim()
        .to_string();
    let identity = sinex_store::RuntimeIdentity {
        service_name: "sinexd".into(),
        host,
        version: env!("CARGO_PKG_VERSION").into(),
        executable_blake3,
        config_blake3: config.input_blake3.clone(),
    };
    eprintln!(
        "{}",
        serde_json::json!({"component":"sinexd","phase":"connecting_store"})
    );
    let store = sinex_store::Store::connect(
        &writer,
        &reader,
        config.store_dir.clone(),
        16,
        identity,
        qualification.clone(),
    )
    .await
    .map_err(|_| {
        "database schema, roles, writer ownership or content store unavailable".to_string()
    })?;
    eprintln!(
        "{}",
        serde_json::json!({"component":"sinexd","phase":"store_ready"})
    );
    store
        .admission
        .set_replay_profile(
            operator_token
                .as_ref()
                .map(|_| config.privacy_rules.iter().map(|r| r.rule()).collect()),
        )
        .await
        .map_err(|_| "cannot configure replay authority".to_string())?;
    if config.automata.iter().any(|a| a == "session") {
        store
            .admission
            .enable_sessions()
            .await
            .map_err(|_| "cannot enable shell session interpretation")?;
    }
    let projection = if let Some(path) = &config.read_model_connection_file {
        store
            .admission
            .enable_shell()
            .await
            .map_err(|_| "cannot enable canonical shell interpretation")?;
        Some(
            sinex_store::Projection::start(
                &secret(path)?,
                store.run_id(),
                config.projects.iter().map(|p| p.model()).collect(),
                qualification.clone(),
            )
            .await
            .map_err(|_| "read-model role or projection unavailable")?,
        )
    } else {
        None
    };
    let (source_stop, source_shutdown) = tokio::sync::watch::channel(false);
    let (source_ended, mut source_ended_rx) = tokio::sync::watch::channel(false);
    let mut source = if let Some(atuin) = config.atuin.clone() {
        let rules: Vec<_> = config.privacy_rules.iter().map(|r| r.rule()).collect();
        let admission = store.admission.clone();
        let observation = crate::atuin::begin_poll(
            &atuin.path,
            &rules,
            &admission,
            if historical_fixture {
                sinex_store::AtuinInterpretation::HistoricalFixture
            } else {
                sinex_store::AtuinInterpretation::Current
            },
        )
        .await?;
        let ended = source_ended.clone();
        let qualification = qualification.clone();
        Some(tokio::spawn(async move {
            let result = crate::atuin::run(
                atuin,
                rules,
                admission,
                source_shutdown,
                observation,
                qualification,
            )
            .await;
            let _ = ended.send(true);
            result
        }))
    } else {
        None
    };
    let zsh_source = if let Some(zsh) = config.zsh.clone() {
        let rules = config.privacy_rules.iter().map(|r| r.rule()).collect();
        let admission = store.admission.clone();
        let stop = source_stop.subscribe();
        let ended = source_ended.clone();
        Some(tokio::spawn(async move {
            let result = crate::zsh::run(zsh, rules, admission, stop).await;
            let _ = ended.send(true);
            result
        }))
    } else {
        None
    };
    let mut browser_sources = Vec::new();
    for browser in config.chromium.clone() {
        let rules: Vec<_> = config.privacy_rules.iter().map(|r| r.rule()).collect();
        let admission = store.admission.clone();
        let attempt = crate::browser::begin(&browser, &rules, &admission).await?;
        let stop = source_stop.subscribe();
        let ended = source_ended.clone();
        let root = config.store_dir.join("acquisition");
        let qualification = qualification.clone();
        browser_sources.push(tokio::spawn(async move {
            let result = crate::browser::run(
                browser,
                root,
                rules,
                admission,
                stop,
                attempt,
                qualification,
            )
            .await;
            let _ = ended.send(true);
            result
        }));
    }
    let state = Arc::new(App {
        run_id: store.run_id(),
        indexed: config.read_model_connection_file.is_some(),
        config,
        token,
        operator_token,
        reader: store.reader.clone(),
        admission: store.admission.clone(),
    });
    let app = Router::new()
        .route("/rpc", post(rpc))
        .layer(DefaultBodyLimit::max(2 * 1024 * 1024))
        .with_state(state);
    let result = axum::serve(listener, app)
        .with_graceful_shutdown(async move {
            tokio::select! {_=shutdown_signal()=>{},_=source_ended_rx.changed()=>{}}
        })
        .await;
    let _ = source_stop.send(true);
    let source_result = if let Some(source) = source.take() {
        source.await.map_err(|_| "source task failed")?
    } else {
        Ok(())
    };
    let zsh_result = if let Some(source) = zsh_source {
        source.await.map_err(|_| "zsh source task failed")?
    } else {
        Ok(())
    };
    let mut browser_error = None;
    for source in browser_sources {
        match source.await {
            Ok(Ok(())) => {}
            Ok(Err(error)) => {
                browser_error = Some(error);
            }
            Err(_) => {
                browser_error = Some("browser source task failed".to_owned());
            }
        }
    }
    if let Some(projection) = projection {
        projection
            .shutdown()
            .await
            .map_err(|_| "projection shutdown failed")?;
    }
    let stopped = store.shutdown().await;
    source_result?;
    zsh_result?;
    if let Some(error) = browser_error {
        return Err(error);
    }
    drop(lock);
    result.map_err(|_| "HTTP server failed".to_string())?;
    stopped.map_err(|_| "writer shutdown failed".to_string())?;
    Ok(())
}
async fn shutdown_signal() {
    let mut terminate = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
        .expect("SIGTERM handler");
    tokio::select! { _ = tokio::signal::ctrl_c() => {}, _ = terminate.recv() => {} }
}
async fn rpc(
    State(state): State<Arc<App>>,
    headers: HeaderMap,
    body: Bytes,
) -> axum::response::Response {
    let supplied = headers
        .get("authorization")
        .and_then(|h| h.to_str().ok())
        .and_then(|h| h.strip_prefix("Bearer "));
    let operator = state
        .operator_token
        .as_deref()
        .is_some_and(|token| Some(token) == supplied);
    let authenticated = operator || supplied == Some(state.token.as_str());
    if !authenticated {
        return (
            StatusCode::UNAUTHORIZED,
            present_response(RpcResponse::failure(
                None,
                RpcError::new(
                    ErrorKind::Refused,
                    "token required",
                    serde_json::Value::Null,
                ),
            )),
        )
            .into_response();
    }
    let request: RpcRequest = match serde_json::from_slice(&body) {
        Ok(r) => r,
        Err(_) => {
            return (
                StatusCode::OK,
                present_response(RpcResponse::failure(None, RpcError::parse_error())),
            )
                .into_response();
        }
    };
    if !crate::output::request_safe(&request.id, &request.params) {
        return (
            StatusCode::OK,
            present_response(crate::output::refusal(Some(request.id))),
        )
            .into_response();
    }
    let method = match request.validate() {
        Ok(m) => m,
        Err(e) => {
            return (
                StatusCode::OK,
                present_response(RpcResponse::failure(Some(request.id), e)),
            )
                .into_response();
        }
    };
    if matches!(
        method,
        Method::ReplayStart | Method::ReplayCancel | Method::Delete | Method::DeleteUndo
    ) && !operator
    {
        return (
            StatusCode::OK,
            present_response(RpcResponse::failure(
                Some(request.id),
                RpcError::new(
                    ErrorKind::Refused,
                    "explicit operator profile required",
                    serde_json::Value::Null,
                ),
            )),
        )
            .into_response();
    }
    // The connection owns this body future. Hyper drops it when the receiver
    // disconnects, so it drops the in-flight read instead of detaching a handler.
    let body = Body::from_stream(futures_util::stream::once(async move {
        let response = execute(state, request, method).await;
        Ok::<_, std::convert::Infallible>(Bytes::from(crate::output::encode(response)))
    }));
    axum::response::Response::builder()
        .status(StatusCode::OK)
        .header("content-type", "application/json")
        .body(body)
        .expect("valid RPC response headers")
}

async fn execute(state: Arc<App>, request: RpcRequest, method: Method) -> RpcResponse {
    if matches!(
        method,
        Method::Status | Method::Sources | Method::Show | Method::Trace
    ) {
        let id = request.id.clone();
        match tokio::time::timeout(
            std::time::Duration::from_secs(10),
            execute_read(state, request, method),
        )
        .await
        {
            Ok(response) => response,
            Err(_) => RpcResponse::failure(Some(id), deadline_error(10000)),
        }
    } else {
        execute_read(state, request, method).await
    }
}
async fn execute_read(state: Arc<App>, request: RpcRequest, method: Method) -> RpcResponse {
    let result = match method {
        Method::Status => match (
            state.reader.schema_status().await,
            state.reader.status().await,
            state.reader.replay_operations().await,
        ) {
            (Ok(schema), Ok(status), Ok(replays)) => serde_json::to_value(Status {
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
                indexed_retrieval: serde_json::Value::Null,
                supported_sources: state.config.sources.clone(),
                supported_methods: SUPPORTED_METHODS.iter().map(|s| s.to_string()).collect(),
            })
            .map_err(|_| {
                RpcError::new(
                    ErrorKind::Internal,
                    "cannot encode status",
                    serde_json::Value::Null,
                )
            }),
            _ => Err(RpcError::new(
                ErrorKind::Internal,
                "database schema or status unavailable",
                serde_json::Value::Null,
            )),
        },
        Method::Delete | Method::DeleteUndo => {
            let params: sinex_model::exclusion::Request =
                serde_json::from_value(request.params.clone()).expect("validated exclusion");
            state.admission.exclude(if method==Method::Delete {sinex_model::exclusion::Action::Exclude}else{sinex_model::exclusion::Action::Undo},params).await.map_err(|error| {
                if matches!(error,sinex_store::Error::Invalid(_)|sinex_store::Error::ReplayRefused(_)) {read_error(error)} else {
                    RpcError::new(ErrorKind::Partial,"mutation outcome unavailable; retry the same action identity",serde_json::json!({"outcome":"indeterminate","action_id":request.params["action_id"]}))
                }
            }).and_then(exclusion_outcome)
        }
        Method::ReplayPlan | Method::ReplayStart | Method::ReplayCancel => {
            let command = match method {
                Method::ReplayPlan => sinex_store::ReplayCommand::Plan(
                    serde_json::from_value(request.params.clone()).expect("validated plan"),
                    state
                        .config
                        .privacy_rules
                        .iter()
                        .map(|r| r.rule())
                        .collect(),
                ),
                Method::ReplayStart => sinex_store::ReplayCommand::Start(
                    serde_json::from_value(request.params.clone()).expect("validated replay"),
                ),
                _ => sinex_store::ReplayCommand::Cancel(
                    serde_json::from_value(request.params.clone()).expect("validated operation"),
                ),
            };
            state
                .admission
                .replay(command)
                .await
                .map_err(|error| {
                    if matches!(method,Method::ReplayStart|Method::ReplayCancel) && !matches!(error,sinex_store::Error::Invalid(_)|sinex_store::Error::ReplayRefused(_)) {
                        RpcError::new(ErrorKind::Partial,"mutation outcome unavailable; recover the same action or inspect the existing operation",serde_json::json!({"outcome":"indeterminate","action_id":request.params.get("action_id"),"operation_id":request.params.get("operation_id")}))
                    } else {read_error(error)}
                })
                .and_then(replay_outcome)
        }
        Method::ReplayStatus => {
            let params: sinex_protocol::ReplayOperation =
                serde_json::from_value(request.params.clone()).expect("validated operation");
            state
                .reader
                .replay_status(params.operation_id)
                .await
                .map_err(read_error)
                .and_then(|value| {
                    value.ok_or_else(|| {
                        RpcError::new(
                            ErrorKind::NotFound,
                            "replay operation not found",
                            serde_json::Value::Null,
                        )
                    })
                })
                .and_then(replay_outcome)
        }
        Method::Version => Ok(serde_json::json!(VersionInfo {
            version: env!("CARGO_PKG_VERSION").into(),
            protocol: "2.0".into()
        })),
        Method::ConfigShow => serde_json::to_value(&state.config).map_err(|_| {
            RpcError::new(
                ErrorKind::Internal,
                "cannot encode configuration",
                serde_json::Value::Null,
            )
        }),
        Method::Catalog => Ok(serde_json::json!(sinex_protocol::rpc::catalog())),
        Method::Sources => state
            .reader
            .inspect_sources(
                serde_json::from_value(request.params.clone()).expect("validated source params"),
                {
                    let mut scopes = Vec::new();
                    if let Some(config) = &state.config.atuin {
                        scopes.push(sinex_model::acquisition::CollectorScope {
                            source: sinex_store::ATUIN_SOURCE.into(),
                            locator: config.path.to_string_lossy().into_owned(),
                            run: state.run_id,
                        });
                    }
                    for browser in &state.config.chromium {
                        scopes.push(sinex_model::acquisition::CollectorScope {
                            source: sinex_model::browser::SOURCE.into(),
                            locator: serde_json::json!([
                                browser.profile,
                                browser.database_generation
                            ])
                            .to_string(),
                            run: state.run_id,
                        });
                    }
                    scopes
                },
            )
            .await
            .map_err(read_error),
        Method::Query | Method::Search => {
            let params: sinex_protocol::QueryParams =
                serde_json::from_value(request.params.clone()).expect("validated query params");
            let timeout_ms = params.timeout_ms.unwrap_or(10_000);
            let indexed = method == Method::Search || sinex_store::is_indexed_query(&params.query);
            if sinex_store::is_recall(&params.query) {
                state
                    .reader
                    .recall_with_timeout(
                        &params.query,
                        &state
                            .config
                            .projects
                            .iter()
                            .map(|p| p.model())
                            .collect::<Vec<_>>(),
                        params.cursor.as_deref(),
                        params.page.unwrap_or(100),
                        std::time::Duration::from_millis(timeout_ms),
                    )
                    .await
                    .map_err(|e| {
                        if matches!(e, sinex_store::Error::Deadline) {
                            deadline_error(timeout_ms)
                        } else {
                            read_error(e)
                        }
                    })
            } else if indexed {
                if let Err(message) = sinex_model::retrieval::bind(&params.query) {
                    Err(RpcError::new(
                        ErrorKind::BadQuery,
                        message,
                        serde_json::Value::Null,
                    ))
                } else if !state.indexed {
                    Err(read_error(sinex_store::Error::ProjectionUnavailable(
                        "indexed retrieval is not configured",
                    )))
                } else {
                    state
                        .reader
                        .search_with_timeout(
                            &params.query,
                            params.cursor.as_deref(),
                            params.page.unwrap_or(20),
                            std::time::Duration::from_millis(timeout_ms),
                        )
                        .await
                        .map_err(|e| {
                            if matches!(e, sinex_store::Error::Deadline) {
                                deadline_error(timeout_ms)
                            } else {
                                read_error(e)
                            }
                        })
                }
            } else if params.page.is_some() {
                Err(RpcError::new(
                    ErrorKind::BadQuery,
                    "raw events retain their fixed 100-row page",
                    serde_json::Value::Null,
                ))
            } else {
                match sinex_store::validate_query(&params.query) {
                    Err(e) => Err(RpcError::new(
                        ErrorKind::BadQuery,
                        e.0,
                        serde_json::Value::Null,
                    )),
                    Ok(()) => state
                        .reader
                        .query_with_timeout(
                            &params.query,
                            params.cursor.as_deref(),
                            std::time::Duration::from_millis(timeout_ms),
                        )
                        .await
                        .map_err(|e| {
                            if matches!(e, sinex_store::Error::Deadline) {
                                deadline_error(timeout_ms)
                            } else {
                                read_error(e)
                            }
                        }),
                }
            }
        }

        Method::Show | Method::Trace => {
            let params: sinex_protocol::ShowParams =
                serde_json::from_value(request.params.clone()).expect("validated show params");
            let id = params
                .reference
                .strip_prefix("sinex:ev/")
                .unwrap_or(&params.reference);
            match if method == Method::Trace {
                state
                    .reader
                    .trace(
                        id,
                        params.bytes,
                        &state
                            .config
                            .privacy_rules
                            .iter()
                            .map(|r| r.rule())
                            .collect::<Vec<_>>(),
                    )
                    .await
            } else {
                state.reader.show(id).await
            } {
                Ok(Some(event)) => {
                    let resolved = if event.get("record_state")
                        == Some(&serde_json::json!("removed"))
                        || event.get("source_state")
                            == Some(&serde_json::json!("removed at source"))
                    {
                        "removed at source"
                    } else if event.get("archive").is_some_and(|a| !a.is_null()) {
                        "archived"
                    } else {
                        "live"
                    };
                    Ok(
                        serde_json::json!({"notice":sinex_protocol::DATA_NOTICE,"ref":format!("sinex:ev/{id}"),"resolved":resolved,"row":event}),
                    )
                }
                Ok(None) => Err(RpcError::new(
                    ErrorKind::NotFound,
                    "live event not found",
                    serde_json::Value::Null,
                )),
                Err(e) => Err(read_error(e)),
            }
        }
    };
    let result = if method == Method::Status {
        match (result, state.reader.projection_status().await) {
            (Ok(mut value), Ok(projection)) => {
                value["indexed_retrieval"] = projection;
                value["indexed_retrieval"]["configured"] = serde_json::json!(state.indexed);
                Ok(value)
            }
            (Err(e), _) => Err(e),
            (_, Err(e)) => Err(read_error(e)),
        }
    } else {
        result
    };
    match result {
        Ok(v) => RpcResponse::success(request.id, v),
        Err(e) => RpcResponse::failure(Some(request.id), e),
    }
}

fn exclusion_outcome(mut value: serde_json::Value) -> Result<serde_json::Value, RpcError> {
    // Effects may already be durable. A presentation refusal must not imply a failed mutation.
    let identity = serde_json::json!({"action_id":value["action_id"],"operation_id":value["operation_id"],"outcome":"retained_but_unpresentable"});
    if crate::output::admit(&mut value).is_err() {
        return Err(RpcError::new(
            ErrorKind::Partial,
            "durable mutation outcome cannot be presented; recover the same action identity",
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
#[cfg(test)]
mod exclusion_tests {
    use super::*;
    #[test]
    fn committed_outcome_presentation_refusal_is_not_no_effect_failure() {
        let value = serde_json::json!({"action_id":"01800000-0000-7000-8000-000000000001","operation_id":"01800000-0000-7000-8000-000000000002","state":"completed","occurrence":{"equivalence_key":"Authorization: Bearer synthetic_sensitive_value"}});
        let error = exclusion_outcome(value).unwrap_err();
        assert_eq!(error.kind, ErrorKind::Partial);
        assert_eq!(error.data["outcome"], "retained_but_unpresentable");
        assert!(error.data["operation_id"].as_str().is_some());
    }
}

fn replay_outcome(value: serde_json::Value) -> Result<serde_json::Value, RpcError> {
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
fn deadline_error(timeout_ms: u64) -> RpcError {
    RpcError::new(
        ErrorKind::Budget,
        "query execution deadline exceeded",
        serde_json::json!({
            "reason": "execution_deadline",
            "timeout_ms": timeout_ms,
            "estimate": null,
            "suggestion": "Narrow the supported text, time, project, source or ID scope, or retry after contention ends.",
        }),
    )
}

fn read_error(error: sinex_store::Error) -> RpcError {
    match error {
        sinex_store::Error::Database(sqlx::Error::Database(error))
            if error.code().as_deref() == Some("57014") =>
        {
            deadline_error(10000)
        }
        sinex_store::Error::ProjectionUnavailable(reason) => RpcError::new(
            ErrorKind::Partial,
            reason,
            serde_json::json!({"reason":"projection_unavailable","complete":false}),
        ),
        sinex_store::Error::ReplayRefused(reason) => {
            RpcError::new(ErrorKind::Refused, reason, serde_json::Value::Null)
        }
        sinex_store::Error::Invalid(reason) => {
            RpcError::new(ErrorKind::BadQuery, reason, serde_json::Value::Null)
        }
        _ => RpcError::new(
            ErrorKind::Internal,
            "retained source or event read unavailable",
            serde_json::Value::Null,
        ),
    }
}

fn present_response(response: RpcResponse) -> impl IntoResponse {
    (
        [(axum::http::header::CONTENT_TYPE, "application/json")],
        crate::output::encode(response),
    )
}

struct PendingApp {
    privacy_rules: Vec<sinex_store::PrivacyRule>,
    reader: sinex_store::ReadPool,
    token: String,
    operator: Option<String>,
}
async fn serve_pending(
    listener: tokio::net::TcpListener,
    config: Config,
    token: String,
    operator: Option<String>,
    reader: String,
) -> Result<(), String> {
    let privacy_rules = config.privacy_rules.iter().map(|r| r.rule()).collect();
    let reader = sinex_store::ReadPool::installed(&reader, config.store_dir)
        .await
        .map_err(super::public_store_error)?;
    let app = Router::new()
        .route("/rpc", post(pending_rpc))
        .layer(DefaultBodyLimit::max(2 * 1024 * 1024))
        .with_state(Arc::new(PendingApp {
            privacy_rules,
            reader,
            token,
            operator,
        }));
    axum::serve(listener, app)
        .with_graceful_shutdown(shutdown_signal())
        .await
        .map_err(|_| "pending schema HTTP server failed".to_owned())
}
async fn pending_rpc(
    State(state): State<Arc<PendingApp>>,
    headers: HeaderMap,
    body: Bytes,
) -> axum::response::Response {
    let supplied = headers
        .get("authorization")
        .and_then(|v| v.to_str().ok())
        .and_then(|s| s.strip_prefix("Bearer "));
    if supplied != Some(state.token.as_str())
        && !state
            .operator
            .as_deref()
            .is_some_and(|v| supplied == Some(v))
    {
        return (
            StatusCode::UNAUTHORIZED,
            present_response(RpcResponse::failure(
                None,
                RpcError::new(
                    ErrorKind::Refused,
                    "token required",
                    serde_json::Value::Null,
                ),
            )),
        )
            .into_response();
    }
    let request: RpcRequest = match serde_json::from_slice(&body) {
        Ok(v) => v,
        Err(_) => {
            return present_response(RpcResponse::failure(None, RpcError::parse_error()))
                .into_response();
        }
    };
    if !crate::output::request_safe(&request.id, &request.params) {
        return present_response(crate::output::refusal(Some(request.id))).into_response();
    }
    let method = match request.validate() {
        Ok(m) => m,
        Err(e) => {
            return present_response(RpcResponse::failure(Some(request.id), e)).into_response();
        }
    };
    let result = tokio::time::timeout(
        std::time::Duration::from_secs(10),
        pending_read(&state, &request, method),
    )
    .await
    .unwrap_or_else(|_| Err(deadline_error(10000)));
    present_response(match result {
        Ok(v) => RpcResponse::success(request.id, v),
        Err(e) => RpcResponse::failure(Some(request.id), e),
    })
    .into_response()
}
async fn pending_read(
    state: &PendingApp,
    request: &RpcRequest,
    method: Method,
) -> Result<serde_json::Value, RpcError> {
    let waiting = state.reader.schema_status().await.map_err(read_error)?;
    let numbers = waiting
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
            serde_json::to_value(Status{
                schema:SchemaStatus{version:1,expected_version:1,compatible:numbers.is_empty(),pending_changes:numbers,unavailable_paths:unavailable},
                writer:WriterOwnership::Unavailable{reason:"installed-schema read mode; apply waiting changes with admin schema --apply --dump-file and restart".into()},
                commit_seq:status.commit_seq,supported_sources:vec![],supported_methods:vec!["status".into(),"query.run".into(),"object.show".into(),"object.trace".into()],replays:vec![],replay_authorized:false,indexed_retrieval:serde_json::json!({"state":if waiting.pending_changes.is_empty(){"restart_required"}else{"schema_changes_pending"},"mode":"installed_schema_read_only"}),
            }).map_err(|_|RpcError::new(ErrorKind::Internal,"cannot encode status",serde_json::Value::Null))
        }
        Method::Query => {
            let params: sinex_protocol::QueryParams =
                serde_json::from_value(request.params.clone()).expect("validated query");
            if params.page.is_some() {
                return Err(RpcError::new(
                    ErrorKind::BadQuery,
                    "raw events retain their fixed 100-row page",
                    serde_json::Value::Null,
                ));
            }
            sinex_store::validate_installed_raw_query(&params.query)
                .map_err(|e| RpcError::new(ErrorKind::BadQuery, e.0, serde_json::Value::Null))?;
            state
                .reader
                .query_with_timeout(
                    &params.query,
                    params.cursor.as_deref(),
                    std::time::Duration::from_millis(params.timeout_ms.unwrap_or(10000)),
                )
                .await
                .map_err(read_error)
        }
        Method::Show | Method::Trace => {
            let params: sinex_protocol::ShowParams =
                serde_json::from_value(request.params.clone()).expect("validated show");
            let id = params
                .reference
                .strip_prefix("sinex:ev/")
                .unwrap_or(&params.reference);
            let row = if method == Method::Trace {
                state
                    .reader
                    .trace(id, params.bytes, &state.privacy_rules)
                    .await
            } else {
                state.reader.show(id).await
            }
            .map_err(read_error)?
            .ok_or_else(|| {
                RpcError::new(
                    ErrorKind::NotFound,
                    "event not found",
                    serde_json::Value::Null,
                )
            })?;
            Ok(
                serde_json::json!({"notice":sinex_protocol::DATA_NOTICE,"ref":format!("sinex:ev/{id}"),"resolved":if row.get("archive").is_some_and(|v|!v.is_null()){"archived"}else{"live"},"row":row}),
            )
        }
        _ => Err(RpcError::new(
            ErrorKind::Partial,
            "path unavailable until ordered schema changes are applied",
            serde_json::json!({"pending_changes":numbers,"unavailable_paths":unavailable}),
        )),
    }
}
