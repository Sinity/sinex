use crate::config::{Config, secret};
use axum::{
    Json, Router,
    body::Bytes,
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
    config: Config,
    token: String,
    reader: sinex_store::ReadPool,
    admission: sinex_store::Admission,
}
pub async fn serve(
    config: Config,
    qualification: Option<sinex_store::QualificationBoundary>,
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
    let writer = secret(&config.writer_connection_file)?;
    let reader = secret(&config.reader_connection_file)?;
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
        qualification,
    )
    .await
    .map_err(|_| {
        "database schema, roles, writer ownership or content store unavailable".to_string()
    })?;
    eprintln!(
        "{}",
        serde_json::json!({"component":"sinexd","phase":"store_ready"})
    );
    let (source_stop, source_shutdown) = tokio::sync::watch::channel(false);
    let (source_ended, mut source_ended_rx) = tokio::sync::watch::channel(false);
    let mut source = config.atuin.clone().map(|atuin| {
        let rules = config.privacy_rules.iter().map(|r| r.rule()).collect();
        let admission = store.admission.clone();
        let ended = source_ended.clone();
        tokio::spawn(async move {
            let result = crate::atuin::run(atuin, rules, admission, source_shutdown).await;
            let _ = ended.send(true);
            result
        })
    });
    let state = Arc::new(App {
        config,
        token,
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
    let stopped = store.shutdown().await;
    source_result?;
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
async fn rpc(State(state): State<Arc<App>>, headers: HeaderMap, body: Bytes) -> impl IntoResponse {
    let authenticated = headers
        .get("authorization")
        .and_then(|h| h.to_str().ok())
        .is_some_and(|h| h.strip_prefix("Bearer ") == Some(state.token.as_str()));
    if !authenticated {
        return (
            StatusCode::UNAUTHORIZED,
            Json(RpcResponse::failure(
                None,
                RpcError::new(
                    ErrorKind::Refused,
                    "token required",
                    serde_json::Value::Null,
                ),
            )),
        );
    }
    let request: RpcRequest = match serde_json::from_slice(&body) {
        Ok(r) => r,
        Err(_) => {
            return (
                StatusCode::OK,
                Json(RpcResponse::failure(None, RpcError::parse_error())),
            );
        }
    };
    let method = match request.validate() {
        Ok(m) => m,
        Err(e) => {
            return (
                StatusCode::OK,
                Json(RpcResponse::failure(Some(request.id), e)),
            );
        }
    };
    let result = match method {
        Method::Status => match (
            state.reader.schema_status().await,
            state.reader.status().await,
        ) {
            (Ok(schema), Ok(status)) => serde_json::to_value(Status {
                schema: SchemaStatus {
                    version: i64::from(schema.version),
                    expected_version: 1,
                    compatible: true,
                },
                writer: if state.admission.is_available() {
                    WriterOwnership::Owned
                } else {
                    WriterOwnership::Unavailable {
                        reason: "admission owner has stopped".into(),
                    }
                },
                commit_seq: status.commit_seq,
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
        Method::Sources => state.reader.sources().await.map_err(read_error),
        Method::Query => {
            let params: sinex_protocol::QueryParams =
                serde_json::from_value(request.params.clone()).expect("validated query params");
            match sinex_store::validate_query(&params.query) {
                Err(e) => Err(RpcError::new(
                    ErrorKind::BadQuery,
                    e.0,
                    serde_json::Value::Null,
                )),
                Ok(()) => state
                    .reader
                    .query(&params.query, params.cursor.as_deref())
                    .await
                    .map_err(read_error),
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
                state.reader.trace(id, params.bytes).await
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
    let result = result.and_then(|mut value| {
        if matches!(method, Method::Query | Method::Show | Method::Trace) {
            sinex_store::present(
                &mut value,
                &state
                    .config
                    .privacy_rules
                    .iter()
                    .map(|r| r.rule())
                    .collect::<Vec<_>>(),
            )
            .map_err(read_error)?;
            if let Some(notice) = value.get_mut("notice") {
                *notice = serde_json::json!(sinex_protocol::DATA_NOTICE);
            }
        }
        Ok(value)
    });
    let response = match result {
        Ok(v) => RpcResponse::success(request.id, v),
        Err(e) => RpcResponse::failure(Some(request.id), e),
    };
    (StatusCode::OK, Json(response))
}

fn read_error(error: sinex_store::Error) -> RpcError {
    match error {
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
