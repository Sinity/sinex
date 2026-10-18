//! The daemon: startup, source tasks, the `/rpc` endpoint and shutdown.
//! Lifecycle lines on stderr (`{"component":"sinexd","phase":...}`) are what
//! the Python harness waits for.
use crate::config::{Config, secret};
use axum::{
    Router,
    body::{Body, Bytes},
    extract::{DefaultBodyLimit, State},
    http::{HeaderMap, StatusCode},
    response::{IntoResponse, Response},
    routing::post,
};
use fs2::FileExt;
use sinex_protocol::rpc::{ErrorKind, Method, RpcError, RpcRequest, RpcResponse};
use sinex_store::{Admission, PrivacyRule, TestPause};
use std::sync::Arc;
use tokio::{sync::watch, task::JoinHandle};

mod errors;
mod methods;
mod pending;

const BODY_LIMIT: usize = 2 * 1024 * 1024;

struct App {
    run_id: uuid::Uuid,
    config: Config,
    rules: Vec<PrivacyRule>,
    projects: Vec<sinex_model::retrieval::Project>,
    token: String,
    operator_token: Option<String>,
    indexed: bool,
    reader: sinex_store::ReadPool,
    admission: Admission,
}

fn lifecycle(phase: &str) {
    eprintln!(
        "{}",
        serde_json::json!({"component":"sinexd","phase":phase})
    );
}

pub async fn serve(
    config: Config,
    pause: Option<TestPause>,
    test_bad_atuin_parse: bool,
) -> Result<(), String> {
    lifecycle("starting");
    // Bind before taking writer ownership or creating state, so a second
    // daemon on the same port fails without side effects.
    let listener = tokio::net::TcpListener::bind(&config.listen)
        .await
        .map_err(|_| "configured endpoint is occupied or unavailable".to_string())?;
    lifecycle("endpoint_reserved");
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
        return pending::serve(listener, config, token, operator_token, reader).await;
    }
    let lock = lock_state_dir(&config)?;
    lifecycle("state_owned");
    let identity = runtime_identity(&config).await?;
    lifecycle("connecting_store");
    let store = sinex_store::Store::connect(
        &writer,
        &reader,
        config.store_dir.clone(),
        16,
        identity,
        pause.clone(),
    )
    .await
    .map_err(|_| {
        "database schema, roles, writer ownership or content store unavailable".to_string()
    })?;
    lifecycle("store_ready");
    let rules = config.rules();
    store
        .admission
        .set_replay_profile(operator_token.as_ref().map(|_| rules.clone()))
        .await
        .map_err(|_| "cannot configure replay profile".to_string())?;
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
                config.project_models(),
                pause.clone(),
            )
            .await
            .map_err(|_| "read-model role or projection unavailable")?,
        )
    } else {
        None
    };
    let (stop, _) = watch::channel(false);
    let (ended, mut ended_rx) = watch::channel(false);
    let sources = spawn_sources(
        &config,
        &rules,
        &store.admission,
        pause,
        test_bad_atuin_parse,
        &stop,
        &ended,
    )
    .await?;
    let state = Arc::new(App {
        run_id: store.run_id(),
        indexed: config.read_model_connection_file.is_some(),
        projects: config.project_models(),
        rules,
        config,
        token,
        operator_token,
        reader: store.reader.clone(),
        admission: store.admission.clone(),
    });
    let app = Router::new()
        .route("/rpc", post(rpc))
        .layer(DefaultBodyLimit::max(BODY_LIMIT))
        .with_state(state);
    // A source task that ends (by error or not) stops the daemon.
    let served = axum::serve(listener, app)
        .with_graceful_shutdown(async move {
            tokio::select! {_ = shutdown_signal() => {}, _ = ended_rx.changed() => {}}
        })
        .await;
    let _ = stop.send(true);
    let sources = join_sources(sources).await;
    if let Some(projection) = projection {
        projection
            .shutdown()
            .await
            .map_err(|_| "projection shutdown failed")?;
    }
    let stopped = store.shutdown().await;
    sources?;
    drop(lock);
    served.map_err(|_| "HTTP server failed".to_string())?;
    stopped.map_err(|_| "writer shutdown failed".to_string())?;
    Ok(())
}

fn lock_state_dir(config: &Config) -> Result<std::fs::File, String> {
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
    Ok(lock)
}

async fn runtime_identity(config: &Config) -> Result<sinex_store::RuntimeIdentity, String> {
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
    Ok(sinex_store::RuntimeIdentity {
        service_name: "sinexd".into(),
        host,
        version: env!("CARGO_PKG_VERSION").into(),
        executable_blake3,
        config_blake3: config.input_blake3.clone(),
    })
}

type SourceTask = (&'static str, JoinHandle<Result<(), String>>);

/// Start one task per configured source. Each first commits its opening
/// observation, so startup fails if the writer cannot record it.
async fn spawn_sources(
    config: &Config,
    rules: &[PrivacyRule],
    admission: &Admission,
    pause: Option<TestPause>,
    test_bad_atuin_parse: bool,
    stop: &watch::Sender<bool>,
    ended: &watch::Sender<bool>,
) -> Result<Vec<SourceTask>, String> {
    fn spawn(
        ended: &watch::Sender<bool>,
        task: impl Future<Output = Result<(), String>> + Send + 'static,
    ) -> JoinHandle<Result<(), String>> {
        let ended = ended.clone();
        tokio::spawn(async move {
            let result = task.await;
            let _ = ended.send(true);
            result
        })
    }
    let mut tasks = Vec::new();
    if let Some(atuin) = config.atuin.clone() {
        let interpretation = if test_bad_atuin_parse {
            sinex_store::AtuinInterpretation::TestBadParse
        } else {
            sinex_store::AtuinInterpretation::Current
        };
        let observation =
            crate::atuin::begin_poll(&atuin.path, rules, admission, interpretation).await?;
        let task = crate::atuin::run(
            atuin,
            rules.to_vec(),
            admission.clone(),
            stop.subscribe(),
            observation,
            pause.clone(),
        );
        tasks.push(("Atuin", spawn(ended, task)));
    }
    if let Some(zsh) = config.zsh.clone() {
        let task = crate::zsh::run(zsh, rules.to_vec(), admission.clone(), stop.subscribe());
        tasks.push(("zsh", spawn(ended, task)));
    }
    for browser in config.chromium.clone() {
        let attempt = crate::browser::begin(&browser, rules, admission).await?;
        let task = crate::browser::run(
            browser,
            config.store_dir.join("acquisition"),
            rules.to_vec(),
            admission.clone(),
            stop.subscribe(),
            attempt,
            pause.clone(),
        );
        tasks.push(("browser", spawn(ended, task)));
    }
    Ok(tasks)
}

/// Wait for every source task and return the first failure.
async fn join_sources(tasks: Vec<SourceTask>) -> Result<(), String> {
    let mut first = Ok(());
    for (name, task) in tasks {
        let result = task
            .await
            .unwrap_or_else(|_| Err(format!("{name} source task failed")));
        if first.is_ok() {
            first = result;
        }
    }
    first
}

async fn shutdown_signal() {
    let mut terminate = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
        .expect("SIGTERM handler");
    tokio::select! { _ = tokio::signal::ctrl_c() => {}, _ = terminate.recv() => {} }
}

/// Checks shared by both endpoints: bearer token, JSON parsing, credential
/// screening of the request and protocol validation. `Err` is the response to
/// send; `Ok` carries whether the caller used the operator token.
fn accept(
    headers: &HeaderMap,
    body: &[u8],
    token: &str,
    operator_token: Option<&str>,
) -> Result<(RpcRequest, Method, bool), Box<Response>> {
    let reply = |response| Box::new(present_response(response).into_response());
    let supplied = headers
        .get("authorization")
        .and_then(|h| h.to_str().ok())
        .and_then(|h| h.strip_prefix("Bearer "));
    let operator = operator_token.is_some_and(|t| supplied == Some(t));
    if !operator && supplied != Some(token) {
        let refusal = RpcError::new(
            ErrorKind::Refused,
            "token required",
            serde_json::Value::Null,
        );
        let response = present_response(RpcResponse::failure(None, refusal));
        return Err(Box::new(
            (StatusCode::UNAUTHORIZED, response).into_response(),
        ));
    }
    let request: RpcRequest = serde_json::from_slice(body)
        .map_err(|_| reply(RpcResponse::failure(None, RpcError::parse_error())))?;
    if !crate::output::request_safe(&request.id, &request.params) {
        return Err(reply(crate::output::refusal(Some(request.id))));
    }
    match request.validate() {
        Ok(method) => Ok((request, method, operator)),
        Err(e) => Err(reply(RpcResponse::failure(Some(request.id), e))),
    }
}

async fn rpc(State(state): State<Arc<App>>, headers: HeaderMap, body: Bytes) -> Response {
    let (request, method, operator) = match accept(
        &headers,
        &body,
        &state.token,
        state.operator_token.as_deref(),
    ) {
        Ok(accepted) => accepted,
        Err(response) => return *response,
    };
    if matches!(
        method,
        Method::ReplayStart | Method::ReplayCancel | Method::Delete | Method::DeleteUndo
    ) && !operator
    {
        let refusal = RpcError::new(
            ErrorKind::Refused,
            "operator token required",
            serde_json::Value::Null,
        );
        return present_response(RpcResponse::failure(Some(request.id), refusal)).into_response();
    }
    // The connection owns this body future. Hyper drops it when the client
    // disconnects, which cancels the in-flight read instead of detaching it.
    let body = Body::from_stream(futures_util::stream::once(async move {
        let response = methods::execute(state, request, method).await;
        Ok::<_, std::convert::Infallible>(Bytes::from(crate::output::encode(response)))
    }));
    Response::builder()
        .status(StatusCode::OK)
        .header("content-type", "application/json")
        .body(body)
        .expect("valid RPC response headers")
}

fn present_response(response: RpcResponse) -> impl IntoResponse {
    (
        [(axum::http::header::CONTENT_TYPE, "application/json")],
        crate::output::encode(response),
    )
}
