//! The HTTP API: JSON over a Unix socket and, when configured, localhost. One
//! endpoint takes every request of the command table; `follow` streams new
//! matching events as server-sent events. Three fixed tokens grant read, agent
//! and operator access.
use crate::{Error, Result};
mod methods;

pub(crate) use methods::Api;

use axum::extract::{Query as UrlQuery, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::sse::{Event, KeepAlive, Sse};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use serde::Deserialize;
use serde_json::{Value, json};
use sinex_model::query::{self, Source, Stage, Table};
use sinex_protocol::{Access, COMMANDS, DATA_NOTICE, Reply};
use sinex_store::query::Page;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tokio::sync::{mpsc, watch};
use tokio_stream::wrappers::ReceiverStream;

/// The three tokens, read from `read.token`, `agent.token` and
/// `operator.token` in a directory. A missing file disables that level.
#[derive(Clone, Default)]
pub(crate) struct Tokens {
    read: Option<String>,
    agent: Option<String>,
    operator: Option<String>,
}

impl Tokens {
    pub(crate) fn load(dir: &Path) -> std::io::Result<Self> {
        let read = |name: &str| -> std::io::Result<Option<String>> {
            match std::fs::read_to_string(dir.join(name)) {
                Ok(s) if !s.trim().is_empty() => Ok(Some(s.trim().to_string())),
                Ok(_) => Ok(None),
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
                Err(e) => Err(e),
            }
        };
        Ok(Self {
            read: read("read.token")?,
            agent: read("agent.token")?,
            operator: read("operator.token")?,
        })
    }

    #[cfg(test)]
    pub(crate) fn new(
        read: Option<String>,
        agent: Option<String>,
        operator: Option<String>,
    ) -> Self {
        Self {
            read,
            agent,
            operator,
        }
    }

    /// The access a presented token grants, the highest first.
    fn grant(&self, presented: &str) -> Option<Access> {
        let matches = |t: &Option<String>| {
            t.as_deref()
                .is_some_and(|t| same(t.as_bytes(), presented.as_bytes()))
        };
        if matches(&self.operator) {
            Some(Access::Operator)
        } else if matches(&self.agent) {
            Some(Access::Agent)
        } else if matches(&self.read) {
            Some(Access::Read)
        } else {
            None
        }
    }
}

/// The classes the operator granted the agent token, from `agent.grants` in the
/// tokens directory (one class per line or comma-separated). Only `guarded` can
/// be granted: the agent token never reads sealed data, so a `sealed` line is
/// refused rather than ignored.
pub(crate) fn agent_grants(dir: &Path) -> std::io::Result<Vec<sinex_model::Class>> {
    let text = match std::fs::read_to_string(dir.join("agent.grants")) {
        Ok(t) => t,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(e) => return Err(e),
    };
    let mut out = Vec::new();
    for word in text
        .split([',', '\n'])
        .map(str::trim)
        .filter(|w| !w.is_empty())
    {
        match word {
            "guarded" => out.push(sinex_model::Class::Guarded),
            "open" => {}
            other => {
                return Err(std::io::Error::other(format!(
                    "agent.grants: `{other}` cannot be granted to the agent token (only `guarded`)"
                )));
            }
        }
    }
    Ok(out)
}

/// Compare without stopping at the first difference.
fn same(a: &[u8], b: &[u8]) -> bool {
    a.len() == b.len() && a.iter().zip(b).fold(0u8, |acc, (x, y)| acc | (x ^ y)) == 0
}

pub(crate) struct Config {
    pub socket: PathBuf,
    pub listen: Option<SocketAddr>,
    pub tokens: Tokens,
}

#[derive(Clone)]
struct AppState {
    api: Api,
    tokens: Arc<Tokens>,
    commits: watch::Receiver<u64>,
    read_models: watch::Receiver<()>,
}

fn actor(access: Access) -> &'static str {
    match access {
        Access::Read => "reader",
        Access::Agent => "agent",
        Access::Operator => "operator",
    }
}

/// A reply as an HTTP response: a failure's status comes from the shared
/// table, so `/v1` answers a missing event 404 and a bad query 400.
fn respond(reply: Reply) -> Response {
    let status = reply
        .failure()
        .and_then(|failure| failure.kind.http_status())
        .and_then(|code| StatusCode::from_u16(code).ok());
    let status = match (&reply, status) {
        (Reply::Ok { .. }, _) => StatusCode::OK,
        (_, Some(status)) => status,
        // A kind with no HTTP status (the daemon reporting a part of itself
        // not running) is still not a success.
        (_, None) => StatusCode::SERVICE_UNAVAILABLE,
    };
    (status, Json(reply)).into_response()
}

fn authorize(state: &AppState, headers: &HeaderMap, needs: Access) -> Result<Access, Response> {
    let presented = headers
        .get(axum::http::header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.strip_prefix("Bearer "))
        .map(str::trim);
    let refuse = |message: String| respond(Reply::failed(Error::refused(message)));
    // A missing or unknown credential is answered before any operation runs,
    // as HTTP authentication: 401, with the same `refused` failure.
    let unauthenticated = |message: &str| {
        (
            StatusCode::UNAUTHORIZED,
            Json(Reply::failed(Error::refused(message))),
        )
            .into_response()
    };
    let Some(presented) = presented else {
        return Err(unauthenticated(
            "no token: send `Authorization: Bearer <token>`",
        ));
    };
    let Some(granted) = state.tokens.grant(presented) else {
        return Err(unauthenticated("unknown token"));
    };
    if granted < needs {
        return Err(refuse(format!(
            "this token grants {} access; the request needs {}",
            actor(granted),
            actor(needs)
        )));
    }
    Ok(granted)
}

pub(crate) fn router(
    api: Api,
    tokens: Tokens,
    commits: watch::Receiver<u64>,
    read_models: watch::Receiver<()>,
) -> Router {
    Router::new()
        .route("/v1", post(call))
        .route("/v1/commands", get(commands))
        .route("/v1/follow", get(follow))
        .with_state(AppState {
            api,
            tokens: Arc::new(tokens),
            commits,
            read_models,
        })
}

async fn call(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(request): Json<sinex_protocol::Call>,
) -> Response {
    match authorize(&state, &headers, request.request.access()) {
        Ok(granted) => respond(state.api.call(request, granted).await),
        Err(r) => r,
    }
}

async fn commands() -> Json<Value> {
    Json(Value::Array(
        COMMANDS
            .iter()
            .map(|c| {
                json!({
                    "name": c.name,
                    "summary": c.summary,
                    "access": c.access,
                    "mcp": c.mcp,
                    "latency": c.latency(),
                    "default_deadline_ms": c.default_deadline().as_millis(),
                    "args": c.args.iter().map(|a| json!({
                        "name": a.name,
                        "kind": format!("{:?}", a.kind).to_lowercase(),
                        "required": a.required,
                        "positional": a.positional,
                        "help": a.help,
                    })).collect::<Vec<_>>(),
                })
            })
            .collect(),
    ))
}

#[derive(Deserialize)]
struct FollowParams {
    query: String,
    deadline: Option<String>,
}

/// New events matching a query, as they are committed: each row once, in
/// commit order, as `data:` lines.
async fn follow(
    State(state): State<AppState>,
    headers: HeaderMap,
    UrlQuery(params): UrlQuery<FollowParams>,
) -> Response {
    let granted = match authorize(&state, &headers, Access::Read) {
        Ok(g) => g,
        Err(r) => return r,
    };
    let deadline = match params
        .deadline
        .as_deref()
        .map(sinex_protocol::deadline)
        .transpose()
    {
        Ok(deadline) => deadline.unwrap_or_default(),
        Err(failure) => return respond(Reply::failed(failure)),
    };
    let duration = deadline.duration(Some(
        sinex_protocol::command("follow")
            .expect("follow")
            .default_deadline(),
    ));
    let caller = state.api.caller(granted).await;
    let ctx = state.api.context();
    let bound = match query::bind(&params.query, &ctx) {
        Ok(q) => q,
        Err(e) => return respond(Reply::failed(e.failure(&params.query))),
    };
    let follows_rows = matches!(
        bound.source,
        Source::Table {
            table: Table::Events,
            ..
        }
    ) && !bound.stages.iter().any(|s| {
        matches!(
            s,
            Stage::Group(_) | Stage::GroupFirst { .. } | Stage::Limit(_) | Stage::AsOf(_)
        )
    });
    if !follows_rows {
        return respond(Reply::failed(Error::bad_request(
            "follow takes a query over events, without grouping, limit or as of",
        )));
    }
    let (tx, rx) = mpsc::channel::<Result<Event, std::convert::Infallible>>(64);
    let mut commits = state.commits.clone();
    let mut read_models = state.read_models.clone();
    let api = state.api.clone();
    let text = params.query.clone();
    tokio::spawn(async move {
        // `commits` counts committed batches; the commit position is read here.
        let position = |api: Api| async move {
            crate::request_deadline::run(duration, async move {
                let mut conn = crate::request_deadline::acquire(&api.reads).await?;
                Ok(sinex_store::query::current_seq(&mut conn).await?)
            })
            .await
            .ok()
        };
        commits.borrow_and_update();
        read_models.borrow_and_update();
        let Some(mut last) = position(api.clone()).await else {
            return;
        };
        // A commit after the notice must stay beyond the initial cursor floor.
        if tx
            .send(Ok(Event::default().event("notice").data(DATA_NOTICE)))
            .await
            .is_err()
        {
            return;
        }
        loop {
            tokio::select! {
                changed = commits.changed() => if changed.is_err() { return; },
                changed = read_models.changed() => if changed.is_err() { return; },
                _ = tx.closed() => return,
            }
            commits.borrow_and_update();
            read_models.borrow_and_update();
            let now = {
                let reads = api.reads.clone();
                let Ok(now) = crate::request_deadline::run(duration, async move {
                    let mut conn = crate::request_deadline::acquire(&reads).await?;
                    Ok(sinex_store::query::follow_position(&mut conn).await?)
                })
                .await
                else {
                    return;
                };
                now
            };
            if now <= last {
                continue;
            }
            let q = sinex_store::query::follow_query(&bound, last, now);
            let mut page = Page {
                size: Some(1000),
                cursor: None,
            };
            loop {
                let (reader, caller, text, q, ctx, selected) = (
                    api.clone(),
                    caller.clone(),
                    text.clone(),
                    q.clone(),
                    ctx.clone(),
                    page.clone(),
                );
                let answer = match crate::request_deadline::run(duration, async move {
                    let mut conn = reader.acquire(&caller).await?;
                    Ok(
                        sinex_store::query::run(&mut conn, &text, Some(&q), &ctx, &selected)
                            .await?,
                    )
                })
                .await
                {
                    Ok(answer) => answer,
                    Err(error) => {
                        let _ = tx
                            .send(Ok(Event::default().event("error").data(error.to_string())))
                            .await;
                        return;
                    }
                };
                let rows = answer.rows;
                for row in rows {
                    if tx
                        .send(Ok(Event::default().data(row.to_string())))
                        .await
                        .is_err()
                    {
                        return;
                    }
                }
                match answer.page.cursor {
                    Some(c) => page.cursor = Some(c),
                    None => break,
                }
            }
            last = now;
        }
    });
    Sse::new(ReceiverStream::new(rx))
        .keep_alive(KeepAlive::default())
        .into_response()
}

/// Serve the API until `shutdown` resolves.
pub(crate) async fn serve(
    config: Config,
    api: Api,
    commits: watch::Receiver<u64>,
    read_models: watch::Receiver<()>,
    shutdown: impl std::future::Future<Output = ()> + Send + 'static,
) -> Result<()> {
    let app = router(api, config.tokens, commits, read_models);
    if let Some(parent) = config.socket.parent() {
        std::fs::create_dir_all(parent).map_err(|e| Error::file(parent, e))?;
    }
    // A socket file left by a stopped daemon is replaced; a live one is not.
    if config.socket.exists() {
        if std::os::unix::net::UnixStream::connect(&config.socket).is_ok() {
            return Err(Error::conflict(format!(
                "{} is in use by a running daemon",
                config.socket.display()
            )));
        }
        std::fs::remove_file(&config.socket).map_err(|e| Error::file(&config.socket, e))?;
    }
    let unix = tokio::net::UnixListener::bind(&config.socket)
        .map_err(|e| Error::file(&config.socket, e))?;
    // Only the daemon's user connects; tokens still decide what a call may do.
    use std::os::unix::fs::PermissionsExt;
    std::fs::set_permissions(&config.socket, std::fs::Permissions::from_mode(0o600))
        .map_err(|e| Error::file(&config.socket, e))?;
    let (stop, stopped) = watch::channel(false);
    let mut unix_stop = stopped.clone();
    let unix_task = tokio::spawn(
        axum::serve(unix, app.clone())
            .with_graceful_shutdown(async move {
                let _ = unix_stop.wait_for(|s| *s).await;
            })
            .into_future(),
    );
    let tcp_task = match config.listen {
        Some(addr) => {
            if !addr.ip().is_loopback() {
                return Err(Error::bad_request(format!(
                    "{addr} is not a loopback address; the API listens on localhost only"
                )));
            }
            let tcp = tokio::net::TcpListener::bind(addr)
                .await
                .map_err(|e| Error::bad_request(format!("{addr}: {e}")))?;
            let mut tcp_stop = stopped.clone();
            Some(tokio::spawn(
                axum::serve(tcp, app)
                    .with_graceful_shutdown(async move {
                        let _ = tcp_stop.wait_for(|s| *s).await;
                    })
                    .into_future(),
            ))
        }
        None => None,
    };
    shutdown.await;
    let _ = stop.send(true);
    unix_task.await??;
    if let Some(t) = tcp_task {
        t.await??;
    }
    let _ = std::fs::remove_file(&config.socket);
    Ok(())
}

#[cfg(test)]
mod tests;
