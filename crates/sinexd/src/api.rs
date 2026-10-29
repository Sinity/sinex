//! The HTTP API: JSON over a Unix socket and, when configured, localhost. One
//! endpoint takes every request of the command table; `follow` streams new
//! matching events as server-sent events. Three fixed tokens grant read, agent
//! and operator access.
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
use sinex_protocol::{Access, COMMANDS, DATA_NOTICE, Reply, Request};
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

fn authorize(state: &AppState, headers: &HeaderMap, needs: Access) -> Result<Access, Response> {
    let presented = headers
        .get(axum::http::header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.strip_prefix("Bearer "))
        .map(str::trim);
    let refuse = |status: StatusCode, reason: String| {
        (
            status,
            Json(Reply::Refused {
                reason,
                not_built: false,
            }),
        )
            .into_response()
    };
    let Some(presented) = presented else {
        return Err(refuse(
            StatusCode::UNAUTHORIZED,
            "no token: send `Authorization: Bearer <token>`".into(),
        ));
    };
    let Some(granted) = state.tokens.grant(presented) else {
        return Err(refuse(StatusCode::UNAUTHORIZED, "unknown token".into()));
    };
    if granted < needs {
        return Err(refuse(
            StatusCode::FORBIDDEN,
            format!(
                "this token grants {} access; the request needs {}",
                actor(granted),
                actor(needs)
            ),
        ));
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
    Json(request): Json<Request>,
) -> Response {
    match authorize(&state, &headers, request.access()) {
        Ok(granted) => Json(state.api.call(request, granted).await).into_response(),
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
    let caller = state.api.caller(granted).await;
    let ctx = state.api.context();
    let bound = match query::bind(&params.query, &ctx) {
        Ok(q) => q,
        Err(e) => {
            return (
                StatusCode::BAD_REQUEST,
                Json(Reply::Refused {
                    reason: e.render(&params.query),
                    not_built: e.not_built,
                }),
            )
                .into_response();
        }
    };
    let follows_rows = matches!(
        bound.source,
        Source::Table {
            table: Table::Events,
            ..
        }
    ) && !bound
        .stages
        .iter()
        .any(|s| matches!(s, Stage::Group(_) | Stage::Limit(_) | Stage::AsOf(_)));
    if !follows_rows {
        return (
            StatusCode::BAD_REQUEST,
            Json(Reply::Refused {
                reason: "follow takes a query over events, without grouping, limit or as of".into(),
                not_built: false,
            }),
        )
            .into_response();
    }
    let (tx, rx) = mpsc::channel::<Result<Event, std::convert::Infallible>>(64);
    let mut commits = state.commits.clone();
    let mut read_models = state.read_models.clone();
    let api = state.api.clone();
    let text = params.query.clone();
    tokio::spawn(async move {
        // `commits` counts committed batches; the commit position is read here.
        let position = |api: Api| async move {
            let mut conn = api.reads.acquire().await.ok()?;
            sinex_store::query::current_seq(&mut conn).await.ok()
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
                let Ok(mut conn) = api.reads.acquire().await else {
                    return;
                };
                let Ok(now) = sinex_store::query::follow_position(&mut conn).await else {
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
                let Ok(mut conn) = api.acquire(&caller).await else {
                    return;
                };
                let answer =
                    match sinex_store::query::run(&mut conn, &text, Some(&q), &ctx, &page).await {
                        Ok(a) => a,
                        Err(e) => {
                            let _ = tx
                                .send(Ok(Event::default().event("error").data(e.to_string())))
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
) -> Result<(), String> {
    let app = router(api, config.tokens, commits, read_models);
    if let Some(parent) = config.socket.parent() {
        std::fs::create_dir_all(parent).map_err(|e| format!("{}: {e}", parent.display()))?;
    }
    // A socket file left by a stopped daemon is replaced; a live one is not.
    if config.socket.exists() {
        if std::os::unix::net::UnixStream::connect(&config.socket).is_ok() {
            return Err(format!(
                "{} is in use by a running daemon",
                config.socket.display()
            ));
        }
        std::fs::remove_file(&config.socket)
            .map_err(|e| format!("{}: {e}", config.socket.display()))?;
    }
    let unix = tokio::net::UnixListener::bind(&config.socket)
        .map_err(|e| format!("{}: {e}", config.socket.display()))?;
    // Only the daemon's user connects; tokens still decide what a call may do.
    use std::os::unix::fs::PermissionsExt;
    std::fs::set_permissions(&config.socket, std::fs::Permissions::from_mode(0o600))
        .map_err(|e| format!("{}: {e}", config.socket.display()))?;
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
                return Err(format!(
                    "{addr} is not a loopback address; the API listens on localhost only"
                ));
            }
            let tcp = tokio::net::TcpListener::bind(addr)
                .await
                .map_err(|e| format!("{addr}: {e}"))?;
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
    unix_task
        .await
        .map_err(|e| e.to_string())?
        .map_err(|e| e.to_string())?;
    if let Some(t) = tcp_task {
        t.await
            .map_err(|e| e.to_string())?
            .map_err(|e| e.to_string())?;
    }
    let _ = std::fs::remove_file(&config.socket);
    Ok(())
}

#[cfg(test)]
mod tests;
