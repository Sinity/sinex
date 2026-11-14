//! Calling the daemon's API over its Unix socket or localhost.
use command_surface::{Failure, FailureKind};
use http_body_util::{BodyExt, Full};
use hyper::body::Bytes;
use hyper::{Request as HttpRequest, StatusCode};
use hyper_util::rt::TokioIo;
use sinex_protocol::{Reply, Request};
use std::path::PathBuf;

#[derive(Clone, Debug)]
pub(crate) enum Endpoint {
    Unix(PathBuf),
    /// `host:port` on the loopback interface.
    Tcp(String),
}

impl Endpoint {
    /// `unix:/path`, a bare path, or `http://127.0.0.1:7171`.
    pub(crate) fn parse(s: &str) -> Result<Endpoint, Failure> {
        if let Some(rest) = s.strip_prefix("http://") {
            return Ok(Endpoint::Tcp(rest.trim_end_matches('/').to_string()));
        }
        let path = s.strip_prefix("unix:").unwrap_or(s);
        if path.starts_with('/') {
            Ok(Endpoint::Unix(PathBuf::from(path)))
        } else {
            Err(Failure::new(
                FailureKind::BadRequest,
                format!(
                    "`{s}` is not an endpoint: use unix:/path/to/socket or http://127.0.0.1:PORT"
                ),
            ))
        }
    }

    fn host(&self) -> String {
        match self {
            Endpoint::Unix(_) => "localhost".to_string(),
            Endpoint::Tcp(h) => h.clone(),
        }
    }
}

#[derive(Debug)]
pub(crate) enum ClientError {
    /// The daemon is not running or not reachable.
    Unreachable(String),
    Protocol(String),
    /// The daemon refused the call with a typed failure.
    Failed(Failure),
}

impl ClientError {
    /// The failure this is, in the shared table: a daemon that cannot be
    /// reached is not running, and an answer this build cannot read comes
    /// from a daemon built from other sources.
    pub(crate) fn failure(self, operation: &str) -> Failure {
        match self {
            ClientError::Unreachable(m) => Failure::new(
                FailureKind::NotRunning,
                format!("the daemon is not reachable: {m}"),
            )
            .with_code("daemon_unreachable")
            .with_hint(
                "start sinexd (systemctl start sinexd, or `sinexd run`) or name its --endpoint",
            ),
            ClientError::Protocol(m) => protocol_mismatch(operation, m),
            ClientError::Failed(failure) => failure,
        }
    }
}

/// An answer whose shape this build does not read.
pub(crate) fn protocol_mismatch(operation: &str, error: impl std::fmt::Display) -> Failure {
    Failure::new(
        FailureKind::NotRunning,
        format!(
            "the daemon answered {operation} in a shape this build does not read ({error}); \
             it speaks another protocol version, so restart it"
        ),
    )
    .with_code("daemon_protocol_mismatch")
}

impl std::fmt::Display for ClientError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ClientError::Unreachable(m) => write!(f, "the daemon is not reachable: {m}"),
            ClientError::Protocol(m) => write!(f, "{m}"),
            ClientError::Failed(failure) => write!(f, "{failure}"),
        }
    }
}

#[derive(Clone, Debug)]
pub(crate) struct Client {
    pub endpoint: Endpoint,
    pub token: String,
    pub deadline: command_surface::Deadline,
}

type Sender = hyper::client::conn::http1::SendRequest<Full<Bytes>>;

/// One call owns its HTTP driver. Dropping an MCP call or CLI read closes
/// the socket, so the daemon can cancel that request's unowned read.
struct Connection {
    sender: Sender,
    driver: tokio::task::JoinHandle<Result<(), hyper::Error>>,
}
impl std::ops::Deref for Connection {
    type Target = Sender;
    fn deref(&self) -> &Sender {
        &self.sender
    }
}
impl std::ops::DerefMut for Connection {
    fn deref_mut(&mut self) -> &mut Sender {
        &mut self.sender
    }
}
impl Drop for Connection {
    fn drop(&mut self) {
        self.driver.abort();
    }
}

async fn handshake<T>(io: T) -> Result<Connection, ClientError>
where
    T: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin + Send + 'static,
{
    let (sender, conn) = hyper::client::conn::http1::handshake(TokioIo::new(io))
        .await
        .map_err(|e| ClientError::Unreachable(e.to_string()))?;
    Ok(Connection {
        sender,
        driver: tokio::spawn(conn),
    })
}

impl Client {
    async fn connect(&self) -> Result<Connection, ClientError> {
        match &self.endpoint {
            Endpoint::Unix(path) => {
                let s = tokio::net::UnixStream::connect(path)
                    .await
                    .map_err(|e| ClientError::Unreachable(format!("{}: {e}", path.display())))?;
                handshake(s).await
            }
            Endpoint::Tcp(addr) => {
                let s = tokio::net::TcpStream::connect(addr)
                    .await
                    .map_err(|e| ClientError::Unreachable(format!("{addr}: {e}")))?;
                handshake(s).await
            }
        }
    }

    async fn connect_before(
        &self,
        duration: Option<std::time::Duration>,
    ) -> Result<Connection, ClientError> {
        let operation = self.connect();
        tokio::pin!(operation);
        let start = std::time::Instant::now();
        loop {
            let Some(left) = duration.map(|duration| duration.saturating_sub(start.elapsed()))
            else {
                return operation.await;
            };
            tokio::select! {
                result = &mut operation => return result,
                _ = tokio::time::sleep(left.min(std::time::Duration::from_secs(86_400))) => {
                    if duration.is_some_and(|duration| start.elapsed() >= duration) {
                        let mut failure = Failure::new(FailureKind::Timeout, "the daemon connection deadline passed").with_code(command_surface::DEADLINE_EXCEEDED);
                        failure.retryable = true;
                        return Err(ClientError::Failed(failure));
                    }
                }
            }
        }
    }

    /// Send one request and return the daemon's reply.
    pub(crate) async fn call(&self, request: &Request) -> Result<(StatusCode, Reply), ClientError> {
        let default = sinex_protocol::command(request.method())
            .expect("declared command")
            .default_deadline();
        let mut sender = self
            .connect_before(self.deadline.connection_timeout(Some(default)))
            .await?;
        let body = serde_json::to_vec(&sinex_protocol::Call {
            request: request.clone(),
            deadline: self.deadline,
        })
        .map_err(|e| ClientError::Protocol(e.to_string()))?;
        let req = HttpRequest::post("/v1")
            .header("host", self.endpoint.host())
            .header("content-type", "application/json")
            .header("authorization", format!("Bearer {}", self.token))
            .body(Full::new(Bytes::from(body)))
            .map_err(|e| ClientError::Protocol(e.to_string()))?;
        let resp = sender
            .send_request(req)
            .await
            .map_err(|e| ClientError::Unreachable(e.to_string()))?;
        let status = resp.status();
        let bytes = resp
            .into_body()
            .collect()
            .await
            .map_err(|e| ClientError::Protocol(e.to_string()))?
            .to_bytes();
        let reply: Reply = serde_json::from_slice(&bytes).map_err(|e| {
            ClientError::Protocol(format!(
                "{status}: {e}: {}",
                String::from_utf8_lossy(&bytes)
            ))
        })?;
        Ok((status, reply))
    }

    /// Stream `follow`: call `on_event(event, data)` for each server-sent
    /// event until the daemon closes the stream or `on_event` returns false.
    pub(crate) async fn follow(
        &self,
        query: &str,
        mut on_event: impl FnMut(&str, &str) -> bool,
    ) -> Result<(), ClientError> {
        let default = sinex_protocol::command("follow")
            .expect("follow command")
            .default_deadline();
        let mut sender = self
            .connect_before(self.deadline.connection_timeout(Some(default)))
            .await?;
        let deadline = match self.deadline {
            command_surface::Deadline::Default => String::new(),
            command_surface::Deadline::None => "&deadline=none".into(),
            command_surface::Deadline::Milliseconds(ms) => format!("&deadline={ms}"),
        };
        let req = HttpRequest::get(format!(
            "/v1/follow?query={}{}",
            percent_encode(query),
            deadline
        ))
        .header("host", self.endpoint.host())
        .header("authorization", format!("Bearer {}", self.token))
        .body(Full::new(Bytes::new()))
        .map_err(|e| ClientError::Protocol(e.to_string()))?;
        let resp = sender
            .send_request(req)
            .await
            .map_err(|e| ClientError::Unreachable(e.to_string()))?;
        let status = resp.status();
        let mut body = resp.into_body();
        if !status.is_success() {
            let bytes = body
                .collect()
                .await
                .map_err(|e| ClientError::Protocol(e.to_string()))?
                .to_bytes();
            let reply: Reply = serde_json::from_slice(&bytes)
                .map_err(|e| ClientError::Protocol(format!("{status}: {e}")))?;
            return Err(match reply {
                Reply::Failed { failure } | Reply::StoppedEarly { failure, .. } => {
                    ClientError::Failed(failure)
                }
                Reply::Ok { .. } => ClientError::Protocol(status.to_string()),
            });
        }
        let mut buffer = String::new();
        while let Some(frame) = body.frame().await {
            let frame = frame.map_err(|e| ClientError::Protocol(e.to_string()))?;
            let Some(data) = frame.data_ref() else {
                continue;
            };
            buffer.push_str(&String::from_utf8_lossy(data));
            // Events end with a blank line.
            while let Some(end) = buffer.find("\n\n") {
                let block: String = buffer.drain(..end + 2).collect();
                let mut event = "message";
                let mut payload = Vec::new();
                for line in block.lines() {
                    if let Some(e) = line.strip_prefix("event:") {
                        event = e.trim();
                    } else if let Some(d) = line.strip_prefix("data:") {
                        payload.push(d.strip_prefix(' ').unwrap_or(d));
                    }
                }
                if !payload.is_empty() && !on_event(event, &payload.join("\n")) {
                    return Ok(());
                }
            }
        }
        Ok(())
    }
}

fn percent_encode(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for b in s.bytes() {
        if b.is_ascii_alphanumeric() || b"-_.~".contains(&b) {
            out.push(b as char);
        } else {
            out.push_str(&format!("%{b:02X}"));
        }
    }
    out
}

#[cfg(test)]
mod tests;
