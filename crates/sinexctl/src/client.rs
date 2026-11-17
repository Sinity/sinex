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

impl std::fmt::Display for Endpoint {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Unix(path) => write!(f, "unix:{}", path.display()),
            Self::Tcp(address) => write!(f, "http://{address}"),
        }
    }
}

#[derive(Debug)]
pub(crate) enum ClientError {
    /// The daemon is not running or not reachable.
    Unreachable {
        endpoint: String,
        error: String,
    },
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
            ClientError::Unreachable { endpoint, error } => Failure::new(
                FailureKind::NotRunning,
                format!("the daemon is not reachable at {endpoint}: {error}"),
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
            ClientError::Unreachable { endpoint, error } => {
                write!(f, "the daemon is not reachable at {endpoint}: {error}")
            }
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

async fn handshake<T>(io: T, endpoint: &Endpoint) -> Result<Connection, ClientError>
where
    T: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin + Send + 'static,
{
    let (sender, conn) = hyper::client::conn::http1::handshake(TokioIo::new(io))
        .await
        .map_err(|error| ClientError::Unreachable {
            endpoint: endpoint.to_string(),
            error: error.to_string(),
        })?;
    Ok(Connection {
        sender,
        driver: tokio::spawn(conn),
    })
}

impl Client {
    fn unreachable(&self, error: impl std::fmt::Display) -> ClientError {
        ClientError::Unreachable {
            endpoint: self.endpoint.to_string(),
            error: error.to_string(),
        }
    }

    async fn connect(&self) -> Result<Connection, ClientError> {
        match &self.endpoint {
            Endpoint::Unix(path) => {
                let s = tokio::net::UnixStream::connect(path)
                    .await
                    .map_err(|error| self.unreachable(error))?;
                handshake(s, &self.endpoint).await
            }
            Endpoint::Tcp(addr) => {
                let s = tokio::net::TcpStream::connect(addr)
                    .await
                    .map_err(|error| self.unreachable(error))?;
                handshake(s, &self.endpoint).await
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
            .map_err(|error| self.unreachable(error))?;
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
            .map_err(|error| self.unreachable(error))?;
        let status = resp.status();
        let mut body = resp.into_body();
        if !status.is_success() {
            return Err(refusal(status, body).await);
        }
        let mut events = Events::default();
        while let Some((event, data)) = events.next(&mut body).await? {
            if !on_event(&event, &data) {
                return Ok(());
            }
        }
        Ok(())
    }

    /// Open `backup prepare`'s stream and wait for the copy job the daemon
    /// starts, returned as its JSON. The daemon keeps the job's snapshot open
    /// while the returned stream is alive; dropping it closes the snapshot.
    pub(crate) async fn backup_start(
        &self,
        output: &str,
    ) -> Result<(String, BackupStream), ClientError> {
        let default = sinex_protocol::command("backup")
            .expect("backup command")
            .default_deadline();
        let mut connection = self
            .connect_before(self.deadline.connection_timeout(Some(default)))
            .await?;
        let req = HttpRequest::get(format!("/v1/backup/prepare?to={}", percent_encode(output)))
            .header("host", self.endpoint.host())
            .header("authorization", format!("Bearer {}", self.token))
            .body(Full::new(Bytes::new()))
            .map_err(|e| ClientError::Protocol(e.to_string()))?;
        let resp = connection
            .send_request(req)
            .await
            .map_err(|error| self.unreachable(error))?;
        let status = resp.status();
        let mut body = resp.into_body();
        if !status.is_success() {
            return Err(refusal(status, body).await);
        }
        let mut events = Events::default();
        match events.next(&mut body).await? {
            Some((event, data)) if event == "snapshot" => Ok((
                data,
                BackupStream {
                    _connection: connection,
                    body,
                    events,
                },
            )),
            Some((event, data)) if event == "error" => Err(ClientError::Failed(
                serde_json::from_str(&data)
                    .map_err(|e| ClientError::Protocol(format!("backup failure: {e}")))?,
            )),
            Some((event, _)) => Err(ClientError::Protocol(format!(
                "backup prepare sent an unexpected {event} event"
            ))),
            None => Err(ClientError::Protocol(
                "backup prepare ended before the daemon's copy job".into(),
            )),
        }
    }
}

/// The open stream of a backup's copy job: while it lives, the daemon keeps
/// the backup's snapshot open.
pub(crate) struct BackupStream {
    _connection: Connection,
    body: hyper::body::Incoming,
    events: Events,
}

impl BackupStream {
    pub(crate) async fn stopped(&mut self) -> Failure {
        match self.events.next(&mut self.body).await {
            Ok(Some((event, data))) if event == "error" => serde_json::from_str(&data)
                .unwrap_or_else(|error| protocol_mismatch("backup", error)),
            Ok(Some((event, _))) => {
                protocol_mismatch("backup", format!("unexpected event {event}"))
            }
            Ok(None) => Failure::new(
                FailureKind::Cancelled,
                "the daemon closed the backup snapshot before its copy finished",
            ),
            Err(error) => error.failure("backup"),
        }
    }
}

/// The typed failure in a refused streaming request's body.
async fn refusal(status: StatusCode, body: hyper::body::Incoming) -> ClientError {
    let bytes = match body.collect().await {
        Ok(collected) => collected.to_bytes(),
        Err(e) => return ClientError::Protocol(e.to_string()),
    };
    match serde_json::from_slice::<Reply>(&bytes) {
        Ok(Reply::Failed { failure } | Reply::StoppedEarly { failure, .. }) => {
            ClientError::Failed(failure)
        }
        Ok(Reply::Ok { .. }) => ClientError::Protocol(status.to_string()),
        Err(e) => ClientError::Protocol(format!("{status}: {e}")),
    }
}

/// Server-sent events read from a response body: each an event name and
/// its data lines joined.
#[derive(Default)]
struct Events {
    buffer: String,
}

impl Events {
    async fn next(
        &mut self,
        body: &mut hyper::body::Incoming,
    ) -> Result<Option<(String, String)>, ClientError> {
        loop {
            // Events end with a blank line; one without data (a keep-alive
            // comment) is skipped.
            while let Some(end) = self.buffer.find("\n\n") {
                let block: String = self.buffer.drain(..end + 2).collect();
                let mut event = "message";
                let mut payload = Vec::new();
                for line in block.lines() {
                    if let Some(e) = line.strip_prefix("event:") {
                        event = e.trim();
                    } else if let Some(d) = line.strip_prefix("data:") {
                        payload.push(d.strip_prefix(' ').unwrap_or(d));
                    }
                }
                if !payload.is_empty() {
                    return Ok(Some((event.to_owned(), payload.join("\n"))));
                }
            }
            let Some(frame) = body.frame().await else {
                return Ok(None);
            };
            let frame = frame.map_err(|e| ClientError::Protocol(e.to_string()))?;
            if let Some(data) = frame.data_ref() {
                self.buffer.push_str(&String::from_utf8_lossy(data));
            }
        }
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
