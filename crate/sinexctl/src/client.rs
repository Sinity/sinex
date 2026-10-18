//! Calling the daemon's API over its Unix socket or localhost.
use http_body_util::{BodyExt, Full};
use hyper::body::Bytes;
use hyper::{Request as HttpRequest, StatusCode};
use hyper_util::rt::TokioIo;
use sinex_protocol::{Reply, Request};
use std::path::PathBuf;

#[derive(Clone, Debug)]
pub enum Endpoint {
    Unix(PathBuf),
    /// `host:port` on the loopback interface.
    Tcp(String),
}

impl Endpoint {
    /// `unix:/path`, a bare path, or `http://127.0.0.1:7171`.
    pub fn parse(s: &str) -> Result<Endpoint, String> {
        if let Some(rest) = s.strip_prefix("http://") {
            return Ok(Endpoint::Tcp(rest.trim_end_matches('/').to_string()));
        }
        let path = s.strip_prefix("unix:").unwrap_or(s);
        if path.starts_with('/') {
            Ok(Endpoint::Unix(PathBuf::from(path)))
        } else {
            Err(format!(
                "`{s}` is not an endpoint: use unix:/path/to/socket or http://127.0.0.1:PORT"
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
pub enum ClientError {
    /// The daemon is not running or not reachable.
    Unreachable(String),
    Protocol(String),
}

impl std::fmt::Display for ClientError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ClientError::Unreachable(m) => write!(f, "the daemon is not reachable: {m}"),
            ClientError::Protocol(m) => write!(f, "{m}"),
        }
    }
}

#[derive(Clone, Debug)]
pub struct Client {
    pub endpoint: Endpoint,
    pub token: String,
}

type Sender = hyper::client::conn::http1::SendRequest<Full<Bytes>>;

async fn handshake<T>(io: T) -> Result<Sender, ClientError>
where
    T: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin + Send + 'static,
{
    let (sender, conn) = hyper::client::conn::http1::handshake(TokioIo::new(io))
        .await
        .map_err(|e| ClientError::Unreachable(e.to_string()))?;
    tokio::spawn(conn);
    Ok(sender)
}

impl Client {
    async fn connect(&self) -> Result<Sender, ClientError> {
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

    /// Send one request and return the daemon's reply.
    pub async fn call(&self, request: &Request) -> Result<(StatusCode, Reply), ClientError> {
        let mut sender = self.connect().await?;
        let body = serde_json::to_vec(request).map_err(|e| ClientError::Protocol(e.to_string()))?;
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
    pub async fn follow(
        &self,
        query: &str,
        mut on_event: impl FnMut(&str, &str) -> bool,
    ) -> Result<(), ClientError> {
        let mut sender = self.connect().await?;
        let req = HttpRequest::get(format!("/v1/follow?query={}", percent_encode(query)))
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
            return Err(ClientError::Protocol(match reply {
                Reply::Refused { reason, .. } => reason,
                Reply::Ok { .. } => status.to_string(),
            }));
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
mod tests {
    use super::*;

    #[test]
    fn endpoints() {
        assert!(matches!(
            Endpoint::parse("unix:/run/sinex/s.sock").unwrap(),
            Endpoint::Unix(_)
        ));
        assert!(matches!(
            Endpoint::parse("/run/sinex/s.sock").unwrap(),
            Endpoint::Unix(_)
        ));
        assert!(
            matches!(Endpoint::parse("http://127.0.0.1:7171/").unwrap(), Endpoint::Tcp(h) if h == "127.0.0.1:7171")
        );
        assert!(Endpoint::parse("relative").is_err());
        assert_eq!(percent_encode("a b&c"), "a%20b%26c");
    }
}
