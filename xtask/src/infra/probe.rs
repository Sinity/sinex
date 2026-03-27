use std::net::{SocketAddr, TcpStream};
use std::time::{Duration, Instant};

use crate::infra::services::nats::NatsManager;
use crate::infra::services::postgres::PostgresManager;
use crate::infra::stack::StackConfig;

#[derive(Debug, Clone)]
pub struct PostgresProbe {
    pub running: bool,
    pub accepting_connections: bool,
    pub latency_ms: u64,
    pub message: Option<String>,
}

impl PostgresProbe {
    #[must_use]
    pub fn ready(&self) -> bool {
        self.running && self.accepting_connections
    }
}

#[derive(Debug, Clone)]
pub struct NatsProbe {
    pub running: bool,
    pub reachable: bool,
    pub latency_ms: u64,
    pub port: u16,
    pub message: Option<String>,
}

impl NatsProbe {
    #[must_use]
    pub fn ready(&self) -> bool {
        self.running && self.reachable
    }
}

#[must_use]
pub fn probe_postgres() -> PostgresProbe {
    let start = Instant::now();
    let config = match StackConfig::for_current_checkout() {
        Ok(config) => config,
        Err(error) => {
            return PostgresProbe {
                running: false,
                accepting_connections: false,
                latency_ms: start.elapsed().as_millis() as u64,
                message: Some(format!("failed to load stack config: {error}")),
            };
        }
    };

    let manager = PostgresManager::new(config.to_shared_pg());
    let running = manager.is_running();
    let (accepting_connections, accepting_issue) = match manager.accepting_connections_probe() {
        Ok(accepting_connections) => (accepting_connections, None),
        Err(error) => (
            false,
            Some(format!("failed to verify PostgreSQL readiness with pg_isready: {error:#}")),
        ),
    };
    let latency_ms = start.elapsed().as_millis() as u64;
    let message = accepting_issue.or_else(|| match (running, accepting_connections) {
        (true, true) => None,
        (true, false) => Some("postmaster is running but not accepting connections".to_string()),
        (false, true) => Some("Postgres socket responds but no managed postmaster is tracked".to_string()),
        (false, false) => Some("Postgres is not running for this checkout".to_string()),
    });

    PostgresProbe {
        running,
        accepting_connections,
        latency_ms,
        message,
    }
}

#[must_use]
pub fn probe_nats() -> NatsProbe {
    let start = Instant::now();
    let config = match StackConfig::for_current_checkout() {
        Ok(config) => config,
        Err(error) => {
            return NatsProbe {
                running: false,
                reachable: false,
                latency_ms: start.elapsed().as_millis() as u64,
                port: 4222,
                message: Some(format!("failed to load stack config: {error}")),
            };
        }
    };

    let port = config.nats.port;
    let manager = NatsManager::new(config.to_shared_nats());
    let running = manager.is_running();
    let reachability = TcpStream::connect_timeout(
        &SocketAddr::from(([127, 0, 0, 1], port)),
        Duration::from_millis(500),
    );
    let reachable = reachability.is_ok();
    let reachability_issue = reachability.err().map(|error| error.to_string());
    let latency_ms = start.elapsed().as_millis() as u64;
    let message = nats_probe_message(running, reachable, port, reachability_issue.as_deref());

    NatsProbe {
        running,
        reachable,
        latency_ms,
        port,
        message,
    }
}

fn nats_probe_message(
    running: bool,
    reachable: bool,
    port: u16,
    reachability_issue: Option<&str>,
) -> Option<String> {
    match (running, reachable) {
        (true, true) => None,
        (true, false) => Some(match reachability_issue {
            Some(issue) => format!(
                "nats-server PID is tracked but port {port} is not accepting connections: {issue}"
            ),
            None => format!("nats-server PID is tracked but port {port} is not accepting connections"),
        }),
        (false, true) => Some(format!(
            "NATS is reachable on port {port}, but no managed nats-server PID is tracked"
        )),
        (false, false) => Some(match reachability_issue {
            Some(issue) => format!("NATS is not reachable on port {port}: {issue}"),
            None => format!("NATS is not reachable on port {port}"),
        }),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use xtask::sandbox::prelude::*;

    #[sinex_test]
    async fn nats_probe_message_includes_connect_error_for_unreachable_port() -> TestResult<()> {
        let message =
            nats_probe_message(false, false, 4222, Some("Connection refused (os error 111)"));

        assert_eq!(
            message.as_deref(),
            Some("NATS is not reachable on port 4222: Connection refused (os error 111)")
        );
        Ok(())
    }

    #[sinex_test]
    async fn nats_probe_message_preserves_pid_drift_signal() -> TestResult<()> {
        let message = nats_probe_message(false, true, 4222, None);

        assert_eq!(
            message.as_deref(),
            Some("NATS is reachable on port 4222, but no managed nats-server PID is tracked")
        );
        Ok(())
    }
}
