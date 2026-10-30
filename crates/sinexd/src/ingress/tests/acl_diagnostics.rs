use async_nats::{Client, Event, ServerError};
use serde_json::json;
use std::sync::atomic::{AtomicUsize, Ordering};

#[derive(Default)]
pub(super) struct Events([AtomicUsize; 10]);

impl Events {
    pub(super) fn record(&self, event: Event) {
        let category = match event {
            Event::Connected => 0,
            Event::Disconnected => 1,
            Event::Closed => 2,
            Event::SlowConsumer(_) | Event::ServerError(ServerError::SlowConsumer(_)) => 3,
            Event::ServerError(ServerError::AuthorizationViolation) => 4,
            Event::ServerError(ServerError::Other(message)) => {
                if message
                    .to_ascii_lowercase()
                    .contains("permissions violation")
                {
                    5
                } else {
                    6
                }
            }
            Event::ClientError(_) => 7,
            Event::LameDuckMode => 8,
            Event::Draining => 9,
        };
        self.0[category].fetch_add(1, Ordering::Relaxed);
    }

    fn snapshot(&self) -> serde_json::Value {
        let labels = [
            "connected",
            "disconnected",
            "closed",
            "slow_consumer",
            "server_auth",
            "server_permission",
            "server_other",
            "client_error",
            "lame_duck",
            "draining",
        ];
        labels
            .into_iter()
            .zip(&self.0)
            .map(|(label, count)| (label.to_owned(), json!(count.load(Ordering::Relaxed))))
            .collect::<serde_json::Map<_, _>>()
            .into()
    }
}

pub(super) async fn ready(client: &Client) -> Result<(), &'static str> {
    // A broker 503 on this same connection proves it processed the preceding SUB.
    // Socket flush alone does not prove that the broker processed the subscription.
    let subject = format!("acl-readiness.{}", uuid::Uuid::now_v7());
    match client.request(subject, Vec::new().into()).await {
        Err(error) if error.kind() == async_nats::RequestErrorKind::NoResponders => Ok(()),
        Err(_) => Err("readiness_request_error"),
        Ok(_) => Err("unexpected_readiness_responder"),
    }
}

pub(super) fn report<T>(
    phase: &str,
    outcome: &Result<Result<T, &'static str>, tokio::time::error::Elapsed>,
    server: &devkit::nats::Server,
    clients: [Option<&Client>; 2],
    events: [&Events; 2],
    subjects: &[&str; 6],
) {
    let category = match outcome {
        Ok(Ok(_)) => "delivered",
        Ok(Err(category)) => category,
        Err(_) => "timeout",
    };
    let log = server.log();
    let lines = log.as_deref().unwrap_or("").lines().collect::<Vec<_>>();
    let count = |needle: &str| lines.iter().filter(|line| line.contains(needle)).count();
    let subject_lines = subjects.map(|subject| count(subject));
    let permissions = lines
        .iter()
        .filter(|line| line.to_ascii_lowercase().contains("permissions violation"))
        .count();
    eprintln!(
        "acl_exchange {}",
        json!({
            "phase": phase, "outcome": category,
            "daemon_state": clients[0].map(|client| format!("{:?}", client.connection_state())),
            "kitty_state": clients[1].map(|client| format!("{:?}", client.connection_state())),
            "daemon_events": events[0].snapshot(), "kitty_events": events[1].snapshot(),
            "server_ready": server.ready(), "server_log_present": log.is_some(),
            "server_log": {
                "permission_lines": permissions, "authorization_lines": count("Authorization Violation"),
                "disconnect_lines": count("Disconnected"), "closed_lines": count("Closed"),
                "error_lines": count("[ERR]"), "fatal_lines": count("[FTL]"),
                // In publication order: create, delete, foreign, old scope, raw GET, own NEXT.
                "synthetic_subject_lines": subject_lines,
            },
        })
    );
    if !matches!(outcome, Ok(Ok(_))) {
        match log
            .as_deref()
            .ok_or("owned_log_unavailable")
            .and_then(retain_log)
        {
            Ok(path) => eprintln!("acl_private_server_log={}", path.display()),
            Err(category) => eprintln!("acl_private_server_log_unavailable={category}"),
        }
    }
}

fn retain_log(log: &str) -> Result<std::path::PathBuf, &'static str> {
    use std::io::Write;
    use std::os::unix::fs::{DirBuilderExt, OpenOptionsExt};
    let base = std::env::var_os("TEST_DURABLE_TMPDIR")
        .map(std::path::PathBuf::from)
        .ok_or("durable_root_not_configured")?;
    if !base.is_absolute() {
        return Err("durable_root_not_absolute");
    }
    std::fs::DirBuilder::new()
        .recursive(true)
        .mode(0o700)
        .create(&base)
        .map_err(|_| "durable_root_create_failed")?;
    let base = base
        .canonicalize()
        .map_err(|_| "durable_root_resolve_failed")?;
    if let Ok(disposable) = std::env::temp_dir().canonicalize()
        && base.starts_with(disposable)
    {
        return Err("durable_root_is_disposable");
    }
    let directory = base.join(format!("producer-acl-{}", uuid::Uuid::now_v7()));
    std::fs::DirBuilder::new()
        .mode(0o700)
        .create(&directory)
        .map_err(|_| "private_directory_create_failed")?;
    let path = directory.join("server.log");
    let mut file = std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o600)
        .open(&path)
        .map_err(|_| "private_log_create_failed")?;
    file.write_all(log.as_bytes())
        .and_then(|()| file.sync_all())
        .map_err(|_| "private_log_sync_failed")?;
    std::fs::File::open(&directory)
        .and_then(|directory| directory.sync_all())
        .map_err(|_| "private_directory_sync_failed")?;
    Ok(path)
}
