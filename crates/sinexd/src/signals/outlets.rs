//! The outlets a handed-off delivery uses: desktop notifications through the
//! shared deliverer, and value targets and action commands named in
//! deployment configuration. Each attempt is recorded unknown before it runs.
use super::*;
use ::signals::DeliveryOutcome;
use ::signals::notifications::{Notice, NotificationConfig, NotificationUpdate, RouteCommand};
use config::{Command, ValueTarget};
use signal_delivery::{Deliverer, NotificationRequest};
use std::time::Duration;

/// Start the host's one desktop deliverer with its persisted state. Committed
/// notices reach it through the returned channel; every state change comes
/// back as an update the caller persists before the deliverer continues.
pub(super) async fn notifications(
    api: &Api,
    config: &Config,
) -> Result<
    (
        mpsc::UnboundedSender<NotificationRequest>,
        mpsc::UnboundedReceiver<NotificationUpdate>,
        tokio::task::JoinHandle<Result<(), String>>,
    ),
    String,
> {
    let mut conn = api.reads.acquire().await.map_err(error)?;
    let state = sinex_store::signals::load(&mut conn, &api.host)
        .await
        .map_err(error)?
        .map_or_else(Default::default, |saved| saved.checkpoint.notifications);
    let notifications = config.notifications.as_ref();
    let connection = match notifications.map(|notifications| &notifications.bus_address) {
        Some(Some(address)) => zbus::connection::Builder::address(address.as_str())
            .map_err(error)?
            .build()
            .await
            .ok(),
        Some(None) => zbus::Connection::session().await.ok(),
        None => None,
    };
    let configuration = NotificationConfig {
        away_command: notifications
            .and_then(|notifications| notifications.away_command.as_ref())
            .map(|command| RouteCommand {
                program: command.program.clone(),
                arguments: command.arguments.clone(),
            }),
        deadline: Duration::from_millis(config.outlet_timeout_ms),
    };
    let deliverer = Deliverer::new(connection, configuration, state);
    let (requests, receive) = mpsc::unbounded_channel();
    let (updates, changed) = mpsc::unbounded_channel();
    let worker = tokio::spawn(async move {
        signal_delivery::run(deliverer, receive, updates)
            .await
            .map_err(error)
    });
    Ok((requests, changed, worker))
}

/// The desktop notice for one delivery the route function decided to show
/// or send away.
pub(super) fn notice(delivery: &::signals::Delivery, showing: u32) -> Notice {
    let fired = &delivery.fired;
    let reference = fired
        .basis
        .first()
        .map(|row| row.reference.clone())
        .filter(|reference| !reference.is_empty())
        .unwrap_or_else(|| evaluate::rule_reference(&fired.rule_name));
    let what = match (&fired.change, fired.value) {
        (Some(change), _) => format!("{} joined, {} left", change.joined.len(), change.left.len()),
        (None, Some(value)) => format!("{value}"),
        (None, None) if fired.basis.len() == 1 => "new match".into(),
        (None, None) => format!("{} matches", fired.basis.len()),
    };
    Notice {
        delivery_id: fired.id.clone(),
        product: fired.trigger.rule.product.clone(),
        icon: "dialog-information".into(),
        desktop_entry: None,
        rule: fired.rule_name.clone(),
        occurrence: fired.trigger.occurrence.id.clone(),
        summary: format!("{} · {what}", fired.rule_name),
        body: delivery.why(),
        lines: Vec::new(),
        command_hint: format!("sinexctl show {reference}"),
        reference,
        current: fired.current,
        transient: false,
        critical: fired.shipped.urgency == ::signals::Urgency::Critical,
        outlets: ::signals::notifications::Outlets::of(&delivery.decision),
        actions: Vec::new(),
        showing,
    }
}

/// Run one value or action outlet. A failure before anything could have been
/// sent is `not_sent`; a failure after it may have acted is `unknown`.
pub(super) async fn effect(config: &Config, pending: &Pending) -> DeliveryOutcome {
    let deadline = Duration::from_millis(config.outlet_timeout_ms);
    match pending {
        Pending::Action { command, reference } => match config.actions.get(command) {
            Some(command) => command_effect(command, reference, deadline).await,
            None => DeliveryOutcome::NotSent("action_command_not_configured".into()),
        },
        Pending::Value { target, delivery } => match config.values.get(target) {
            Some(target) => match serde_json::to_vec(delivery) {
                Ok(bytes) => value_effect(target, bytes, deadline).await,
                Err(_) => DeliveryOutcome::NotSent("value_encode".into()),
            },
            None => DeliveryOutcome::NotSent("value_target_not_configured".into()),
        },
        Pending::Notice { .. } | Pending::Clear => {
            DeliveryOutcome::NotSent("not_an_effect_outlet".into())
        }
    }
}

async fn command_effect(
    command: &Command,
    reference: &str,
    deadline: Duration,
) -> DeliveryOutcome {
    let mut process = tokio::process::Command::new(&command.program);
    process
        .args(&command.arguments)
        .arg(reference)
        .stdin(std::process::Stdio::null())
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .kill_on_drop(true);
    let Ok(mut child) = process.spawn() else {
        return DeliveryOutcome::NotSent("action_spawn".into());
    };
    match tokio::time::timeout(deadline, child.wait()).await {
        Ok(Ok(status)) if status.success() => DeliveryOutcome::Delivered,
        Ok(Ok(_)) => DeliveryOutcome::NotSent("action_exit".into()),
        // A spawned process may already have acted; stopping it never turns
        // that into a claim that nothing happened.
        Ok(Err(_)) | Err(_) => {
            let _ = child.start_kill();
            let _ = child.wait().await;
            DeliveryOutcome::Unknown
        }
    }
}

async fn value_effect(target: &ValueTarget, bytes: Vec<u8>, deadline: Duration) -> DeliveryOutcome {
    match target {
        ValueTarget::File { path } => file_value(path, &bytes).await,
        ValueTarget::Loopback { url } => http_value(url, bytes, deadline).await,
    }
}

/// Written beside the target and renamed over it, so a reader sees the old
/// value or the new one, never a partial file.
async fn file_value(path: &std::path::Path, bytes: &[u8]) -> DeliveryOutcome {
    use tokio::io::AsyncWriteExt;
    let Some(directory) = path.parent() else {
        return DeliveryOutcome::NotSent("value_file_parent".into());
    };
    let Ok(staged) = tempfile::TempPath::try_from_path(
        directory.join(format!(".sinex-signal-{}", uuid::Uuid::now_v7())),
    ) else {
        return DeliveryOutcome::NotSent("value_stage_path".into());
    };
    let written = async {
        let mut file = tokio::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .mode(0o600)
            .open(&staged)
            .await?;
        file.write_all(bytes).await?;
        file.sync_all().await
    }
    .await;
    if written.is_err() {
        return DeliveryOutcome::NotSent("value_stage_write".into());
    }
    if tokio::fs::rename(&staged, path).await.is_err() {
        return DeliveryOutcome::NotSent("value_rename".into());
    }
    // The rename has published the value; only its durability is unconfirmed
    // if the directory cannot be synced.
    match async {
        tokio::fs::File::open(directory)
            .await?
            .sync_all()
            .await
    }
    .await
    {
        Ok(()) => DeliveryOutcome::Delivered,
        Err(_) => DeliveryOutcome::Unknown,
    }
}

async fn http_value(url: &str, bytes: Vec<u8>, deadline: Duration) -> DeliveryOutcome {
    let Ok(client) = reqwest::Client::builder()
        .no_proxy()
        .redirect(reqwest::redirect::Policy::none())
        .retry(reqwest::retry::never())
        .build()
    else {
        return DeliveryOutcome::NotSent("value_http_client".into());
    };
    let response = tokio::time::timeout(
        deadline,
        client
            .post(url)
            .header("content-type", "application/json")
            .body(bytes)
            .send(),
    )
    .await;
    match response {
        // A value target acknowledges the POST through its status. Error text
        // and URLs stay private, including configured query strings.
        Ok(Ok(response)) if response.status().is_success() => DeliveryOutcome::Delivered,
        Ok(Ok(_)) => DeliveryOutcome::NotSent("value_http_status".into()),
        // Connection and TLS failures precede the request.
        Ok(Err(error)) if error.is_connect() || error.is_builder() => {
            DeliveryOutcome::NotSent("value_http_connect".into())
        }
        Ok(Err(_)) | Err(_) => DeliveryOutcome::Unknown,
    }
}

#[cfg(test)]
mod tests;
