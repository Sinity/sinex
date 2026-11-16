//! The outlets a handed-off delivery uses: desktop notifications through the
//! shared deliverer, and value targets and action commands named in
//! deployment configuration. Each attempt is recorded unknown before it runs.
use super::*;
use crate::{Error, Result};
use ::signals::DeliveryOutcome;
use ::signals::notifications::{Notice, NotificationConfig, RouteCommand};
use config::{Command, ValueTarget};
use signal_delivery::{Deliverer, NotificationRequest, NotificationUpdate};
use std::time::Duration;

/// Start the host's one desktop deliverer with its persisted state. Committed
/// notices reach it through the returned channel; every state change comes
/// back as an update the caller persists before the deliverer continues.
pub(super) async fn notifications(
    api: &Api,
    config: &Config,
) -> Result<(
    mpsc::UnboundedSender<NotificationRequest>,
    mpsc::UnboundedReceiver<NotificationUpdate>,
    tokio::task::JoinHandle<Result<()>>,
)> {
    let mut conn = api.reads.acquire().await?;
    let state = sinex_store::signals::load(&mut conn, &api.host)
        .await?
        .map_or_else(Default::default, |saved| saved.checkpoint.notifications);
    let notifications = config.notifications.as_ref();
    let connection = match notifications.map(|notifications| &notifications.bus_address) {
        Some(Some(address)) => zbus::connection::Builder::address(address.as_str())?
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
            .map_err(Error::from)
    });
    Ok((requests, changed, worker))
}

/// The desktop notice for one delivery the route function decided to show
/// or send away.
/// What a delivery is about: its first matched row's reference, else its
/// rule's citation.
pub(super) fn reference(delivery: &::signals::Delivery) -> String {
    delivery
        .fired
        .basis
        .first()
        .map(|row| row.reference.clone())
        .filter(|reference| !reference.is_empty())
        .unwrap_or_else(|| evaluate::rule_reference(&delivery.fired.rule_name))
}

pub(super) fn notice(delivery: &::signals::Delivery, showing: u32) -> Notice {
    let fired = &delivery.fired;
    let reference = reference(delivery);
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

async fn command_effect(command: &Command, reference: &str, deadline: Duration) -> DeliveryOutcome {
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
    use sinex_transport::durable::{ReplaceStep, write_replacing};
    use std::io::Write;
    if path.parent().is_none() {
        return DeliveryOutcome::NotSent("value_file_parent".into());
    }
    let path = path.to_owned();
    let bytes = bytes.to_vec();
    match tokio::task::spawn_blocking(move || {
        write_replacing(&path, 0o600, |file| file.write_all(&bytes))
    })
    .await
    {
        Ok(Ok(())) => DeliveryOutcome::Delivered,
        Ok(Err((ReplaceStep::Write, _))) => DeliveryOutcome::NotSent("value_stage_write".into()),
        Ok(Err((ReplaceStep::Rename, _))) => DeliveryOutcome::NotSent("value_rename".into()),
        Ok(Err((ReplaceStep::SyncDirectory, _))) | Err(_) => DeliveryOutcome::Unknown,
    }
}

async fn http_value(url: &str, bytes: Vec<u8>, deadline: Duration) -> DeliveryOutcome {
    let url = url.to_owned();
    tokio::task::spawn_blocking(move || {
        let agent: ureq::Agent = ureq::Agent::config_builder()
            .max_redirects(0)
            .proxy(None)
            .http_status_as_error(false)
            .timeout_global(Some(deadline))
            .build()
            .into();
        match agent
            .post(&url)
            .header("content-type", "application/json")
            .send(&bytes[..])
        {
            Ok(response) if response.status().is_success() => DeliveryOutcome::Delivered,
            Ok(_) => DeliveryOutcome::NotSent("value_http_status".into()),
            Err(
                ureq::Error::ConnectionFailed
                | ureq::Error::HostNotFound
                | ureq::Error::BadUri(_)
                | ureq::Error::Tls(_),
            ) => DeliveryOutcome::NotSent("value_http_connect".into()),
            Err(_) => DeliveryOutcome::Unknown,
        }
    })
    .await
    .unwrap_or(DeliveryOutcome::Unknown)
}

#[cfg(test)]
mod tests;
