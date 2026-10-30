//! Effects use deployment-owned destinations after the unknown attempt commits.
use super::*;
use ::signals::notifications::{Notice, NotificationConfig, RouteCommand};
use config::{Command, ValueTarget};
use signal_delivery::{
    AttemptContext, CancellationToken, Deliverer, NotificationRequest, NotificationUpdate,
};
use std::time::Duration;

pub(super) async fn notifications(
    api: &Api,
    config: &Config,
    condition: signal_delivery::PolicyCheck,
    cancellation: CancellationToken,
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
    let connection = match notifications {
        Some(notification) if notification.route != ::signals::notifications::Route::Command => {
            match &notification.bus_address {
                Some(address) => zbus::connection::Builder::address(address.as_str())
                    .map_err(error)?
                    .build()
                    .await
                    .ok(),
                None => zbus::Connection::session().await.ok(),
            }
        }
        _ => None,
    };
    let configuration = NotificationConfig {
        route: notifications.map_or(::signals::notifications::Route::Command, |notification| {
            notification.route
        }),
        command: notifications
            .and_then(|notification| notification.command.as_ref())
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
        signal_delivery::run(
            deliverer,
            receive,
            updates,
            condition,
            Arc::new(|| millis(Timestamp::now()).expect("host delivery clock fits milliseconds")),
            cancellation,
            None,
        )
        .await
        .map_err(error)
    });
    Ok((requests, changed, worker))
}

pub(super) fn notice(pending: &PendingDelivery, passive_count: u64) -> Notice {
    let delivery = &pending.delivery;
    let reference = delivery
        .basis
        .first()
        .map(|row| row.reference.clone())
        .unwrap_or_else(|| {
            format!(
                "sinex:key/sinex.signals/signal.rule/{}",
                delivery.trigger.rule.id
            )
        });
    let mut notice = pending.notice.clone().unwrap_or_else(|| Notice {
        delivery_id: delivery.id.clone(),
        product: "sinex".into(),
        icon: "dialog-information".into(),
        rule: delivery.trigger.rule.id.clone(),
        occurrence: delivery.trigger.occurrence.id.clone(),
        summary: pending
            .rule
            .as_ref()
            .map_or_else(|| "Sinex signal".into(), |rule| rule.rule.name.clone()),
        body: serde_json::to_string(&delivery.basis).unwrap_or_default(),
        reference: reference.clone(),
        command_hint: format!("sinexctl show {reference}"),
        interrupt: false,
        current: pending
            .rule
            .as_ref()
            .is_some_and(::signals::ActiveRule::current),
        data_loss: false,
        budget_passive_count: None,
    });
    notice.interrupt = delivery.channel == ::signals::Channel::Interrupt;
    notice.data_loss = false;
    notice.budget_passive_count = (delivery.downgrade
        == Some(::signals::Downgrade::InterruptBudgetSpent))
    .then_some(passive_count);
    notice
}

pub(super) fn freeze_notice(pending: &mut PendingDelivery) {
    if pending.notice.is_none() {
        pending.notice = Some(notice(pending, 0));
    }
}

pub(super) async fn effect(
    config: &Config,
    pending: &PendingDelivery,
    attempt: &AttemptContext,
) -> ::signals::DeliveryOutcome {
    use ::signals::DeliveryOutcome;
    if let Err(interrupted) = attempt.check() {
        return DeliveryOutcome::NotSent(interrupted.reason().into());
    }
    let delivery = &pending.delivery;
    let Some(rule) = &pending.rule else {
        return DeliveryOutcome::NotSent("no_local_rule_outlet".into());
    };
    let reference = delivery
        .basis
        .first()
        .map(|row| row.reference.clone())
        .unwrap_or_else(|| notice(pending, 0).reference);
    match delivery.channel {
        ::signals::Channel::Action => match rule
            .rule
            .command
            .as_ref()
            .and_then(|name| config.actions.get(name))
        {
            Some(command) => command_effect(command, &reference, attempt).await,
            None => DeliveryOutcome::NotSent("action_command_not_configured".into()),
        },
        ::signals::Channel::Value => match rule
            .rule
            .target
            .as_ref()
            .and_then(|name| config.values.get(name))
        {
            Some(target) => value_effect(target, delivery, attempt).await,
            None => DeliveryOutcome::NotSent("value_target_not_configured".into()),
        },
        _ => DeliveryOutcome::NotSent("not_an_effect_channel".into()),
    }
}

async fn command_effect(
    command: &Command,
    reference: &str,
    attempt: &AttemptContext,
) -> ::signals::DeliveryOutcome {
    use ::signals::DeliveryOutcome;
    if let Err(interrupted) = attempt.check() {
        return DeliveryOutcome::NotSent(interrupted.reason().into());
    }
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
    match attempt.run(child.wait()).await {
        Ok(Ok(status)) if status.success() => DeliveryOutcome::Delivered,
        Ok(Ok(_)) => DeliveryOutcome::NotSent("action_exit".into()),
        // A spawned process may already have acted. Cancellation never converts
        // that phase into a claim that no physical effect was possible.
        Ok(Err(_)) | Err(_) => {
            let _ = child.start_kill();
            let _ = child.wait().await;
            DeliveryOutcome::Unknown
        }
    }
}

// Observe submitted private staging work before dropping its cleanup owner.
// After interruption, this acknowledgement grants no right to publish it.
async fn stage<F: std::future::Future>(
    attempt: &AttemptContext,
    work: F,
) -> Result<F::Output, (signal_delivery::Interrupted, bool)> {
    let mut work = std::pin::pin!(work);
    let mut submitted = false;
    let result = attempt
        .run(std::future::poll_fn(|context| {
            submitted = true;
            std::future::Future::poll(work.as_mut(), context)
        }))
        .await;
    match result {
        Ok(result) => Ok(result),
        Err(interrupted) => {
            if submitted {
                let _ = work.await;
            }
            Err((interrupted, submitted))
        }
    }
}

async fn file_value(
    path: &std::path::Path,
    bytes: &[u8],
    attempt: &AttemptContext,
) -> ::signals::DeliveryOutcome {
    use ::signals::DeliveryOutcome;
    use tokio::io::AsyncWriteExt;
    if let Err(interrupted) = attempt.check() {
        return DeliveryOutcome::NotSent(interrupted.reason().into());
    }
    let Some(directory) = path.parent() else {
        return DeliveryOutcome::NotSent("value_file_parent".into());
    };
    let Ok(staged) = tempfile::TempPath::try_from_path(
        directory.join(format!(".sinex-signal-{}", uuid::Uuid::now_v7())),
    ) else {
        return DeliveryOutcome::NotSent("value_stage_path".into());
    };
    let mut file = match stage(
        attempt,
        tokio::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .mode(0o600)
            .open(&staged),
    )
    .await
    {
        Ok(Ok(file)) => file,
        Ok(Err(_)) => return DeliveryOutcome::NotSent("value_stage_create".into()),
        Err((interrupted, _)) => return DeliveryOutcome::NotSent(interrupted.reason().into()),
    };
    let mut remaining = bytes;
    while !remaining.is_empty() {
        match stage(attempt, file.write(remaining)).await {
            Ok(Ok(0)) | Ok(Err(_)) => return DeliveryOutcome::NotSent("value_stage_write".into()),
            Ok(Ok(written)) => remaining = &remaining[written..],
            Err((interrupted, _)) => return DeliveryOutcome::NotSent(interrupted.reason().into()),
        }
    }
    match stage(attempt, file.sync_all()).await {
        Ok(Ok(())) => {}
        Ok(Err(_)) => return DeliveryOutcome::NotSent("value_stage_sync".into()),
        Err((interrupted, _)) => return DeliveryOutcome::NotSent(interrupted.reason().into()),
    }
    drop(file);
    if let Err(interrupted) = attempt.check() {
        return DeliveryOutcome::NotSent(interrupted.reason().into());
    }
    // The rename can publish after it has been submitted to the filesystem.
    // Always observe its result, retaining Unknown if the attempt expired.
    match stage(attempt, tokio::fs::rename(&staged, path)).await {
        Ok(Ok(())) => {}
        Err((interrupted, false)) => return DeliveryOutcome::NotSent(interrupted.reason().into()),
        Ok(Err(_)) | Err((_, true)) => return DeliveryOutcome::Unknown,
    }
    match stage(attempt, async {
        let directory = tokio::fs::File::open(directory).await?;
        directory.sync_all().await
    })
    .await
    {
        Ok(Ok(())) => DeliveryOutcome::Delivered,
        Ok(Err(_)) | Err(_) => DeliveryOutcome::Unknown,
    }
}

async fn http_value(
    url: &str,
    bytes: Vec<u8>,
    attempt: &AttemptContext,
) -> ::signals::DeliveryOutcome {
    use ::signals::DeliveryOutcome;
    if let Err(interrupted) = attempt.check() {
        return DeliveryOutcome::NotSent(interrupted.reason().into());
    }
    let client = match reqwest::Client::builder()
        .no_proxy()
        .redirect(reqwest::redirect::Policy::none())
        .retry(reqwest::retry::never())
        .build()
    {
        Ok(client) => client,
        Err(_) => return DeliveryOutcome::NotSent("value_http_client".into()),
    };
    let mut submitted = false;
    let response = attempt
        .run(async {
            submitted = true;
            client
                .post(url)
                .header("content-type", "application/json")
                .body(bytes)
                .send()
                .await
        })
        .await;
    let response = match response {
        Ok(Ok(response)) => response,
        // Connection/TLS failures precede an HTTP request. Error text and URLs
        // remain private, including configured query strings.
        Ok(Err(error)) if error.is_connect() || error.is_builder() => {
            return DeliveryOutcome::NotSent("value_http_connect".into());
        }
        Err(interrupted) if !submitted => {
            return DeliveryOutcome::NotSent(interrupted.reason().into());
        }
        Ok(Err(_)) | Err(_) => return DeliveryOutcome::Unknown,
    };
    // A value target acknowledges the POST through its status, not a response
    // body. An unrelated streaming body cannot revoke that definitive result.
    if response.status().is_success() {
        DeliveryOutcome::Delivered
    } else {
        DeliveryOutcome::NotSent("value_http_status".into())
    }
}

async fn value_effect(
    target: &ValueTarget,
    delivery: &::signals::Delivery,
    attempt: &AttemptContext,
) -> ::signals::DeliveryOutcome {
    use ::signals::DeliveryOutcome;
    if let Err(interrupted) = attempt.check() {
        return DeliveryOutcome::NotSent(interrupted.reason().into());
    }
    let Ok(bytes) = serde_json::to_vec(delivery) else {
        return DeliveryOutcome::NotSent("value_encode".into());
    };
    match target {
        ValueTarget::File { path } => file_value(path, &bytes, attempt).await,
        ValueTarget::Loopback { url } => http_value(url, bytes, attempt).await,
    }
}

#[cfg(test)]
mod tests;
