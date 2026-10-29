//! Effects use deployment-owned destinations after the unknown attempt commits.
use super::*;
use ::signals::notifications::{Notice, NotificationConfig, RouteCommand};
use config::{Command, ValueTarget};
use signal_delivery::{Deliverer, NotificationRequest, NotificationUpdate};
use std::time::Duration;

pub(super) async fn notifications(
    api: &Api,
    config: &Config,
    condition: signal_delivery::PolicyCheck,
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
        signal_delivery::run(deliverer, receive, updates, condition, Arc::new(||millis(Timestamp::now()).expect("host delivery clock fits milliseconds")))
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

pub(super) async fn effect(
    config: &Config,
    pending: &PendingDelivery,
) -> ::signals::DeliveryOutcome {
    let delivery = &pending.delivery;
    let Some(rule) = &pending.rule else {
        return ::signals::DeliveryOutcome::NotSent("no_local_rule_outlet".into());
    };
    let reference = delivery
        .basis
        .first()
        .map(|row| row.reference.clone())
        .unwrap_or_else(|| notice(pending, 0).reference);
    let result = match delivery.channel {
        ::signals::Channel::Action => match rule
            .rule
            .command
            .as_ref()
            .and_then(|name| config.actions.get(name))
        {
            Some(command) => command_effect(command, &reference, config.outlet_timeout_ms).await,
            None => Err("action_command_not_configured".into()),
        },
        ::signals::Channel::Value => match rule
            .rule
            .target
            .as_ref()
            .and_then(|name| config.values.get(name))
        {
            Some(target) => value_effect(target, delivery, config.outlet_timeout_ms).await,
            None => Err("value_target_not_configured".into()),
        },
        _ => Err("not_an_effect_channel".into()),
    };
    match result {
        Ok(()) => ::signals::DeliveryOutcome::Delivered,
        Err(reason) => ::signals::DeliveryOutcome::NotSent(reason),
    }
}

async fn command_effect(command: &Command, reference: &str, timeout: u64) -> Result<(), String> {
    let mut process = tokio::process::Command::new(&command.program);
    process
        .args(&command.arguments)
        .arg(reference)
        .stdin(std::process::Stdio::null())
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .kill_on_drop(true);
    let mut child = process
        .spawn()
        .map_err(|error| format!("action_spawn:{error}"))?;
    let status = tokio::time::timeout(Duration::from_millis(timeout), child.wait())
        .await
        .map_err(|_| "action_deadline".to_string())?
        .map_err(|error| format!("action_wait:{error}"))?;
    if status.success() {
        Ok(())
    } else {
        Err(format!("action_exit:{status}"))
    }
}

async fn value_effect(
    target: &ValueTarget,
    delivery: &::signals::Delivery,
    timeout: u64,
) -> Result<(), String> {
    let bytes = serde_json::to_vec(delivery).map_err(error)?;
    match target {
        ValueTarget::File { path } => {
            let path = path.clone();
            tokio::task::spawn_blocking(move || -> Result<(), String> {
                use std::io::Write;
                let directory = path.parent().ok_or("value file has no parent")?;
                let mut staged = tempfile::NamedTempFile::new_in(directory).map_err(error)?;
                staged.write_all(&bytes).map_err(error)?;
                staged.as_file().sync_all().map_err(error)?;
                staged.persist(&path).map_err(error)?;
                std::fs::File::open(directory)
                    .and_then(|directory| directory.sync_all())
                    .map_err(error)
            })
            .await
            .map_err(error)?
        }
        ValueTarget::Loopback { url } => {
            let url = url.clone();
            tokio::task::spawn_blocking(move || {
                let agent: ureq::Agent = ureq::Agent::config_builder()
                    .timeout_global(Some(Duration::from_millis(timeout)))
                    .max_redirects(0)
                    .build()
                    .into();
                agent
                    .post(url)
                    .header("content-type", "application/json")
                    .send(&bytes)
                    .and_then(|response| {
                        if response.status().is_success() {
                            Ok(response)
                        } else {
                            Err(ureq::Error::StatusCode(response.status().as_u16()))
                        }
                    })
                    .map(|_| ())
                    .map_err(|error| format!("value_http:{error}"))
            })
            .await
            .map_err(error)?
        }
    }
}
