//! Deployment-owned outlets and host attention settings.
use crate::{Error, Result};
use serde::Deserialize;
use std::collections::BTreeMap;
use std::path::PathBuf;

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Config {
    pub interrupt_budget_per_hour: usize,
    pub quiet_hours: Option<::signals::QuietHours>,
    pub notifications: Option<Notifications>,
    #[serde(default)]
    pub values: BTreeMap<String, ValueTarget>,
    #[serde(default)]
    pub actions: BTreeMap<String, Command>,
    pub outlet_timeout_ms: u64,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Command {
    pub program: PathBuf,
    #[serde(default)]
    pub arguments: Vec<String>,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub(crate) enum ValueTarget {
    File { path: PathBuf },
    Loopback { url: String },
}

/// Desktop notifications for interrupting notices, and the command that
/// receives each notice sent away while the operator is away.
#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Notifications {
    pub away_command: Option<Command>,
    /// A supplied bus address is used by isolated fixtures and host deployment;
    /// an absent address uses the host session bus.
    pub bus_address: Option<String>,
}

/// Programs that start or resume an agent session. No delivery may run one,
/// because a run would spend quota with no one acting.
const AGENT_LAUNCHERS: &[&str] = &["claude", "codex"];

impl Config {
    pub(crate) fn configuration(&self) -> ::signals::Configuration {
        ::signals::Configuration {
            value_targets: self.values.keys().cloned().collect(),
            action_commands: self.actions.keys().cloned().collect(),
        }
    }
    pub(crate) fn host_settings(&self) -> ::signals::HostSettings {
        ::signals::HostSettings {
            product: super::PRODUCT.into(),
            quiet_hours: self.quiet_hours,
            away_command: self
                .notifications
                .as_ref()
                .is_some_and(|notifications| notifications.away_command.is_some()),
            deep_work_deferral: ::signals::DeepWorkDeferral::default(),
        }
    }
    pub(crate) fn validate(&self) -> Result<()> {
        if self.outlet_timeout_ms == 0 {
            return Err(Error::bad_request(
                "signals.outlet_timeout_ms must be positive",
            ));
        }
        if self
            .quiet_hours
            .is_some_and(|hours| hours.from >= 1440 || hours.to >= 1440)
        {
            return Err(Error::bad_request(
                "signal quiet hours must be civil minutes",
            ));
        }
        for command in self.actions.values().chain(
            self.notifications
                .iter()
                .flat_map(|notifications| notifications.away_command.iter()),
        ) {
            if !command.program.is_absolute() {
                return Err(Error::bad_request(
                    "signal command programs must be absolute paths",
                ));
            }
        }
        for target in self.values.values() {
            match target {
                ValueTarget::File { path } if !path.is_absolute() => {
                    return Err(Error::bad_request(
                        "signal value files must be absolute paths",
                    ));
                }
                ValueTarget::Loopback { url } => {
                    let uri: axum::http::Uri = url.parse().map_err(|error| {
                        Error::bad_request(format!("signal value URL: {error}"))
                    })?;
                    if !matches!(uri.scheme_str(), Some("http" | "https"))
                        || uri
                            .authority()
                            .is_none_or(|authority| authority.as_str().contains('@'))
                        || uri
                            .host()
                            .and_then(|host| {
                                host.trim_matches(['[', ']'])
                                    .parse::<std::net::IpAddr>()
                                    .ok()
                            })
                            .is_none_or(|address| !address.is_loopback())
                    {
                        return Err(Error::bad_value(
                            "signal value URLs must use an explicit loopback HTTP address",
                        ));
                    }
                }
                _ => {}
            }
        }
        ::signals::refuse_agent_launchers(
            self.actions
                .iter()
                .map(|(name, command)| (name.as_str(), command.program.as_path())),
            AGENT_LAUNCHERS,
        )
        .map_err(Error::bad_value)?;
        Ok(())
    }
}
