//! Deployment-owned outlets and host attention settings.
use serde::Deserialize;
use std::collections::BTreeMap;
use std::path::PathBuf;

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Config {
    pub interrupt_budget_per_hour: usize,
    pub quiet_hours: Option<::signals::QuietHours>,
    #[serde(default)]
    pub private: Vec<::signals::Interval>,
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

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Notifications {
    pub route: ::signals::notifications::Route,
    pub command: Option<Command>,
    /// A supplied bus address is used by isolated fixtures and host deployment;
    /// an absent address uses the host session bus only for an enabled route.
    pub bus_address: Option<String>,
}

impl Config {
    pub(crate) fn configuration(&self) -> ::signals::Configuration {
        ::signals::Configuration {
            value_targets: self.values.keys().cloned().collect(),
            action_commands: self.actions.keys().cloned().collect(),
        }
    }
    pub(crate) fn validate(&self) -> Result<(), String> {
        if self.outlet_timeout_ms == 0 {
            return Err("signals.outlet_timeout_ms must be positive".into());
        }
        if self
            .quiet_hours
            .is_some_and(|hours| hours.from >= 1440 || hours.to >= 1440)
        {
            return Err("signal quiet hours must be civil minutes".into());
        }
        if self
            .private
            .iter()
            .any(|interval| interval.end <= interval.start)
        {
            return Err("signal private intervals must have positive length".into());
        }
        for command in self.actions.values().chain(
            self.notifications
                .iter()
                .flat_map(|notifications| notifications.command.iter()),
        ) {
            if !command.program.is_absolute() {
                return Err("signal command programs must be absolute paths".into());
            }
        }
        for target in self.values.values() {
            match target {
                ValueTarget::File { path } if !path.is_absolute() => {
                    return Err("signal value files must be absolute paths".into());
                }
                ValueTarget::Loopback { url } => {
                    let uri: axum::http::Uri = url
                        .parse()
                        .map_err(|error| format!("signal value URL: {error}"))?;
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
                        return Err(
                            "signal value URLs must use an explicit loopback HTTP address".into(),
                        );
                    }
                }
                _ => {}
            }
        }
        if let Some(notifications) = &self.notifications
            && matches!(
                notifications.route,
                ::signals::notifications::Route::Command | ::signals::notifications::Route::Both
            )
            && notifications.command.is_none()
        {
            return Err("signal notification command route requires a configured command".into());
        }
        Ok(())
    }
}
