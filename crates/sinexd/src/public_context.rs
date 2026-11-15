//! Public host settings: one immutable startup configuration and one committed policy revision.
use crate::Result;
use query_language::time::DaySettingWire;
use sinex_model::Timestamp;
use sinex_protocol::{PublicContext, PublicInterruptBudget};
use sinex_store::signals::HostPolicyView;

#[derive(Clone)]
pub(crate) struct Provider {
    pub(crate) day: DaySettingWire,
    pub(crate) host: String,
    pub(crate) settings_identity: Option<String>,
}

/// Only public policy fields enter this identity. Private outlets and intervals are excluded.
pub(crate) fn settings_identity(
    day: &DaySettingWire,
    host: &str,
    config: &crate::signals::Config,
) -> Result<String> {
    let bytes = serde_json::to_vec(&(
        "sinex.public_context.v1",
        day,
        host,
        config.interrupt_budget_per_hour,
        config.quiet_hours,
    ))?;
    Ok(blake3::hash(&bytes).to_hex().to_string())
}

impl Provider {
    pub(crate) fn new(
        day: DaySettingWire,
        host: String,
        config: Option<&crate::signals::Config>,
    ) -> Result<Self> {
        let settings_identity = config
            .map(|config| settings_identity(&day, &host, config))
            .transpose()?;
        Ok(Self {
            day,
            host,
            settings_identity,
        })
    }
    pub(crate) async fn snapshot(
        &self,
        conn: &mut sqlx::PgConnection,
        at: Timestamp,
    ) -> Result<PublicContext> {
        let policy = sinex_store::signals::public_host_policy(
            conn,
            &self.host,
            self.settings_identity.is_some(),
            self.settings_identity.as_deref().unwrap_or(""),
            at,
        )
        .await?;
        Ok(match policy {
            HostPolicyView::Available {
                checkpoint_revision,
                settings_identity,
                quiet_hours,
                interrupt_budget,
                observed_at,
            } => PublicContext::Available {
                settings_identity,
                settings_revision: checkpoint_revision,
                day: self.day.clone(),
                quiet_hours,
                interrupt_budget: PublicInterruptBudget {
                    per_hour: interrupt_budget.per_hour,
                    used: interrupt_budget.used,
                    remaining: interrupt_budget.remaining,
                    window_ms: interrupt_budget.window_ms,
                },
                observed_at,
            },
            HostPolicyView::Pending { reason } => PublicContext::Pending { reason },
            HostPolicyView::Unavailable { reason } => PublicContext::Unavailable { reason },
        })
    }
}

#[cfg(test)]
mod tests;
