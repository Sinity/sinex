//! Public owner settings are one immutable startup configuration and one committed policy revision.
use base64::{Engine, engine::general_purpose::URL_SAFE_NO_PAD};
use query_language::time::DaySettingWire;
use sinex_model::Timestamp;
use sinex_protocol::{CoverageStatus, PublicContext, PublicContextRequest, PublicInterruptBudget};
use sinex_store::signals::HostPolicyProjection;

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
) -> Result<String, String> {
    let bytes = serde_json::to_vec(&(
        "sinex.public_context.v1",
        day,
        host,
        config.interrupt_budget_per_hour,
        config.quiet_hours,
    ))
    .map_err(|error| error.to_string())?;
    Ok(blake3::hash(&bytes).to_hex().to_string())
}

impl Provider {
    pub(crate) fn new(
        day: DaySettingWire,
        host: String,
        config: Option<&crate::signals::Config>,
    ) -> Result<Self, String> {
        let settings_identity = config
            .map(|config| settings_identity(&day, &host, config))
            .transpose()?;
        Ok(Self {
            day,
            host,
            settings_identity,
        })
    }
    pub(crate) fn grant_offline_emergency(
        &self,
        context: &mut PublicContext,
        producer: &str,
        write: Option<&crate::ingress::authority::WriteGrant>,
        invoked_at: Timestamp,
    ) -> Result<(), String> {
        let PublicContext::Available {
            settings_identity,
            offline_emergency,
            ..
        } = context
        else {
            return Ok(());
        };
        *offline_emergency = None;
        let Some(write) = write else {
            return Ok(());
        };
        let Some(seconds) = write.emergency_lease_seconds else {
            return Ok(());
        };
        if seconds == 0
            || seconds > 3600
            || !write.forward_signals
            || write.family != sinex_protocol::signals::EMERGENCY_PRODUCT
            || write.emergency_sinex_blocked_host.as_deref() != Some(self.host.as_str())
            || write.host.as_deref() != Some(self.host.as_str())
        {
            return Err("offline emergency deployment capability is invalid".into());
        }
        let expires = invoked_at
            .unix_ns()
            .checked_add(i128::from(seconds) * 1_000_000_000)
            .ok_or("offline emergency lease exceeds timestamp range")?;
        *offline_emergency = Some(sinex_protocol::OfflineEmergency {
            producer: producer.into(),
            host: self.host.clone(),
            settings_identity: settings_identity.clone(),
            rule: sinex_protocol::signals::emergency_rule(),
            route: sinex_protocol::OfflineEmergencyRoute::Desktop,
            issued_at: invoked_at,
            expires_at: Timestamp::from_unix_ns(expires).map_err(|error| error.to_string())?,
        });
        Ok(())
    }
    pub(crate) async fn sign_snapshot(
        &self,
        conn: &mut sqlx::PgConnection,
        producer: &str,
        context: &mut PublicContext,
    ) -> Result<(), String> {
        if let PublicContext::Available { context_token, .. } = context {
            context_token.clear();
        } else {
            return Ok(());
        }
        let key = sinex_store::owner_protocol::key(conn)
            .await
            .map_err(|error| error.to_string())?;
        let token = sign(&key, producer, &self.host, context)?;
        if let PublicContext::Available { context_token, .. } = context {
            *context_token = token;
        }
        Ok(())
    }
    pub(crate) async fn snapshot(
        &self,
        conn: &mut sqlx::PgConnection,
        producer: &str,
        request: &PublicContextRequest,
        at: Timestamp,
    ) -> Result<PublicContext, String> {
        let projection = sinex_store::signals::public_host_policy(
            conn,
            &self.host,
            self.settings_identity.is_some(),
            self.settings_identity.as_deref().unwrap_or(""),
            at,
        )
        .await
        .map_err(|error| error.to_string())?;
        let mut result = match projection {
            HostPolicyProjection::Available {
                checkpoint_revision,
                settings_identity,
                quiet_hours,
                interrupt_budget,
                observed_at,
            } => PublicContext::Available {
                context_token: String::new(),
                settings_identity,
                owner_revision: checkpoint_revision,
                day: self.day.clone(),
                quiet_hours,
                interrupt_budget: PublicInterruptBudget {
                    per_hour: interrupt_budget.per_hour,
                    used: interrupt_budget.used,
                    remaining: interrupt_budget.remaining,
                    window_ms: interrupt_budget.window_ms,
                },
                observed_at,
                offline_emergency: None,
                coverage: if request.coverage.is_some() {
                    CoverageStatus::Incomplete {
                        reason: "coverage_not_established".into(),
                    }
                } else {
                    CoverageStatus::NotRequested
                },
            },
            HostPolicyProjection::Pending { reason } => PublicContext::Pending { reason },
            HostPolicyProjection::Unavailable { reason } => PublicContext::Unavailable { reason },
        };
        if matches!(result, PublicContext::Available { .. }) {
            let key = sinex_store::owner_protocol::key(conn)
                .await
                .map_err(|error| error.to_string())?;
            let token = sign(&key, producer, &self.host, &result)?;
            if let PublicContext::Available { context_token, .. } = &mut result {
                *context_token = token;
            }
        }
        Ok(result)
    }
}

#[derive(serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct Claims {
    producer: String,
    host: String,
    context: PublicContext,
}
fn signing_key(key: &[u8; 32]) -> [u8; 32] {
    blake3::derive_key("sinex.owner_protocol.public_context.v1", key)
}
fn sign(
    key: &[u8; 32],
    producer: &str,
    host: &str,
    context: &PublicContext,
) -> Result<String, String> {
    let bytes = serde_json::to_vec(&Claims {
        producer: producer.into(),
        host: host.into(),
        context: context.clone(),
    })
    .map_err(|error| error.to_string())?;
    let tag = blake3::keyed_hash(&signing_key(key), &bytes);
    Ok(format!(
        "{}.{}",
        URL_SAFE_NO_PAD.encode(bytes),
        URL_SAFE_NO_PAD.encode(tag.as_bytes())
    ))
}
fn open(key: &[u8; 32], producer: &str, host: &str, token: &str) -> Result<PublicContext, String> {
    let invalid = || "invalid owner context token".to_string();
    let (body, tag) = token.split_once('.').ok_or_else(invalid)?;
    let bytes = URL_SAFE_NO_PAD.decode(body).map_err(|_| invalid())?;
    let tag: [u8; 32] = URL_SAFE_NO_PAD
        .decode(tag)
        .map_err(|_| invalid())?
        .try_into()
        .map_err(|_| invalid())?;
    if blake3::Hash::from(tag) != blake3::keyed_hash(&signing_key(key), &bytes) {
        return Err(invalid());
    }
    let mut claims: Claims = serde_json::from_slice(&bytes).map_err(|_| invalid())?;
    if claims.producer != producer || claims.host != host {
        return Err(invalid());
    }
    let PublicContext::Available { context_token, .. } = &mut claims.context else {
        return Err(invalid());
    };
    if !context_token.is_empty() {
        return Err(invalid());
    }
    *context_token = token.into();
    Ok(claims.context)
}
pub(crate) async fn verify_token(
    conn: &mut sqlx::PgConnection,
    producer: &str,
    host: &str,
    token: &str,
) -> Result<PublicContext, String> {
    let key = sinex_store::owner_protocol::key(conn)
        .await
        .map_err(|error| error.to_string())?;
    open(&key, producer, host, token)
}

#[cfg(test)]
mod tests;
