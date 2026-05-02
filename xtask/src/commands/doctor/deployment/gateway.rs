use super::super::{DEPLOYMENT_READY_TIMEOUT, workspace_tls_dir};
use super::{DeploymentReadinessItem, env_truthy, path_from_env_or_default};
use color_eyre::eyre::{Result, WrapErr};
use serde_json::Value as JsonValue;
use sinex_primitives::{DeploymentReadinessDescriptor, rpc::system::SystemHealthResponse};
use std::path::PathBuf;

#[derive(Debug)]
pub(in crate::commands::doctor) struct GatewayProbeClient {
    client: reqwest::Client,
    client_identity_path: Option<(PathBuf, PathBuf)>,
}

pub(in crate::commands::doctor) fn normalize_gateway_base_url(url: &str) -> String {
    let trimmed = url.trim_end_matches('/');
    trimmed.strip_suffix("/rpc").unwrap_or(trimmed).to_string()
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(in crate::commands::doctor) struct GatewayProbeTlsPaths {
    pub(in crate::commands::doctor) trust_anchor: Option<PathBuf>,
    pub(in crate::commands::doctor) client_cert: Option<PathBuf>,
    pub(in crate::commands::doctor) client_key: Option<PathBuf>,
}

pub(in crate::commands::doctor) fn resolve_gateway_probe_tls_paths(
    descriptor: Option<&DeploymentReadinessDescriptor>,
) -> GatewayProbeTlsPaths {
    let default_tls_dir = workspace_tls_dir();
    GatewayProbeTlsPaths {
        trust_anchor: descriptor
            .and_then(|value| value.secrets.gateway_tls_trust_anchor_file.clone())
            .or_else(|| {
                path_from_env_or_default("SINEX_RPC_CA_CERT", default_tls_dir.join("ca.pem"))
            }),
        client_cert: path_from_env_or_default(
            "SINEX_RPC_CLIENT_CERT",
            default_tls_dir.join("client.pem"),
        ),
        client_key: path_from_env_or_default(
            "SINEX_RPC_CLIENT_KEY",
            default_tls_dir.join("client-key.pem"),
        ),
    }
}

fn descriptor_gateway_base_url(descriptor: Option<&DeploymentReadinessDescriptor>) -> Option<&str> {
    descriptor.and_then(|value| value.gateway.base_url.as_deref())
}

pub(in crate::commands::doctor) async fn build_gateway_probe_client(
    base_url: &str,
    descriptor: Option<&DeploymentReadinessDescriptor>,
) -> Result<GatewayProbeClient> {
    let mut builder = reqwest::Client::builder()
        .timeout(DEPLOYMENT_READY_TIMEOUT)
        .use_rustls_tls();
    let requires_tls = base_url.starts_with("https://");
    let tls_paths = resolve_gateway_probe_tls_paths(descriptor);

    if requires_tls && let Some(ca_path) = tls_paths.trust_anchor.as_ref() {
        let pem = tokio::fs::read(ca_path).await.wrap_err_with(|| {
            format!(
                "failed to read RPC CA certificate from {}",
                ca_path.display()
            )
        })?;
        let cert = reqwest::Certificate::from_pem(&pem).wrap_err_with(|| {
            format!(
                "failed to parse RPC CA certificate at {}",
                ca_path.display()
            )
        })?;
        builder = builder.add_root_certificate(cert);
    }

    let client_identity_path = match (tls_paths.client_cert, tls_paths.client_key) {
        (Some(cert_path), Some(key_path)) => {
            let mut pem = tokio::fs::read(&cert_path).await.wrap_err_with(|| {
                format!(
                    "failed to read RPC client certificate from {}",
                    cert_path.display()
                )
            })?;
            pem.extend_from_slice(&tokio::fs::read(&key_path).await.wrap_err_with(|| {
                format!("failed to read RPC client key from {}", key_path.display())
            })?);
            let identity = reqwest::Identity::from_pem(&pem).wrap_err_with(|| {
                format!(
                    "failed to parse client identity from {} and {}",
                    cert_path.display(),
                    key_path.display()
                )
            })?;
            builder = builder.identity(identity);
            Some((cert_path, key_path))
        }
        (Some(_), None) | (None, Some(_)) => {
            color_eyre::eyre::bail!(
                "SINEX_RPC_CLIENT_CERT and SINEX_RPC_CLIENT_KEY must both be set for gateway mTLS probing"
            );
        }
        (None, None) => None,
    };

    let client = builder
        .build()
        .wrap_err("failed to construct HTTP client for gateway readiness")?;
    Ok(GatewayProbeClient {
        client,
        client_identity_path,
    })
}

/// Check 9: gateway readiness endpoint responds and reports serving=true.
pub(crate) async fn check_gateway_ready(
    gateway_url: Option<&str>,
    descriptor: Option<&DeploymentReadinessDescriptor>,
) -> DeploymentReadinessItem {
    if descriptor.is_some_and(|value| !value.expectations.gateway_ready) {
        return DeploymentReadinessItem::skip(
            "gateway-ready",
            "Gateway runtime is not expected in the deployment descriptor",
        );
    }

    let base_url = normalize_gateway_base_url(
        descriptor_gateway_base_url(descriptor)
            .or(gateway_url)
            .unwrap_or("https://127.0.0.1:9999"),
    );
    let ready_url = format!("{base_url}/ready");

    let mtls_expected = descriptor.is_some_and(|value| value.gateway.require_client_tls)
        || env_truthy("SINEX_GATEWAY_REQUIRE_CLIENT_TLS")
        || std::env::var("SINEX_GATEWAY_TLS_CLIENT_CA").is_ok();
    let probe_client = match build_gateway_probe_client(&base_url, descriptor).await {
        Ok(client) => client,
        Err(error) => {
            return DeploymentReadinessItem::fail("gateway-ready", error.to_string());
        }
    };

    let response = match probe_client.client.get(&ready_url).send().await {
        Ok(response) => response,
        Err(error) => {
            return DeploymentReadinessItem::fail(
                "gateway-ready",
                if mtls_expected && probe_client.client_identity_path.is_none() {
                    format!(
                        "Cannot reach {ready_url}: {error}; gateway mTLS appears enabled, but no RPC client identity was available from SINEX_RPC_CLIENT_CERT/SINEX_RPC_CLIENT_KEY or .sinex/tls/client.pem + client-key.pem"
                    )
                } else {
                    format!("Cannot reach {ready_url}: {error}")
                },
            );
        }
    };

    let status = response.status();
    let body_text = match response.text().await {
        Ok(body) => body,
        Err(error) => {
            return DeploymentReadinessItem::fail(
                "gateway-ready",
                format!("Failed to read readiness body from {ready_url}: {error}"),
            );
        }
    };

    interpret_gateway_ready_response(&ready_url, status, &body_text)
}
pub(in crate::commands::doctor) fn interpret_gateway_ready_response(
    ready_url: &str,
    status: reqwest::StatusCode,
    body_text: &str,
) -> DeploymentReadinessItem {
    let trimmed = body_text.trim();
    if trimmed.is_empty() {
        return DeploymentReadinessItem::fail(
            "gateway-ready",
            format!("{ready_url} returned HTTP {status} with an empty body"),
        );
    }

    let body: JsonValue = match serde_json::from_str(trimmed) {
        Ok(body) => body,
        Err(error) => {
            return DeploymentReadinessItem::fail(
                "gateway-ready",
                format!(
                    "{ready_url} returned HTTP {status} with a non-JSON body: {error}; body={}",
                    summarize_gateway_probe_body(trimmed)
                ),
            );
        }
    };

    let response: SystemHealthResponse = match serde_json::from_value(body) {
        Ok(response) => response,
        Err(error) => {
            return DeploymentReadinessItem::fail(
                "gateway-ready",
                format!(
                    "{ready_url} returned HTTP {status} with a non-conforming health body: {error}; body={}",
                    summarize_gateway_probe_body(trimmed)
                ),
            );
        }
    };

    if status.is_success() && response.serving {
        DeploymentReadinessItem::pass(
            "gateway-ready",
            format!(
                "{ready_url} returned HTTP {status} (status={}, healthy={}, reasons={})",
                response.status,
                response.healthy,
                summarize_gateway_degradation_reasons(&response)
            ),
        )
    } else {
        DeploymentReadinessItem::fail(
            "gateway-ready",
            format!(
                "{ready_url} returned HTTP {status} (status={}, serving={}, healthy={}, reasons={}, components={})",
                response.status,
                response.serving,
                response.healthy,
                summarize_gateway_degradation_reasons(&response),
                summarize_gateway_components(&response)
            ),
        )
    }
}

fn summarize_gateway_degradation_reasons(response: &SystemHealthResponse) -> String {
    if response.degradation_reasons.is_empty() {
        "none".to_string()
    } else {
        response.degradation_reasons.join("; ")
    }
}

fn summarize_gateway_components(response: &SystemHealthResponse) -> String {
    let replay = &response.components.replay_control;
    format!(
        "database={} connected={}, nats={} connected={} latency_ms={:?}, replay_control={} enabled={} connected={} last_error={}",
        response.components.database.status,
        response.components.database.connected,
        response.components.nats.status,
        response.components.nats.connected,
        response.components.nats.latency_ms,
        replay.status,
        replay.enabled,
        replay.connected,
        replay.last_error.as_deref().unwrap_or("none"),
    )
}

fn summarize_gateway_probe_body(body_text: &str) -> String {
    const MAX_CHARS: usize = 200;

    let compact = body_text.split_whitespace().collect::<Vec<_>>().join(" ");
    if compact.chars().count() <= MAX_CHARS {
        compact
    } else {
        let summary = compact.chars().take(MAX_CHARS).collect::<String>();
        format!("{summary}...")
    }
}
