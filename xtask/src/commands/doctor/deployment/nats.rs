use super::DeploymentReadinessItem;
use color_eyre::eyre::{Result, WrapErr};
use sinex_primitives::{
    DeploymentReadinessDescriptor, environment::SinexEnvironment, nats::NatsConnectionConfig,
};
use std::collections::BTreeSet;

pub(in crate::commands::doctor) fn apply_descriptor_nats_overrides(
    mut config: NatsConnectionConfig,
    descriptor: Option<&DeploymentReadinessDescriptor>,
) -> NatsConnectionConfig {
    let Some(descriptor) = descriptor else {
        return config;
    };

    if let Some(url) = descriptor.nats.servers.first() {
        config.url.clone_from(url);
    }

    if config.ca_cert.is_none() {
        config
            .ca_cert
            .clone_from(&descriptor.secrets.nats_ca_cert_file);
    }
    if config.client_cert.is_none() {
        config
            .client_cert
            .clone_from(&descriptor.secrets.nats_client_cert_file);
    }
    if config.client_key.is_none() {
        config
            .client_key
            .clone_from(&descriptor.secrets.nats_client_key_file);
    }
    if config.token_file.is_none() {
        config
            .token_file
            .clone_from(&descriptor.secrets.nats_token_file);
    }
    if config.creds_file.is_none() {
        config
            .creds_file
            .clone_from(&descriptor.secrets.nats_creds_file);
    }
    if config.nkey_seed_file.is_none() {
        config
            .nkey_seed_file
            .clone_from(&descriptor.secrets.nats_nkey_seed_file);
    }

    config
}

pub(in crate::commands::doctor) fn resolve_deployment_nats_config(
    base_config: NatsConnectionConfig,
    nats_url: Option<&str>,
    descriptor: Option<&DeploymentReadinessDescriptor>,
) -> NatsConnectionConfig {
    let descriptor_declares_server = descriptor.is_some_and(|value| !value.nats.servers.is_empty());

    let mut config = apply_descriptor_nats_overrides(base_config, descriptor);
    if !descriptor_declares_server
        && config.url == NatsConnectionConfig::default().url
        && let Some(url) = nats_url
    {
        config.url = url.to_string();
    }

    config
}

pub(in crate::commands::doctor) fn required_nats_stream_names() -> Result<Vec<String>> {
    let env = SinexEnvironment::current()
        .wrap_err("failed to resolve SINEX_ENVIRONMENT for NATS readiness")?;
    Ok(vec![
        env.nats_stream_name("SINEX_RAW_EVENTS"),
        env.nats_stream_name("SINEX_RAW_EVENTS_CONFIRMATIONS"),
        env.nats_stream_name("SOURCE_MATERIAL"),
    ])
}

/// Check 8: NATS streams exist — connect and list streams.
pub(super) async fn check_nats_streams(
    nats_url: Option<&str>,
    descriptor: Option<&DeploymentReadinessDescriptor>,
) -> DeploymentReadinessItem {
    if descriptor.is_some_and(|value| !value.expectations.nats_streams) {
        return DeploymentReadinessItem::skip(
            "nats-streams",
            "JetStream runtime is not expected in the deployment descriptor",
        );
    }

    use futures::StreamExt;

    let nats_config =
        resolve_deployment_nats_config(NatsConnectionConfig::from_env(), nats_url, descriptor);

    let client = match nats_config.connect().await {
        Ok(c) => c,
        Err(e) => {
            return DeploymentReadinessItem::fail(
                "nats-streams",
                format!("Cannot connect to NATS at {}: {e}", nats_config.url),
            );
        }
    };

    let jetstream = async_nats::jetstream::new(client);
    let mut streams = jetstream.streams();
    let mut names: Vec<String> = Vec::new();
    let mut list_error: Option<String> = None;
    while let Some(result) = streams.next().await {
        match result {
            Ok(stream) => names.push(stream.config.name.clone()),
            Err(e) => {
                list_error = Some(format!("Error listing NATS streams: {e}"));
                break;
            }
        }
    }

    if let Some(err) = list_error {
        return DeploymentReadinessItem::fail("nats-streams", err);
    }

    let required_streams = match required_nats_stream_names() {
        Ok(streams) => streams,
        Err(error) => {
            return DeploymentReadinessItem::fail("nats-streams", error.to_string());
        }
    };
    let available: BTreeSet<String> = names.iter().cloned().collect();
    let missing: Vec<String> = required_streams
        .iter()
        .filter(|name| !available.contains(name.as_str()))
        .cloned()
        .collect();

    if missing.is_empty() {
        DeploymentReadinessItem::pass(
            "nats-streams",
            format!(
                "Connected to NATS at {}; required streams present: {}",
                nats_config.url,
                names.join(", ")
            ),
        )
    } else {
        DeploymentReadinessItem::fail(
            "nats-streams",
            format!(
                "Connected to NATS at {}; missing required JetStream streams: {}; present: {}",
                nats_config.url,
                missing.join(", "),
                if names.is_empty() {
                    "<none>".to_string()
                } else {
                    names.join(", ")
                }
            ),
        )
    }
}
