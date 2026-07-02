use super::*;
use crate::{
    DeploymentDatabaseRuntime, DeploymentGatewayRuntime, DeploymentNatsRuntime,
    DeploymentReadinessMode, DeploymentSecrets, DeploymentTarget,
};
use std::env;
use xtask::sandbox::sinex_test;

#[sinex_test]
async fn configured_path_treats_empty_override_as_disabled() -> TestResult<()> {
    let previous = env::var_os("SINEX_RUNTIME_TARGET_CONFIG");
    unsafe { env::set_var("SINEX_RUNTIME_TARGET_CONFIG", "") };

    let configured = RuntimeTargetDescriptor::configured_path();

    match previous {
        Some(value) => unsafe { env::set_var("SINEX_RUNTIME_TARGET_CONFIG", value) },
        None => unsafe { env::remove_var("SINEX_RUNTIME_TARGET_CONFIG") },
    }

    assert!(configured.is_none());
    Ok(())
}

#[sinex_test]
async fn load_from_path_sets_source_path() -> TestResult<()> {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("runtime-target.json");
    std::fs::write(
        &path,
        r#"{"version":1,"name":"prod","kind":"deployed_host","gateway":{"base_url":"https://127.0.0.1:9999"}}"#,
    )
    .expect("write descriptor");

    let descriptor = RuntimeTargetDescriptor::load_from_path(&path).expect("descriptor loads");

    assert_eq!(descriptor.name, "prod");
    assert_eq!(descriptor.kind, RuntimeTargetKind::DeployedHost);
    assert_eq!(
        descriptor.gateway.base_url.as_deref(),
        Some("https://127.0.0.1:9999")
    );
    assert_eq!(descriptor.source_path.as_deref(), Some(path.as_path()));
    Ok(())
}

#[sinex_test]
async fn deployment_readiness_maps_to_runtime_target() -> TestResult<()> {
    let readiness = DeploymentReadinessDescriptor {
        version: 1,
        mode: DeploymentReadinessMode::Enabled,
        source: Some("nixos".to_string()),
        managed_units: vec!["sinexd.service".to_string()],
        target: Some(DeploymentTarget {
            user: "sinity".to_string(),
            uid: Some(1000),
            home: Some(PathBuf::from("/home/sinity")),
        }),
        database: DeploymentDatabaseRuntime {
            enabled: true,
            host: Some("127.0.0.1".to_string()),
            port: Some(5432),
            name: Some("sinex_prod".to_string()),
            user: Some("sinex".to_string()),
            local_auth: Some("scram-sha-256".to_string()),
            password_required: true,
        },
        gateway: DeploymentGatewayRuntime {
            base_url: Some("https://127.0.0.1:9999".to_string()),
            require_client_tls: true,
        },
        nats: DeploymentNatsRuntime {
            servers: vec!["tls://127.0.0.1:4222".to_string()],
        },
        secrets: DeploymentSecrets {
            api_admin_token_file: Some(PathBuf::from("/run/agenix/sinex-api-admin-token")),
            gateway_tls_trust_anchor_file: Some(PathBuf::from(
                "/var/lib/sinex/run/gateway-ca.pem",
            )),
            nats_creds_file: Some(PathBuf::from("/run/agenix/sinex-nats-client-creds")),
            ..DeploymentSecrets::default()
        },
        ..DeploymentReadinessDescriptor::default()
    };

    let target = RuntimeTargetDescriptor::from_deployment_readiness(&readiness);

    assert_eq!(target.name, "deployed-host:sinity");
    assert_eq!(target.kind, RuntimeTargetKind::DeployedHost);
    assert_eq!(target.source.as_deref(), Some("nixos"));
    assert_eq!(
        target.database.url.as_deref(),
        Some("postgresql://sinex@127.0.0.1:5432/sinex_prod")
    );
    assert_eq!(target.nats.environment.as_deref(), Some("prod"));
    assert_eq!(
        target.gateway.token_file.as_deref(),
        Some(Path::new("/run/agenix/sinex-api-admin-token"))
    );
    assert_eq!(
        target.gateway.token_role,
        Some(RuntimeTargetGatewayTokenRole::Admin)
    );
    assert_eq!(target.services.managed_units, ["sinexd.service"]);
    Ok(())
}

#[sinex_test]
async fn gateway_token_role_applies_expected_suffix() -> TestResult<()> {
    assert_eq!(
        RuntimeTargetGatewayTokenRole::Admin.apply_to_token("sinex_secret\n"),
        "sinex_secret:admin"
    );
    assert_eq!(
        RuntimeTargetGatewayTokenRole::Admin.apply_to_token("sinex_secret:admin"),
        "sinex_secret:admin"
    );
    assert_eq!(
        RuntimeTargetGatewayTokenRole::Readonly.apply_to_token("sinex_secret:admin"),
        "sinex_secret:admin"
    );
    Ok(())
}
