use std::convert::TryInto;
use std::net::{IpAddr, Ipv4Addr, TcpListener};
use std::path::{Path, PathBuf};

use color_eyre::eyre::{Result, eyre};
use rcgen::{
    BasicConstraints, CertificateParams, ExtendedKeyUsagePurpose, IsCa, KeyPair, KeyUsagePurpose,
    SanType,
};
use reqwest::{Certificate as ReqwestCert, Client};
use serde_json::json;
use sinex_gateway::{ServiceContainer, rpc_server};
use tempfile::TempDir;
use tokio::time::{Duration, Instant, sleep};
use xtask::sandbox::prelude::*;
use xtask::sandbox::timing::Timeouts;

struct CertBundle {
    ca_pem: String,
    server_cert_path: PathBuf,
    server_key_path: PathBuf,
}

fn reserve_port() -> Result<u16> {
    let listener = TcpListener::bind("127.0.0.1:0")?;
    let port = listener.local_addr()?.port();
    drop(listener);
    Ok(port)
}

fn write_tls_bundle(dir: &Path) -> Result<CertBundle> {
    let mut ca_params = CertificateParams::default();
    ca_params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
    ca_params.key_usages = vec![
        KeyUsagePurpose::KeyCertSign,
        KeyUsagePurpose::DigitalSignature,
        KeyUsagePurpose::KeyEncipherment,
    ];
    // Give the CA a distinct DN so the server cert's issuer != subject.
    // Without this, both certs use the default DN and OpenSSL considers
    // the server cert self-signed (issuer == subject → X509 code 18).
    ca_params
        .distinguished_name
        .push(rcgen::DnType::CommonName, "Sinex Test CA");
    let ca_key = KeyPair::generate().map_err(|e| eyre!("CA key error: {e}"))?;
    let ca = ca_params
        .self_signed(&ca_key)
        .map_err(|e| eyre!("CA cert error: {e}"))?;
    let ca_pem = ca.pem();

    let mut server_params = CertificateParams::default();
    server_params.is_ca = IsCa::NoCa;
    server_params.extended_key_usages = vec![ExtendedKeyUsagePurpose::ServerAuth];
    server_params
        .distinguished_name
        .push(rcgen::DnType::CommonName, "localhost");
    server_params.subject_alt_names = vec![
        SanType::DnsName(
            "localhost"
                .try_into()
                .map_err(|e| eyre!("invalid DNS SAN: {e}"))?,
        ),
        SanType::IpAddress(IpAddr::V4(Ipv4Addr::LOCALHOST)),
    ];
    let server_key = KeyPair::generate().map_err(|e| eyre!("server key error: {e}"))?;
    let server = server_params
        .signed_by(&server_key, &ca, &ca_key)
        .map_err(|e| eyre!("server cert error: {e}"))?;
    let server_pem = server.pem();
    let server_key_pem = server_key.serialize_pem();

    let server_cert_path = dir.join("gateway-cert.pem");
    let server_key_path = dir.join("gateway-key.pem");
    // Write full chain (leaf + CA) so rustls presents the complete chain to clients
    let chain_pem = format!("{server_pem}{ca_pem}");
    std::fs::write(&server_cert_path, chain_pem)?;
    std::fs::write(&server_key_path, server_key_pem)?;

    Ok(CertBundle {
        ca_pem,
        server_cert_path,
        server_key_path,
    })
}

async fn wait_for_tls_response(client: &Client, url: &str, token: &str) -> Result<()> {
    let deadline = Instant::now() + Duration::from_secs(Timeouts::SHORT);
    let payload = json!({
        "jsonrpc": "2.0",
        "id": 1,
        "method": "system.health",
        "params": {}
    });
    let mut last_err = None;

    while Instant::now() < deadline {
        let resp = client
            .post(url)
            .header("content-type", "application/json")
            .header("authorization", format!("Bearer {token}"))
            .json(&payload)
            .send()
            .await;

        match resp {
            Ok(response) => {
                let _ = response.text().await;
                return Ok(());
            }
            Err(err) => {
                last_err = Some(err);
                sleep(Duration::from_millis(100)).await;
            }
        }
    }

    Err(eyre!(
        "TLS gateway did not respond before timeout: {:?}",
        last_err
    ))
}

#[sinex_test]
async fn gateway_tls_accepts_handshake(ctx: TestContext) -> Result<()> {
    let temp = TempDir::new()?;
    let bundle = write_tls_bundle(temp.path())?;
    let annex_path = temp.path().join("annex");
    let annex_path = annex_path.to_string_lossy().to_string();

    let mut env = EnvGuard::new();
    env.set("SINEX_RPC_TOKEN", "test-token");
    env.set("SINEX_REPLAY_CONTROL_OPTIONAL", "1");
    env.set("SINEX_ANNEX_PATH", &annex_path);
    // Ensure host environment CA settings don't bleed into the test
    env.clear("SINEX_GATEWAY_TLS_CLIENT_CA");
    env.set(
        "SINEX_GATEWAY_TLS_CERT",
        bundle
            .server_cert_path
            .to_str()
            .ok_or_else(|| eyre!("TLS cert path is not valid UTF-8"))?,
    );
    env.set(
        "SINEX_GATEWAY_TLS_KEY",
        bundle
            .server_key_path
            .to_str()
            .ok_or_else(|| eyre!("TLS key path is not valid UTF-8"))?,
    );
    let _env = env;

    let port = reserve_port()?;
    let tcp_listen = format!("127.0.0.1:{port}");
    let config = sinex_gateway::config::GatewayConfig::load().with_cli_overrides(
        Some(ctx.database_url().to_string()),
        Some(tcp_listen.clone()),
        None,
    );
    let services = ServiceContainer::from_database_url(ctx.database_url()).await?;
    let (_shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
    let server_handle = tokio::spawn({
        let services = services.clone();
        let config = config.clone();
        async move {
            let _ = rpc_server::run(&config, services, shutdown_rx).await;
        }
    });

    let ca = ReqwestCert::from_pem(bundle.ca_pem.as_bytes())?;
    let client = Client::builder().add_root_certificate(ca).build()?;
    let url = format!("https://127.0.0.1:{port}/rpc");
    let result = wait_for_tls_response(&client, &url, "test-token").await;

    server_handle.abort();
    result
}
