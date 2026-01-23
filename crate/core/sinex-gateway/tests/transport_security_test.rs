use std::convert::TryInto;
use std::net::{IpAddr, Ipv4Addr, TcpListener};
use std::path::{Path, PathBuf};
use std::sync::Mutex;

use color_eyre::eyre::{eyre, Result};
use once_cell::sync::Lazy;
use rcgen::{
    BasicConstraints, CertificateParams, ExtendedKeyUsagePurpose, IsCa, KeyPair, KeyUsagePurpose,
    SanType,
};
use reqwest::{Certificate as ReqwestCert, Client};
use serde_json::json;
use sinex_gateway::{rpc_server, ServiceContainer};
use sinex_test_utils::{sinex_test, timing_utils::Timeouts, TestContext};
use tempfile::TempDir;
use tokio::time::{sleep, Duration, Instant};

static ENV_LOCK: Lazy<Mutex<()>> = Lazy::new(|| Mutex::new(()));

struct EnvVarGuard {
    key: &'static str,
    prev: Option<String>,
}

impl EnvVarGuard {
    fn set(key: &'static str, value: &str) -> Self {
        let prev = std::env::var(key).ok();
        std::env::set_var(key, value);
        Self { key, prev }
    }
}

impl Drop for EnvVarGuard {
    fn drop(&mut self) {
        if let Some(prev) = &self.prev {
            std::env::set_var(self.key, prev);
        } else {
            std::env::remove_var(self.key);
        }
    }
}

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
    let ca_key = KeyPair::generate().map_err(|e| eyre!("CA key error: {e}"))?;
    let ca = ca_params
        .self_signed(&ca_key)
        .map_err(|e| eyre!("CA cert error: {e}"))?;
    let ca_pem = ca.pem();

    let mut server_params = CertificateParams::default();
    server_params.is_ca = IsCa::NoCa;
    server_params.extended_key_usages = vec![ExtendedKeyUsagePurpose::ServerAuth];
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
    std::fs::write(&server_cert_path, server_pem)?;
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
    let _lock = ENV_LOCK.lock().unwrap();
    let temp = TempDir::new()?;
    let bundle = write_tls_bundle(temp.path())?;
    let annex_path = temp.path().join("annex");
    let annex_path = annex_path.to_string_lossy().to_string();

    let _token = EnvVarGuard::set("SINEX_RPC_TOKEN", "test-token");
    let _bypass = EnvVarGuard::set("SINEX_ALLOW_REPLAY_CONTROL_BYPASS", "1");
    let _annex = EnvVarGuard::set("SINEX_ANNEX_PATH", &annex_path);
    let _cert = EnvVarGuard::set(
        "SINEX_GATEWAY_TLS_CERT",
        bundle
            .server_cert_path
            .to_str()
            .ok_or_else(|| eyre!("TLS cert path is not valid UTF-8"))?,
    );
    let _key = EnvVarGuard::set(
        "SINEX_GATEWAY_TLS_KEY",
        bundle
            .server_key_path
            .to_str()
            .ok_or_else(|| eyre!("TLS key path is not valid UTF-8"))?,
    );

    let services = ServiceContainer::new(Some(ctx.database_url().to_string())).await?;
    let (_shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
    let port = reserve_port()?;
    let tcp_listen = format!("127.0.0.1:{port}");
    let server_handle = tokio::spawn({
        let services = services.clone();
        async move {
            let _ = rpc_server::run(Some(tcp_listen.as_str()), services, shutdown_rx).await;
        }
    });

    let ca = ReqwestCert::from_pem(bundle.ca_pem.as_bytes())?;
    let client = Client::builder().add_root_certificate(ca).build()?;
    let url = format!("https://127.0.0.1:{port}/rpc");
    let result = wait_for_tls_response(&client, &url, "test-token").await;

    server_handle.abort();
    result
}
