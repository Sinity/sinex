use std::net::TcpListener;
use std::path::Path;
use std::time::Duration;

use sinexd::api::{ServiceContainer, config::GatewayConfig, rpc_server};
use tempfile::NamedTempFile;
use tokio::sync::watch;
use xtask::sandbox::{EnvGuard, TestContext, timing::Timeouts};

pub const TEST_TOKEN: &str = "test-token:admin";

pub const GATEWAY_ENV_KEYS: &[&str] = &[
    "SINEX_API_TLS_CERT",
    "SINEX_API_TLS_KEY",
    "SINEX_API_TLS_CLIENT_CA",
    "SINEX_API_TOKEN",
    "SINEX_NATS_URL",
];

pub struct TestGateway {
    pub port: u16,
    _env: EnvGuard,
    _shutdown_tx: watch::Sender<bool>,
    handle: tokio::task::JoinHandle<()>,
    _cert_file: NamedTempFile,
    _key_file: NamedTempFile,
}

impl TestGateway {
    pub fn rpc_url(&self) -> String {
        format!("https://127.0.0.1:{}/rpc", self.port)
    }

    #[allow(dead_code)]
    pub fn base_url(&self) -> String {
        format!("https://127.0.0.1:{}", self.port)
    }
}

impl Drop for TestGateway {
    fn drop(&mut self) {
        self.handle.abort();
    }
}

pub fn configure_test_gateway_env(nats_url: &str, cert_path: &Path, key_path: &Path) -> EnvGuard {
    let mut env = EnvGuard::with_keys(GATEWAY_ENV_KEYS);
    env.set("SINEX_API_TLS_CERT", cert_path.as_os_str());
    env.set("SINEX_API_TLS_KEY", key_path.as_os_str());
    env.clear("SINEX_API_TLS_CLIENT_CA");
    env.set("SINEX_API_TOKEN", TEST_TOKEN);
    env.set("SINEX_NATS_URL", nats_url);
    env
}

pub async fn start_test_gateway(ctx: &TestContext) -> color_eyre::Result<TestGateway> {
    let nats_url = ctx.nats_handle()?.client_url().to_string();
    let cert = rcgen::generate_simple_self_signed(vec!["localhost".into(), "127.0.0.1".into()])?;
    let cert_file = NamedTempFile::new()?;
    let key_file = NamedTempFile::new()?;
    tokio::fs::write(cert_file.path(), cert.cert.pem()).await?;
    tokio::fs::write(key_file.path(), cert.key_pair.serialize_pem()).await?;
    let env = configure_test_gateway_env(&nats_url, cert_file.path(), key_file.path());

    let port = reserve_port()?;
    let mut config = GatewayConfig::load_with_database_url(ctx.database_url().to_string())?;
    config.database_url = ctx.database_url().to_string();
    config.tcp_listen = format!("127.0.0.1:{port}");
    config.rpc_rate_limit_enabled = false;
    let services = ServiceContainer::new(&config).await?;
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let mut server_handle = tokio::spawn({
        let services = services.clone();
        async move {
            if let Err(error) = rpc_server::run(&config, services, shutdown_rx).await {
                eprintln!("Gateway startup failed: {error:#}");
            }
        }
    });

    let port_timeout = Duration::from_secs(Timeouts::STANDARD);
    tokio::select! {
        result = wait_for_port(port, port_timeout) => {
            result?;
        }
        join_result = &mut server_handle => {
            match join_result {
                Ok(()) => {
                    return Err(color_eyre::eyre::eyre!(
                        "Gateway server exited before binding port {port}"
                    ));
                }
                Err(error) => {
                    return Err(color_eyre::eyre::eyre!(
                        "Gateway server task panicked: {error}"
                    ));
                }
            }
        }
    }

    Ok(TestGateway {
        port,
        _env: env,
        _shutdown_tx: shutdown_tx,
        handle: server_handle,
        _cert_file: cert_file,
        _key_file: key_file,
    })
}

fn reserve_port() -> color_eyre::Result<u16> {
    let listener = TcpListener::bind("127.0.0.1:0")?;
    let port = listener.local_addr()?.port();
    drop(listener);
    Ok(port)
}

async fn wait_for_port(port: u16, timeout: Duration) -> color_eyre::Result<()> {
    let deadline = tokio::time::Instant::now() + timeout;
    loop {
        match tokio::net::TcpStream::connect(format!("127.0.0.1:{port}")).await {
            Ok(_) => return Ok(()),
            Err(_) if tokio::time::Instant::now() < deadline => {
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
            Err(error) => {
                return Err(color_eyre::eyre::eyre!(
                    "Gateway port {port} not ready after {timeout:?}: {error}"
                ));
            }
        }
    }
}
