use sinex_gateway::config::GatewayConfig;
use xtask::sandbox::prelude::*;

struct EnvGuard {
    saved: Vec<(String, Option<String>)>,
}

impl EnvGuard {
    fn new(keys: &[&str]) -> Self {
        Self {
            saved: keys
                .iter()
                .map(|key| ((*key).to_string(), std::env::var(key).ok()))
                .collect(),
        }
    }

    fn set(&mut self, key: &str, value: &str) {
        unsafe { std::env::set_var(key, value) };
    }

    fn remove(&mut self, key: &str) {
        unsafe { std::env::remove_var(key) };
    }
}

impl Drop for EnvGuard {
    fn drop(&mut self) {
        for (key, value) in self.saved.drain(..) {
            unsafe {
                match value {
                    Some(value) => std::env::set_var(key, value),
                    None => std::env::remove_var(key),
                }
            }
        }
    }
}

#[sinex_test]
async fn gateway_config_load_namespaces_database_url_from_env() -> TestResult<()> {
    let mut env = EnvGuard::new(&["DATABASE_URL"]);
    env.set("DATABASE_URL", "postgresql://gateway-config/sinex");

    let config = GatewayConfig::load()?;
    assert_eq!(config.database_url, "postgresql://gateway-config/sinex");
    Ok(())
}

#[sinex_test]
async fn gateway_config_load_requires_database_url() -> TestResult<()> {
    let mut env = EnvGuard::new(&["DATABASE_URL"]);
    env.remove("DATABASE_URL");

    let error = GatewayConfig::load().expect_err("missing database url should fail gateway config load");
    let message = error.to_string();

    assert!(message.contains("Database URL not provided"));
    Ok(())
}

#[sinex_test]
async fn gateway_config_load_rejects_malformed_database_url() -> TestResult<()> {
    let mut env = EnvGuard::new(&["DATABASE_URL"]);
    env.set("DATABASE_URL", "not-a-database-url");

    let error = GatewayConfig::load().expect_err("malformed database url should fail gateway config load");
    let message = error.to_string();

    assert!(message.contains("failed to parse DATABASE_URL"));
    Ok(())
}

#[sinex_test]
async fn gateway_cli_database_override_uses_effective_database_url() -> TestResult<()> {
    let config = GatewayConfig::default().with_cli_overrides(
        Some("postgresql://gateway-cli/sinex".to_string()),
        None,
        None,
    );
    assert_eq!(config.database_url, "postgresql://gateway-cli/sinex");
    Ok(())
}

#[sinex_test]
async fn gateway_config_rejects_invalid_numeric_env_overrides() -> TestResult<()> {
    let mut env = EnvGuard::new(&["SINEX_GATEWAY_MAX_CONCURRENCY"]);
    env.set("SINEX_GATEWAY_MAX_CONCURRENCY", "many");

    let error = GatewayConfig::load().expect_err("invalid env should fail gateway config load");
    let message = error.to_string();

    assert!(message.contains("SINEX_GATEWAY_MAX_CONCURRENCY"));
    assert!(message.contains("many"));
    Ok(())
}
