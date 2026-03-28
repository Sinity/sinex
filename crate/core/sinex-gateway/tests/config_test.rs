use sinex_gateway::config::GatewayConfig;
use sinex_primitives::environment::environment;
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

    let config = GatewayConfig::load();
    let expected = environment()
        .database_url("postgresql://gateway-config/sinex")
        .unwrap_or_else(|_| "postgresql://gateway-config/sinex".to_string());

    assert_eq!(config.database_url, expected);
    Ok(())
}

#[sinex_test]
async fn gateway_cli_database_override_uses_effective_database_url() -> TestResult<()> {
    let config = GatewayConfig::default().with_cli_overrides(
        Some("postgresql://gateway-cli/sinex".to_string()),
        None,
        None,
    );
    let expected = environment()
        .database_url("postgresql://gateway-cli/sinex")
        .unwrap_or_else(|_| "postgresql://gateway-cli/sinex".to_string());

    assert_eq!(config.database_url, expected);
    Ok(())
}
