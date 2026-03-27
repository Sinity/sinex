//! Pool configuration and sizing logic.

use color_eyre::eyre::{Result, WrapErr, eyre};
use std::path::PathBuf;
use toml::Value;
use url::Url;

pub(super) const MIN_POOL_SIZE: usize = 64;
pub(super) const POOL_SIZE_MULTIPLIER: usize = 2;
pub(super) const SLOT_MAX_CONNECTIONS: u32 = 8;
pub(super) const ADMIN_MAX_CONNECTIONS: u32 = 8;

/// Database pool configuration
pub(super) struct PoolConfig {
    pub(super) size: usize,
    pub(super) admin_url: String,
    pub(super) base_url: String,
    pub(super) slot_max_connections: u32,
    pub(super) admin_max_connections: u32,
}

impl Default for PoolConfig {
    fn default() -> Self {
        let base_url = std::env::var("DATABASE_URL").unwrap_or_else(|_| {
            crate::infra::stack::StackConfig::for_current_checkout().map_or_else(
                |_| "postgresql:///sinex_dev?host=/run/postgresql".to_string(),
                |cfg| cfg.database_url(),
            )
        });
        let admin_url = std::env::var("DATABASE_URL_SUPERUSER")
            .unwrap_or_else(|_| force_user(&replace_db_name(&base_url, "postgres"), "postgres"));
        let size = default_pool_size();

        Self {
            size,
            admin_url,
            base_url,
            slot_max_connections: SLOT_MAX_CONNECTIONS,
            admin_max_connections: ADMIN_MAX_CONNECTIONS,
        }
    }
}

impl PoolConfig {
    pub(super) fn apply_connection_budget(&mut self, budget: u32) {
        let per_slot = self.slot_max_connections.max(1);
        let usable_budget = budget.saturating_sub(self.admin_max_connections);
        let max_size = (usable_budget / per_slot).max(1);
        if (self.size as u32) > max_size {
            self.size = max_size as usize;
        }
    }
}

pub(super) fn default_pool_size() -> usize {
    let cpu_count =
        std::thread::available_parallelism().map_or(MIN_POOL_SIZE, std::num::NonZero::get);
    let test_threads = match nextest_test_threads(cpu_count) {
        Ok(Some(value)) => value,
        Ok(None) => cpu_count,
        Err(error) => {
            eprintln!(
                "⚠️  Failed to detect nextest test thread count from .config/nextest.toml: {error:#}. \
                 Using CPU count ({cpu_count})"
            );
            cpu_count
        }
    }
    .max(1);
    let target = test_threads.saturating_mul(POOL_SIZE_MULTIPLIER);
    target.max(MIN_POOL_SIZE)
}

fn nextest_test_threads(cpu_count: usize) -> Result<Option<usize>> {
    if !is_nextest_run() && nextest_profile_name().is_none() {
        return Ok(None);
    }

    let profile = nextest_profile_name().unwrap_or_else(|| "default".to_string());
    let Some(config_path) = find_nextest_config() else {
        return Ok(None);
    };
    let raw = std::fs::read_to_string(&config_path)
        .wrap_err_with(|| format!("failed to read {}", config_path.display()))?;
    let config: Value = toml::from_str(&raw)
        .wrap_err_with(|| format!("failed to parse {}", config_path.display()))?;
    nextest_test_threads_from_config(&config, &profile, cpu_count)
}

fn nextest_test_threads_from_config(
    config: &Value,
    profile: &str,
    cpu_count: usize,
) -> Result<Option<usize>> {
    let Some(profile_cfg) = config.get("profile").and_then(|profiles| profiles.get(profile)) else {
        return Ok(None);
    };
    let Some(test_threads) = profile_cfg.get("test-threads") else {
        return Ok(None);
    };
    match test_threads {
        Value::Integer(value) if *value > 0 => Ok(Some(*value as usize)),
        Value::String(value) => parse_num_cpus_expression(value, cpu_count),
        _ => Ok(None),
    }
}

fn parse_num_cpus_expression(value: &str, cpu_count: usize) -> Result<Option<usize>> {
    let trimmed = value.trim();
    if trimmed == "num-cpus" {
        return Ok(Some(cpu_count));
    }
    if let Some(rest) = trimmed.strip_prefix("num-cpus-") {
        let delta: usize = rest
            .parse()
            .map_err(|err| eyre!("invalid nextest test-threads expression `{trimmed}`: {err}"))?;
        return Ok(Some(cpu_count.saturating_sub(delta).max(1)));
    }
    if let Some(rest) = trimmed.strip_prefix("num-cpus+") {
        let delta: usize = rest
            .parse()
            .map_err(|err| eyre!("invalid nextest test-threads expression `{trimmed}`: {err}"))?;
        return Ok(Some(cpu_count.saturating_add(delta).max(1)));
    }
    Ok(None)
}

fn nextest_profile_name() -> Option<String> {
    for key in ["NEXTEST_PROFILE", "NEXTEST_PROFILE_NAME"] {
        if let Ok(value) = std::env::var(key)
            && !value.trim().is_empty()
        {
            return Some(value);
        }
    }
    None
}

fn find_nextest_config() -> Option<PathBuf> {
    let mut dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    loop {
        let candidate = dir.join(".config/nextest.toml");
        if candidate.is_file() {
            return Some(candidate);
        }
        if !dir.pop() {
            return None;
        }
    }
}

pub(super) fn is_nextest_run() -> bool {
    std::env::var_os("NEXTEST_RUN_ID").is_some() || std::env::var_os("NEXTEST").is_some()
}

pub(super) fn force_user(url: &str, user: &str) -> String {
    if let Ok(mut parsed) = Url::parse(url) {
        let _ = parsed.set_username(user);
        return parsed.to_string();
    }

    if url.contains('?') {
        format!("{url}&user={user}")
    } else {
        format!("{url}?user={user}")
    }
}

pub(crate) fn replace_db_name(url: &str, db: &str) -> String {
    if let Ok(mut parsed) = Url::parse(url) {
        parsed.set_path(&format!("/{db}"));
        return parsed.to_string();
    }

    let (head, tail) = url.rsplit_once('/').unwrap_or((url, ""));
    let replaced_tail = if let Some((_, query)) = tail.split_once('?') {
        format!("{db}?{query}")
    } else {
        db.to_string()
    };
    format!("{head}/{replaced_tail}")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sandbox::sinex_test;

    #[sinex_test]
    async fn test_parse_num_cpus_expression_supports_offsets() -> Result<()> {
        assert_eq!(parse_num_cpus_expression("num-cpus", 24)?, Some(24));
        assert_eq!(parse_num_cpus_expression("num-cpus-2", 24)?, Some(22));
        assert_eq!(parse_num_cpus_expression("num-cpus+3", 24)?, Some(27));
        Ok(())
    }

    #[sinex_test]
    async fn test_parse_num_cpus_expression_rejects_invalid_offsets() -> Result<()> {
        let err =
            parse_num_cpus_expression("num-cpus-bad", 24).expect_err("invalid offset should fail");
        assert!(
            err.to_string().contains("invalid nextest test-threads expression"),
            "unexpected error: {err:#}"
        );
        Ok(())
    }

    #[sinex_test]
    async fn test_nextest_test_threads_from_config_parses_profile() -> Result<()> {
        let config: Value = toml::from_str(
            r#"
            [profile.default]
            test-threads = "num-cpus-1"
            "#,
        )?;
        assert_eq!(
            nextest_test_threads_from_config(&config, "default", 24)?,
            Some(23)
        );
        Ok(())
    }

    #[sinex_test]
    async fn test_nextest_test_threads_from_config_ignores_missing_profile() -> Result<()> {
        let config: Value = toml::from_str(
            r#"
            [profile.ci]
            test-threads = 8
            "#,
        )?;
        assert_eq!(nextest_test_threads_from_config(&config, "default", 24)?, None);
        Ok(())
    }

    #[sinex_test]
    async fn test_replace_db_name_preserves_query_parameters() -> Result<()> {
        let replaced = replace_db_name(
            "postgresql://postgres@localhost/sinex_dev?host=/run/postgresql&sslmode=disable",
            "sinex_test_pool_1",
        );
        assert_eq!(
            replaced,
            "postgresql://postgres@localhost/sinex_test_pool_1?host=/run/postgresql&sslmode=disable"
        );
        Ok(())
    }
}
