//! Pool configuration and sizing logic.

use color_eyre::eyre::{Result, WrapErr, eyre};
use std::path::PathBuf;
use toml::Value;
use url::Url;

pub(super) const MIN_POOL_SIZE: usize = 48;
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
        let base_url = test_database_url().unwrap_or_else(|| {
            crate::infra::stack::StackConfig::for_current_checkout().map_or_else(
                |_| "postgresql:///sinex_dev?host=/run/postgresql".to_string(),
                |cfg| cfg.database_url(),
            )
        });
        let admin_url = test_database_superuser_url()
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

pub(super) fn test_database_url() -> Option<String> {
    std::env::var("SINEX_TEST_DATABASE_URL")
        .ok()
        .filter(|url| !url.trim().is_empty())
        .or_else(|| {
            std::env::var("DATABASE_URL")
                .ok()
                .filter(|url| !url.trim().is_empty())
        })
}

pub(super) fn test_database_superuser_url() -> Result<String, std::env::VarError> {
    std::env::var("SINEX_TEST_DATABASE_URL_SUPERUSER")
        .or_else(|_| std::env::var("DATABASE_URL_SUPERUSER"))
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
    if let Some(size) = configured_pool_size() {
        return size;
    }

    let test_threads = detected_nextest_test_threads_or_cpu_count();
    let target = test_threads.saturating_mul(POOL_SIZE_MULTIPLIER);
    target.max(MIN_POOL_SIZE)
}

fn configured_pool_size() -> Option<usize> {
    let raw = std::env::var("SINEX_TEST_DB_POOL_SIZE").ok()?;
    match parse_configured_pool_size(&raw) {
        Ok(value) => value,
        Err(error) => {
            eprintln!(
                "warning: ignoring invalid SINEX_TEST_DB_POOL_SIZE={raw:?}: {error}. \
                 Use a positive integer or `auto`."
            );
            None
        }
    }
}

fn parse_configured_pool_size(raw: &str) -> Result<Option<usize>> {
    let trimmed = raw.trim();
    if trimmed.is_empty() || trimmed.eq_ignore_ascii_case("auto") {
        return Ok(None);
    }

    let size: usize = trimmed
        .parse()
        .map_err(|err| eyre!("expected positive integer or `auto`: {err}"))?;
    if size == 0 {
        return Err(eyre!("pool size must be greater than zero"));
    }
    Ok(Some(size))
}

fn detected_nextest_test_threads_or_cpu_count() -> usize {
    let cpu_count =
        std::thread::available_parallelism().map_or(MIN_POOL_SIZE, std::num::NonZero::get);
    match nextest_test_threads(cpu_count) {
        Ok(Some(value)) => value.max(1),
        Ok(None) => cpu_count.max(1),
        Err(error) => {
            eprintln!(
                "⚠️  Failed to detect nextest test thread count from .config/nextest.toml: {error:#}. \
                 Using CPU count ({cpu_count})"
            );
            cpu_count.max(1)
        }
    }
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
    let Some(profile_cfg) = config
        .get("profile")
        .and_then(|profiles| profiles.get(profile))
    else {
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
#[path = "config_test.rs"]
mod tests;
