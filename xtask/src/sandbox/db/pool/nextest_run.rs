//! Nextest-run-scoped preparation cache for lazy sandbox pool startup.
//!
//! Under nextest, each test binary runs in its own process. Without a shared run-scoped
//! preparation artifact, every child repeats template validation and stale-slot pruning before
//! it can even start looking for a slot. That turns "lazy provisioning" into repeated
//! initialization churn. This module makes the expensive preparation once-per-nextest-run while
//! keeping the per-process slot objects local and cheap.

use crate::config::config;
use crate::sandbox::prelude::*;
use color_eyre::eyre::{WrapErr, eyre};
use nix::fcntl::{FlockArg, flock};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fs::{self, OpenOptions};
use std::os::fd::AsRawFd;
use std::path::{Path, PathBuf};

use super::LazySlotPruneSummary;
use super::eagerly_recreate_pruned_lazy_slot_databases;
use super::ensure_template_database;
use super::meta::TemplateInfo;
use super::prune_stale_lazy_slot_databases;
use super::schema_fingerprint;

#[derive(Debug)]
pub(super) struct NextestLazyPoolPreparation {
    pub(super) expected_fingerprint: Option<String>,
    pub(super) expected_extensions: HashMap<String, String>,
    pub(super) slot_names: Vec<String>,
    pub(super) prune_summary: LazySlotPruneSummary,
    pub(super) reused: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
struct CachedLazyPoolPreparation {
    expected_fingerprint: Option<String>,
    expected_extensions: HashMap<String, String>,
    slot_names: Vec<String>,
    prepared_at_rfc3339: String,
}

impl CachedLazyPoolPreparation {
    fn matches_request(&self, slot_names: &[String], expected_fingerprint: &Option<String>) -> bool {
        self.slot_names == slot_names && self.expected_fingerprint == *expected_fingerprint
    }
}

struct LockedPreparationState {
    _lock_file: fs::File,
    state_path: PathBuf,
}

pub(super) async fn prepare_nextest_lazy_pool(
    admin_url: &str,
    base_url: &str,
    slot_max_connections: u32,
    pool_size: usize,
) -> TestResult<NextestLazyPoolPreparation> {
    let expected_fingerprint = Some(schema_fingerprint()?);
    let slot_names: Vec<String> = (0..pool_size)
        .map(|index| format!("sinex_test_pool_{index}"))
        .collect();

    let Some(run_id) = nextest_run_id() else {
        return prepare_without_cache(
            admin_url,
            base_url,
            slot_max_connections,
            slot_names,
            expected_fingerprint,
        )
        .await;
    };

    let locked_state = lock_preparation_state(&config().state_dir, &run_id)?;
    if let Some(cached) = load_cached_preparation(&locked_state.state_path)?
        && cached.matches_request(&slot_names, &expected_fingerprint)
    {
        return Ok(NextestLazyPoolPreparation {
            expected_fingerprint: cached.expected_fingerprint,
            expected_extensions: cached.expected_extensions,
            slot_names: cached.slot_names,
            prune_summary: LazySlotPruneSummary::default(),
            reused: true,
        });
    }

    let prepared = prepare_without_cache(
        admin_url,
        base_url,
        slot_max_connections,
        slot_names,
        expected_fingerprint,
    )
    .await?;
    let cached = CachedLazyPoolPreparation {
        expected_fingerprint: prepared.expected_fingerprint.clone(),
        expected_extensions: prepared.expected_extensions.clone(),
        slot_names: prepared.slot_names.clone(),
        prepared_at_rfc3339: Timestamp::now().format_rfc3339(),
    };
    store_cached_preparation(&locked_state.state_path, &cached)?;

    Ok(prepared)
}

async fn prepare_without_cache(
    admin_url: &str,
    base_url: &str,
    slot_max_connections: u32,
    slot_names: Vec<String>,
    expected_fingerprint: Option<String>,
) -> TestResult<NextestLazyPoolPreparation> {
    let TemplateInfo { extensions, .. } = ensure_template_info(
        admin_url,
        base_url,
        slot_max_connections,
    )
    .await?;
    let mut prune_summary =
        prune_stale_lazy_slot_databases(admin_url, &slot_names, &expected_fingerprint, &extensions)
            .await?;
    eagerly_recreate_pruned_lazy_slot_databases(admin_url, &mut prune_summary).await?;

    Ok(NextestLazyPoolPreparation {
        expected_fingerprint,
        expected_extensions: extensions,
        slot_names,
        prune_summary,
        reused: false,
    })
}

async fn ensure_template_info(
    admin_url: &str,
    base_url: &str,
    slot_max_connections: u32,
) -> TestResult<TemplateInfo> {
    let template_guard =
        ensure_template_database(admin_url, base_url, slot_max_connections).await?;
    let info = template_guard.info.clone();
    template_guard.release().await?;
    Ok(info)
}

fn nextest_run_id() -> Option<String> {
    std::env::var("NEXTEST_RUN_ID")
        .ok()
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
}

fn lock_preparation_state(state_dir: &Path, run_id: &str) -> TestResult<LockedPreparationState> {
    let (state_path, lock_path) = preparation_paths_in(state_dir, run_id);
    if let Some(parent) = state_path.parent() {
        fs::create_dir_all(parent)
            .wrap_err_with(|| format!("failed to create {}", parent.display()))?;
    }

    let lock_file = OpenOptions::new()
        .create(true)
        .write(true)
        .truncate(false)
        .open(&lock_path)
        .wrap_err_with(|| format!("failed to open {}", lock_path.display()))?;
    flock(lock_file.as_raw_fd(), FlockArg::LockExclusive)
        .map_err(|error| eyre!("failed to lock {}: {error}", lock_path.display()))?;

    Ok(LockedPreparationState {
        _lock_file: lock_file,
        state_path,
    })
}

fn load_cached_preparation(path: &Path) -> TestResult<Option<CachedLazyPoolPreparation>> {
    let raw = match fs::read_to_string(path) {
        Ok(raw) => raw,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => {
            return Err(error)
                .wrap_err_with(|| format!("failed to read {}", path.display()));
        }
    };

    match serde_json::from_str(&raw) {
        Ok(cached) => Ok(Some(cached)),
        Err(error) => {
            eprintln!(
                "⚠️  Ignoring unreadable nextest lazy pool preparation state at {}: {error}",
                path.display()
            );
            let _ = fs::remove_file(path);
            Ok(None)
        }
    }
}

fn store_cached_preparation(path: &Path, cached: &CachedLazyPoolPreparation) -> TestResult<()> {
    let raw = serde_json::to_string_pretty(cached)?;
    fs::write(path, raw).wrap_err_with(|| format!("failed to write {}", path.display()))?;
    Ok(())
}

fn preparation_paths_in(state_dir: &Path, run_id: &str) -> (PathBuf, PathBuf) {
    let dir = state_dir.join("sandbox-db-pool/nextest-runs");
    let state_path = dir.join(format!("{run_id}.json"));
    let lock_path = dir.join(format!("{run_id}.lock"));
    (state_path, lock_path)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sandbox::sinex_test;

    #[sinex_test]
    async fn cached_lazy_pool_preparation_matches_identical_request() -> TestResult<()> {
        let slot_names = vec!["sinex_test_pool_0".to_string(), "sinex_test_pool_1".to_string()];
        let cached = CachedLazyPoolPreparation {
            expected_fingerprint: Some("abc".to_string()),
            expected_extensions: HashMap::from([("timescaledb".to_string(), "2.20".to_string())]),
            slot_names: slot_names.clone(),
            prepared_at_rfc3339: Timestamp::now().format_rfc3339(),
        };

        assert!(cached.matches_request(&slot_names, &Some("abc".to_string())));
        assert!(!cached.matches_request(&slot_names, &Some("def".to_string())));
        Ok(())
    }

    #[sinex_test]
    async fn cached_preparation_roundtrip_uses_repo_local_state_layout() -> TestResult<()> {
        let temp = tempfile::tempdir()?;
        let (state_path, lock_path) = preparation_paths_in(temp.path(), "run-123");
        assert!(
            state_path.ends_with("sandbox-db-pool/nextest-runs/run-123.json"),
            "unexpected state path: {}",
            state_path.display()
        );
        assert!(
            lock_path.ends_with("sandbox-db-pool/nextest-runs/run-123.lock"),
            "unexpected lock path: {}",
            lock_path.display()
        );

        let cached = CachedLazyPoolPreparation {
            expected_fingerprint: Some("fingerprint".to_string()),
            expected_extensions: HashMap::from([("pg_trgm".to_string(), "1.6".to_string())]),
            slot_names: vec!["sinex_test_pool_0".to_string()],
            prepared_at_rfc3339: Timestamp::now().format_rfc3339(),
        };
        if let Some(parent) = state_path.parent() {
            fs::create_dir_all(parent)?;
        }
        store_cached_preparation(&state_path, &cached)?;

        let loaded = load_cached_preparation(&state_path)?
            .ok_or_else(|| eyre!("cached preparation should load"))?;
        assert_eq!(loaded, cached);
        Ok(())
    }

    #[sinex_test]
    async fn unreadable_cached_preparation_is_ignored_and_removed() -> TestResult<()> {
        let temp = tempfile::tempdir()?;
        let (state_path, _) = preparation_paths_in(temp.path(), "run-bad");
        if let Some(parent) = state_path.parent() {
            fs::create_dir_all(parent)?;
        }
        fs::write(&state_path, "{ definitely-not-json")?;

        let loaded = load_cached_preparation(&state_path)?;
        assert!(loaded.is_none(), "corrupt preparation state should be ignored");
        assert!(
            !state_path.exists(),
            "corrupt preparation state should be removed for a clean retry"
        );
        Ok(())
    }
}
