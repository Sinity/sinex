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
use nix::fcntl::{Flock, FlockArg};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fs::{self, OpenOptions};
use std::path::{Path, PathBuf};
use time::Duration as TimeDuration;

use super::LazySlotPruneSummary;
use super::eagerly_recreate_pruned_lazy_slot_databases;
use super::meta::TemplateInfo;
use super::prune_stale_lazy_slot_databases;
use super::schema_fingerprint;
use super::template::ensure_templates_for_keys;

#[derive(Debug)]
pub(super) struct NextestLazyPoolPreparation {
    pub(super) expected_fingerprint: Option<String>,
    pub(super) expected_extensions: HashMap<String, String>,
    pub(super) slot_names: Vec<String>,
    pub(super) prune_summary: LazySlotPruneSummary,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
struct DeferredStaleSlot {
    slot_name: String,
    stale_reason: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
struct CachedLazyPoolPreparation {
    expected_fingerprint: Option<String>,
    expected_extensions: HashMap<String, String>,
    slot_names: Vec<String>,
    #[serde(default)]
    deferred_stale_slots: Vec<DeferredStaleSlot>,
    #[serde(default)]
    next_deferred_retry_at_rfc3339: Option<String>,
    prepared_at_rfc3339: String,
}

impl CachedLazyPoolPreparation {
    fn matches_request(
        &self,
        slot_names: &[String],
        expected_fingerprint: &Option<String>,
    ) -> bool {
        self.slot_names == slot_names && self.expected_fingerprint == *expected_fingerprint
    }

    fn from_preparation(prepared: &NextestLazyPoolPreparation) -> Self {
        let mut cached = Self {
            expected_fingerprint: prepared.expected_fingerprint.clone(),
            expected_extensions: prepared.expected_extensions.clone(),
            slot_names: prepared.slot_names.clone(),
            deferred_stale_slots: Vec::new(),
            next_deferred_retry_at_rfc3339: None,
            prepared_at_rfc3339: Timestamp::now().format_rfc3339(),
        };
        cached.record_deferred_stale_slots(&prepared.prune_summary);
        cached
    }

    fn into_preparation(self, prune_summary: LazySlotPruneSummary) -> NextestLazyPoolPreparation {
        NextestLazyPoolPreparation {
            expected_fingerprint: self.expected_fingerprint,
            expected_extensions: self.expected_extensions,
            slot_names: self.slot_names,
            prune_summary,
        }
    }

    fn has_deferred_stale_slots(&self) -> bool {
        !self.deferred_stale_slots.is_empty()
    }

    fn deferred_retry_due(&self, now: Timestamp) -> bool {
        let Some(retry_at) = self.next_deferred_retry_at_rfc3339.as_deref() else {
            return true;
        };
        match Timestamp::parse_rfc3339(retry_at) {
            Ok(retry_at) => retry_at <= now,
            Err(_) => true,
        }
    }

    fn deferred_slot_names(&self) -> Vec<String> {
        self.deferred_stale_slots
            .iter()
            .map(|slot| slot.slot_name.clone())
            .collect()
    }

    fn record_deferred_stale_slots(&mut self, prune_summary: &LazySlotPruneSummary) {
        self.deferred_stale_slots = prune_summary
            .locked_stale_slots
            .iter()
            .map(|(slot_name, stale_reason)| DeferredStaleSlot {
                slot_name: slot_name.clone(),
                stale_reason: stale_reason.clone(),
            })
            .collect();
        self.next_deferred_retry_at_rfc3339 = if self.deferred_stale_slots.is_empty() {
            None
        } else {
            Some((Timestamp::now() + TimeDuration::seconds(2)).format_rfc3339())
        };
    }
}

struct LockedPreparationState {
    _lock_file: Flock<fs::File>,
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

    let (state_path, _lock_path) = preparation_paths_in(&config().state_dir, &run_id);
    if let Some(prepared) = try_reuse_cached_preparation(
        &state_path,
        &slot_names,
        &expected_fingerprint,
        Timestamp::now(),
    )? {
        return Ok(prepared);
    }

    let locked_state = lock_preparation_state(&config().state_dir, &run_id)?;
    if let Some(mut cached) = load_cached_preparation(&locked_state.state_path)?
        && cached.matches_request(&slot_names, &expected_fingerprint)
    {
        if cached.has_deferred_stale_slots() {
            let prune_summary = retry_deferred_stale_slots(admin_url, &mut cached).await?;
            store_cached_preparation(&locked_state.state_path, &cached)?;
            return Ok(cached.into_preparation(prune_summary));
        }

        return Ok(cached.into_preparation(LazySlotPruneSummary::default()));
    }

    let prepared = prepare_without_cache(
        admin_url,
        base_url,
        slot_max_connections,
        slot_names,
        expected_fingerprint,
    )
    .await?;
    let cached = CachedLazyPoolPreparation::from_preparation(&prepared);
    store_cached_preparation(&locked_state.state_path, &cached)?;

    Ok(prepared)
}

fn try_reuse_cached_preparation(
    state_path: &Path,
    slot_names: &[String],
    expected_fingerprint: &Option<String>,
    now: Timestamp,
) -> TestResult<Option<NextestLazyPoolPreparation>> {
    let Some(cached) = load_cached_preparation(state_path)? else {
        return Ok(None);
    };
    if !cached.matches_request(slot_names, expected_fingerprint) {
        return Ok(None);
    }
    if cached.has_deferred_stale_slots() && cached.deferred_retry_due(now) {
        return Ok(None);
    }

    Ok(Some(
        cached.into_preparation(LazySlotPruneSummary::default()),
    ))
}

async fn retry_deferred_stale_slots(
    admin_url: &str,
    cached: &mut CachedLazyPoolPreparation,
) -> TestResult<LazySlotPruneSummary> {
    if !cached.has_deferred_stale_slots() {
        return Ok(LazySlotPruneSummary::default());
    }

    let deferred_slots = cached.deferred_slot_names();
    let mut prune_summary = prune_stale_lazy_slot_databases(
        admin_url,
        &deferred_slots,
        &cached.expected_fingerprint,
        &cached.expected_extensions,
    )
    .await?;
    eagerly_recreate_pruned_lazy_slot_databases(admin_url, &mut prune_summary).await?;
    cached.record_deferred_stale_slots(&prune_summary);
    Ok(prune_summary)
}

async fn prepare_without_cache(
    admin_url: &str,
    base_url: &str,
    slot_max_connections: u32,
    slot_names: Vec<String>,
    expected_fingerprint: Option<String>,
) -> TestResult<NextestLazyPoolPreparation> {
    let TemplateInfo { extensions, .. } =
        ensure_template_info(admin_url, base_url, slot_max_connections, &slot_names).await?;
    let mut prune_summary =
        prune_stale_lazy_slot_databases(admin_url, &slot_names, &expected_fingerprint, &extensions)
            .await?;
    eagerly_recreate_pruned_lazy_slot_databases(admin_url, &mut prune_summary).await?;

    Ok(NextestLazyPoolPreparation {
        expected_fingerprint,
        expected_extensions: extensions,
        slot_names,
        prune_summary,
    })
}

async fn ensure_template_info(
    admin_url: &str,
    base_url: &str,
    slot_max_connections: u32,
    slot_names: &[String],
) -> TestResult<TemplateInfo> {
    ensure_templates_for_keys(admin_url, base_url, slot_max_connections, slot_names).await
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
    let lock_file = Flock::lock(lock_file, FlockArg::LockExclusive)
        .map_err(|(_lock_file, error)| eyre!("failed to lock {}: {error}", lock_path.display()))?;

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
            return Err(error).wrap_err_with(|| format!("failed to read {}", path.display()));
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
    let tmp_path = path.with_extension("json.tmp");
    fs::write(&tmp_path, raw)
        .wrap_err_with(|| format!("failed to write {}", tmp_path.display()))?;
    fs::rename(&tmp_path, path)
        .wrap_err_with(|| format!("failed to replace {}", path.display()))?;
    Ok(())
}

fn preparation_paths_in(state_dir: &Path, run_id: &str) -> (PathBuf, PathBuf) {
    let dir = state_dir.join("sandbox-db-pool/nextest-runs");
    let state_path = dir.join(format!("{run_id}.json"));
    let lock_path = dir.join(format!("{run_id}.lock"));
    (state_path, lock_path)
}

#[cfg(test)]
#[path = "nextest_run_test.rs"]
mod tests;
