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
    if let Some(mut cached) = load_cached_preparation(&locked_state.state_path)? {
        if cached.matches_request(&slot_names, &expected_fingerprint) {
            if cached.has_deferred_stale_slots() {
                let prune_summary = retry_deferred_stale_slots(admin_url, &mut cached).await?;
                store_cached_preparation(&locked_state.state_path, &cached)?;
                return Ok(cached.into_preparation(prune_summary));
            }

            return Ok(cached.into_preparation(LazySlotPruneSummary::default()));
        }
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
mod tests {
    use super::*;
    use crate::sandbox::sinex_test;

    #[sinex_test]
    async fn cached_lazy_pool_preparation_matches_identical_request() -> TestResult<()> {
        let slot_names = vec![
            "sinex_test_pool_0".to_string(),
            "sinex_test_pool_1".to_string(),
        ];
        let cached = CachedLazyPoolPreparation {
            expected_fingerprint: Some("abc".to_string()),
            expected_extensions: HashMap::from([("timescaledb".to_string(), "2.20".to_string())]),
            slot_names: slot_names.clone(),
            deferred_stale_slots: Vec::new(),
            next_deferred_retry_at_rfc3339: None,
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
            deferred_stale_slots: Vec::new(),
            next_deferred_retry_at_rfc3339: None,
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
    async fn cached_preparation_reuse_does_not_wait_for_writer_lock() -> TestResult<()> {
        let temp = tempfile::tempdir()?;
        let slot_names = vec![
            "sinex_test_pool_0".to_string(),
            "sinex_test_pool_1".to_string(),
        ];
        let (state_path, lock_path) = preparation_paths_in(temp.path(), "run-456");
        if let Some(parent) = state_path.parent() {
            fs::create_dir_all(parent)?;
        }

        let cached = CachedLazyPoolPreparation {
            expected_fingerprint: Some("fingerprint".to_string()),
            expected_extensions: HashMap::from([("timescaledb".to_string(), "2.20".to_string())]),
            slot_names: slot_names.clone(),
            deferred_stale_slots: Vec::new(),
            next_deferred_retry_at_rfc3339: None,
            prepared_at_rfc3339: Timestamp::now().format_rfc3339(),
        };
        store_cached_preparation(&state_path, &cached)?;

        let lock_file = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(false)
            .open(&lock_path)?;
        flock(lock_file.as_raw_fd(), FlockArg::LockExclusive)?;

        let reused = try_reuse_cached_preparation(
            &state_path,
            &slot_names,
            &Some("fingerprint".to_string()),
            Timestamp::now(),
        )?
        .ok_or_else(|| eyre!("cached preparation should be reusable"))?;

        assert_eq!(reused.slot_names, slot_names);
        assert_eq!(
            reused.expected_extensions.get("timescaledb"),
            Some(&"2.20".to_string())
        );
        assert!(reused.prune_summary.pruned_slots.is_empty());
        Ok(())
    }

    #[sinex_test]
    async fn deferred_cached_preparation_waits_for_retry_deadline() -> TestResult<()> {
        let cached = CachedLazyPoolPreparation {
            expected_fingerprint: Some("fingerprint".to_string()),
            expected_extensions: HashMap::new(),
            slot_names: vec!["sinex_test_pool_0".to_string()],
            deferred_stale_slots: vec![DeferredStaleSlot {
                slot_name: "sinex_test_pool_0".to_string(),
                stale_reason: "schema drift".to_string(),
            }],
            next_deferred_retry_at_rfc3339: Some(
                (Timestamp::now() + TimeDuration::seconds(60)).format_rfc3339(),
            ),
            prepared_at_rfc3339: Timestamp::now().format_rfc3339(),
        };
        let temp = tempfile::tempdir()?;
        let (state_path, _) = preparation_paths_in(temp.path(), "run-deferred");
        if let Some(parent) = state_path.parent() {
            fs::create_dir_all(parent)?;
        }
        store_cached_preparation(&state_path, &cached)?;

        let reused = try_reuse_cached_preparation(
            &state_path,
            &cached.slot_names,
            &cached.expected_fingerprint,
            Timestamp::now(),
        )?;
        assert!(
            reused.is_some(),
            "deferred stale slot should still reuse cache before retry deadline"
        );

        Ok(())
    }

    #[sinex_test]
    async fn retry_deferred_stale_slots_repairs_schema_drifted_slot() -> TestResult<()> {
        use super::super::connect_admin_with_retry;
        use super::super::drop_database_if_exists_admin;
        use super::super::load_pool_meta;
        use super::super::recreate_pool_database;
        use super::super::reset;
        use super::super::url_with_db_name;
        use super::super::wait_for_database_absence_admin;

        let config = super::super::config::PoolConfig::default();
        let db_name = format!("sinex_test_pool_retry_deferred_{}", std::process::id());
        let slot_url = url_with_db_name(&config.base_url, &db_name)?;
        let mut admin_conn = connect_admin_with_retry(&config.admin_url).await?;

        drop_database_if_exists_admin(&mut admin_conn, &db_name).await?;
        wait_for_database_absence_admin(&mut admin_conn, &db_name).await?;
        recreate_pool_database(&db_name, &slot_url).await?;
        let meta = load_pool_meta(&mut admin_conn, &db_name)
            .await?
            .ok_or_else(|| eyre!("missing pool metadata after slot recreation"))?;

        let slot_pool = sqlx::postgres::PgPoolOptions::new()
            .max_connections(2)
            .connect(&slot_url)
            .await?;
        sqlx::query(
            r#"
            ALTER TABLE raw.source_material_registry
                DROP CONSTRAINT IF EXISTS source_material_registry_status_check,
                ADD CONSTRAINT source_material_registry_status_check
                CHECK (status IN ('sensing', 'completed', 'recovered_partial', 'failed'))
            "#,
        )
        .execute(&slot_pool)
        .await?;
        let drift = reset::schema_mismatch_reason(&slot_pool).await?;
        assert!(
            drift.is_some(),
            "test fixture should create real schema drift"
        );
        slot_pool.close().await;

        let mut cached = CachedLazyPoolPreparation {
            expected_fingerprint: meta.fingerprint,
            expected_extensions: meta.extensions,
            slot_names: vec![db_name.clone()],
            deferred_stale_slots: vec![DeferredStaleSlot {
                slot_name: db_name.clone(),
                stale_reason: "actual schema drift".to_string(),
            }],
            next_deferred_retry_at_rfc3339: None,
            prepared_at_rfc3339: Timestamp::now().format_rfc3339(),
        };

        let summary = retry_deferred_stale_slots(&config.admin_url, &mut cached).await?;
        assert_eq!(summary.pruned_slots, vec![db_name.clone()]);
        assert_eq!(summary.eagerly_recreated_slots, vec![db_name.clone()]);
        assert!(
            cached.deferred_stale_slots.is_empty(),
            "successful deferred retry should clear stale-slot backlog"
        );

        let repaired_pool = sqlx::postgres::PgPoolOptions::new()
            .max_connections(2)
            .connect(&slot_url)
            .await?;
        let repaired_drift = reset::schema_mismatch_reason(&repaired_pool).await?;
        assert!(
            repaired_drift.is_none(),
            "deferred retry should restore schema-clean slot, got {repaired_drift:?}"
        );
        repaired_pool.close().await;

        drop_database_if_exists_admin(&mut admin_conn, &db_name).await?;
        wait_for_database_absence_admin(&mut admin_conn, &db_name).await?;
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
        assert!(
            loaded.is_none(),
            "corrupt preparation state should be ignored"
        );
        assert!(
            !state_path.exists(),
            "corrupt preparation state should be removed for a clean retry"
        );
        Ok(())
    }
}
