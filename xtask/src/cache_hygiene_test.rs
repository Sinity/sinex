use super::*;
use xtask::sandbox::prelude::{EnvGuard, sinex_test};

#[sinex_test]
async fn prune_keeps_newest_n_per_crate() -> xtask::sandbox::TestResult<()> {
    let temp = tempfile::tempdir()?;
    let inc = temp.path().join("incremental");
    std::fs::create_dir(&inc)?;
    // Create 5 hashes for crate "foo"
    for (i, hash) in ["aaaa", "bbbb", "cccc", "dddd", "eeee"].iter().enumerate() {
        let d = inc.join(format!("foo-{hash}"));
        std::fs::create_dir(&d)?;
        // Different mtimes via touch via sleep is fragile; use filetime crate if needed.
        // For this test rely on creation order.
        std::fs::write(d.join("dummy"), vec![0u8; 100])?;
        std::thread::sleep(std::time::Duration::from_millis(20));
        let _ = i;
    }
    // Also create one hash for "bar" that should survive.
    std::fs::create_dir(inc.join("bar-zzzz"))?;
    std::fs::write(inc.join("bar-zzzz/dummy"), vec![0u8; 100])?;

    let (deleted, _bytes) = prune_incremental(&inc, 3)?;
    assert_eq!(deleted, 2, "expected to delete 2 oldest foo-* dirs");

    let remaining: Vec<_> = std::fs::read_dir(&inc)?
        .flatten()
        .map(|e| e.file_name().to_string_lossy().to_string())
        .collect();
    assert_eq!(remaining.len(), 4, "3 foo + 1 bar = 4 remaining");
    assert!(remaining.iter().any(|n| n == "bar-zzzz"));
    Ok(())
}

#[sinex_test]
async fn disk_usage_reads_valid_filesystem() -> xtask::sandbox::TestResult<()> {
    // /tmp should always exist
    let u = disk_usage(Path::new("/tmp"))?;
    assert!(u.total_gb > 0.0);
    assert!(u.percent_used >= 0.0 && u.percent_used <= 100.0);
    Ok(())
}

#[sinex_test]
async fn disk_refusal_requires_percent_and_low_absolute_free_space()
-> xtask::sandbox::TestResult<()> {
    let large_mount = DiskUsage {
        mount: "/realm".to_string(),
        total_gb: 4096.0,
        used_gb: 3738.0,
        free_gb: 358.0,
        percent_used: 91.3,
    };
    assert!(large_mount.should_auto_reclaim());
    assert!(
        !large_mount.refuse(),
        "large mounts with hundreds of GiB free should not require \
         SINEX_PREFLIGHT_SKIP_DISK_CHECK"
    );

    let nearly_full_mount = DiskUsage {
        mount: "/cache".to_string(),
        total_gb: 500.0,
        used_gb: 460.0,
        free_gb: 40.0,
        percent_used: 92.0,
    };
    assert!(nearly_full_mount.refuse());
    Ok(())
}

#[sinex_test]
async fn global_retention_deletes_stale_inactive_roots_over_budget()
-> xtask::sandbox::TestResult<()> {
    let temp = tempfile::tempdir()?;
    let user_root = temp.path().join("var/cache/sinex/sinity");
    let active = user_root.join("active");
    let stale = user_root.join("stale");
    std::fs::create_dir_all(active.join("target"))?;
    std::fs::create_dir_all(stale.join("target"))?;
    std::fs::write(active.join("target/artifact"), vec![0u8; 1024])?;
    std::fs::write(stale.join("target/artifact"), vec![0u8; 1024])?;

    let mut env = EnvGuard::with_keys(&[
        "SINEX_GLOBAL_CACHE_MAX_GB",
        "SINEX_GLOBAL_CACHE_KEEP_INACTIVE",
    ]);
    env.set("SINEX_GLOBAL_CACHE_MAX_GB", "0.000001");
    env.set("SINEX_GLOBAL_CACHE_KEEP_INACTIVE", "0");

    let report = enforce_global_retention(&user_root, Some(&active))?;

    assert!(active.exists(), "active cache root must be preserved");
    assert!(
        !stale.exists(),
        "inactive stale cache root should be removed"
    );
    assert_eq!(report.deleted_roots(), 1);
    assert!(report.deleted_bytes() >= 1024);
    Ok(())
}

#[sinex_test]
async fn global_retention_preserves_configured_recent_inactive_roots()
-> xtask::sandbox::TestResult<()> {
    let temp = tempfile::tempdir()?;
    let user_root = temp.path().join("var/cache/sinex/sinity");
    let active = user_root.join("active");
    let old = user_root.join("old");
    let recent = user_root.join("recent");
    for root in [&active, &old, &recent] {
        std::fs::create_dir_all(root.join("target"))?;
        std::fs::write(root.join("target/artifact"), vec![0u8; 1024])?;
        std::thread::sleep(std::time::Duration::from_millis(20));
    }

    let mut env = EnvGuard::with_keys(&[
        "SINEX_GLOBAL_CACHE_MAX_GB",
        "SINEX_GLOBAL_CACHE_KEEP_INACTIVE",
    ]);
    env.set("SINEX_GLOBAL_CACHE_MAX_GB", "0.000001");
    env.set("SINEX_GLOBAL_CACHE_KEEP_INACTIVE", "1");

    let report = enforce_global_retention(&user_root, Some(&active))?;

    assert!(active.exists(), "active cache root must be preserved");
    assert!(recent.exists(), "newest inactive cache root should be kept");
    assert!(!old.exists(), "older inactive cache root should be removed");
    assert_eq!(report.deleted_roots(), 1);
    Ok(())
}

#[sinex_test]
async fn target_path_maps_to_user_cache_root_and_active_root() -> xtask::sandbox::TestResult<()>
{
    let temp = tempfile::tempdir()?;
    let target = temp
        .path()
        .join("var/cache/sinex/sinity/active/target/debug");
    std::fs::create_dir_all(&target)?;

    let (user_root, active_root) =
        sinex_user_cache_root_for_target(&target).expect("target should map to cache roots");

    assert_eq!(user_root, temp.path().join("var/cache/sinex/sinity"));
    assert_eq!(
        active_root,
        temp.path().join("var/cache/sinex/sinity/active")
    );
    Ok(())
}

#[sinex_test]
async fn non_var_target_does_not_map_to_cache_root() -> xtask::sandbox::TestResult<()> {
    let temp = tempfile::tempdir()?;
    let target = temp.path().join(".sinex/cache/target");
    std::fs::create_dir_all(&target)?;

    assert!(
        sinex_user_cache_root_for_target(&target).is_none(),
        "checkout-local target dirs are not themselves Sinnix cache roots"
    );
    Ok(())
}
