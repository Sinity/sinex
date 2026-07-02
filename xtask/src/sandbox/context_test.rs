// Inline because these helpers are private to sandbox context initialization/parsing.
use super::*;

struct EnvGuard {
    key: &'static str,
    original: Option<std::ffi::OsString>,
}

impl EnvGuard {
    fn set(key: &'static str, value: Option<std::ffi::OsString>) -> Self {
        let original = std::env::var_os(key);
        match value {
            Some(value) => unsafe { std::env::set_var(key, value) },
            None => unsafe { std::env::remove_var(key) },
        }
        Self { key, original }
    }
}

impl Drop for EnvGuard {
    fn drop(&mut self) {
        match self.original.take() {
            Some(value) => unsafe { std::env::set_var(self.key, value) },
            None => unsafe { std::env::remove_var(self.key) },
        }
    }
}

#[sinex_test]
async fn load_env_filter_defaults_when_rust_log_is_missing() -> ::xtask::sandbox::TestResult<()> {
    let _guard = EnvGuard::set("RUST_LOG", None);

    let filter = load_env_filter("info").expect("default filter should load");

    assert_eq!(filter.to_string(), "info");
    Ok(())
}

#[sinex_test]
async fn load_env_filter_rejects_invalid_rust_log_directive() -> ::xtask::sandbox::TestResult<()> {
    let _guard = EnvGuard::set("RUST_LOG", Some(std::ffi::OsString::from("[broken")));

    let error = load_env_filter("info").expect_err("invalid directive should fail");

    assert!(
        error
            .to_string()
            .contains("Invalid RUST_LOG directive `[broken`")
    );
    Ok(())
}

#[cfg(unix)]
#[sinex_test]
async fn load_env_filter_rejects_non_utf8_rust_log() -> ::xtask::sandbox::TestResult<()> {
    use std::os::unix::ffi::OsStringExt;

    let _guard = EnvGuard::set("RUST_LOG", Some(std::ffi::OsString::from_vec(vec![0xff])));

    let error = load_env_filter("info").expect_err("non-utf8 directive should fail");

    assert!(error.to_string().contains("RUST_LOG is not valid UTF-8"));
    Ok(())
}

#[sinex_test]
async fn background_invocation_id_defaults_to_none_when_missing() -> ::xtask::sandbox::TestResult<()>
{
    let _guard = EnvGuard::set("XTASK_BG_INVOCATION_ID", None);

    assert_eq!(
        background_invocation_id().expect("missing invocation ID should be allowed"),
        None
    );
    Ok(())
}

#[sinex_test]
async fn background_invocation_id_rejects_invalid_integer() -> ::xtask::sandbox::TestResult<()> {
    let _guard = EnvGuard::set(
        "XTASK_BG_INVOCATION_ID",
        Some(std::ffi::OsString::from("not-a-number")),
    );

    let error =
        background_invocation_id().expect_err("invalid invocation ID should not be ignored");

    assert!(
        error
            .to_string()
            .contains("Invalid XTASK_BG_INVOCATION_ID `not-a-number`")
    );
    Ok(())
}

#[cfg(unix)]
#[sinex_test]
async fn background_invocation_id_rejects_non_utf8_value() -> ::xtask::sandbox::TestResult<()> {
    use std::os::unix::ffi::OsStringExt;

    let _guard = EnvGuard::set(
        "XTASK_BG_INVOCATION_ID",
        Some(std::ffi::OsString::from_vec(vec![0xff])),
    );

    let error =
        background_invocation_id().expect_err("non-utf8 invocation ID should not be ignored");

    assert!(
        error
            .to_string()
            .contains("XTASK_BG_INVOCATION_ID is not valid UTF-8")
    );
    Ok(())
}

#[sinex_test]
async fn snapshot_background_registry_reports_busy_when_lock_is_held()
-> ::xtask::sandbox::TestResult<()> {
    let background = Arc::new(AsyncMutex::new(BackgroundRegistry::default()));
    let _guard = background.lock().await;

    let snapshot = snapshot_background_registry(background.as_ref());

    assert_eq!(
        snapshot,
        BackgroundSnapshot {
            pending: None,
            labels: Vec::new(),
            busy: true,
        }
    );
    Ok(())
}

#[sinex_test]
async fn snapshot_background_registry_reports_pending_labels_when_available()
-> ::xtask::sandbox::TestResult<()> {
    let background = Arc::new(AsyncMutex::new(BackgroundRegistry::default()));
    {
        let mut guard = background.lock().await;
        guard.add_hook("cleanup-hook", futures::future::ready(()).boxed());
    }

    let snapshot = snapshot_background_registry(background.as_ref());

    assert_eq!(
        snapshot,
        BackgroundSnapshot {
            pending: Some(1),
            labels: vec!["cleanup-hook".to_string()],
            busy: false,
        }
    );
    Ok(())
}

#[sinex_test]
async fn db_evidence_captures_source_material_registry(ctx: Sandbox) -> TestResult<()> {
    let material_id = ctx
        .create_source_material(Some("evidence-material"))
        .await?;

    let summary = ctx.capture_db_evidence("db").await?;

    assert!(summary.source_material_count >= 1);
    assert!(
        summary
            .recent_source_materials
            .iter()
            .any(|material| material.id == material_id.to_uuid().to_string())
    );
    assert!(
        ctx.evidence_snapshot().captures.iter().any(|capture| {
            capture.kind == EvidenceCollectorKind::Database
                && capture.status == EvidenceCollectorStatus::Captured
        }),
        "database capture should be attached to test evidence"
    );
    Ok(())
}

#[sinex_test]
async fn nats_evidence_captures_namespaced_streams(ctx: Sandbox) -> TestResult<()> {
    let ctx = ctx.with_nats().shared().await?;
    let _helper = crate::sandbox::nats::JetStreamTestHelper::new(&ctx, "evidence").await?;

    let summary = ctx.capture_nats_evidence("nats").await?;

    assert!(summary.enabled);
    assert!(
        summary
            .streams
            .iter()
            .any(|stream| stream.name.contains("SINEX_RAW_EVENTS_evidence")),
        "expected helper-created events stream in evidence: {:?}",
        summary.streams
    );
    assert!(
        ctx.evidence_snapshot().captures.iter().any(|capture| {
            capture.kind == EvidenceCollectorKind::Nats
                && capture.status == EvidenceCollectorStatus::Captured
        }),
        "NATS capture should be attached to test evidence"
    );
    Ok(())
}

#[sinex_test]
async fn material_directory_evidence_reports_wal_files(ctx: Sandbox) -> TestResult<()> {
    let temp = tempfile::tempdir()?;
    let wal_path = temp.path().join("state.wal");
    std::fs::write(&wal_path, b"wal-entry")?;
    std::fs::write(temp.path().join("payload.jsonl"), b"{}\n")?;

    let summary = ctx.capture_material_directory_evidence("spool", temp.path())?;

    assert!(summary.exists);
    assert_eq!(summary.file_count, 2);
    assert_eq!(summary.wal_files.len(), 1);
    assert_eq!(summary.wal_files[0].path, wal_path.display().to_string());
    assert!(
        ctx.evidence_snapshot().captures.iter().any(|capture| {
            capture.kind == EvidenceCollectorKind::MaterialSpool
                && capture.status == EvidenceCollectorStatus::Captured
        }),
        "material spool capture should be attached to test evidence"
    );
    Ok(())
}
