use super::*;
use crate::fmt::render_finite_envelope;
use sinex_primitives::views::{ReadinessCaveatId, VIEW_ENVELOPE_SCHEMA_VERSION};
use std::num::NonZeroUsize;
use xtask::sandbox::prelude::*;

fn sweep_summary(orphaned_entries: usize) -> BlobSweepSummary {
    BlobSweepSummary {
        content_store_path: "/tmp/sinex-cas".to_string(),
        mode: "dry-run",
        total_unused_entries: orphaned_entries,
        db_backed_entries: 0,
        orphaned_entries,
        dropped_entries: 0,
        staged_entries: 0,
        protected_recent_entries: 0,
        recheck_protected_entries: 0,
        quarantined_entries: 0,
        pending_delete_entries: 0,
        restored_entries: 0,
        orphaned_keys: Vec::new(),
    }
}

fn fsck_summary() -> BlobFsckSummary {
    BlobFsckSummary {
        content_store_path: "/tmp/sinex-cas".to_string(),
        mode: "dry-run",
        referenced: 0,
        orphaned: 2,
        corrupt: 1,
        malformed: 1,
        missing: 3,
        removed: 0,
        orphaned_bytes: 1024,
        protected_recent: 0,
        staged: 0,
        recheck_protected: 0,
        quarantined: 0,
        pending_deletes: 0,
        restored: 0,
        entries_scanned: 0,
        bytes_verified: 0,
        sample: None,
        incomplete: false,
        stop_reason: None,
        details: Vec::new(),
    }
}

fn migrate_summary(total_annex_blobs: usize, failed: usize) -> BlobMigrateSummary {
    BlobMigrateSummary {
        content_store_path: "/tmp/sinex-cas".to_string(),
        mode: "dry-run",
        from: "git-annex".to_string(),
        to: "local-cas".to_string(),
        total_annex_blobs,
        already_migrated: 0,
        migrated: 0,
        failed,
        migrated_keys: Vec::new(),
    }
}

#[sinex_test]
async fn blob_sweep_envelope_caveats_empty_and_orphaned_scans() -> TestResult<()> {
    let empty = blob_sweep_envelope(sweep_summary(0));
    assert_eq!(empty.source_surface, "sinexctl.ops.blob.sweep-orphans");
    assert_eq!(empty.caveats.len(), 1);
    assert_eq!(
        empty.caveats[0].id,
        ReadinessCaveatId::CoverageUnmeasurable.as_str()
    );
    assert_eq!(empty.query_echo.as_ref().unwrap()["mode"], "dry-run");

    let orphaned = blob_sweep_envelope(sweep_summary(2));
    assert!(
        orphaned
            .caveats
            .iter()
            .any(|caveat| caveat.id == ReadinessCaveatId::SourceAbsent.as_str())
    );
    Ok(())
}

#[sinex_test]
async fn blob_fsck_envelope_caveats_missing_corrupt_and_orphaned() -> TestResult<()> {
    let envelope = blob_fsck_envelope(fsck_summary());
    let caveat_ids = envelope
        .caveats
        .iter()
        .map(|caveat| caveat.id.as_str())
        .collect::<Vec<_>>();

    assert_eq!(envelope.source_surface, "sinexctl.ops.blob.fsck");
    assert!(caveat_ids.contains(&ReadinessCaveatId::SourceAbsent.as_str()));
    assert!(caveat_ids.contains(&ReadinessCaveatId::WindowPartial.as_str()));
    Ok(())
}

#[sinex_test]
async fn blob_fsck_sample_parses_rejects_zero_and_translates_budget() -> TestResult<()> {
    let full =
        BlobFsckCommand::try_parse_from(["sinexctl", "--content-store-path", "/tmp/sinex-cas"])?;
    assert_eq!(full.sample, None);
    assert_eq!(full.cas_fsck_options().max_entries, None);

    let sampled = BlobFsckCommand::try_parse_from([
        "sinexctl",
        "--content-store-path",
        "/tmp/sinex-cas",
        "--sample",
        "7",
    ])?;
    assert_eq!(sampled.sample.map(NonZeroUsize::get), Some(7));
    assert_eq!(sampled.cas_fsck_options().max_entries, Some(7));

    let zero = BlobFsckCommand::try_parse_from([
        "sinexctl",
        "--content-store-path",
        "/tmp/sinex-cas",
        "--sample",
        "0",
    ]);
    assert!(zero.is_err(), "--sample 0 must be rejected by clap");
    Ok(())
}

#[sinex_test]
async fn sampled_blob_fsck_envelope_reports_incomplete_entry_budget_and_all_findings()
-> TestResult<()> {
    let mut summary = fsck_summary();
    summary.sample = Some(1);
    summary.incomplete = true;
    summary.stop_reason = Some("EntryBudget".to_string());

    let envelope = blob_fsck_envelope(summary);
    let messages = envelope
        .caveats
        .iter()
        .map(|caveat| caveat.message.as_str())
        .collect::<Vec<_>>();

    assert!(messages.iter().any(|message| message.contains("orphaned")));
    assert!(messages.iter().any(|message| message.contains("missing")));
    assert!(messages.iter().any(|message| message.contains("corrupt")));
    assert!(messages.iter().any(|message| {
        message.contains("coverage is incomplete") && message.contains("EntryBudget")
    }));
    assert!(envelope.payload.incomplete);
    assert_eq!(envelope.payload.stop_reason.as_deref(), Some("EntryBudget"));
    assert_eq!(envelope.query_echo.as_ref().unwrap()["sample"], 1);
    Ok(())
}

#[sinex_test]
async fn blob_migrate_envelope_caveats_empty_and_failed_migration() -> TestResult<()> {
    let empty = blob_migrate_envelope(migrate_summary(0, 0));
    assert_eq!(
        empty.caveats[0].id,
        ReadinessCaveatId::CoverageUnmeasurable.as_str()
    );

    let failed = blob_migrate_envelope(migrate_summary(3, 1));
    assert!(
        failed
            .caveats
            .iter()
            .any(|caveat| caveat.id == ReadinessCaveatId::WindowPartial.as_str())
    );
    assert_eq!(failed.query_echo.as_ref().unwrap()["from"], "git-annex");
    Ok(())
}

#[sinex_test]
async fn blob_verify_integrity_envelope_caveats_empty_limited_missing_and_mismatch()
-> TestResult<()> {
    let report = BlobVerifyIntegrityReport {
        examined: 0,
        matched: 0,
        mismatched: 2,
        missing_offsets: 1,
        missing_blob: 1,
        missing_cas_file: 1,
        read_errors: 1,
        archived_mismatches: 0,
        mismatches: Vec::new(),
    };
    let envelope = blob_verify_integrity_envelope(report, 100, None);
    let caveat_ids = envelope
        .caveats
        .iter()
        .map(|caveat| caveat.id.as_str())
        .collect::<Vec<_>>();

    assert_eq!(
        envelope.source_surface,
        "sinexctl.ops.blob.verify-integrity"
    );
    assert!(caveat_ids.contains(&ReadinessCaveatId::CoverageUnmeasurable.as_str()));
    assert!(caveat_ids.contains(&ReadinessCaveatId::WindowPartial.as_str()));
    assert!(caveat_ids.contains(&ReadinessCaveatId::SourceAbsent.as_str()));
    assert_eq!(envelope.query_echo.as_ref().unwrap()["limit"], 100);
    Ok(())
}

#[sinex_test]
async fn blob_fsck_envelope_renders_finite_json() -> TestResult<()> {
    let envelope = blob_fsck_envelope(fsck_summary());
    let rendered = render_finite_envelope(&envelope, OutputFormat::Json)?
        .expect("json renders finite envelope");
    let parsed: serde_json::Value = serde_json::from_str(&rendered)?;

    assert_eq!(parsed["schema_version"], VIEW_ENVELOPE_SCHEMA_VERSION);
    assert_eq!(parsed["source_surface"], "sinexctl.ops.blob.fsck");
    assert_eq!(parsed["payload"]["missing"], 3);
    assert_eq!(parsed["query_echo"]["content_store_path"], "/tmp/sinex-cas");
    Ok(())
}
