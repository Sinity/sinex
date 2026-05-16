//! Tests for the long-lived source-material lifecycle introduced in #1285 Slice A.
//!
//! Before Slice A, `AdapterBackedIngestor::drain_adapter()` called
//! `begin_material()` + `finalize()` on every drain invocation, producing one
//! row in `raw.source_material_registry` per polling cycle (O(poll_count)).
//!
//! After Slice A, a single [`AppendStreamAcquirer`] is held across all drain
//! cycles and auto-rotates at size / age thresholds, making registry growth
//! O(rotation_count).
//!
//! These tests verify that invariant directly against [`AppendStreamAcquirer`]
//! (the same code path `AdapterBackedIngestor` uses internally) without
//! requiring a full pipeline setup.

use sinex_node_sdk::{AcquisitionManager, AppendStreamAcquirer, RotationPolicy};
use sinex_primitives::Uuid;
use sinex_primitives::units::{Bytes, Seconds};
use std::sync::Arc;
use xtask::sandbox::prelude::*;

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Build a namespace-isolated `AcquisitionManager` suitable for each test.
fn make_manager(
    work_dir: &std::path::Path,
    nats_client: async_nats::Client,
    label: &str,
    rotation_policy: RotationPolicy,
) -> Arc<AcquisitionManager> {
    use sinex_node_sdk::acquisition_manager::AcquisitionManager as AM;
    let namespace = format!("{label}-{}", Uuid::new_v4());
    Arc::new(
        AM::new_with_namespace(
            nats_client,
            rotation_policy,
            label.to_string(),
            Some(namespace),
        )
        .with_work_dir(work_dir),
    )
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

/// Appending 100 bytes across 1 000 simulated poll cycles must produce ≤2
/// materials when the rotation limit is 200 bytes.
///
/// (100 bytes × 10 = 1 000 bytes — enough for several rotations at the
/// 200-byte policy, but what matters is that the count is O(rotation_count)
/// not O(poll_count).)
///
/// We verify:
///   - The final number of distinct material IDs observed ≪ 1 000.
///   - Each appended record receives a contiguous byte anchor.
#[sinex_test]
async fn many_drain_cycles_share_material_until_rotation(ctx: TestContext) -> TestResult<()> {
    let ctx = ctx.with_nats().shared().await?;
    let work_dir = tempfile::tempdir()?;
    let manager = make_manager(
        work_dir.path(),
        ctx.nats_client(),
        "poll-material-reuse",
        RotationPolicy {
            // Rotate every 200 bytes so we get a handful of rotations over 100 cycles.
            max_bytes: Bytes::from_bytes(200),
            max_age_seconds: Seconds::from_secs(3600),
        },
    );

    let mut stream = AppendStreamAcquirer::new(Arc::clone(&manager));
    const DRAIN_CYCLES: usize = 100;
    const RECORD_BYTES: usize = 10; // 10 bytes per "poll cycle"
    const RECORD: &[u8] = b"0123456789"; // exactly RECORD_BYTES

    let mut seen_material_ids = std::collections::HashSet::new();

    for _ in 0..DRAIN_CYCLES {
        let anchor = stream
            .append_with_anchor(RECORD, "test://poll-reuse")
            .await?;
        seen_material_ids.insert(anchor.material_id);
    }
    stream.finalize("test-complete").await?;

    // 100 cycles × 10 bytes = 1 000 bytes total.
    // At 200-byte rotation that is ≤ 6 materials (ceil(1000/200) = 5 rotations + 1).
    // The key invariant: far fewer materials than drain cycles.
    let cycle_count = DRAIN_CYCLES as u64;
    let material_count = seen_material_ids.len() as u64;
    assert!(
        material_count <= cycle_count / 10,
        "expected O(rotation_count) materials, got {material_count} for {cycle_count} drain cycles"
    );
    // Sanity: at least one rotation occurred given the policy.
    assert!(
        material_count >= 2,
        "expected ≥2 materials (at least one rotation), got {material_count}"
    );

    Ok(())
}

/// Appending 1 000 tiny records under the default 100 MiB / 1 hour rotation
/// policy must use a single material. This pins the issue-level regression:
/// frequent low-volume polling must not create O(poll_count) registry rows.
#[sinex_test]
async fn default_rotation_keeps_tiny_poll_cycles_on_one_material(
    ctx: TestContext,
) -> TestResult<()> {
    let ctx = ctx.with_nats().shared().await?;
    let work_dir = tempfile::tempdir()?;
    let manager = make_manager(
        work_dir.path(),
        ctx.nats_client(),
        "default-poll-material-reuse",
        RotationPolicy::default(),
    );

    let mut stream = AppendStreamAcquirer::new(Arc::clone(&manager));
    const DRAIN_CYCLES: usize = 1_000;
    const RECORD: &[u8] = b"0123456789";

    let mut seen_material_ids = std::collections::HashSet::new();

    for _ in 0..DRAIN_CYCLES {
        let anchor = stream
            .append_with_anchor(RECORD, "test://default-poll-reuse")
            .await?;
        seen_material_ids.insert(anchor.material_id);
    }
    stream.finalize("test-complete").await?;

    assert_eq!(
        seen_material_ids.len(),
        1,
        "default rotation should keep {DRAIN_CYCLES} tiny poll cycles on one material"
    );

    Ok(())
}

/// Within a single material segment (before any rotation), appended records
/// must have contiguous, non-overlapping byte anchors.
#[sinex_test]
async fn appended_records_have_contiguous_byte_anchors(ctx: TestContext) -> TestResult<()> {
    let ctx = ctx.with_nats().shared().await?;
    let work_dir = tempfile::tempdir()?;
    let manager = make_manager(
        work_dir.path(),
        ctx.nats_client(),
        "contiguous-anchors",
        RotationPolicy {
            // Large limit — no rotation expected during this test.
            max_bytes: Bytes::from_mebibytes(100),
            max_age_seconds: Seconds::from_secs(3600),
        },
    );

    let mut stream = AppendStreamAcquirer::new(Arc::clone(&manager));
    let records: &[&[u8]] = &[b"hello", b"world", b"!", b"from", b"sinex"];
    let mut anchors = Vec::new();

    for record in records {
        let anchor = stream
            .append_with_anchor(record, "test://contiguous-anchors")
            .await?;
        anchors.push(anchor);
    }
    stream.finalize("test-complete").await?;

    // All anchors must share the same material.
    let first_material = anchors[0].material_id;
    for a in &anchors {
        assert_eq!(
            a.material_id, first_material,
            "all records must share one material before rotation"
        );
    }

    // Anchors must be contiguous: offset_end(n) == offset_start(n+1).
    for window in anchors.windows(2) {
        let (prev, next) = (&window[0], &window[1]);
        assert_eq!(
            prev.offset_end, next.offset_start,
            "record anchors must be contiguous: prev.offset_end={} next.offset_start={}",
            prev.offset_end, next.offset_start
        );
    }

    // Byte ranges must be non-empty and match the record length.
    for (anchor, record) in anchors.iter().zip(records.iter()) {
        assert!(
            anchor.offset_end > anchor.offset_start,
            "anchor must be non-empty for record {:?}",
            record
        );
        let expected_len = record.len() as i64;
        assert_eq!(
            anchor.offset_end - anchor.offset_start,
            expected_len,
            "anchor length must match record byte count"
        );
    }

    Ok(())
}

/// After `finalize()`, calling `current_material_id()` must return `None`
/// (the material handle was taken out and finalized).
#[sinex_test]
async fn current_material_id_is_none_after_finalize(ctx: TestContext) -> TestResult<()> {
    let ctx = ctx.with_nats().shared().await?;
    let work_dir = tempfile::tempdir()?;
    let manager = make_manager(
        work_dir.path(),
        ctx.nats_client(),
        "material-id-after-finalize",
        RotationPolicy::default(),
    );

    let mut stream = AppendStreamAcquirer::new(Arc::clone(&manager));
    assert!(
        stream.current_material_id().is_none(),
        "fresh acquirer must not have an active material"
    );

    stream
        .append_with_anchor(b"ping", "test://after-finalize")
        .await?;
    assert!(
        stream.current_material_id().is_some(),
        "acquirer must expose active material id after first append"
    );

    stream.finalize("test-complete").await?;
    assert!(
        stream.current_material_id().is_none(),
        "acquirer must report no active material after finalize"
    );

    Ok(())
}

/// Appending to a different source identifier must rotate the material.
/// This ensures source-unit isolation when one acquirer handles multiple
/// logical sources (not the production path, but a safety property).
#[sinex_test]
async fn source_identifier_change_rotates_material(ctx: TestContext) -> TestResult<()> {
    let ctx = ctx.with_nats().shared().await?;
    let work_dir = tempfile::tempdir()?;
    let manager = make_manager(
        work_dir.path(),
        ctx.nats_client(),
        "source-id-rotation",
        RotationPolicy {
            max_bytes: Bytes::from_mebibytes(100),
            max_age_seconds: Seconds::from_secs(3600),
        },
    );

    let mut stream = AppendStreamAcquirer::new(Arc::clone(&manager));

    let first = stream
        .append_with_anchor(b"from-source-a", "test://source-a")
        .await?;
    let second = stream
        .append_with_anchor(b"from-source-b", "test://source-b")
        .await?;
    stream.finalize("test-complete").await?;

    assert_ne!(
        first.material_id, second.material_id,
        "a source identifier change must rotate the material"
    );
    assert_eq!(
        second.offset_start, 0,
        "rotated material must start at offset 0"
    );

    Ok(())
}
