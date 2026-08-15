use super::*;

fn manifest_with_postgres_row_counts(
    row_counts: Option<BTreeMap<String, i64>>,
) -> SnapshotManifest {
    SnapshotManifest {
        snapshot_id: "01930000-0000-7000-8000-000000000000".to_string(),
        created_at: "2026-08-11T00:00:00Z".to_string(),
        sinex_version: "0.0.0-test".to_string(),
        git_sha: None,
        host: "test-host".to_string(),
        mode: "quiesce".to_string(),
        quiesce_receipt: None,
        source_ids: Vec::new(),
        components: vec![ComponentRecord {
            name: "postgres".to_string(),
            path: "postgres/sinex_prod.dump".to_string(),
            bytes: 1,
            blake3: "deadbeef".to_string(),
            extras: Some(ComponentExtras::Postgres(PostgresExtras { row_counts })),
        }],
        totals: Totals {
            uncompressed_bytes: 1,
            archive_bytes: None,
        },
    }
}

/// sinex-l9uq item 1: `capture_postgres_component`'s
/// `exec::pg_row_counts(database_url).unwrap_or_default()` silently turns any
/// row-count query failure (permissions, transient DB error) into an empty
/// `{}` map -- the manifest then carries no authoritative expected row
/// counts, but `expected_postgres_row_counts` and the restore-side match
/// formula (`expected.map(|e| observed.is_some_and(|o| o == &e))`) can't
/// distinguish "capture legitimately saw zero durable tables" from "capture
/// failed and we lost the signal entirely" -- both collapse to `Some({})`,
/// and an empty-vs-empty comparison reports a real match. This means restore
/// verification can pass with ZERO actual integrity signal when the observed
/// side also happens to be empty (e.g. a botched restore into an empty DB).
///
/// This test proves the fail-closed consequence directly: missing expected
/// evidence compared against an empty observed map must report a failed match,
/// not a genuine restore-integrity match.
#[test]
fn postgres_row_counts_match_is_not_vacuously_true_when_expected_came_from_a_capture_failure() {
    // Simulates a manifest produced without authoritative row-count evidence
    // after a capture-time query failure.
    let manifest = manifest_with_postgres_row_counts(None);
    let expected = expected_postgres_row_counts(&manifest);
    assert_eq!(
        expected, None,
        "sanity: expected_postgres_row_counts should surface the empty map \
         as-is from the manifest (this is the data-loss point sinex-l9uq \
         item 1 describes -- the failure signal is already gone by here)"
    );

    // Restore observes an ALSO-empty row-count map -- which in a real
    // failure scenario could mean the restored database is genuinely
    // missing all its data, not that it correctly matches an empty capture.
    let observed_after_a_broken_restore: BTreeMap<String, i64> = BTreeMap::new();
    let postgres_component_is_non_empty = manifest
        .components
        .iter()
        .any(|component| component.name == "postgres" && component.bytes > 0);
    let postgres_row_counts_match = expected
        .map(|e| Some(&observed_after_a_broken_restore).is_some_and(|o| o == &e))
        .or_else(|| postgres_component_is_non_empty.then_some(false));

    assert_eq!(
        postgres_row_counts_match,
        Some(false),
        "missing expected row-count evidence must fail closed rather than \
         report a genuine restore-integrity match"
    );

    let failed = restore_failed_checks(&RestoreFailedCheckInput {
        source_ids_match: true,
        component_blake3_matches: &BTreeMap::new(),
        postgres_row_counts_match,
        nats_member_paths_match: None,
        cas_blob_count_matches: None,
        private_mode_state_matches_manifest: true,
    });
    assert_eq!(failed, ["postgres_row_counts_match"]);
}

#[test]
fn component_hash_policy_is_symmetric_for_nats_summary_metadata() {
    let temp = tempfile::tempdir().expect("create temporary hash fixture");
    let nats = temp.path().join("nats");
    std::fs::create_dir_all(nats.join("jetstream")).expect("create temporary NATS fixture");
    std::fs::write(nats.join("jetstream/state"), b"state").expect("write temporary NATS state");

    let before_summary = component_blake3("nats", &nats).expect("hash NATS state");
    std::fs::write(nats.join("streams.summary.json"), b"summary-v1").expect("write NATS summary");
    let after_summary = component_blake3("nats", &nats).expect("hash NATS state with summary");

    assert_eq!(
        before_summary, after_summary,
        "NATS summary metadata must be excluded from the capture and restore hash"
    );
}

#[test]
fn live_entry_copy_tolerates_a_vanished_top_level_file() {
    let temp = tempfile::tempdir().expect("create temporary copy fixture");
    let source = temp.path().join("vanished-file");
    let destination = temp.path().join("destination");

    let result = super::exec::cp_entry_live(&source, &destination);

    assert!(
        result.is_ok(),
        "vanished live source must be skipped: {result:?}"
    );
    assert!(!destination.exists());
}
