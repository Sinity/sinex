use super::*;

fn manifest_with_postgres_row_counts(row_counts: BTreeMap<String, i64>) -> SnapshotManifest {
    SnapshotManifest {
        snapshot_id: "01930000-0000-7000-8000-000000000000".to_string(),
        created_at: "2026-08-11T00:00:00Z".to_string(),
        sinex_version: "0.0.0-test".to_string(),
        git_sha: None,
        host: "test-host".to_string(),
        mode: "quiesce".to_string(),
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
/// This test proves the vacuous-pass consequence directly: an
/// empty-due-to-failure expected map compared against an empty observed map
/// (which could just as easily be a genuinely broken restore) reports
/// `Some(true)` -- a false "match" -- rather than the indeterminate/failed
/// signal the AC requires. Fixing this needs pg_row_counts' failure to be
/// distinguishable in the manifest (e.g. `Option<BTreeMap<..>>` with `None`
/// meaning "capture couldn't determine this"), not just a comparison tweak.
#[test]
fn postgres_row_counts_match_is_not_vacuously_true_when_expected_came_from_a_capture_failure() {
    // Simulates unwrap_or_default() swallowing a pg_row_counts() error during
    // capture: the manifest's only postgres component has an empty row_counts
    // map, indistinguishable from "genuinely zero durable tables".
    let manifest = manifest_with_postgres_row_counts(BTreeMap::new());
    let expected = expected_postgres_row_counts(&manifest);
    assert_eq!(
        expected,
        Some(BTreeMap::new()),
        "sanity: expected_postgres_row_counts should surface the empty map \
         as-is from the manifest (this is the data-loss point sinex-l9uq \
         item 1 describes -- the failure signal is already gone by here)"
    );

    // Restore observes an ALSO-empty row-count map -- which in a real
    // failure scenario could mean the restored database is genuinely
    // missing all its data, not that it correctly matches an empty capture.
    let observed_after_a_broken_restore: BTreeMap<String, i64> = BTreeMap::new();
    let postgres_row_counts_match = expected
        .map(|e| Some(&observed_after_a_broken_restore).is_some_and(|o| o == &e));

    assert_ne!(
        postgres_row_counts_match,
        Some(true),
        "an empty expected row-count map (which can only arise from a lost \
         capture-time failure signal, per sinex-l9uq item 1) must not be \
         reported as a genuine restore-integrity match against an \
         also-empty observed map -- there is no real evidence of a match \
         here, only two independent absences of data"
    );
}
