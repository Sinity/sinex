use super::*;
use sinex_source_core::Settings;

fn binding() -> Binding {
    Binding::of(
        &sinex_sources::sources::zsh::SPEC,
        std::path::Path::new("/synthetic/history"),
        Settings::new("synthetic"),
    )
}

#[devkit::test]
fn source_read_outcomes_distinguish_empty_partial_and_failed_reads() {
    let at = Timestamp::from_unix_secs(10).unwrap();
    let empty = outcome(&binding(), at, &Ok(Totals::default()));
    assert_eq!(empty.state, CoverageState::RecordedUnknownCoverage);
    assert_eq!(empty.completeness, CoverageCompleteness::Unknown);
    assert_eq!(empty.events, 0);
    assert!(empty.reason.unwrap().contains("successfully"));
    let mut totals = Totals::default();
    totals.failures = 2;
    totals.events_offered = 3;
    let partial = outcome(&binding(), at, &Ok(totals));
    assert_eq!(partial.state, CoverageState::Partial);
    assert_eq!(partial.events, 3);
    assert_eq!(partial.failures, 2);
    let failed = outcome(&binding(), at, &Err("input unavailable".into()));
    assert_eq!(failed.state, CoverageState::Missing);
    assert_eq!(failed.reason.as_deref(), Some("input unavailable"));
}

async fn open(
    db: &devkit::pg::TestDatabase,
    root: &std::path::Path,
) -> crate::reader_context::ReaderContext {
    crate::reader_context::ReaderContext::open(
        &db.url(None),
        &root.join("store"),
        sinex_model::time_settings::TimeSettings::new("Europe/Warsaw", 4, 0)
            .expect("fixture day setting"),
    )
    .await
    .unwrap()
}

fn zsh_reader(input: &std::path::Path) -> crate::binding_reader::BindingReader {
    let binding = Binding::of(
        &sinex_sources::sources::zsh::SPEC,
        input,
        Settings::new("synthetic"),
    );
    crate::binding_reader::BindingReader::new(&sinex_sources::sources::zsh::FORMAT, binding)
        .unwrap()
}

/// One round of `readers`, returning whether each binding's read failed.
async fn round(
    context: &mut crate::reader_context::ReaderContext,
    readers: &mut [crate::binding_reader::BindingReader],
    rounds: &mut crate::binding_reader::Rounds,
) -> Vec<bool> {
    let (_stop, stop) = tokio::sync::watch::channel(false);
    crate::binding_reader::read(
        &mut context.read_conn,
        &context.blobs,
        &context.writer,
        &crate::material_access::Sink(context.blobs.clone()),
        readers,
        rounds,
        stop,
        Access::default(),
        &mut |_, _| {},
    )
    .await
    .into_iter()
    .map(|(_, result)| result.is_err())
    .collect()
}

async fn count(context: &mut crate::reader_context::ReaderContext, sql: &str) -> i64 {
    sqlx::query_scalar(sql)
        .fetch_one(&mut context.read_conn)
        .await
        .unwrap()
}

const COVERAGE_READINGS: &str =
    "SELECT count(*) FROM core.events WHERE event_type='source.coverage'";
const LIVE_COVERAGE: &str = "SELECT count(*) FROM core.events e WHERE e.event_type='source.coverage' AND NOT EXISTS (SELECT 1 FROM audit.archived a WHERE a.event_id=e.id)";
const OBSERVATION_MATERIALS: &str = "SELECT count(*) FROM raw.materials m JOIN raw.inputs i ON i.id=m.input_id WHERE i.kind='source-observation'";
const RUNS: &str = "SELECT count(*) FROM core.runs WHERE name IN ('terminal.zsh-history','source.observation')";
const OPEN_RUNS: &str = "SELECT count(*) FROM core.runs WHERE name IN ('terminal.zsh-history','source.observation') AND ended_at IS NULL";

/// law: coverage-on-change
#[devkit::test]
async fn coverage_on_change(db: devkit::pg::TestDatabase) {
    let root = tempfile::tempdir().unwrap();
    let input = root.path().join("history");
    std::fs::write(&input, b"").unwrap();
    let mut context = open(&db, root.path()).await;
    let mut readers = vec![zsh_reader(&input)];
    let mut rounds = crate::binding_reader::Rounds::default();
    for _ in 0..100 {
        assert_eq!(round(&mut context, &mut readers, &mut rounds).await, [false]);
    }
    assert_eq!(count(&mut context, COVERAGE_READINGS).await, 1);
    assert_eq!(count(&mut context, OBSERVATION_MATERIALS).await, 1);
    assert_eq!(
        count(&mut context, RUNS).await,
        2,
        "one source run and one coverage run over every round"
    );
    rounds.end(&mut context.read_conn).await.unwrap();
    assert_eq!(count(&mut context, OPEN_RUNS).await, 0);

    // A restarted reader continues from the stored reading.
    let mut rounds = crate::binding_reader::Rounds::resume(&mut context.read_conn, "synthetic")
        .await
        .unwrap();
    let mut readers = vec![zsh_reader(&input)];
    assert_eq!(round(&mut context, &mut readers, &mut rounds).await, [false]);
    assert_eq!(count(&mut context, COVERAGE_READINGS).await, 1);

    // A failure replaces the current reading; the earlier one stays in history.
    std::fs::remove_file(&input).unwrap();
    for _ in 0..3 {
        assert_eq!(round(&mut context, &mut readers, &mut rounds).await, [true]);
    }
    assert_eq!(count(&mut context, COVERAGE_READINGS).await, 2);
    assert_eq!(count(&mut context, LIVE_COVERAGE).await, 1);
    let live = sinex_store::live::of_types(&mut context.read_conn, &["source.coverage".into()])
        .await
        .unwrap();
    assert_eq!(live[0].payload.as_ref().unwrap()["state"], "missing");
    rounds.end(&mut context.read_conn).await.unwrap();
    context.close().await.unwrap();
}

#[devkit::test]
async fn binding_reads_store_their_evidence_with_bytes_under_one_run(
    db: devkit::pg::TestDatabase,
) {
    let root = tempfile::tempdir().unwrap();
    let input = root.path().join("history");
    std::fs::write(&input, b"").unwrap();
    let mut context = open(&db, root.path()).await;
    let mut readers = vec![zsh_reader(&input)];
    let mut rounds = crate::binding_reader::Rounds::default();
    for failed in [false, false, true] {
        if failed {
            std::fs::remove_file(&input).unwrap();
        }
        assert_eq!(
            round(&mut context, &mut readers, &mut rounds).await,
            [failed]
        );
    }
    rounds.end(&mut context.read_conn).await.unwrap();
    let evidence: Vec<(serde_json::Value, Option<uuid::Uuid>)> = sqlx::query_as(
        "SELECT payload, material_id FROM core.events WHERE event_type='source.coverage' ORDER BY commit_seq"
    ).fetch_all(&mut context.read_conn).await.unwrap();
    assert_eq!(evidence.len(), 2, "the unchanged second read writes nothing");
    assert_eq!(evidence[0].0["state"], "recorded_unknown_coverage");
    assert_eq!(evidence[1].0["state"], "missing");
    let mut observed_runs = std::collections::BTreeSet::new();
    for (payload, material) in evidence {
        let blob: String = sqlx::query_scalar("SELECT blob FROM raw.materials WHERE id=$1")
            .bind(material.expect("coverage has retained material"))
            .fetch_one(&mut context.read_conn)
            .await
            .unwrap();
        let bytes = context.blobs.read(&blob).unwrap();
        let retained: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(retained, payload);
        assert_eq!(payload["observed_source"], "terminal.zsh-history");
        let observed_run: uuid::Uuid =
            serde_json::from_value(payload["observed_run"].clone()).unwrap();
        observed_runs.insert(observed_run);
        let native: (String, String, String, String, String) = sqlx::query_as(
            "SELECT m.name,m.kind,m.version,m.build,r.host FROM core.runs r JOIN core.manifests m ON m.id=r.manifest_id WHERE r.id=$1"
        ).bind(observed_run).fetch_one(&mut context.read_conn).await.unwrap();
        assert_eq!(native.0, "terminal.zsh-history");
        assert_eq!(native.1, "source");
        assert_eq!(
            native.2,
            sinex_sources::sources::zsh::SPEC.parser_version.to_string()
        );
        assert!(
            !native.3.is_empty(),
            "the actual native run records its build"
        );
        assert_eq!(native.4, "synthetic");
        let identity: (String, String) = sqlx::query_as("SELECT m.name,m.kind FROM core.events e JOIN core.runs r ON r.id=e.run_id JOIN core.manifests m ON m.id=r.manifest_id WHERE e.material_id=$1")
            .bind(material.unwrap()).fetch_one(&mut context.read_conn).await.unwrap();
        assert_eq!(
            identity,
            (coverage::OBSERVATION.name.into(), "source".into())
        );
    }
    assert_eq!(
        observed_runs.len(),
        1,
        "every read of a binding belongs to the one source run of its reader"
    );
    context.close().await.unwrap();
}

#[devkit::test]
async fn invalid_saved_binding_settings_do_not_publish_open_coverage(db: devkit::pg::TestDatabase) {
    let root = tempfile::tempdir().unwrap();
    let url = db.url(None);
    let mut context = crate::reader_context::ReaderContext::open(
        &url,
        &root.path().join("store"),
        sinex_model::time_settings::TimeSettings::new("Europe/Warsaw", 4, 0).unwrap(),
    )
    .await
    .unwrap();
    let binding = binding();
    bindings::save(
        &mut context.read_conn,
        &bindings::Stored {
            binding: binding.name.clone(),
            source: binding.source.clone(),
            input: "synthetic".into(),
            settings: serde_json::json!({"host":"synthetic","class":false}),
        },
    )
    .await
    .unwrap();
    let observation = outcome(&binding, Timestamp::now(), &Ok(Totals::default()));
    let error = record(
        &mut context.read_conn,
        &context.blobs,
        &context.writer,
        Access::default(),
        &mut std::collections::BTreeMap::new(),
        observation,
    )
    .await
    .unwrap_err();
    assert!(error.contains("invalid settings"));
    let records = sinex_store::live::of_types(&mut context.read_conn, &["source.coverage".into()])
        .await
        .unwrap();
    assert!(
        records.is_empty(),
        "invalid source privacy settings must refuse publication"
    );
    context.close().await.unwrap();
}
