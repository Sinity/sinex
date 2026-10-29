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

#[devkit::test]
async fn binding_reads_store_empty_unchanged_and_failed_evidence_with_bytes(
    db: devkit::pg::TestDatabase,
) {
    let root = tempfile::tempdir().unwrap();
    let input = root.path().join("history");
    std::fs::write(&input, b"").unwrap();
    let url = db.url(None);
    let mut context = crate::reader_context::ReaderContext::open(
        &url,
        &root.path().join("store"),
        sinex_model::time_settings::TimeSettings::new("Europe/Warsaw", 4, 0)
            .expect("fixture day setting"),
    )
    .await
    .unwrap();
    let format = &sinex_sources::sources::zsh::FORMAT;
    let binding = Binding::of(
        &sinex_sources::sources::zsh::SPEC,
        &input,
        Settings::new("synthetic"),
    );
    let run = sinex_store::runs::start(
        &mut context.read_conn,
        &sinex_store::runs::Manifest {
            name: "synthetic reads",
            kind: "source",
            version: "1",
            build: "synthetic",
            emits: &["command.executed", "source.coverage"],
        },
        "synthetic",
    )
    .await
    .unwrap();
    let mut readers = vec![crate::binding_reader::BindingReader::new(format, binding).unwrap()];
    let (_stop, stop) = tokio::sync::watch::channel(false);
    for failed in [false, false, true] {
        if failed {
            std::fs::remove_file(&input).unwrap();
        }
        let results = crate::binding_reader::read(
            &mut context.read_conn,
            &context.blobs,
            &context.writer,
            &crate::material_access::Sink(context.blobs.clone()),
            &mut readers,
            stop.clone(),
            Access::default(),
            &mut |_, _| {},
        )
        .await;
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].1.is_err(), failed);
    }
    let evidence: Vec<(serde_json::Value, Option<uuid::Uuid>)> = sqlx::query_as(
        "SELECT payload, material_id FROM core.events WHERE event_type='source.coverage' ORDER BY commit_seq"
    ).fetch_all(&mut context.read_conn).await.unwrap();
    assert_eq!(evidence.len(), 3);
    assert_eq!(evidence[0].0["state"], "recorded_unknown_coverage");
    assert_eq!(evidence[1].0["state"], "recorded_unknown_coverage");
    assert_eq!(evidence[2].0["state"], "missing");
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
        assert_eq!(payload["observed_run"], serde_json::to_value(run).unwrap());
        let identity: (String, String) = sqlx::query_as("SELECT m.name,m.kind FROM core.events e JOIN core.runs r ON r.id=e.run_id JOIN core.manifests m ON m.id=r.manifest_id WHERE e.material_id=$1")
            .bind(material.unwrap()).fetch_one(&mut context.read_conn).await.unwrap();
        assert_eq!(identity, (coverage::OBSERVATION.name.into(), "source".into()));
    }
    context.close().await.unwrap();
}

#[devkit::test]
async fn invalid_saved_binding_settings_do_not_publish_open_coverage(db: devkit::pg::TestDatabase) {
    let root = tempfile::tempdir().unwrap();
    let url = db.url(None);
    let mut context = crate::reader_context::ReaderContext::open(&url, &root.path().join("store"),
        sinex_model::time_settings::TimeSettings::new("Europe/Warsaw",4,0).unwrap()).await.unwrap();
    let binding = binding();
    bindings::save(&mut context.read_conn, &bindings::Stored {
        binding:binding.name.clone(),source:binding.source.clone(),input:"synthetic".into(),
        settings:serde_json::json!({"host":"synthetic","class":false}),
    }).await.unwrap();
    let observation = outcome(&binding,Timestamp::now(),&Ok(Totals::default()));
    let error = record(&mut context.read_conn,&context.blobs,&context.writer,Access::default(),observation).await.unwrap_err();
    assert!(error.contains("invalid settings"));
    let records = sinex_store::live::of_types(&mut context.read_conn,&["source.coverage".into()]).await.unwrap();
    assert!(records.is_empty(),"invalid source privacy settings must refuse publication");
    context.close().await.unwrap();
}
