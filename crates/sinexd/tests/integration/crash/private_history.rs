use super::*;

#[devkit::test]
#[ignore = "requires an explicitly supplied private history snapshot and expected answers"]
async fn an_external_history_survives_a_stopped_admission_and_remains_queryable() {
    let directory = PathBuf::from(
        std::env::var_os("SINEX_TEST_HISTORY_DIR")
            .expect("set SINEX_TEST_HISTORY_DIR to the private fixture directory"),
    );
    let expected: serde_json::Value =
        serde_json::from_slice(&std::fs::read(directory.join("expected.json")).unwrap()).unwrap();
    let expected: BTreeMap<String, String> =
        serde_json::from_value(expected["commands"].clone()).unwrap();
    assert!(
        !expected.is_empty(),
        "the private fixture has no present commands"
    );

    let inputs = Inputs::new(0, 0);
    std::fs::copy(directory.join("history.db"), &inputs.atuin).unwrap();
    let target = Target::new(&inputs);
    kill_at(
        &target,
        &mut target.backfill(&inputs),
        None,
        &lock("core.events"),
    )
    .await;
    consistent(&target).await;
    finish(&mut target.backfill(&inputs));
    consistent(&target).await;

    let mut connection = target.connect().await;
    let rows: Vec<(String, String)> = sqlx::query_as(
        "SELECT e.payload->>'native_id', e.payload->>'command' FROM core.events e
         WHERE e.source = 'terminal.atuin-history' AND e.record_state = 'present'
           AND EXISTS (SELECT 1 FROM core.live_equivalence l WHERE l.event_id = e.id)
         ORDER BY e.payload->>'native_id'",
    )
    .fetch_all(&mut connection)
    .await
    .unwrap();
    assert_eq!(rows.len(), expected.len(), "the present row counts differ");
    let actual: BTreeMap<String, String> = rows.into_iter().collect();
    assert!(
        actual == expected,
        "captured commands differ from the independent answers"
    );
    let stored = scalar(&mut connection, "SELECT count(*) FROM core.events").await;
    assert_eq!(
        scalar(&mut connection, "SELECT count(*) FROM raw.failures").await,
        0,
        "a private source record failed"
    );

    let mut query = target.sinexd(&["admin", "query"]);
    query.args([
        "--page",
        "10",
        "events where source = terminal.atuin-history and state = present fields event_type",
    ]);
    let answer: serde_json::Value = serde_json::from_str(&finish(&mut query)).unwrap();
    assert_eq!(answer["total"].as_u64(), Some(expected.len() as u64));
    let rows = answer["rows"].as_array().unwrap();
    assert!(!rows.is_empty());
    assert!(
        rows.iter()
            .all(|row| row["event_type"] == "command.executed")
    );

    let native_id = expected.first_key_value().unwrap().0;
    let question = format!(
        "events where source = terminal.atuin-history and state = present and payload.native_id = {} fields payload.native_id",
        serde_json::to_string(native_id).unwrap(),
    );
    let mut query = target.sinexd(&["admin", "query"]);
    query.arg(question);
    let answer: serde_json::Value = serde_json::from_str(&finish(&mut query)).unwrap();
    assert_eq!(answer["total"].as_u64(), Some(1));
    assert!(
        answer["rows"][0]["payload.native_id"].as_str() == Some(native_id.as_str()),
        "the public query did not return the expected record"
    );

    finish(&mut target.backfill(&inputs));
    consistent(&target).await;
    assert_eq!(
        scalar(&mut connection, "SELECT count(*) FROM core.events").await,
        stored,
        "reading the unchanged copy again stores no new events"
    );
}
