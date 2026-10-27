use super::*;

#[devkit::test]
async fn moment_marks_are_authored_material_records_recalled_by_number_and_visibility(
    db: devkit::pg::TestDatabase,
) {
    let w = world(db).await;
    admit(
        &w,
        &["synthetic marked command", "synthetic next command"],
        0,
    )
    .await;
    let (_, denied) = call(&w, Some(READ), json!({"method":"mark","text":"synthetic"})).await;
    assert_eq!(denied["outcome"], "refused");
    let (status, _) = call(&w, Some(AGENT), json!({"method":"mark","text":"synthetic"})).await;
    assert_eq!(status, StatusCode::FORBIDDEN);
    let mut citations = Vec::new();
    for (at, note) in [
        ("2026-09-14T08:00:00Z", "synthetic first mark"),
        ("2026-09-14T08:01:00Z", "synthetic second mark"),
    ] {
        let (_, answer) = call(
            &w,
            Some(OPERATOR),
            json!({"method":"mark","text":note,"at":at,"cwd":"/src/tide"}),
        )
        .await;
        assert_eq!(answer["outcome"], "ok", "{answer}");
        citations.push(answer["result"]["citation"].clone());
    }
    let (_, shown) = call(
        &w,
        Some(OPERATOR),
        json!({"method":"show","reference":citations[0]}),
    )
    .await;
    assert_eq!(shown["result"]["event"]["payload"]["by"], "operator");
    assert_eq!(shown["result"]["event"]["time_quality"], "operator");
    for (around, note) in [
        ("mark", "synthetic second mark"),
        ("mark 2", "synthetic first mark"),
    ] {
        let (_, answer) = call(
            &w,
            Some(OPERATOR),
            json!({"method":"recall","around":around}),
        )
        .await;
        assert_eq!(answer["outcome"], "ok", "{answer}");
        assert_eq!(answer["result"]["recall"]["mark"]["payload"]["text"], note);
    }
    let (_, hidden) = call(
        &w,
        Some(OPERATOR),
        json!({"method":"hide","reference":citations[1]}),
    )
    .await;
    assert_eq!(hidden["outcome"], "ok", "{hidden}");
    let (_, answer) = call(
        &w,
        Some(OPERATOR),
        json!({"method":"recall","around":"mark"}),
    )
    .await;
    assert_eq!(
        answer["result"]["recall"]["mark"]["payload"]["text"],
        "synthetic first mark"
    );
    let (_, ungranted) = call(&w, Some(READ), json!({"method":"recall","around":"mark"})).await;
    assert_eq!(
        ungranted["result"]["recall"]["status"],
        "no_known_moment_mark"
    );
}

#[devkit::test]
async fn saved_queries_bind_parameters_and_replace_definitions_through_admission(
    db: devkit::pg::TestDatabase,
) {
    let w = world(db).await;
    admit(&w, &["synthetic first", "synthetic second"], 0).await;
    let (_,answer) = call(&w,Some(OPERATOR),json!({"method":"query-save","name":"in-directory","query":"events where payload.cwd = $directory count","parameter":["directory:text"]})).await;
    assert_eq!(answer["outcome"], "ok", "{answer}");
    let (_, answer) = call(
        &w,
        Some(OPERATOR),
        json!({"method":"query","query":"query:in-directory(directory=\"/src/tide\")"}),
    )
    .await;
    assert_eq!(answer["outcome"], "ok", "{answer}");
    assert_eq!(answer["result"]["rows"][0]["count"], 2);
    let (_, answer) = call(
        &w,
        Some(OPERATOR),
        json!({"method":"query","query":"query:in-directory(directory=\"x or source exists\")"}),
    )
    .await;
    assert_eq!(answer["result"]["rows"][0]["count"], 0);
    let (_, answer) = call(
        &w,
        Some(READ),
        json!({"method":"query","query":"query:in-directory(directory=x)"}),
    )
    .await;
    assert_eq!(answer["outcome"], "refused");
    let (_,answer) = call(&w,Some(OPERATOR),json!({"method":"query-save","name":"in-directory","query":"events where source = $source count","parameter":["source:text"]})).await;
    assert_eq!(answer["outcome"], "ok", "{answer}");
    let (_, answer) = call(&w, Some(OPERATOR), json!({"method":"query-list"})).await;
    assert_eq!(answer["result"]["queries"].as_array().unwrap().len(), 1);
    let (_, answer) = call(
        &w,
        Some(OPERATOR),
        json!({"method":"query","query":"query:in-directory(source=terminal.atuin)"}),
    )
    .await;
    assert_eq!(answer["result"]["rows"][0]["count"], 2);
}

#[devkit::test]
async fn context_windows_count_back_from_the_end_and_timeline_uses_the_same_query_rows(
    db: devkit::pg::TestDatabase,
) {
    let w = world(db).await;
    admit(
        &w,
        &["synthetic before", "synthetic within", "synthetic end"],
        0,
    )
    .await;
    for since in ["60s", "2026-09-14T08:01:00Z"] {
        let (_, answer) = call(
            &w,
            Some(READ),
            json!({"method":"context","since":since,"until":"2026-09-14T08:02:00Z"}),
        )
        .await;
        assert_eq!(answer["outcome"], "ok", "{answer}");
        assert_eq!(answer["result"]["total"], 1);
        assert_eq!(
            answer["result"]["rows"][0]["payload"]["command"],
            "synthetic within"
        );
        assert!(answer["result"]["recall"]["citation"].is_string());
    }
    let (_,a) = call(&w,Some(READ),json!({"method":"timeline","query":"where source = terminal.atuin since 2026-09-14T08:01:00Z"})).await;
    let (_,b) = call(&w,Some(READ),json!({"method":"query","query":"events where source = terminal.atuin since 2026-09-14T08:01:00Z"})).await;
    assert_eq!(a["result"]["rows"], b["result"]["rows"]);
    assert_eq!(a["result"]["total"], 2);
    let (_,completion) = call(&w,Some(READ),json!({"method":"query-complete","query":"events where event_type = command.executed and payload.dur"})).await;
    assert_eq!(
        completion["result"]["fields"][0]["name"],
        "payload.duration_ns"
    );
}

#[devkit::test]
async fn time_citations_follow_replaced_spans_and_recall_includes_states_already_holding(
    db: devkit::pg::TestDatabase,
) {
    let w = world(db).await;
    let parent = command("synthetic spanning parent", 0);
    let start = Timestamp::from_unix_secs(1_789_372_800).unwrap();
    let end = Timestamp::from_unix_secs(1_789_376_400).unwrap();
    let mut session = sinex_model::Event::build(
        &sinex_model::event_types::ShellSession {
            host: "synthetic".into(),
            started_at: start,
            ended_at: end,
            day: "2026-09-14".into(),
            commands: 1,
            failed: 0,
            cwds: vec!["/src/tide".into()],
        },
        "synthetic.sessions",
        Origin::Derived {
            parents: vec![parent.id],
        },
        ReadingOrder(1),
        [1; 32],
        Class::Open,
    )
    .unwrap();
    let first_id = session.id;
    admit_events(&w, vec![parent, session.clone()]).await;
    let reference = "sinex:at/2026-09-14T08:30:00Z/shell.session";
    let (_, shown) = call(
        &w,
        Some(READ),
        json!({"method":"show","reference":reference}),
    )
    .await;
    assert_eq!(shown["outcome"], "ok", "{shown}");
    assert_eq!(shown["result"]["event"]["id"], first_id.to_string());
    let (_, context) = call(
        &w,
        Some(READ),
        json!({"method":"context","since":"2026-09-14T08:30:00Z","until":"2026-09-14T08:31:00Z"}),
    )
    .await;
    assert_eq!(context["outcome"], "ok", "{context}");
    assert_eq!(context["result"]["total"], 1);
    session.id = sinex_model::EventId::new();
    session.reading_order = ReadingOrder(2);
    session.record_hash = [2; 32];
    session.payload["ended_at"] = json!(Timestamp::from_unix_secs(1_789_380_000).unwrap());
    let second_id = session.id;
    admit_events(&w, vec![session]).await;
    let (_, shown) = call(
        &w,
        Some(READ),
        json!({"method":"show","reference":reference}),
    )
    .await;
    assert_eq!(shown["outcome"], "ok", "{shown}");
    assert_eq!(shown["result"]["event"]["id"], second_id.to_string());
    let (_,window) = call(&w,Some(READ),json!({"method":"show","reference":"sinex:window/2026-09-14T08:30:00Z/2026-09-14T08:31:00Z"})).await;
    assert_eq!(window["outcome"], "ok", "{window}");
    assert_eq!(window["result"]["total"], 1);
    assert_eq!(window["result"]["rows"][0]["id"], second_id.to_string());
}

#[devkit::test]
async fn replacing_a_saved_definition_invalidates_its_previous_continuation(
    db: devkit::pg::TestDatabase,
) {
    let w = world(db).await;
    admit(
        &w,
        &["synthetic one", "synthetic two", "synthetic three"],
        0,
    )
    .await;
    let (_,saved) = call(&w,Some(OPERATOR),json!({"method":"query-save","name":"paged","query":"events where payload.cwd = $directory fields id","parameter":["directory:text"]})).await;
    assert_eq!(saved["outcome"], "ok", "{saved}");
    let (_, page) = call(
        &w,
        Some(OPERATOR),
        json!({"method":"query","query":"query:paged(directory=\"/src/tide\")","page":1}),
    )
    .await;
    assert_eq!(page["outcome"], "ok", "{page}");
    let cursor = page["result"]["page"]["cursor"].clone();
    assert!(cursor.is_string());
    let (_,changed) = call(&w,Some(OPERATOR),json!({"method":"query-save","name":"paged","query":"events where payload.cwd = $directory sort when desc fields id","parameter":["directory:text"]})).await;
    assert_eq!(changed["outcome"], "ok", "{changed}");
    let (_,continued) = call(&w,Some(OPERATOR),json!({"method":"query","query":"query:paged(directory=\"/src/tide\")","page":1,"cursor":cursor})).await;
    assert_eq!(continued["outcome"], "refused", "{continued}");
}
