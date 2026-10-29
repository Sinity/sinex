//! Recall through the public API, real writer and actual shell derivation.
use super::*;
use sinex_derivations::{Env, Keyed, derived_event::derived, session::ShellSessions};
use sinex_model::event_types::{CommandFused, CommandImported};

fn facts(host: &str, rows: &[(i64, &str, Option<i64>)], class: Class) -> Vec<Event> {
    let mut events = Vec::new();
    let mut fused = Vec::new();
    for &(i, cwd, duration) in rows {
        let mut raw = command(&format!("synthetic {host} command {i}"), i);
        raw.payload["native_id"] = json!(format!("{host}-{i}"));
        raw.payload["cwd"] = json!(cwd);
        raw.payload["hostname"] = json!(host);
        raw.payload["duration_ns"] = json!(duration);
        raw.class = class;
        let c = raw.payload_as::<CommandExecuted>().unwrap();
        let command = derived(
            &CommandFused {
                record: c.native_id,
                command: c.command,
                host: host.into(),
                started_at: c.started_at,
                cwd: Some(c.cwd),
                session: Some(c.session),
                duration_ns: c.duration_ns,
                exit_code: c.exit_code,
                observed_by: vec!["terminal.atuin".into()],
            },
            "shell.fusion",
            [&raw],
        );
        events.push(raw);
        fused.push(command);
    }
    let env = Env::at(
        Timestamp::from_unix_secs(1_789_459_200).unwrap(),
        sinex_model::time_settings::TimeSettings::new("Europe/Warsaw", 4, 0)
            .expect("fixture day setting"),
    );
    let sessions = ShellSessions
        .compute(&json!({"host":host,"day":"2026-09-14"}), &fused, &env)
        .outputs;
    events.extend(fused);
    events.extend(sessions);
    events
}

fn ids(body: &Value) -> Vec<String> {
    body["result"]["rows"]
        .as_array()
        .unwrap()
        .iter()
        .map(|r| r["id"].as_str().unwrap().to_string())
        .collect()
}

#[devkit::test]
async fn recall_last_selects_recorded_groups_and_includes_the_observed_upper_point(
    db: devkit::pg::TestDatabase,
) {
    let w = world(db).await;
    let first = facts(
        "box",
        &[
            (0, "/src/tide", Some(2_000_000_000)),
            (1, "/src/other", None),
            (2, "/src/tide/tools", None),
        ],
        Class::Open,
    );
    let mut expected: Vec<_> = first.iter().map(|e| e.id.to_string()).collect();
    expected.sort();
    let mut all = first;
    all.extend(facts("box", &[(20, "/src/tidepool", None)], Class::Open));
    admit_events(&w, all).await;
    let (_, listed) = call(
        &w,
        Some(READ),
        json!({"method":"query","query":"shell-sessions sort start"}),
    )
    .await;
    assert_eq!(listed["outcome"], "ok", "{listed}");
    assert_eq!(listed["result"]["total"], 2);
    assert!(listed["result"]["rows"][0]["end"].is_null());
    assert!(listed["result"]["rows"][0]["open"].is_null());
    assert_eq!(
        listed["result"]["rows"][0]["cwds"],
        json!(["/src/tide", "/src/other", "/src/tide/tools"])
    );
    let (_, latest) = call(&w, Some(READ), json!({"method":"recall","around":"last"})).await;
    assert_eq!(
        latest["result"]["recall"]["session"]["row"]["cwds"],
        json!(["/src/tidepool"])
    );
    let (_, scoped) = call(
        &w,
        Some(READ),
        json!({"method":"recall","around":"last","cwd":"/src/tide"}),
    )
    .await;
    assert_eq!(scoped["outcome"], "ok", "{scoped}");
    let mut actual = ids(&scoped);
    actual.sort();
    assert_eq!(actual, expected);
    assert_eq!(
        scoped["result"]["recall"]["session"]["bounds"],
        "recorded_commands"
    );
    assert_eq!(scoped["result"]["recall"]["inclusive_end"], true);
    let by_source = scoped["result"]["recall"]["by_source"].as_array().unwrap();
    assert_eq!(
        by_source
            .iter()
            .map(|s| s["events"].as_u64().unwrap())
            .sum::<u64>(),
        expected.len() as u64
    );
    let (_, query) = call(
        &w,
        Some(READ),
        json!({"method":"query","query":"recall last where cwds ^ /src/tide"}),
    )
    .await;
    assert_eq!(ids(&query), ids(&scoped));
    let (_, during) = call(
        &w,
        Some(READ),
        json!({"method":"query","query":"commands during (shell-sessions where cwds ^ /src/tide)"}),
    )
    .await;
    assert_eq!(during["result"]["total"], 3, "{during}");
    let (_, containing) = call(&w,Some(READ),json!({"method":"query","query":"shell-sessions containing (commands where cwd = /src/tide/tools)"})).await;
    assert_eq!(containing["result"]["total"], 1, "{containing}");
}

#[devkit::test]
async fn recall_last_pages_keep_the_selected_commit_and_equal_start_ties_are_stable(
    db: devkit::pg::TestDatabase,
) {
    let w = world(db).await;
    let mut events = facts(
        "first",
        &[(0, "/src/tide", None), (1, "/src/tide", None)],
        Class::Open,
    );
    events.extend(facts(
        "second",
        &[(0, "/src/other", None), (1, "/src/other", None)],
        Class::Open,
    ));
    let expected_anchor = events
        .iter()
        .filter(|e| e.event_type == "shell.session")
        .map(|e| e.id.to_string())
        .min()
        .unwrap();
    admit_events(&w, events).await;
    let (_, answer) = call(&w, Some(READ), json!({"method":"recall","around":"last"})).await;
    assert_eq!(
        answer["result"]["recall"]["session"]["row"]["id"],
        expected_anchor
    );
    let expected = ids(&answer);
    let (_, first) = call(
        &w,
        Some(READ),
        json!({"method":"recall","around":"last","page":2}),
    )
    .await;
    let seq = first["result"]["resolved"]["commit_seq"].clone();
    let mut collected = ids(&first);
    let mut cursor = first["result"]["page"]["cursor"].clone();
    admit_events(&w, facts("later", &[(20, "/src/later", None)], Class::Open)).await;
    while let Some(c) = cursor.as_str() {
        let (_, page) = call(
            &w,
            Some(READ),
            json!({"method":"recall","around":"last","page":2,"cursor":c}),
        )
        .await;
        assert_eq!(page["result"]["resolved"]["commit_seq"], seq);
        assert_eq!(
            page["result"]["recall"]["session"]["row"]["id"],
            expected_anchor
        );
        collected.extend(ids(&page));
        cursor = page["result"]["page"]["cursor"].clone();
    }
    assert_eq!(collected, expected);
    let (_, changed) = call(&w, Some(READ), json!({"method":"recall","around":"last"})).await;
    assert_ne!(
        changed["result"]["recall"]["session"]["row"]["id"],
        expected_anchor
    );
}

#[devkit::test]
async fn recall_cwd_quotes_paths_and_rejects_incompatible_windows(db: devkit::pg::TestDatabase) {
    let w = world(db).await;
    let path = "/src/tide (notes) \" where cwds = /else";
    admit_events(&w, facts("box", &[(0, path, None)], Class::Open)).await;
    let (_, answer) = call(
        &w,
        Some(READ),
        json!({"method":"recall","around":"last","cwd":path}),
    )
    .await;
    assert_eq!(answer["outcome"], "ok", "{answer}");
    assert_eq!(
        answer["result"]["recall"]["session"]["row"]["cwds"],
        json!([path])
    );
    for around in [None, Some("yesterday"), Some("2026-09-14T08:00:00Z")] {
        let (_, body) = call(
            &w,
            Some(READ),
            json!({"method":"recall","around":around,"cwd":path}),
        )
        .await;
        assert_eq!(body["outcome"], "refused");
        assert!(
            body["reason"]
                .as_str()
                .unwrap()
                .contains("only to recall last")
        );
    }
    let (_, absent) = call(
        &w,
        Some(READ),
        json!({"method":"recall","around":"last","cwd":"/unmatched"}),
    )
    .await;
    assert_eq!(
        absent["result"]["recall"]["status"],
        "no_known_timed_shell_session"
    );
    assert!(absent["result"]["recall"]["no_data"].is_null());
    let (_, relative) = call(
        &w,
        Some(READ),
        json!({"method":"recall","around":"last","cwd":"relative"}),
    )
    .await;
    assert_eq!(relative["outcome"], "refused");
}

#[devkit::test]
async fn recall_last_never_invents_a_span_from_untimed_or_withheld_evidence(
    db: devkit::pg::TestDatabase,
) {
    let w = world(db).await;
    let untimed = Event::build(
        &CommandImported {
            command: "synthetic untimed".into(),
            hostname: "box".into(),
            started_at: None,
            header_elapsed: None,
            paths: vec![],
            n: 0,
        },
        "terminal.zsh",
        Origin::Record {
            location: RecordLocation {
                material: MaterialId::new(),
                anchor: 0,
            },
            output_index: 0,
        },
        ReadingOrder(1),
        [3; 32],
        Class::Open,
    )
    .unwrap();
    admit_events(&w, vec![untimed]).await;
    let (_, none) = call(&w, Some(READ), json!({"method":"recall","around":"last"})).await;
    assert_eq!(
        none["result"]["recall"]["status"],
        "no_known_timed_shell_session"
    );
    assert!(none["result"]["recall"]["by_source"].is_null());
    let (_, counted) = call(
        &w,
        Some(READ),
        json!({"method":"query","query":"recall last count"}),
    )
    .await;
    assert_eq!(counted["result"]["total"], 1, "{counted}");
    assert_eq!(counted["result"]["rows"][0]["count"], 0, "{counted}");
    assert_eq!(
        counted["result"]["recall"]["status"],
        "no_known_timed_shell_session"
    );
    let (_, observations) = call(
        &w,
        Some(READ),
        json!({"method":"query","query":"events where event_type = command.imported"}),
    )
    .await;
    assert_eq!(observations["result"]["total"], 1);
    assert!(observations["result"]["rows"][0]["when"].is_null());
    admit_events(
        &w,
        facts("public", &[(0, "/src/public", None)], Class::Open),
    )
    .await;
    admit_events(
        &w,
        facts("private", &[(20, "/src/private", None)], Class::Guarded),
    )
    .await;
    let (_, hidden) = call(&w, Some(READ), json!({"method":"recall","around":"last"})).await;
    assert_eq!(hidden["outcome"], "refused", "{hidden}");
    assert!(hidden["reason"].as_str().unwrap().contains("unavailable"));
    assert!(!hidden.to_string().contains("/src/private"));
    let (_, visible) = call(
        &w,
        Some(OPERATOR),
        json!({"method":"recall","around":"last"}),
    )
    .await;
    assert_eq!(visible["outcome"], "ok", "{visible}");
    assert_eq!(
        visible["result"]["recall"]["session"]["row"]["cwds"],
        json!(["/src/private"])
    );
}

#[devkit::test]
async fn recall_brief_and_machine_render_one_stored_context_pack(db: devkit::pg::TestDatabase) {
    let w = world(db).await;
    admit_events(
        &w,
        facts(
            "synthetic",
            &[(0, "/src/tide", None), (1, "/src/tide", Some(0))],
            Class::Open,
        ),
    )
    .await;
    let (_, recall) = call(
        &w,
        Some(READ),
        json!({"method":"recall","around":"last","cwd":"/src/tide","page":1}),
    )
    .await;
    assert_eq!(recall["outcome"], "ok", "{recall}");
    let citation = recall["result"]["context_pack"].as_str().unwrap();
    let (_, brief) = call(
        &w,
        Some(READ),
        json!({"method":"brief","day":citation,"budget":4000}),
    )
    .await;
    assert_eq!(brief["outcome"], "ok", "{brief}");
    assert_eq!(
        brief["result"]["context_pack"],
        recall["result"]["context_pack"]
    );
    assert_eq!(brief["result"]["machine"], recall["result"]["machine"]);
    assert_eq!(
        brief["result"]["presentation"],
        recall["result"]["presentation"]
    );
    let (_, stored) = call(
        &w,
        Some(READ),
        json!({"method":"show","reference":citation}),
    )
    .await;
    assert_eq!(stored["outcome"], "ok", "{stored}");
    assert!(
        stored["result"]["event"]["payload"]["analysis"]["answer"]["rows"]
            .as_array()
            .unwrap()
            .len()
            > 1
    );
    let old_cursor = recall["result"]["page"]["cursor"].as_str().unwrap();
    admit_events(&w, facts("newer", &[(20, "/src/tide", None)], Class::Open)).await;
    let (_, continued) = call(
        &w,
        Some(READ),
        json!({"method":"recall","around":"last","cwd":"/src/tide","page":1,"cursor":old_cursor}),
    )
    .await;
    assert_eq!(continued["outcome"], "ok", "{continued}");
    assert_eq!(
        continued["result"]["context_pack"],
        recall["result"]["context_pack"]
    );
    assert_eq!(
        continued["result"]["resolved"],
        recall["result"]["resolved"]
    );
}

#[devkit::test]
async fn saved_recall_and_context_continuations_keep_their_stored_selection(
    db: devkit::pg::TestDatabase,
) {
    let w = world(db).await;
    admit_events(
        &w,
        facts(
            "synthetic",
            &[(0, "/src/tide", None), (1, "/src/tide", None)],
            Class::Open,
        ),
    )
    .await;
    let (_,saved) = call(&w,Some(OPERATOR),json!({"method":"query-save","name":"remember-tide","query":"recall last where cwds ^ /src/tide"})).await;
    assert_eq!(saved["outcome"], "ok", "{saved}");
    let (_, first) = call(
        &w,
        Some(OPERATOR),
        json!({"method":"query","query":"query:remember-tide","page":1}),
    )
    .await;
    assert_eq!(first["outcome"], "ok", "{first}");
    assert!(first["result"]["context_pack"].is_string());
    let cursor = first["result"]["page"]["cursor"].as_str().unwrap();
    let (_,changed) = call(&w,Some(OPERATOR),json!({"method":"query-save","name":"remember-tide","query":"recall last where cwds ^ /elsewhere"})).await;
    assert_eq!(changed["outcome"], "ok", "{changed}");
    let (_, next) = call(
        &w,
        Some(OPERATOR),
        json!({"method":"query","query":"query:remember-tide","page":1,"cursor":cursor}),
    )
    .await;
    assert_eq!(next["outcome"], "ok", "{next}");
    assert_eq!(
        next["result"]["context_pack"],
        first["result"]["context_pack"]
    );
    assert_eq!(next["result"]["resolved"], first["result"]["resolved"]);
    let (_, context) = call(
        &w,
        Some(READ),
        json!({"method":"context","since":"2026-09-14T00:00:00Z","page":1}),
    )
    .await;
    assert_eq!(context["outcome"], "ok", "{context}");
    let cursor = context["result"]["page"]["cursor"].as_str().unwrap();
    let (_, next) = call(
        &w,
        Some(READ),
        json!({"method":"context","since":"2026-09-14T00:00:00Z","page":1,"cursor":cursor}),
    )
    .await;
    assert_eq!(next["outcome"], "ok", "{next}");
    assert_eq!(
        next["result"]["context_pack"],
        context["result"]["context_pack"]
    );
    assert_eq!(next["result"]["resolved"], context["result"]["resolved"]);
}

#[devkit::test]
async fn a_stored_recall_cursor_keeps_the_callers_grants(db: devkit::pg::TestDatabase) {
    let w = world(db).await;
    admit_events(
        &w,
        facts(
            "synthetic-private",
            &[(0, "/src/private", None), (1, "/src/private", None)],
            Class::Guarded,
        ),
    )
    .await;
    let (_, first) = call(
        &w,
        Some(OPERATOR),
        json!({"method":"recall","around":"last","page":1}),
    )
    .await;
    assert_eq!(first["outcome"], "ok", "{first}");
    let cursor = first["result"]["page"]["cursor"].as_str().unwrap();
    let (_, other) = call(
        &w,
        Some(READ),
        json!({"method":"recall","around":"last","page":1,"cursor":cursor}),
    )
    .await;
    assert_eq!(other["outcome"], "refused", "{other}");
    assert!(!other.to_string().contains("synthetic-private"));
}

#[devkit::test]
async fn archived_parent_recall_is_an_explicit_historical_preview(db: devkit::pg::TestDatabase) {
    let w = world(db).await;
    let events = facts(
        "synthetic",
        &[(0, "/src/tide", None), (1, "/src/tide", None)],
        Class::Open,
    );
    let original = events
        .iter()
        .find(|event| event.event_type == "command.executed")
        .unwrap()
        .id;
    admit_events(&w, events).await;
    let before = sinex_store::query::current_seq(&mut w.reads.acquire().await.unwrap())
        .await
        .unwrap();
    let (_, ordinary) = call(&w, Some(READ), json!({"method":"recall","around":"last"})).await;
    assert_eq!(ordinary["outcome"], "ok", "{ordinary}");
    assert!(ordinary["result"]["context_pack"].is_string());
    let (_, hidden) = call(
        &w,
        Some(OPERATOR),
        json!({"method":"hide","reference":format!("sinex:ev/{original}")}),
    )
    .await;
    assert_eq!(hidden["outcome"], "ok", "{hidden}");
    let packs: i64 =
        sqlx::query_scalar("SELECT count(*) FROM core.events WHERE event_type='context.pack'")
            .fetch_one(&w.reads)
            .await
            .unwrap();
    let (_, historical) = call(
        &w,
        Some(READ),
        json!({"method":"query","query":format!("recall last as of seq {before}"),"page":1}),
    )
    .await;
    assert_eq!(historical["outcome"], "ok", "{historical}");
    assert_eq!(
        historical["result"]["historical_preview"]["materialized"],
        false
    );
    assert_eq!(historical["result"]["resolved"]["commit_seq"], before);
    assert!(historical["result"]["context_pack"].is_null());
    assert!(
        historical["result"]["historical_preview"]["raw_citations"]
            .as_array()
            .unwrap()
            .contains(&json!(format!("sinex:ev/{original}")))
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT count(*) FROM core.events WHERE event_type='context.pack'"
        )
        .fetch_one(&w.reads)
        .await
        .unwrap(),
        packs
    );
    let cursor = historical["result"]["page"]["cursor"].as_str().unwrap();
    let (_,next) = call(&w,Some(READ),json!({"method":"query","query":format!("recall last as of seq {before}"),"page":1,"cursor":cursor})).await;
    assert_eq!(next["outcome"], "ok", "{next}");
    assert_eq!(next["result"]["resolved"], historical["result"]["resolved"]);
    let (_, current) = call(&w, Some(READ), json!({"method":"recall","around":"last"})).await;
    assert_eq!(current["outcome"], "ok", "{current}");
    assert!(current["result"]["context_pack"].is_string());
    assert!(current["result"]["historical_preview"].is_null());
}

#[devkit::test]
async fn unchanged_explicit_recall_recompute_reuses_its_read_context_and_reading(
    db: devkit::pg::TestDatabase,
) {
    let w = world(db).await;
    admit_events(
        &w,
        facts(
            "synthetic",
            &[(0, "/src/tide", None), (1, "/src/tide", None)],
            Class::Open,
        ),
    )
    .await;
    let (_, before) = call(&w, Some(READ), json!({"method":"recall","around":"last"})).await;
    assert_eq!(before["outcome"], "ok", "{before}");
    let count: i64 =
        sqlx::query_scalar("SELECT count(*) FROM core.events WHERE event_type='context.pack'")
            .fetch_one(&w.reads)
            .await
            .unwrap();
    let connection = w.reads.acquire().await.unwrap().detach();
    let mut runner = crate::derivation_runner::DerivationRunner::for_one(
        connection,
        w.writer.clone(),
        "recall.products",
        "h",
        &Default::default(),
        sinex_model::time_settings::TimeSettings::from_parts(
            jiff::tz::TimeZone::UTC,
            jiff::civil::Time::constant(4, 0, 0, 0),
        ),
    )
    .await
    .unwrap();
    let comparison = runner.compare("recall.products", None, 5).await.unwrap();
    assert_eq!(
        (
            comparison.same,
            comparison.added,
            comparison.changed,
            comparison.removed
        ),
        (1, 0, 0, 0)
    );
    assert_eq!(
        runner.recompute_all("recall.products", None).await.unwrap(),
        1
    );
    runner.catch_up().await.unwrap();
    runner.end_runs().await.unwrap();
    let (_, after) = call(&w, Some(READ), json!({"method":"recall","around":"last"})).await;
    assert_eq!(after["outcome"], "ok", "{after}");
    assert_eq!(
        after["result"]["context_pack"],
        before["result"]["context_pack"]
    );
    assert_eq!(after["result"]["resolved"], before["result"]["resolved"]);
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT count(*) FROM core.events WHERE event_type='context.pack'"
        )
        .fetch_one(&w.reads)
        .await
        .unwrap(),
        count
    );
}

#[devkit::test]
async fn a_stored_work_resume_card_is_shared_by_brief_and_machine_without_recomputation(
    db: devkit::pg::TestDatabase,
) {
    use sinex_model::event_types::{activity::ActivityEvidence, work::ContextPack};
    let w = world(db).await;
    let mut failed = command("synthetic failing check", 0);
    failed.payload["exit_code"] = json!(1);
    let evidence = ActivityEvidence {
        event: failed.id,
        record: "synthetic-check".into(),
        event_type: failed.event_type.clone(),
        source: failed.source.clone(),
    };
    admit_events(&w, vec![failed.clone()]).await;
    let position = sinex_store::query::current_seq(&mut w.reads.acquire().await.unwrap())
        .await
        .unwrap();
    let pack = derived(
        &ContextPack {
            evaluated_at: Some(Timestamp::now()),
            day_setting: Some("04:00:00 UTC".into()),
            producer_versions: None,
            read_as: Some(position),
            analysis: None,
            pack: "resume:synthetic-objective".into(),
            episode: "synthetic-objective".into(),
            at: failed.ts_orig.at.unwrap(),
            stated_goal: None,
            trail: vec![evidence.clone()],
            last_edit: None,
            last_edit_path: None,
            open_failures: vec![evidence],
            coverage: vec!["check revision and identity were not recorded".into()],
            profile: "resume/v1".into(),
        },
        "context.packs",
        [&failed],
    );
    let citation = format!("sinex:ev/{}", pack.id);
    admit_events(&w, vec![pack]).await;
    let (_, first) = call(&w, Some(READ), json!({"method":"brief","day":citation})).await;
    assert_eq!(first["outcome"], "ok", "{first}");
    assert_eq!(first["result"]["context_pack"], citation);
    assert_eq!(first["result"]["machine"]["context_pack"], citation);
    assert_eq!(first["result"]["machine"]["read_as"], position);
    assert!(
        first["result"]["text"]
            .as_str()
            .unwrap()
            .contains("Unresolved recorded check failure")
    );
    let (_, second) = call(
        &w,
        Some(READ),
        json!({"method":"brief","day":citation,"budget":10000}),
    )
    .await;
    assert_eq!(second["result"]["context_pack"], citation);
    let mut conn = w.reads.acquire().await.unwrap();
    assert_eq!(
        sinex_store::live::of_types(&mut conn, &["context.pack".into()])
            .await
            .unwrap()
            .len(),
        1
    );
    assert!(
        sinex_store::live::of_types(&mut conn, &["analysis.request".into()])
            .await
            .unwrap()
            .is_empty()
    );
}
