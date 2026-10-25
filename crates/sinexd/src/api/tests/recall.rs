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
        let canonical = derived(
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
        fused.push(canonical);
    }
    let env = Env::at(Timestamp::from_unix_secs(1_789_459_200).unwrap());
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
        latest["result"]["recall"]["anchor"]["row"]["cwds"],
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
    assert_eq!(scoped["result"]["recall"]["anchor"]["kind"], "shell_group");
    assert_eq!(
        scoped["result"]["recall"]["anchor"]["bounds"],
        "observed_envelope"
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
    let (_, broad) = call(&w, Some(READ), json!({"method":"query","query":"sessions"})).await;
    assert_eq!(broad["outcome"], "refused");
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
        answer["result"]["recall"]["anchor"]["row"]["id"],
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
            page["result"]["recall"]["anchor"]["row"]["id"],
            expected_anchor
        );
        collected.extend(ids(&page));
        cursor = page["result"]["page"]["cursor"].clone();
    }
    assert_eq!(collected, expected);
    let (_, changed) = call(&w, Some(READ), json!({"method":"recall","around":"last"})).await;
    assert_ne!(
        changed["result"]["recall"]["anchor"]["row"]["id"],
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
        answer["result"]["recall"]["anchor"]["row"]["cwds"],
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
            n: 0,
        },
        "terminal.zsh",
        Origin::Record {
            location: RecordLocation {
                material: MaterialId::new(),
                anchor: 0,
            },
            ordinal: 0,
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
        json!({"method":"query","query":"events where kind = command.imported"}),
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
        visible["result"]["recall"]["anchor"]["row"]["cwds"],
        json!(["/src/private"])
    );
}
