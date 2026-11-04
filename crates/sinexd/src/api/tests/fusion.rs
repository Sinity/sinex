//! Duplicate decisions through the public commands.
use super::*;
use sinex_model::event_types::CommandImported;

fn captured<P: sinex_model::EventPayload>(payload: &P, source: &str, anchor: u64) -> Event {
    Event::build(
        payload,
        source,
        Origin::Record {
            location: RecordLocation {
                material: MaterialId::new(),
                anchor,
            },
            output_index: 0,
        },
        ReadingOrder(1),
        [anchor as u8 + 1; 32],
        Class::Open,
    )
    .unwrap()
}

fn atuin_row(host: &str) -> Event {
    captured(
        &CommandExecuted {
            native_id: "synthetic-row".into(),
            command: "make".into(),
            cwd: "/src/tide".into(),
            session: "s".into(),
            hostname: format!("{host}:user"),
            started_at: Timestamp::from_unix_secs(1_789_372_800).unwrap(),
            duration_ns: None,
            exit_code: Some(0),
            deleted_at: None,
            author: None,
            intent: None,
            shell: None,
            extra: Default::default(),
        },
        "terminal.atuin-history",
        0,
    )
}

fn history_line(host: &str) -> Event {
    captured(
        &CommandImported {
            command: "make".into(),
            hostname: host.into(),
            started_at: Some(Timestamp::from_unix_ns(1_789_372_800_400_000_000).unwrap()),
            header_elapsed: None,
            paths: vec![],
            n: 0,
        },
        "terminal.zsh-history",
        1,
    )
}

async fn ids(w: &World) -> (String, String) {
    let row: uuid::Uuid =
        sqlx::query_scalar("SELECT id FROM core.events WHERE event_type = 'command.executed'")
            .fetch_one(&w.reads)
            .await
            .unwrap();
    let line: uuid::Uuid =
        sqlx::query_scalar("SELECT id FROM core.events WHERE event_type = 'command.imported'")
            .fetch_one(&w.reads)
            .await
            .unwrap();
    (format!("sinex:ev/{row}"), format!("sinex:ev/{line}"))
}

#[devkit::test]
async fn duplicate_decisions_are_authored_records_citing_both_records(
    db: devkit::pg::TestDatabase,
) {
    let w = world(db).await;
    admit_events(&w, vec![atuin_row("kestrel"), history_line("kestrel")]).await;
    let (row, line) = ids(&w).await;
    let (status, _) = call(
        &w,
        Some(READ),
        json!({"method":"distinct","a":row,"b":line}),
    )
    .await;
    assert_eq!(status, StatusCode::FORBIDDEN);
    let (_, first) = call(
        &w,
        Some(OPERATOR),
        json!({"method":"distinct","a":line,"b":row}),
    )
    .await;
    assert_eq!(first["outcome"], "ok", "{first}");
    let first = &first["result"];
    assert_eq!(first["family"], "command");
    assert_eq!(first["decision"], "distinct");
    assert_eq!(first["author"], "operator");
    assert_eq!(first["root"], first["rev"]);
    assert_eq!(first["revises"], json!([]));
    let pair = first["pair"].as_array().unwrap();
    assert!(pair[0].as_str() < pair[1].as_str());
    assert_eq!(pair[0], "command.executed:synthetic-row");

    // The author's next decision on the pair revises the first.
    let (_, second) = call(
        &w,
        Some(OPERATOR),
        json!({"method":"same","a":row,"b":line}),
    )
    .await;
    assert_eq!(second["outcome"], "ok", "{second}");
    let second = &second["result"];
    assert_eq!(second["root"], first["root"]);
    assert_ne!(second["rev"], first["rev"]);
    assert_eq!(
        second["revises"],
        json!([format!(
            "{}@{}",
            first["root"].as_str().unwrap(),
            first["rev"].as_str().unwrap()
        )])
    );

    // Withdrawing writes a record of its own that withdraws the decision.
    let (_, withdrawn) = call(
        &w,
        Some(OPERATOR),
        json!({"method":"withdraw-decision","a":row,"b":line}),
    )
    .await;
    assert_eq!(withdrawn["outcome"], "ok", "{withdrawn}");
    assert_eq!(withdrawn["result"]["decision"], Value::Null);
    assert_eq!(withdrawn["result"]["withdraws"], json!([first["root"]]));
    let stored: Vec<Value> = sqlx::query_scalar(
        "SELECT payload FROM core.events WHERE event_type = 'curation.duplicate' ORDER BY commit_seq",
    )
    .fetch_all(&w.reads)
    .await
    .unwrap();
    assert_eq!(stored.len(), 3, "nothing is replaced: every revision stays");
    assert!(
        stored
            .iter()
            .all(|p| p["author"] == "operator" && p["via"] == "api")
    );
    // With nothing current, a second withdrawal has nothing to withdraw.
    let (_, again) = call(
        &w,
        Some(OPERATOR),
        json!({"method":"withdraw-decision","a":row,"b":line}),
    )
    .await;
    assert_eq!(again["outcome"], "refused", "{again}");
}

#[devkit::test]
async fn decisions_outside_a_family_or_across_scopes_are_refused(db: devkit::pg::TestDatabase) {
    let w = world(db).await;
    admit_events(&w, vec![atuin_row("kestrel"), history_line("other-host")]).await;
    let (row, line) = ids(&w).await;
    for (method, a, b, reason) in [
        ("same", &row, &row, "two different records"),
        ("same", &row, &line, "scopes"),
    ] {
        let (_, answer) = call(&w, Some(OPERATOR), json!({"method":method,"a":a,"b":b})).await;
        assert_eq!(answer["outcome"], "refused", "{answer}");
        assert!(
            answer["reason"].as_str().unwrap().contains(reason),
            "{answer}"
        );
    }
    // Different hosts may still be decided distinct.
    let (_, answer) = call(
        &w,
        Some(OPERATOR),
        json!({"method":"distinct","a":row,"b":line}),
    )
    .await;
    assert_eq!(answer["outcome"], "ok", "{answer}");
    admit_events(&w, vec![command("not a family member", 5)]).await;
    let other: uuid::Uuid =
        sqlx::query_scalar("SELECT id FROM core.events WHERE source = 'terminal.atuin'")
            .fetch_one(&w.reads)
            .await
            .unwrap();
    let (_, answer) = call(
        &w,
        Some(OPERATOR),
        json!({"method":"same","a":row,"b":format!("sinex:ev/{other}")}),
    )
    .await;
    assert_eq!(answer["outcome"], "refused", "{answer}");
    assert!(
        answer["reason"]
            .as_str()
            .unwrap()
            .contains("no fusion family"),
        "{answer}"
    );
}

/// Library equality is a statement between separate items, independent of
/// fusion's time windows; changing or withdrawing it keeps both identities.
#[devkit::test]
async fn library_items_share_duplicate_decisions_without_becoming_one_item(
    db: devkit::pg::TestDatabase,
) {
    use sinex_model::event_types::library::{Item, item_id};
    let w = world(db).await;
    let item = |key: &str, seconds: i64, anchor| {
        captured(
            &Item {
                item: item_id(key),
                revision: format!("revision-{key}"),
                replaces: None,
                content: format!("blake3:{}", "ab".repeat(32)),
                media_type: "text/plain".into(),
                copies: vec![format!("fixture:/{key}")],
                label: Some(key.into()),
                by: "fixture".into(),
                at: Timestamp::from_unix_secs(seconds).unwrap(),
            },
            "derive.library-items",
            anchor,
        )
    };
    let a = item("first", 1, 10);
    let b = item("second", 86_400 * 100, 11);
    let references = [format!("sinex:ev/{}", a.id), format!("sinex:ev/{}", b.id)];
    admit_events(&w, vec![a, b]).await;
    let mut first_root = Value::Null;
    for method in ["same", "distinct", "withdraw-decision"] {
        let (_, answer) = call(
            &w,
            Some(OPERATOR),
            json!({"method":method,"a":references[0],"b":references[1]}),
        )
        .await;
        assert_eq!(answer["outcome"], "ok", "{answer}");
        let decision = &answer["result"];
        assert_eq!(decision["family"], Value::Null);
        assert_eq!(decision["family_key"], Value::Null);
        if method == "same" {
            first_root = decision["root"].clone();
        }
        if method == "distinct" {
            assert_eq!(decision["root"], first_root);
        }
        if method == "withdraw-decision" {
            assert_eq!(decision["withdraws"], json!([first_root]));
        }
    }
    let items: i64 =
        sqlx::query_scalar("SELECT count(*) FROM core.events WHERE event_type='library.item'")
            .fetch_one(&w.reads)
            .await
            .unwrap();
    let decisions: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM core.events WHERE event_type='curation.duplicate'",
    )
    .fetch_one(&w.reads)
    .await
    .unwrap();
    assert_eq!((items, decisions), (2, 3));
}
