//! The curation laws against a real database and the real writer: requests
//! are planned, stored as request material and admitted like every write.
use super::super::tests::{World, world};
use super::super::{Library, Stored};
use authored::{History, RootState};
use proptest::strategy::{Strategy, ValueTree};
use serde_json::{Value, json};
use sinex_library::curation::{Snapshot, header};
use sinex_library::plan::{self, Caller, Outcome};
use sinex_library::shown;
use sinex_model::Timestamp;
use sinex_model::event_types::library::{Curation, CurationKind, LibraryRecord, ValueOrigin};
use sinex_protocol::Request;

async fn item(w: &mut World, name: &str, text: &str) -> String {
    let path = w.root.join(name);
    std::fs::write(&path, text).unwrap();
    w.ok(Request::LibraryAdd {
        path: path.display().to_string(),
        reference: None,
    })
    .await;
    w.item_at(&path).await
}

fn t(item: &str) -> Value {
    json!({ "item": item })
}

async fn view(w: &mut World, item: &str) -> Value {
    w.ok(Request::LibraryItem { item: item.into() }).await
}

fn tag_state(view: &Value, tag: &str) -> Option<String> {
    view["tags"]
        .as_array()
        .unwrap()
        .iter()
        .find(|t| t["tag"] == tag)
        .map(|t| t["state"].as_str().unwrap().to_owned())
}

/// The first record a request wrote, as `<root>@<rev>`.
fn written(answer: &Value, n: usize) -> String {
    answer["written"][n]["record"].as_str().unwrap().to_owned()
}

fn root(record: &str) -> String {
    record.split_once('@').unwrap().0.to_owned()
}

async fn entries(w: &mut World, collection: &str) -> Vec<Value> {
    w.ok(Request::LibraryCollection {
        collection: Some(collection.into()),
    })
    .await["entries"]
        .as_array()
        .unwrap()
        .clone()
}

/// law: lib.withdrawal-keeps-history
#[devkit::test]
async fn untag_uncollect_and_undo_keep_history_and_withdrawing_them_restores(
    db: devkit::pg::TestDatabase,
) {
    let mut w = world(db).await;
    let a = item(&mut w, "a.txt", "a").await;

    w.curate(json!([{"act": "tag", "target": t(&a), "tag": "kept"}]))
        .await;
    let untag = w
        .curate(json!([{"act": "untag", "target": t(&a), "tag": "kept"}]))
        .await;
    let v = view(&mut w, &a).await;
    assert_eq!(tag_state(&v, "kept"), None, "the view excludes it");
    let withdrawn: Vec<&Value> = v["history"]
        .as_array()
        .unwrap()
        .iter()
        .filter(|r| r["kind"] == "tag" && r["key"] == "kept")
        .collect();
    assert_eq!(withdrawn.len(), 1, "the record stays live as history");
    assert_eq!(withdrawn[0]["withdrawn"], true);
    w.curate(json!([{"act": "withdraw", "record": root(&written(&untag, 0))}]))
        .await;
    assert_eq!(
        tag_state(&view(&mut w, &a).await, "kept").as_deref(),
        Some("applies")
    );

    w.curate(json!([{"act": "collect", "target": t(&a), "collection": "c"}]))
        .await;
    let entry = entries(&mut w, "c").await[0]["entry"]
        .as_str()
        .unwrap()
        .to_owned();
    let uncollect = w
        .curate(json!([{"act": "uncollect", "entry": entry}]))
        .await;
    assert!(entries(&mut w, "c").await.is_empty());
    w.curate(json!([{"act": "withdraw", "record": root(&written(&uncollect, 0))}]))
        .await;
    assert_eq!(entries(&mut w, "c").await[0]["entry"], json!(entry));

    let note = w
        .curate(json!([{"act": "note", "target": t(&a), "text": "synthetic note"}]))
        .await;
    let undo = w
        .curate(json!([{"act": "undo", "request": note["request"]}]))
        .await;
    assert!(
        view(&mut w, &a).await["notes"]
            .as_array()
            .unwrap()
            .is_empty()
    );
    w.curate(json!([{"act": "undo", "request": undo["request"]}]))
        .await;
    assert_eq!(view(&mut w, &a).await["notes"][0]["text"], "synthetic note");
}

/// law: lib.undo-refuses-dependents
#[devkit::test]
async fn undo_with_a_later_dependent_record_is_refused_and_changes_nothing(
    db: devkit::pg::TestDatabase,
) {
    let mut w = world(db).await;
    let a = item(&mut w, "a.txt", "a").await;
    let first = w
        .curate(json!([{"act": "set", "target": t(&a), "property": "medium", "value": "one"}]))
        .await;
    let second = w
        .curate(json!([{"act": "set", "target": t(&a), "property": "medium", "value": "two"}]))
        .await;
    assert_eq!(second["writes"][0]["revised"][0], json!(written(&first, 0)));
    let refused = w
        .curate_as(
            "operator",
            json!([{"act": "undo", "request": first["request"]}]),
        )
        .await
        .unwrap_err();
    assert!(refused.contains(&written(&second, 0)), "{refused}");
    assert_eq!(
        view(&mut w, &a).await["properties"]["medium"]["value"],
        "two"
    );
    w.curate(json!([{"act": "undo", "request": second["request"]}]))
        .await;
    assert_eq!(
        view(&mut w, &a).await["properties"]["medium"]["value"],
        "one"
    );
}

/// law: lib.conflict-on-same-revision
#[devkit::test]
async fn two_replacements_from_one_revision_give_one_success_and_one_conflict(
    db: devkit::pg::TestDatabase,
) {
    let mut w = world(db).await;
    let a = item(&mut w, "a.txt", "a").await;
    let base = w
        .curate(json!([{"act": "set", "target": t(&a), "property": "medium", "value": "one"}]))
        .await;
    let seen = written(&base, 0);
    let won = w
        .curate(json!([{"act": "set", "target": t(&a), "property": "medium", "value": "two", "expects": seen}]))
        .await;
    let current = written(&won, 0);
    for late in [
        json!({"act": "set", "target": t(&a), "property": "medium", "value": "three", "expects": seen}),
        json!({"act": "set", "target": t(&a), "property": "medium", "value": null, "expects": seen}),
        json!({"act": "withdraw", "record": root(&seen), "expects": seen}),
    ] {
        let refused = w
            .curate_as("operator", json!([late.clone()]))
            .await
            .unwrap_err();
        assert!(refused.contains(&current), "{late}: {refused}");
    }
    assert_eq!(
        view(&mut w, &a).await["properties"]["medium"]["value"],
        "two"
    );
}

/// law: lib.atomic-actions
#[devkit::test]
async fn a_failed_part_of_a_request_leaves_none_of_its_records(db: devkit::pg::TestDatabase) {
    let mut w = world(db).await;
    let a = item(&mut w, "a.txt", "a").await;
    let first = w
        .curate(json!([{"act": "set", "target": t(&a), "property": "medium", "value": "one"}]))
        .await;
    let seen = written(&first, 0);
    w.curate(json!([{"act": "set", "target": t(&a), "property": "medium", "value": "two"}]))
        .await;
    let before: i64 = count(&mut w).await;
    for refused in [
        // Invalid: rating on an undefined scale.
        json!([
            {"act": "tag", "target": t(&a), "tag": "x"},
            {"act": "collect", "target": t(&a), "collection": "c"},
            {"act": "rate", "target": t(&a), "scale": "undefined", "value": 1},
        ]),
        // Refused by the planner: the third write's base is stale.
        json!([
            {"act": "tag", "target": t(&a), "tag": "x"},
            {"act": "collect", "target": t(&a), "collection": "c"},
            {"act": "set", "target": t(&a), "property": "medium", "value": "z", "expects": seen},
        ]),
    ] {
        assert!(w.curate_as("operator", refused).await.is_err());
        assert_eq!(count(&mut w).await, before);
    }
    let v = view(&mut w, &a).await;
    assert_eq!(tag_state(&v, "x"), None);
    assert!(v["collections"].as_array().unwrap().is_empty());
}

async fn count(w: &mut World) -> i64 {
    sqlx::query_scalar("SELECT count(*) FROM core.events WHERE event_type = 'library.curation'")
        .fetch_one(&mut w.conn)
        .await
        .unwrap()
}

/// law: lib.computed-authorship
#[devkit::test]
async fn authorship_comes_from_the_caller_and_decides_precedence(db: devkit::pg::TestDatabase) {
    let mut w = world(db).await;
    let a = item(&mut w, "a.txt", "a").await;
    let set = |value: &str| json!([{"act": "set", "target": t(&a), "property": "medium", "value": value}]);
    w.curate_as("agent:one", set("agent value")).await.unwrap();
    w.curate_as("agent:two", set("other agent value"))
        .await
        .unwrap();
    // Two agents disagreeing is a conflict listing both.
    let v = view(&mut w, &a).await;
    assert_eq!(v["properties"]["medium"]["state"], "conflict");
    assert_eq!(v["properties"]["medium"]["level"], "agent");
    let supporters: Vec<&str> = v["properties"]["medium"]["values"]
        .as_array()
        .unwrap()
        .iter()
        .flat_map(|pair| pair[1].as_array().unwrap())
        .map(|s| s["author"].as_str().unwrap())
        .collect();
    assert_eq!(supporters.len(), 2, "{v}");
    assert!(supporters.contains(&"agent:one") && supporters.contains(&"agent:two"));
    // The operator's value ranks above every agent's.
    let operator = w.curate(set("operator value")).await;
    let v = view(&mut w, &a).await;
    assert_eq!(v["properties"]["medium"]["value"], "operator value");
    assert_eq!(v["properties"]["medium"]["level"], "operator");
    // Each record carries the author the daemon computed, never a field of
    // the request.
    for record in v["history"].as_array().unwrap() {
        assert!(
            ["operator", "agent:one", "agent:two"].contains(&record["author"].as_str().unwrap()),
            "{record}"
        );
    }
    assert!(
        serde_json::from_value::<Vec<sinex_model::event_types::library::Act>>(json!([
            {"act": "set", "target": t(&a), "property": "medium", "value": "x", "author": "operator"}
        ]))
        .is_err(),
        "no request names an author"
    );
    // Withdrawing the operator's value shows the agents' conflict again.
    w.curate(json!([{"act": "withdraw", "record": root(&written(&operator, 0))}]))
        .await;
    assert_eq!(
        view(&mut w, &a).await["properties"]["medium"]["level"],
        "agent"
    );
}

/// law: lib.pinned-entries
#[devkit::test]
async fn entries_pin_their_revision_and_removal_addresses_one_entry(db: devkit::pg::TestDatabase) {
    let mut w = world(db).await;
    let a = item(&mut w, "a.txt", "first bytes").await;
    let b = item(&mut w, "b.txt", "b").await;
    w.curate(json!([
        {"act": "collect", "target": t(&a), "collection": "c"},
        {"act": "collect", "target": t(&b), "collection": "c"},
        {"act": "collect", "target": t(&a), "collection": "c"},
    ]))
    .await;
    let listed = entries(&mut w, "c").await;
    let items: Vec<&str> = listed.iter().map(|e| e["item"].as_str().unwrap()).collect();
    assert_eq!(items, [a.as_str(), b.as_str(), a.as_str()]);
    let pinned = listed[0]["revision"].clone();
    w.curate(json!([{"act": "uncollect", "entry": listed[2]["entry"]}]))
        .await;
    let left = entries(&mut w, "c").await;
    assert_eq!(left.len(), 2);
    assert_eq!(left[0]["entry"], listed[0]["entry"], "the first A remains");
    // Editing A gives it a new revision; the entry still opens the old one.
    std::fs::write(w.root.join("a.txt"), "second bytes").unwrap();
    w.ok(Request::LibraryAdd {
        path: w.root.join("a.txt").display().to_string(),
        reference: None,
    })
    .await;
    assert_ne!(view(&mut w, &a).await["revision"], pinned);
    assert_eq!(entries(&mut w, "c").await[0]["revision"], pinned);
    // Moving an entry is a revision of that entry only.
    let moved = w
        .curate(json!([{"act": "move", "entry": left[0]["entry"], "after": left[1]["entry"]}]))
        .await;
    assert_eq!(moved["writes"][0]["revised"].as_array().unwrap().len(), 1);
    let order: Vec<Value> = entries(&mut w, "c")
        .await
        .iter()
        .map(|e| e["entry"].clone())
        .collect();
    assert_eq!(order, [left[1]["entry"].clone(), left[0]["entry"].clone()]);
}

/// law: lib.tag.any-label
#[devkit::test]
async fn punctuation_labels_are_written_and_shown_as_ordinary_tags(db: devkit::pg::TestDatabase) {
    let mut w = world(db).await;
    let a = item(&mut w, "a.txt", "a").await;
    w.curate(json!([
        {"act": "tag", "target": t(&a), "tag": "-"},
        {"act": "tag", "target": t(&a), "tag": "!x"},
    ]))
    .await;
    let v = view(&mut w, &a).await;
    assert_eq!(tag_state(&v, "-").as_deref(), Some("applies"));
    assert_eq!(tag_state(&v, "!x").as_deref(), Some("applies"));
    // Tagging again writes nothing.
    let again = w
        .curate(json!([{"act": "tag", "target": t(&a), "tag": "-"}]))
        .await;
    assert!(again["written"].as_array().unwrap().is_empty());
    assert!(again["writes"][0]["already"].is_array());
}

#[devkit::test]
async fn a_retried_request_is_answered_without_writing_and_other_content_is_refused(
    db: devkit::pg::TestDatabase,
) {
    let mut w = world(db).await;
    let a = item(&mut w, "a.txt", "a").await;
    let writes: Vec<sinex_model::event_types::library::Act> =
        serde_json::from_value(json!([{"act": "note", "target": t(&a), "text": "once"}])).unwrap();
    let send = |writes| Request::LibraryCurate {
        request: Some("retry-1".into()),
        writes,
    };
    let first = w.ok(send(writes.clone())).await;
    let before = count(&mut w).await;
    let again = w.ok(send(writes)).await;
    assert_eq!(again["repeat"], true);
    assert_eq!(again["written"], first["written"]);
    assert_eq!(count(&mut w).await, before);
    let other =
        serde_json::from_value(json!([{"act": "note", "target": t(&a), "text": "twice"}])).unwrap();
    assert!(w.run(send(other)).await.is_err());
}

#[devkit::test]
async fn a_plan_is_not_admitted_after_curation_changed_under_it(db: devkit::pg::TestDatabase) {
    let mut w = world(db).await;
    let a = item(&mut w, "a.txt", "a").await;
    let acts =
        serde_json::from_value::<Vec<_>>(json!([{"act": "tag", "target": t(&a), "tag": "x"}]))
            .unwrap();
    let caller = Caller {
        author: "operator".into(),
        via: "api".into(),
    };
    let at = Timestamp::now();
    let Outcome::Written(stale) = plan::plan(&mut w.conn, None, &acts, &caller, at)
        .await
        .unwrap()
    else {
        panic!("a new request")
    };
    // Another request commits a curation record after the plan read.
    w.curate(json!([{"act": "tag", "target": t(&a), "tag": "x"}]))
        .await;
    let library = Library {
        writer: &w.writer,
        blobs: &w.blobs,
        host: "h",
        roots: &[],
        time_settings: sinex_model::time_settings::TimeSettings::new("Europe/Warsaw", 4, 0)
            .unwrap(),
    };
    let refused = library
        .store_planned(&mut w.conn, stale, "operator", at, "curate")
        .await
        .unwrap_err();
    assert!(
        refused.contains("changed while this request was planned"),
        "{refused}"
    );
    let applied = view(&mut w, &a).await["tags"][0]["applied_by"]
        .as_array()
        .unwrap()
        .len();
    assert_eq!(applied, 1, "only the request that planned on current state");
}

#[devkit::test]
async fn a_stale_base_or_withdrawal_expectation_is_refused_inside_admission(
    db: devkit::pg::TestDatabase,
) {
    let mut w = world(db).await;
    let a = item(&mut w, "a.txt", "a").await;
    let first = w
        .curate(json!([{"act": "set", "target": t(&a), "property": "medium", "value": "one"}]))
        .await;
    let caller = Caller {
        author: "operator".into(),
        via: "api".into(),
    };
    // The statement's current revision when each stale request is planned.
    let mut current = written(&first, 0);
    for withdrawal in [false, true] {
        let acts = if withdrawal {
            json!([{"act": "withdraw", "record": root(&current), "expects": current}])
        } else {
            json!([{"act": "set", "target": t(&a), "property": "medium", "value": "late"}])
        };
        let acts = serde_json::from_value::<Vec<_>>(acts).unwrap();
        let at = Timestamp::now();
        let Outcome::Written(stale) = plan::plan(&mut w.conn, None, &acts, &caller, at)
            .await
            .unwrap()
        else {
            panic!("a new request")
        };
        // Another request revises the record after the plan read it.
        let value = if withdrawal { "later" } else { "between" };
        let between = w
            .curate(json!([{"act": "set", "target": t(&a), "property": "medium", "value": value}]))
            .await;
        current = written(&between, 0);
        let library = Library {
            writer: &w.writer,
            blobs: &w.blobs,
            host: "h",
            roots: &[],
            time_settings: sinex_model::time_settings::TimeSettings::new("Europe/Warsaw", 4, 0)
                .unwrap(),
        };
        let refused = library
            .store_planned(&mut w.conn, stale, "operator", at, "curate")
            .await
            .unwrap_err();
        assert!(refused.contains("is not current"), "{refused}");
    }
    assert_eq!(
        view(&mut w, &a).await["properties"]["medium"]["value"],
        "later"
    );
}

#[devkit::test]
async fn expects_names_only_a_revision_of_the_callers_own_statement(db: devkit::pg::TestDatabase) {
    let mut w = world(db).await;
    let a = item(&mut w, "a.txt", "a").await;
    let operator = w
        .curate(json!([{"act": "set", "target": t(&a), "property": "medium", "value": "one"}]))
        .await;
    let tag = w
        .curate(json!([{"act": "tag", "target": t(&a), "tag": "x"}]))
        .await;
    for (author, expects) in [
        ("agent", written(&operator, 0)),
        ("operator", written(&tag, 0)),
    ] {
        let refused = w
            .curate_as(
                author,
                json!([{"act": "set", "target": t(&a), "property": "medium", "value": "two", "expects": expects}]),
            )
            .await
            .unwrap_err();
        assert!(refused.contains("is not a revision of your"), "{refused}");
    }
    let v = view(&mut w, &a).await;
    assert_eq!(v["properties"]["medium"]["value"], "one");
    assert_eq!(tag_state(&v, "x").as_deref(), Some("applies"));
}

#[devkit::test]
async fn untagging_after_a_rejection_leaves_no_statement(db: devkit::pg::TestDatabase) {
    let mut w = world(db).await;
    let a = item(&mut w, "a.txt", "a").await;
    w.curate(json!([{"act": "reject", "target": t(&a), "tag": "x"}]))
        .await;
    w.curate(json!([{"act": "tag", "target": t(&a), "tag": "x"}]))
        .await;
    assert_eq!(
        tag_state(&view(&mut w, &a).await, "x").as_deref(),
        Some("applies")
    );
    w.curate(json!([{"act": "untag", "target": t(&a), "tag": "x"}]))
        .await;
    assert_eq!(tag_state(&view(&mut w, &a).await, "x"), None);
}

/// Stored curation records written as they are, without planning, for
/// laws about reading them back.
async fn store_raw(w: &mut World, records: Vec<Curation>) {
    let library = Library {
        writer: &w.writer,
        blobs: &w.blobs,
        host: "h",
        roots: &[],
        time_settings: sinex_model::time_settings::TimeSettings::new("Europe/Warsaw", 4, 0)
            .unwrap(),
    };
    library
        .write(
            &mut w.conn,
            records.into_iter().map(LibraryRecord::Curation).collect(),
            "operator",
            Stored {
                action: "test",
                via: "test",
                at: Timestamp::now(),
                cited: Vec::new(),
                curation: false,
            },
        )
        .await
        .unwrap();
}

fn stored(header: &authored::Header, case: usize) -> Curation {
    let name = |id: &str| format!("c{case}{id}");
    Curation {
        root: name(&header.root),
        rev: name(&header.rev),
        revises: header
            .revises
            .iter()
            .map(|r| format!("{}@{}", name(&r.root), name(&r.rev)))
            .collect(),
        withdraws: header
            .withdraws
            .iter()
            .map(|t| match t {
                authored::Target::Root(root) => name(root),
                authored::Target::Revision(r) => format!("{}@{}", name(&r.root), name(&r.rev)),
            })
            .collect(),
        kind: if header.kind == authored::generator::WITHDRAWAL {
            CurationKind::Withdrawal
        } else {
            CurationKind::Note
        },
        target: None,
        key: None,
        value: None,
        origin: ValueOrigin::Authored,
        cites: None,
        author: header.author.clone(),
        via: header.via.clone(),
        at: Timestamp::now(),
        request: name(&header.rev),
        request_digest: String::new(),
        slots: Vec::new(),
    }
}

/// law: lib.currency.shared
/// law: authored.currency.middle-withdrawal
#[devkit::test]
async fn stored_currency_equals_the_shared_function_on_generated_histories(
    db: devkit::pg::TestDatabase,
) {
    let mut w = world(db).await;
    let mut runner = proptest::test_runner::TestRunner::deterministic();
    let strategy = authored::generator::history(24);
    for case in 0..12 {
        let mut history = strategy.new_tree(&mut runner).unwrap().current();
        let reference: Vec<RootState> = history.states().unwrap();
        // Arrive in reverse order, split over two requests.
        let mut records: Vec<Curation> = history
            .records()
            .iter()
            .map(|header| stored(header, case))
            .collect();
        records.reverse();
        let later = records.split_off(records.len() / 2);
        for part in [records, later] {
            if !part.is_empty() {
                store_raw(&mut w, part).await;
            }
        }
        for expected in reference {
            // Each record read on its own loads what its currency needs.
            let mut snapshot = Snapshot::new();
            let root = format!("c{case}{}", expected.root);
            snapshot
                .load_roots(&mut w.conn, [root.clone()])
                .await
                .unwrap();
            let state = snapshot.state(&root).unwrap();
            let heads: Vec<String> = state
                .heads()
                .iter()
                .map(|r| r.rev.trim_start_matches(&format!("c{case}")).to_owned())
                .collect();
            let want: Vec<String> = expected.heads().iter().map(|r| r.rev.clone()).collect();
            assert_eq!(heads, want, "case {case}, record {}", expected.root);
            assert_eq!(state.revisions.len(), expected.revisions.len());
        }
    }

    // The hand-stated case through ordinary requests: R1 <- R2 <- R3.
    let a = item(&mut w, "a.txt", "a").await;
    let set = |value: &str| json!([{"act": "set", "target": t(&a), "property": "medium", "value": value}]);
    let r1 = written(&w.curate(set("one")).await, 0);
    let r2 = written(&w.curate(set("two")).await, 0);
    let r3 = written(&w.curate(set("three")).await, 0);
    let shown = |v: Value| v["properties"]["medium"]["value"].clone();
    let withdraw_r2 = w.curate(json!([{"act": "withdraw", "record": r2}])).await;
    assert_eq!(shown(view(&mut w, &a).await), "three", "R3 stays current");
    assert_eq!(
        view(&mut w, &a).await["properties"]["medium"]["state"],
        "value"
    );
    let withdraw_r3 = w.curate(json!([{"act": "withdraw", "record": r3}])).await;
    assert_eq!(
        shown(view(&mut w, &a).await),
        "one",
        "R1 with R2 and R3 withdrawn"
    );
    w.curate(json!([{"act": "withdraw", "record": root(&written(&withdraw_r2, 0))}]))
        .await;
    assert_eq!(shown(view(&mut w, &a).await), "two");
    w.curate(json!([{"act": "withdraw", "record": root(&written(&withdraw_r3, 0))}]))
        .await;
    assert_eq!(shown(view(&mut w, &a).await), "three");
    let _ = r1;
}

/// The projection the function gives over every live curation record,
/// computed from the whole live set held in memory, independent of the
/// writer's refresh and of the loading the daemon does.
async fn expected_projection(w: &mut World) -> Vec<sinex_store::library_shown::Row> {
    let rows = sinex_store::payloads::live_all(
        &mut w.conn,
        "library.curation",
        sinex_store::payloads::RecordStates::Any,
    )
    .await
    .unwrap();
    let all: Vec<Curation> = rows
        .into_iter()
        .map(|row| serde_json::from_value(row).unwrap())
        .collect();
    let mut history = History::new();
    for record in &all {
        history.push(header(record).unwrap(), Vec::new());
    }
    let current = history.current().unwrap();
    let current: Vec<Curation> = all
        .into_iter()
        .filter(|r| current.contains(&authored::Revision::new(&r.root, &r.rev)))
        .collect();
    let mut rows = sinex_library::projection::rows(&shown::values(&current));
    rows.sort_by(|a, b| (&a.target, &a.kind, &a.key).cmp(&(&b.target, &b.kind, &b.key)));
    rows
}

async fn curation_events(w: &mut World, archived: bool) -> Vec<uuid::Uuid> {
    sqlx::query_scalar(
        "SELECT e.id FROM core.events e
         WHERE e.event_type = 'library.curation'
           AND EXISTS (SELECT 1 FROM audit.archived a WHERE a.event_id = e.id) = $1
         ORDER BY e.commit_seq",
    )
    .bind(archived)
    .fetch_all(&mut w.conn)
    .await
    .unwrap()
}

async fn replay_curation(w: &mut World) {
    let report = crate::replay::run(
        &mut w.conn,
        &w.blobs,
        &w.writer,
        crate::replay::Options {
            scope: crate::replay::Scope {
                event_types: Some(vec!["library.curation".into()]),
                ..Default::default()
            },
            compare: false,
            resume: None,
            actor: "operator".into(),
            unlock: None,
        },
    )
    .await
    .unwrap();
    assert_eq!(report.failures, 0);
}

/// law: lib.projection.equals-function
#[devkit::test]
async fn the_projection_equals_the_function_after_generated_writes_withdrawals_archives_and_replays(
    db: devkit::pg::TestDatabase,
) {
    let mut w = world(db).await;
    let items = [
        item(&mut w, "a.txt", "a").await,
        item(&mut w, "b.txt", "b").await,
    ];
    let authors = ["operator", "agent:one", "agent:two"];
    let mut runner = proptest::test_runner::TestRunner::deterministic();
    let steps = proptest::collection::vec(proptest::arbitrary::any::<(u8, u8, u8, u8)>(), 12..20);
    for case in 0..6 {
        for (kind, a, b, c) in steps.new_tree(&mut runner).unwrap().current() {
            let target = t(&items[usize::from(a) % items.len()]);
            let author = authors[usize::from(b) % authors.len()];
            let tag = format!("t{}", c % 3);
            let property = format!("p{}", c % 2);
            let value = match b % 3 {
                0 => Value::Null,
                n => json!(format!("v{n}")),
            };
            let act = match kind % 9 {
                0 | 1 => Some(json!({"act": "tag", "target": target, "tag": tag})),
                2 => Some(json!({"act": "untag", "target": target, "tag": tag})),
                3 => Some(json!({"act": "reject", "target": target, "tag": tag})),
                4 | 5 => Some(
                    json!({"act": "set", "target": target, "property": property, "value": value}),
                ),
                6 => {
                    let records: Vec<String> = sqlx::query_scalar(
                        "SELECT payload->>'root' FROM core.events WHERE event_type = 'library.curation' ORDER BY commit_seq",
                    )
                    .fetch_all(&mut w.conn)
                    .await
                    .unwrap();
                    records
                        .get(usize::from(c) % records.len().max(1))
                        .map(|root| json!({"act": "withdraw", "record": root}))
                }
                7 => {
                    let (live, archived) = (
                        curation_events(&mut w, false).await,
                        curation_events(&mut w, true).await,
                    );
                    let (act, pool) = if c % 2 == 0 {
                        (sinex_model::lifecycle::Act::Archive, live)
                    } else {
                        (sinex_model::lifecycle::Act::Restore, archived)
                    };
                    if !pool.is_empty() {
                        // A restore that would collide with a newer live
                        // reading (after a replay) is refused and moves
                        // nothing; that is part of the sequence too.
                        let scope = sinex_model::lifecycle::Scope::Events {
                            events: vec![sinex_model::EventId(pool[usize::from(a) % pool.len()])],
                            query: None,
                        };
                        crate::lifecycle::run(
                            &mut w.conn,
                            &w.writer,
                            act,
                            "operator",
                            scope,
                            false,
                        )
                        .await
                        .unwrap();
                    }
                    None
                }
                _ => {
                    replay_curation(&mut w).await;
                    None
                }
            };
            if let Some(act) = act {
                // Refusals (nothing to untag, a slot already held) are part
                // of the sequence; they write nothing.
                let _ = w.curate_as(author, json!([act])).await;
            }
            let mut stored = sinex_store::library_shown::all(&mut w.conn).await.unwrap();
            stored.sort_by(|a, b| (&a.target, &a.kind, &a.key).cmp(&(&b.target, &b.kind, &b.key)));
            assert_eq!(stored, expected_projection(&mut w).await, "case {case}");
        }
    }
    assert!(
        !sinex_store::library_shown::all(&mut w.conn)
            .await
            .unwrap()
            .is_empty(),
        "the sequences left shown values to compare"
    );
}
