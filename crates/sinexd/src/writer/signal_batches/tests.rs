use super::*;
use serde_json::json;
use sinex_model::event_types::{
    operator::LogEntry,
    signal::{DeliveryRecorded, HandoffRecorded},
};
use sinex_model::{
    Class, Event, EventPayload, MaterialId, ReadingOrder, RecordLocation, RunId, Timestamp,
};
use sinex_store::{AdmissionContext, NoDerivations, privacy};

fn batch(events: Vec<Event>) -> Batch {
    Batch {
        run: RunId::new(),
        kind: BatchKind::Api {
            actor: "synthetic".into(),
        },
        records: vec![RecordOutcome::Events {
            location: None,
            events,
        }],
        archive: vec![],
    }
}
async fn write(conn: &mut AdmissionContext, batch: &Batch) -> Outcome {
    let mut tx = conn.begin().await.unwrap();
    let outcome = admit(&mut tx, batch, &NoDerivations).await.unwrap();
    tx.commit().await.unwrap();
    outcome
}
async fn fixture(conn: &mut AdmissionContext) -> (Batch, Event, EventId, EventId) {
    let material = MaterialId::new();
    let native_material = MaterialId::new();
    let parent = Event::build(
        &LogEntry {
            written_at: None,
            label: None,
            text: "synthetic copied detail".into(),
        },
        "synthetic.input",
        Origin::Record {
            location: RecordLocation {
                material: native_material,
                anchor: 0,
            },
            output_index: 0,
        },
        ReadingOrder(1),
        [1; 32],
        Class::Open,
    )
    .unwrap();
    let parent_id = parent.id;
    write(conn, &batch(vec![parent.clone()])).await;
    let read = sinex_store::ReadMark::now(conn).await.unwrap();
    let hash = format!("blake3:{}", "0".repeat(64));
    sqlx::query("INSERT INTO core.blobs(hash,size,status) VALUES($1,0,'present')")
        .bind(&hash)
        .execute(&mut **conn)
        .await
        .unwrap();
    sqlx::query("INSERT INTO raw.materials(id,blob,length,manifest,parser,parser_version) VALUES($1,$2,0,$3,'signals',1)").bind(material.0).bind(hash).bind(json!({"signal_read_mark":read})).execute(&mut **conn).await.unwrap();
    let trigger = ::signals::Trigger {
        rule: ::signals::RuleVersion {
            product: "sinex".into(),
            id: "authored:synthetic".into(),
            revision: 1,
        },
        occurrence: ::signals::Occurrence {
            id: "native".into(),
            episode: "native".into(),
        },
    };
    let id = trigger.delivery_id();
    let at = Timestamp::from_unix_secs(1_800_000_000).unwrap();
    let handoff = Event::build(
        &HandoffRecorded {
            delivery_id: id.clone(),
            channel: ::signals::Channel::Value,
            handed_at: at,
        },
        sinex_sources::sources::signals::SPEC.name,
        Origin::Record {
            location: RecordLocation {
                material,
                anchor: 0,
            },
            output_index: 0,
        },
        ReadingOrder(1),
        [2; 32],
        Class::Open,
    )
    .unwrap();
    let handoff_id = handoff.id;
    let delivery = ::signals::Delivery {
        id: id.clone(),
        trigger,
        matched: 1,
        at: 1,
        basis: vec![::signals::Row {
            occurrence: ::signals::Occurrence {
                id: "native".into(),
                episode: "native".into(),
            },
            reference: format!("sinex:ev/{parent_id}"),
            observed: 1,
            start: 1,
            end: None,
            origin: Default::default(),
            evidence: json!({"text":"synthetic copied detail"}),
        }],
        channel: ::signals::Channel::Value,
        downgrade: None,
        change: None,
        value: Some(1.0),
        scope: None,
    };
    let decision = Event::build(
        &DeliveryRecorded {
            delivery_id: id,
            delivery,
            handed_at: at,
        },
        sinex_sources::sources::signals::SPEC.name,
        Origin::Derived {
            parents: vec![parent_id, handoff_id],
        },
        ReadingOrder(1),
        [3; 32],
        Class::Open,
    )
    .unwrap();
    let decision_id = decision.id;
    (
        batch(vec![handoff, decision]),
        parent,
        handoff_id,
        decision_id,
    )
}
async fn visible(conn: &mut AdmissionContext, id: EventId) -> serde_json::Value {
    sqlx::query_scalar("SELECT payload FROM core.visible_events WHERE id=$1")
        .bind(id.0)
        .fetch_one(&mut **conn)
        .await
        .unwrap()
}
#[devkit::test]
async fn uncertified_completion_cannot_borrow_another_deliverys_current_material_mark(
    db: devkit::pg::TestDatabase,
) {
    let mut conn = AdmissionContext::connect(&db.url(None)).await.unwrap();
    let (offered, _, handoff, decision) = fixture(&mut conn).await;
    let fact = offered.events().find(|event| event.id == handoff).unwrap();
    let material = fact.location().unwrap().0.material;
    let delivery = fact.payload["delivery_id"].as_str().unwrap();
    sqlx::query("UPDATE raw.materials SET manifest=jsonb_set(manifest,'{uncertified_signals}',$2) WHERE id=$1")
        .bind(material.0)
        .bind(json!([delivery]))
        .execute(&mut *conn)
        .await
        .unwrap();
    assert!(
        sinex_store::signals::material_read_mark(&mut conn, material, "another-certified-delivery")
            .await
            .unwrap()
            .is_some()
    );
    let outcome = write(&mut conn, &offered).await;
    assert_eq!(outcome.admitted, 1);
    assert_eq!(outcome.superseded_input, 1);
    assert!(visible(&mut conn, handoff).await.get("basis").is_none());
    let copied: bool = sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM core.events WHERE id=$1)")
        .bind(decision.0)
        .fetch_one(&mut *conn)
        .await
        .unwrap();
    assert!(
        !copied,
        "the shared material's valid epoch cannot certify this completion"
    );
}
#[devkit::test]
async fn copied_delivery_follows_input_policy_while_physical_handoff_remains(
    db: devkit::pg::TestDatabase,
) {
    let mut conn = AdmissionContext::connect(&db.url(None)).await.unwrap();
    let (offered, _, handoff, decision) = fixture(&mut conn).await;
    assert_eq!(write(&mut conn, &offered).await.admitted, 2);
    assert_eq!(
        visible(&mut conn, decision).await["delivery"]["basis"][0]["evidence"]["text"],
        "synthetic copied detail"
    );
    privacy::add_rule(
        &mut conn,
        &privacy::NewRule {
            name: "synthetic masking".into(),
            source: "synthetic.input".into(),
            event_type: Some("operator_log.entry".into()),
            field: Some("/text".into()),
            matcher: "pattern".into(),
            matcher_arg: Some("synthetic copied detail".into()),
            class: Class::Open,
            action: privacy::Action::Mask,
            author: "synthetic".into(),
        },
    )
    .await
    .unwrap();
    privacy::projection::ensure_current(&mut conn)
        .await
        .unwrap();
    assert_eq!(
        visible(&mut conn, decision).await["unavailable"],
        "unavailable_dependency"
    );
    let (state, changed, payload): (String, bool, serde_json::Value) =
        sqlx::query_as("SELECT state,changed,payload FROM privacy.event_outputs WHERE event_id=$1")
            .bind(decision.0)
            .fetch_one(&mut *conn)
            .await
            .unwrap();
    assert_eq!(state, "unavailable_dependency");
    assert!(changed);
    assert_eq!(payload, json!({"unavailable":"privacy_dependency_changed"}));
    let actual = visible(&mut conn, handoff).await;
    assert_eq!(
        actual.as_object().unwrap().len(),
        3,
        "physical fact contains only ID, channel and time"
    );
    assert!(actual.get("basis").is_none());
    let outcome = write(&mut conn, &offered).await;
    assert_eq!(outcome.admitted, 0);
    assert_eq!(
        outcome.superseded_input, 1,
        "old copied bytes never acquire a fresh policy mark on reread"
    );
    assert_eq!(
        visible(&mut conn, decision).await["unavailable"],
        "unavailable_dependency"
    );
}
#[devkit::test]
async fn replayed_input_archives_copied_decision_but_never_repeats_physical_handoff(
    db: devkit::pg::TestDatabase,
) {
    let mut conn = AdmissionContext::connect(&db.url(None)).await.unwrap();
    let (offered, parent, handoff, decision) = fixture(&mut conn).await;
    write(&mut conn, &offered).await;
    let mut replacement = parent.clone();
    replacement.id = EventId::new();
    let mut replay = batch(vec![replacement]);
    replay.archive.push(sinex_model::ArchiveRequest::Replay {
        scope: sinex_model::ReplayScope {
            materials: vec![parent.location().unwrap().0.material],
            ..Default::default()
        },
        operation: sinex_model::OperationId::new(),
    });
    write(&mut conn, &replay).await;
    let archived: bool =
        sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM audit.archived WHERE event_id=$1)")
            .bind(decision.0)
            .fetch_one(&mut *conn)
            .await
            .unwrap();
    assert!(archived);
    let actual: bool =
        sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM audit.archived WHERE event_id=$1)")
            .bind(handoff.0)
            .fetch_one(&mut *conn)
            .await
            .unwrap();
    assert!(!actual);
    let outcome = write(&mut conn, &offered).await;
    assert_eq!(outcome.admitted, 0);
    assert_eq!(outcome.superseded_input, 1);
    let count: i64 = sqlx::query_scalar("SELECT count(*) FROM core.events WHERE event_type=$1")
        .bind(HandoffRecorded::EVENT_TYPE)
        .fetch_one(&mut *conn)
        .await
        .unwrap();
    assert_eq!(count, 1);
}
