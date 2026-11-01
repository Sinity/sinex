use super::*;
use sqlx::{Connection, PgConnection};

fn forwarded() -> Forwarded {
    let trigger = ::signals::Trigger {
        rule: ::signals::RuleVersion {
            product: "synthetic-peer".into(),
            id: "authored:controlled".into(),
            revision: 1,
        },
        occurrence: ::signals::Occurrence {
            id: "controlled-native".into(),
            episode: "one".into(),
        },
    };
    let id = trigger.delivery_id();
    let delivery = ::signals::Delivery {
        id: id.clone(),
        trigger,
        matched: 0,
        at: 0,
        basis: vec![],
        channel: ::signals::Channel::Interrupt,
        downgrade: None,
        change: None,
        value: None,
        scope: None,
    };
    Forwarded {
        kind: ForwardedKind::Intent,
        available: true,
        producer: "synthetic-peer".into(),
        event: sinex_model::EventId::new(),
        intent: ::signals::FiredIntent {
            delivery,
            author: ::signals::Author::Agent("fixture".into()),
            quiet_hours: None,
            rate_limit: None,
            current: true,
        },
        notice: ::signals::notifications::Notice {
            delivery_id: id,
            product: "synthetic-peer".into(),
            icon: "fixture".into(),
            rule: "authored:controlled".into(),
            occurrence: "controlled-native".into(),
            summary: "Synthetic".into(),
            body: "Synthetic".into(),
            reference: "status".into(),
            command_hint: "synthetic status".into(),
            interrupt: true,
            current: true,
            data_loss: false,
            budget_passive_count: None,
        },
    }
}

#[test]
fn frozen_basis_ignores_global_progress_but_rejects_relevant_authority_changes() {
    let original = sinex_store::signals::MatchBasis {
        parents: vec![sinex_model::EventId::new()],
        read: sinex_store::ReadMark {
            changes: 3,
            tombstoned: 4,
        },
        settings_identity: "configured-policy".into(),
    };
    let mut current = original.clone();
    current.read.changes += 5;
    current.read.tombstoned += 7;
    assert!(
        same_input_basis(&original, &current),
        "unrelated global progress is not a new input authority"
    );
    current = original.clone();
    current.parents = vec![sinex_model::EventId::new()];
    assert!(!same_input_basis(&original, &current));
    current = original.clone();
    current.settings_identity = "other-policy".into();
    assert!(!same_input_basis(&original, &current));
}

#[test]
fn offered_notice_must_match_frozen_words_and_group_while_passive_count_may_advance() {
    let peer = forwarded();
    let mut pending = PendingDelivery {
        handed_at: Timestamp::now(),
        delivery: peer.intent.delivery,
        rule: None,
        notice: Some(peer.notice),
        producer: Some(peer.producer),
        import: None,
        settings_identity: "configured-policy".into(),
        parents: vec![peer.event],
    };
    pending.delivery.channel = ::signals::Channel::Notice;
    pending.delivery.downgrade = Some(::signals::Downgrade::InterruptBudgetSpent);
    let mut offered = outlets::notice(&pending, 1);
    assert!(frozen_notice_matches(&offered, &pending, None));
    offered.budget_passive_count = Some(19);
    assert!(
        frozen_notice_matches(&offered, &pending, None),
        "numeric presentation progress does not replace the intent"
    );
    offered.body = "different evidence".into();
    assert!(!frozen_notice_matches(&offered, &pending, None));
    offered = outlets::notice(&pending, 1);
    offered.budget_passive_count = None;
    assert!(
        !frozen_notice_matches(&offered, &pending, None),
        "a different grouping class is not presentation progress"
    );
}

#[devkit::test]
async fn local_notice_words_survive_jsonb_order_changes_and_inherited_held_outboxes(
    db: devkit::pg::TestDatabase,
) {
    let peer = forwarded();
    let mut pending = PendingDelivery {
        handed_at: Timestamp::now(),
        delivery: peer.intent.delivery,
        rule: None,
        notice: None,
        producer: None,
        import: None,
        settings_identity: "configured-policy".into(),
        parents: vec![peer.event],
    };
    pending.delivery.basis.push(::signals::Row {
        occurrence: ::signals::Occurrence {
            id: "native-field-order".into(),
            episode: "one".into(),
        },
        reference: "synthetic:record".into(),
        observed: 0,
        start: 0,
        end: None,
        origin: ::signals::Origin::Source,
        evidence: serde_json::json!({"deliberately_long_first_key":"first", "a":"second"}),
    });
    let words = outlets::notice(&pending, 0);
    let mut conn = PgConnection::connect(&db.url(None)).await.unwrap();
    let roundtrip: serde_json::Value = sqlx::query_scalar("SELECT $1::jsonb")
        .bind(serde_json::to_value(&pending).unwrap())
        .fetch_one(&mut conn)
        .await
        .unwrap();
    let legacy: PendingDelivery = serde_json::from_value(roundtrip).unwrap();
    let canonical_words = outlets::notice(&legacy, 0);
    assert_ne!(
        canonical_words.body, words.body,
        "the independent JSONB oracle must actually reorder the evidence object"
    );
    assert!(
        frozen_notice_matches(&canonical_words, &legacy, None),
        "an inherited unattempted outbox uses its canonical loaded reading"
    );
    assert!(
        frozen_notice_matches(&words, &legacy, Some(&words)),
        "an inherited held attempt retains its already owned words"
    );
    outlets::freeze_notice(&mut pending);
    let roundtrip: serde_json::Value = sqlx::query_scalar("SELECT $1::jsonb")
        .bind(serde_json::to_value(&pending).unwrap())
        .fetch_one(&mut conn)
        .await
        .unwrap();
    let frozen: PendingDelivery = serde_json::from_value(roundtrip).unwrap();
    assert_eq!(outlets::notice(&frozen, 0).body, words.body);
    assert!(frozen_notice_matches(&words, &frozen, None));
}
