use super::*;
use sqlx::Connection;

fn calendar(at: Timestamp) -> ::signals::DayCalendar {
    let context = sinex_model::query::Context::new(
        jiff::Timestamp::from_nanosecond(at.unix_ns()).unwrap(),
        jiff::tz::TimeZone::get("UTC").unwrap(),
        jiff::civil::Time::constant(4, 0, 0, 0),
    );
    ::signals::DayCalendar::new(context.clock().unwrap().day)
}

fn checkpoint(at: Timestamp) -> Checkpoint {
    let clock = i64::try_from(at.unix_ns() / 1_000_000).unwrap();
    let calendar = calendar(at);
    let mut budget = ::signals::InterruptBudget {
        per_hour: 2,
        handoffs: vec![],
    };
    assert!(budget.admit(clock, &::signals::Author::Operator));
    assert!(budget.admit(clock, &::signals::Author::Agent("fixture".into())));
    assert_eq!(budget.handoffs.len(), 1);
    Checkpoint {
        settings_identity: "configured-day-policy".into(),
        evaluator: ::signals::Evaluator::new(
            clock,
            vec![],
            ::signals::Policy {
                private: vec![::signals::Interval {
                    start: clock + 1,
                    end: clock + 1000,
                }],
            },
            &calendar,
        )
        .unwrap(),
        host_policy: ::signals::HostPolicy::new(
            budget,
            Some(::signals::QuietHours {
                from: 1320,
                to: 480,
            }),
        ),
        notifications: Default::default(),
        forwarded: BTreeMap::new(),
        outbox: BTreeMap::new(),
        rule_events: BTreeMap::new(),
        rules: BTreeMap::new(),
        match_basis: BTreeMap::new(),
        current_inputs: BTreeMap::new(),
        rows: BTreeMap::new(),
        attempts: BTreeMap::new(),
        observed_seq: 0,
        passive_count: 0,
    }
}

#[devkit::test]
async fn public_policy_is_available_only_for_one_committed_config_and_exposes_counts(
    db: devkit::pg::TestDatabase,
) {
    let mut conn = PgConnection::connect(&db.url(None)).await.unwrap();
    let at = Timestamp::from_unix_secs(1_800_000_000).unwrap();
    assert!(matches!(
        public_host_policy(&mut conn, "fixture", false, "configured-day-policy", at)
            .await
            .unwrap(),
        HostPolicyProjection::Unavailable { .. }
    ));
    assert!(matches!(
        public_host_policy(&mut conn, "fixture", true, "configured-day-policy", at)
            .await
            .unwrap(),
        HostPolicyProjection::Pending { .. }
    ));
    let state = checkpoint(at);
    let mut tx = conn.begin().await.unwrap();
    assert_eq!(save(&mut tx, "fixture", None, &state).await.unwrap(), 1);
    tx.commit().await.unwrap();
    let projection = public_host_policy(&mut conn, "fixture", true, "configured-day-policy", at)
        .await
        .unwrap();
    assert_eq!(
        projection,
        HostPolicyProjection::Available {
            checkpoint_revision: 1,
            settings_identity: "configured-day-policy".into(),
            quiet_hours: Some(::signals::QuietHours {
                from: 1320,
                to: 480
            }),
            interrupt_budget: InterruptBudgetProjection {
                per_hour: 2,
                used: 1,
                remaining: 1,
                window_ms: ::signals::HOUR
            },
            observed_at: at
        }
    );
    let public = serde_json::to_value(projection).unwrap();
    for private in [
        "evaluator",
        "host_policy",
        "handoffs",
        "consumed",
        "pending",
        "private",
        "notifications",
        "forwarded",
        "outbox",
        "rule_events",
    ] {
        assert!(public.get(private).is_none(), "{private}");
    }
    let pending = serde_json::to_value(
        public_host_policy(&mut conn, "fixture", true, "new-config", at)
            .await
            .unwrap(),
    )
    .unwrap();
    assert_eq!(
        pending,
        serde_json::json!({"state":"pending","reason":"host_policy_settings_changed"})
    );
    sqlx::query("SET ROLE sinex_reader")
        .execute(&mut conn)
        .await
        .unwrap();
    let denied = sqlx::query("SELECT state FROM core.signal_checkpoints")
        .fetch_all(&mut conn)
        .await
        .unwrap_err();
    assert_eq!(
        denied
            .as_database_error()
            .and_then(|error| error.code())
            .as_deref(),
        Some("42501")
    );
}

#[devkit::test]
async fn stale_checkpoint_never_overwrites_consumption_or_new_settings(
    db: devkit::pg::TestDatabase,
) {
    let mut conn = PgConnection::connect(&db.url(None)).await.unwrap();
    let mut state = checkpoint(Timestamp::from_unix_secs(1_800_000_000).unwrap());
    let mut tx = conn.begin().await.unwrap();
    save(&mut tx, "fixture", None, &state).await.unwrap();
    tx.commit().await.unwrap();
    state.settings_identity = "new-config".into();
    let mut tx = conn.begin().await.unwrap();
    assert_eq!(save(&mut tx, "fixture", Some(1), &state).await.unwrap(), 2);
    tx.commit().await.unwrap();
    let mut stale = state.clone();
    stale.settings_identity = "stale".into();
    stale.host_policy.budget.handoffs.clear();
    let mut tx = conn.begin().await.unwrap();
    assert!(save(&mut tx, "fixture", Some(1), &stale).await.is_err());
    tx.rollback().await.unwrap();
    let saved = load(&mut conn, "fixture").await.unwrap().unwrap();
    assert_eq!(saved.revision, 2);
    assert_eq!(
        serde_json::to_value(saved.checkpoint).unwrap(),
        serde_json::to_value(state).unwrap()
    );
}

fn lineage_event(
    native: &str,
    material: sinex_model::MaterialId,
    output: u64,
) -> sinex_model::Event {
    use sinex_model::{
        Class, Event, Origin, ReadingOrder, RecordLocation, event_types::operator::LogEntry,
    };
    // Two synthetic source kinds exercise native-key and keyless coordinates
    // without deriving identity from the delivered presentation.
    if native.is_empty() {
        Event::build(
            &LogEntry {
                written_at: None,
                label: None,
                text: "same synthetic words".into(),
            },
            "synthetic.native",
            Origin::Record {
                location: RecordLocation {
                    material,
                    anchor: 0,
                },
                output_index: output,
            },
            ReadingOrder(1),
            [1; 32],
            Class::Open,
        )
        .unwrap()
    } else {
        use sinex_model::event_types::CommandExecuted;
        Event::build(
            &CommandExecuted {
                native_id: native.into(),
                command: "same synthetic words".into(),
                cwd: "/synthetic".into(),
                session: "session".into(),
                hostname: "host".into(),
                started_at: Timestamp::from_unix_secs(1_800_000_000).unwrap(),
                duration_ns: None,
                exit_code: Some(0),
                deleted_at: None,
                author: None,
                intent: None,
                shell: None,
                extra: Default::default(),
            },
            "synthetic.native",
            Origin::Record {
                location: RecordLocation {
                    material,
                    anchor: 0,
                },
                output_index: output,
            },
            ReadingOrder(1),
            [1; 32],
            Class::Open,
        )
        .unwrap()
    }
}
async fn admit_lineage(
    conn: &mut crate::AdmissionContext,
    events: Vec<sinex_model::Event>,
    archive: Vec<sinex_model::ArchiveRequest>,
) {
    let batch = sinex_model::Batch {
        run: sinex_model::RunId::new(),
        kind: sinex_model::BatchKind::Api {
            actor: "synthetic".into(),
        },
        records: vec![sinex_model::RecordOutcome::Events {
            location: None,
            events,
        }],
        archive,
    };
    let mut tx = conn.begin().await.unwrap();
    tx.admit(&batch, &crate::NoDerivations).await.unwrap();
    tx.commit().await.unwrap();
}
#[devkit::test]
async fn fresh_signal_evidence_requires_native_lineage_not_equal_words(
    db: devkit::pg::TestDatabase,
) {
    let mut conn = crate::AdmissionContext::connect(&db.url(None))
        .await
        .unwrap();
    let material = sinex_model::MaterialId::new();
    let first = lineage_event("native-a", material, 0);
    let first_id = first.id;
    let unrelated = lineage_event("native-b", material, 1);
    let unrelated_id = unrelated.id;
    let keyless = lineage_event("", material, 0);
    let keyless_id = keyless.id;
    let another_output = lineage_event("", material, 1);
    let output_id = another_output.id;
    admit_lineage(
        &mut conn,
        vec![first.clone(), unrelated, keyless.clone(), another_output],
        vec![],
    )
    .await;
    assert!(
        !same_input_lineage(&mut conn, &[first_id], &[unrelated_id])
            .await
            .unwrap()
    );
    assert!(
        !same_input_lineage(&mut conn, &[keyless_id], &[output_id])
            .await
            .unwrap()
    );
    let mut reread = first;
    reread.id = EventId::new();
    let reread_id = reread.id;
    let mut keyless_reread = keyless;
    keyless_reread.id = EventId::new();
    let keyless_reread_id = keyless_reread.id;
    admit_lineage(
        &mut conn,
        vec![reread, keyless_reread],
        vec![sinex_model::ArchiveRequest::Replay {
            scope: sinex_model::ReplayScope {
                materials: vec![material],
                ..Default::default()
            },
            operation: sinex_model::OperationId::new(),
        }],
    )
    .await;
    assert!(
        same_input_lineage(
            &mut conn,
            &[first_id, keyless_id],
            &[keyless_reread_id, reread_id]
        )
        .await
        .unwrap(),
        "reading IDs and parent order change without inventing a new native occurrence"
    );
    sqlx::query("DELETE FROM privacy.original_events WHERE event_id=$1")
        .bind(first_id.0)
        .execute(&mut *conn)
        .await
        .unwrap();
    assert!(
        !same_input_lineage(&mut conn, &[first_id], &[reread_id])
            .await
            .unwrap(),
        "unknown original lineage cannot be proved by equal current words"
    );
}
