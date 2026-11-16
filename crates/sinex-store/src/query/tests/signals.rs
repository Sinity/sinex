//! Signal adapters preserve record identity independently of displayed fields.
use super::*;
async fn standing_query(
    conn: &mut PgConnection,
    text: &str,
    context: &Context,
    seq: i64,
) -> crate::Result<crate::query::signals::SignalAnswer> {
    crate::query::initialize_calendar(conn).await?;
    crate::query::signals::standing_query(conn, text, context, seq, "sinex.signals").await
}

#[devkit::test]
async fn standing_queries_keep_record_and_group_keys_through_fields_and_replay(
    db: devkit::pg::TestDatabase,
) {
    let mut conn = crate::AdmissionContext::connect(&db.url(None))
        .await
        .unwrap();
    let ids = world(&mut conn).await;
    let mut tx = conn.begin().await.unwrap();
    let seq = current_seq(&mut tx).await.unwrap();
    let plain = standing_query(
        &mut tx,
        "commands | sort by when | fields command",
        &ctx(),
        seq,
    )
    .await
    .unwrap();
    assert_eq!(plain.rows.len(), 5);
    assert!(plain.complete);
    assert!(plain.rows.iter().all(|row| {
        row.reference
            .as_deref()
            .is_some_and(|reference| reference.starts_with("sinex:ev/"))
    }));
    assert!(
        plain
            .rows
            .iter()
            .all(|row| { row.identity.starts_with("native:") })
    );
    assert!(
        plain
            .rows
            .iter()
            .all(|row| row.result.as_object().unwrap().len() == 1)
    );
    let keys = plain
        .rows
        .iter()
        .map(|row| row.identity.clone())
        .collect::<std::collections::BTreeSet<_>>();
    assert_eq!(keys.len(), 5);
    let group = standing_query(
        &mut tx,
        "commands | group by cwd | count | sort by cwd | fields count",
        &ctx(),
        seq,
    )
    .await
    .unwrap();
    assert_eq!(group.rows.len(), 4);
    assert_eq!(group.parents.len(), 5);
    assert!(group.rows.iter().all(|row| row.reference.is_none()));
    assert_eq!(
        group
            .rows
            .iter()
            .map(|row| row.identity.as_str())
            .collect::<Vec<_>>(),
        [
            "[\"/home/iris\"]",
            "[\"/src/tide\"]",
            "[\"/src/tide/tools\"]",
            "[\"/src/tidepool\"]"
        ]
    );
    tx.rollback().await.unwrap();
    // A new interpretation of one derived key changes the event ID,
    // while the standing occurrence remains the same cited key.
    let old = ids[0].0;
    let mut event = Event::build(
        &CommandFused {
            record: format!("command.executed:{}", ids[0].1),
            command: "a changed interpretation".into(),
            host: "kestrel".into(),
            started_at: Timestamp::from_unix_secs(BASE).unwrap(),
            cwd: Some("/src/tide".into()),
            session: Some("s".into()),
            duration_ns: Some(2_000_000_000),
            exit_code: Some(0),
            execution: None,
            members: vec![sinex_model::event_types::fused::Member {
                record: format!("fixture:{}", "terminal.atuin"),
                source: "terminal.atuin".into(),
                event_type: "command.executed".into(),
                joined: sinex_model::event_types::fused::Joined::Lead,
            }],
            basis: sinex_model::event_types::fused::Basis::Rule,
            ambiguous: 1,
            competitors: vec![],
            rule_version: 1,
        },
        "derive.shell-fusion",
        Origin::Derived {
            parents: vec![EventId(ids[0].1)],
        },
        ReadingOrder(2),
        [9; 32],
        Class::Open,
    )
    .unwrap();
    let new = event.id;
    event.reading_order = ReadingOrder(2);
    let batch = Batch {
        run: RunId::new(),
        kind: BatchKind::Api {
            actor: "test".into(),
        },
        records: vec![RecordOutcome::Events {
            location: None,
            events: vec![event],
        }],
        archive: vec![],
    };
    let mut tx = conn.begin().await.unwrap();
    let outcome = tx.admit(&batch, &NoDerivations).await.unwrap();
    assert_eq!(outcome.replaced, 1);
    tx.commit().await.unwrap();
    let seq = current_seq(&mut conn).await.unwrap();
    let replayed = standing_query(
        &mut conn,
        "commands | sort by when | fields command",
        &ctx(),
        seq,
    )
    .await
    .unwrap();
    assert_eq!(
        replayed
            .rows
            .iter()
            .map(|row| row.identity.clone())
            .collect::<std::collections::BTreeSet<_>>(),
        keys
    );
    assert!(replayed.rows.iter().any(|row| row.event == Some(new)));
    assert!(
        replayed
            .rows
            .iter()
            .all(|row| row.event != Some(EventId(old)))
    );
}

#[devkit::test]
async fn signal_observations_do_not_feed_standing_event_queries(db: devkit::pg::TestDatabase) {
    let mut conn = crate::AdmissionContext::connect(&db.url(None))
        .await
        .unwrap();
    world(&mut conn).await;
    let payload = sinex_model::event_types::signal::OutcomeRecorded {
        delivery_id: "fixture".into(),
        outcome: ::signals::Outcome::Shown,
        recorded_at: Timestamp::from_unix_secs(BASE).unwrap(),
    };
    let event = Event::build(
        &payload,
        "sinex.signals",
        Origin::Record {
            location: RecordLocation {
                material: MaterialId::new(),
                record_offset: 0,
            },
            output_index: 0,
        },
        ReadingOrder(1),
        [7; 32],
        Class::Open,
    )
    .unwrap();
    let batch = Batch {
        run: RunId::new(),
        kind: BatchKind::Api {
            actor: "test".into(),
        },
        records: vec![RecordOutcome::Events {
            location: None,
            events: vec![event],
        }],
        archive: vec![],
    };
    let mut tx = conn.begin().await.unwrap();
    assert_eq!(tx.admit(&batch, &NoDerivations).await.unwrap().admitted, 1);
    tx.commit().await.unwrap();
    let seq = current_seq(&mut conn).await.unwrap();
    let answer = standing_query(
        &mut conn,
        "events | group by event_type | count",
        &ctx(),
        seq,
    )
    .await
    .unwrap();
    assert_eq!(answer.rows.len(), 2);
    assert!(answer.rows.iter().all(|row| {
        !row.result["event_type"]
            .as_str()
            .unwrap()
            .starts_with("signal.")
    }));
    assert_eq!(answer.parents.len(), 10);
}

async fn admit_signal_fixture(conn: &mut crate::AdmissionContext, event: Event) -> crate::Outcome {
    let batch = Batch {
        run: RunId::new(),
        kind: BatchKind::Api {
            actor: "fixture".into(),
        },
        records: vec![RecordOutcome::Events {
            location: None,
            events: vec![event],
        }],
        archive: vec![],
    };
    let mut tx = conn.begin().await.unwrap();
    let result = tx.admit(&batch, &NoDerivations).await.unwrap();
    tx.commit().await.unwrap();
    result
}
fn fired(reports: Vec<::signals::Report>) -> usize {
    reports
        .iter()
        .filter(|report| matches!(report, ::signals::Report::Fired(_)))
        .count()
}
fn observation(identity: String, reference: String, at: i64) -> ::signals::Row {
    ::signals::Row {
        occurrence: ::signals::Occurrence {
            id: identity,
            period: "record".into(),
        },
        reference,
        observed: at,
        start: at,
        end: None,
        origin: ::signals::Origin::Source,
        evidence: json!({"fixture":true}),
    }
}
#[devkit::test]
async fn privacy_key_rules_and_new_readings_do_not_refire_the_fact(db: devkit::pg::TestDatabase) {
    let mut conn = crate::AdmissionContext::connect(&db.url(None))
        .await
        .unwrap();
    let ids = world(&mut conn).await;
    let seq = current_seq(&mut conn).await.unwrap();
    let question = "commands | sort by when | fields command";
    let before = standing_query(&mut conn, question, &ctx(), seq)
        .await
        .unwrap();
    let identity = before.rows[0].identity.clone();
    assert!(identity.starts_with("native:"));
    assert!(!identity.contains(&ids[0].1.to_string()));
    let context = Context::new(
        "2026-10-29T12:00:00Z".parse().unwrap(),
        jiff::tz::TimeZone::get("UTC").unwrap(),
        jiff::civil::Time::constant(4, 0, 0, 0),
    );
    let calendar = ::signals::DayCalendar::new(context.clock().unwrap().day);
    let active = ::signals::ActiveRule {
        version: ::signals::RuleVersion {
            product: "sinex".into(),
            id: "authored:identity-fixture".into(),
            revision: 1,
        },
        rule: serde_json::from_value(json!({"name":"identity-fixture","query":question})).unwrap(),
        author: ::signals::Author::Agent("fixture".into()),
        via: "fixture".into(),
        live: false,
        shipped: Default::default(),
    };
    let at = 1_800_000_000_000;
    let mut evaluator = ::signals::Evaluator::new(at - 1, vec![active.clone()], &calendar).unwrap();
    evaluator
        .observe(::signals::Evaluation {
            rule: active.version.clone(),
            at,
            complete: true,
            rows: vec![observation(
                identity.clone(),
                before.rows[0].reference.clone().unwrap(),
                at,
            )],
            value: None,
        })
        .unwrap();
    assert_eq!(fired(evaluator.reports(at, &calendar).unwrap()), 1);
    let mut reread = Event::build(
        &CommandFused {
            record: format!("command.executed:{}", ids[0].1),
            command: "new interpretation".into(),
            host: "kestrel".into(),
            started_at: Timestamp::from_unix_secs(BASE).unwrap(),
            cwd: Some("/src/tide".into()),
            session: Some("s".into()),
            duration_ns: None,
            exit_code: Some(0),
            execution: None,
            members: vec![sinex_model::event_types::fused::Member {
                record: format!("fixture:{}", "terminal.atuin"),
                source: "terminal.atuin".into(),
                event_type: "command.executed".into(),
                joined: sinex_model::event_types::fused::Joined::Lead,
            }],
            basis: sinex_model::event_types::fused::Basis::Rule,
            ambiguous: 1,
            competitors: vec![],
            rule_version: 1,
        },
        "derive.shell-fusion",
        Origin::Derived {
            parents: vec![EventId(ids[0].1)],
        },
        ReadingOrder(2),
        [51; 32],
        Class::Open,
    )
    .unwrap();
    let next_id = reread.id;
    reread.reading_order = ReadingOrder(2);
    let result = admit_signal_fixture(&mut conn, reread).await;
    assert_eq!(result.replaced, 1);
    let seq = current_seq(&mut conn).await.unwrap();
    let after = standing_query(&mut conn, question, &ctx(), seq)
        .await
        .unwrap();
    let current = after
        .rows
        .iter()
        .find(|row| row.event == Some(next_id))
        .unwrap();
    assert_eq!(current.identity, identity);
    evaluator
        .observe(::signals::Evaluation {
            rule: active.version.clone(),
            at: at + 1,
            complete: true,
            rows: vec![observation(
                current.identity.clone(),
                current.reference.clone().unwrap(),
                at,
            )],
            value: None,
        })
        .unwrap();
    assert_eq!(fired(evaluator.reports(at + 1, &calendar).unwrap()), 0);
    crate::privacy::add_rule(
        &mut conn,
        &crate::privacy::NewRule {
            name: "record key protection".into(),
            source: "derive.shell-fusion".into(),
            event_type: Some("command.fused".into()),
            field: Some("/record".into()),
            matcher: "all".into(),
            matcher_arg: None,
            class: Class::Open,
            action: crate::privacy::Action::Hash,
            author: "fixture".into(),
        },
    )
    .await
    .unwrap();
    // Rules apply when a reading is admitted (the mechanism Sinex runs until
    // output-time rules are built; docs/privacy.md describes that design), so
    // a rule written after both readings leaves the fact's identity
    // as it was.
    let protected = standing_query(&mut conn, question, &ctx(), seq)
        .await
        .unwrap();
    assert_eq!(
        protected
            .rows
            .iter()
            .map(|row| &row.identity)
            .collect::<Vec<_>>(),
        before
            .rows
            .iter()
            .map(|row| &row.identity)
            .collect::<Vec<_>>()
    );
}

#[devkit::test]
async fn keyless_record_coordinates_survive_rereading_and_distinguish_outputs(
    db: devkit::pg::TestDatabase,
) {
    use sinex_model::event_types::operator::LogEntry;
    let mut conn = crate::AdmissionContext::connect(&db.url(None))
        .await
        .unwrap();
    let material = MaterialId::new();
    let payload = |text: &str| LogEntry {
        written_at: None,
        label: None,
        text: text.into(),
    };
    let build = |text: &str, output, order| {
        Event::build(
            &payload(text),
            "fixture.notes",
            Origin::Record {
                location: RecordLocation {
                    material,
                    record_offset: 17,
                },
                output_index: output,
            },
            ReadingOrder(order),
            *blake3::hash(text.as_bytes()).as_bytes(),
            Class::Open,
        )
        .unwrap()
    };
    assert_eq!(
        admit_signal_fixture(&mut conn, build("first reading", 0, 1))
            .await
            .admitted,
        1
    );
    let seq = current_seq(&mut conn).await.unwrap();
    let before = standing_query(
        &mut conn,
        "events where source = fixture.notes",
        &ctx(),
        seq,
    )
    .await
    .unwrap();
    assert_eq!(before.rows.len(), 1);
    let original = before.rows[0].identity.clone();
    assert_eq!(
        admit_signal_fixture(&mut conn, build("second reading", 0, 2))
            .await
            .replaced,
        1
    );
    assert_eq!(
        admit_signal_fixture(&mut conn, build("separate output", 1, 2))
            .await
            .admitted,
        1
    );
    let seq = current_seq(&mut conn).await.unwrap();
    let after = standing_query(
        &mut conn,
        "events where source = fixture.notes | sort by output",
        &ctx(),
        seq,
    )
    .await
    .unwrap();
    assert_eq!(after.rows.len(), 2);
    assert_eq!(after.rows[0].identity, original);
    assert_ne!(after.rows[1].identity, original);
    assert_ne!(after.rows[0].reference, before.rows[0].reference);
}

#[devkit::test]
async fn rule_over_notifications_ignores_signal_origin_events(db: devkit::pg::TestDatabase) {
    use sinex_model::event_types::session::{CaptureProgram, NotificationShown};
    let mut conn = crate::AdmissionContext::connect(&db.url(None))
        .await
        .unwrap();
    let mut kept = Vec::new();
    for (index, product) in [None, Some("sinex"), Some("polylogue")]
        .into_iter()
        .enumerate()
    {
        let payload = NotificationShown {
            captured_by: CaptureProgram::Sinex,
            received_at: Timestamp::from_unix_secs(BASE).unwrap(),
            app: "fixture".into(), summary: "Notice".into(), body: "Details".into(),
            icon: None, notification_id: Some(index as u32), replaces_id: None,
            actions: vec![], hints: Default::default(), expire_timeout_ms: -1,
            sender: ":1.2".into(),
            extra: product.map(|product| serde_json::json!({"signal_origin": {"product":product,"delivery":"delivery-1"}}).as_object().unwrap().clone()).unwrap_or_default(),
        };
        let event = Event::build(
            &payload,
            "desktop.notifications",
            Origin::Record {
                location: RecordLocation {
                    material: MaterialId::new(),
                    record_offset: 0,
                },
                output_index: 0,
            },
            ReadingOrder(1),
            [index as u8; 32],
            Class::Open,
        )
        .unwrap();
        if product.is_none() {
            kept.push(event.id);
        }
        assert_eq!(admit_signal_fixture(&mut conn, event).await.admitted, 1);
    }
    let seq = current_seq(&mut conn).await.unwrap();
    let answer = standing_query(
        &mut conn,
        "events where event_type = notification.shown | fields payload.app",
        &ctx(),
        seq,
    )
    .await
    .unwrap();
    assert_eq!(answer.rows.len(), 1);
    assert_eq!(answer.parents, kept);
    assert_eq!(answer.rows[0].result["app"], "fixture");
    let grouped = standing_query(
        &mut conn,
        "events where event_type = notification.shown | group by payload.app | count",
        &ctx(),
        seq,
    )
    .await
    .unwrap();
    assert_eq!(grouped.rows.len(), 1);
    assert_eq!(grouped.rows[0].result["count"], 1);
    assert_eq!(grouped.parents, kept);
}
