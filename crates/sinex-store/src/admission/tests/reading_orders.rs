use super::*;

fn output(parent: &Event, content: u8, order: i64) -> Event {
    Event::build(
        &sinex_model::event_types::derived::TagApplied {
            tag: "synthetic".into(),
            record: format!("command.executed:{}", parent.id),
            event_type: "command.executed".into(),
            rules: vec![format!("synthetic-{content}")],
            at: parent.ts_orig.at,
        },
        "synthetic.orders",
        Origin::Derived {
            parents: vec![parent.id],
        },
        ReadingOrder(order),
        [content; 32],
        Class::Open,
    )
    .unwrap()
}

fn derived(parent: &Event, events: Vec<Event>, position: i64) -> Batch {
    Batch {
        run: RunId::new(),
        kind: BatchKind::Derived {
            derivation: "synthetic.orders".into(),
            scope: crate::DerivedScope::Position {
                inputs: vec![parent.id.0],
                position,
                read: Default::default(),
            }
            .to_value(),
        },
        records: vec![RecordOutcome::Events {
            location: None,
            events,
        }],
        archive: vec![],
    }
}

async fn progress(conn: &mut PgConnection) -> (i64, i64) {
    (
        crate::derivations::position(conn, "synthetic.orders")
            .await
            .unwrap(),
        crate::derivations::reading_orders(conn, &["synthetic.orders".into()])
            .await
            .unwrap()["synthetic.orders"],
    )
}

#[devkit::test]
async fn output_order_and_position_commit_and_roll_back_together(db: devkit::pg::TestDatabase) {
    let mut conn = crate::AdmissionContext::connect(&db.url(None))
        .await
        .unwrap();
    let parent = keyed("synthetic.parent", 1, 1, 1);
    admit_one(&mut conn, &batch(vec![parent.clone()])).await;
    let first = output(&parent, 1, 7);
    admit_one(&mut conn, &derived(&parent, vec![first.clone()], 10)).await;
    assert_eq!(progress(&mut conn).await, (10, 7));
    let high = 1_i64 << 40;
    let mut tx = conn.begin().await.unwrap();
    tx.admit(
        &derived(&parent, vec![output(&parent, 2, high)], 20),
        &NoDerivations,
    )
    .await
    .unwrap();
    assert_eq!(progress(&mut tx).await, (20, high));
    tx.rollback().await.unwrap();
    assert_eq!(progress(&mut conn).await, (10, 7));
    admit_one(&mut conn, &derived(&parent, vec![], 20)).await;
    assert_eq!(progress(&mut conn).await, (20, 7));
    // An offered repeat with a larger order is not a stored interpretation.
    let mut repeated = first;
    repeated.reading_order = ReadingOrder(high * 2);
    let result = admit_one(&mut conn, &derived(&parent, vec![repeated], 30)).await;
    assert_eq!(result.repeats, 1);
    assert_eq!(progress(&mut conn).await, (30, 7));
    admit_one(
        &mut conn,
        &derived(&parent, vec![output(&parent, 2, high)], 40),
    )
    .await;
    assert_eq!(progress(&mut conn).await, (40, high));
    drop(conn);
    let mut reopened = crate::AdmissionContext::connect(&db.url(None))
        .await
        .unwrap();
    assert_eq!(progress(&mut reopened).await, (40, high));
}
