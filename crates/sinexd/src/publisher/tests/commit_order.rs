use super::*;

type Tuple = (i64, EventId, &'static str);

pub(super) fn tuple(message: &Confirmed) -> Option<Tuple> {
    match message {
        Confirmed::Event(event) => Some((event.commit_seq, event.id, "event")),
        Confirmed::Withheld(event) => Some((event.commit_seq, event.event, "event")),
        Confirmed::Archived(a) => Some((a.archived_seq, a.event, "archived")),
        Confirmed::Restored(a) => Some((a.restored_seq, a.event, "restored")),
        Confirmed::Tombstoned {
            commit_seq: Some(position),
            event,
            ..
        } => Some((*position, *event, "tombstoned")),
        Confirmed::PositionComplete { .. } => None,
        other => panic!("unexpected incomplete history: {other:?}"),
    }
}

pub(super) async fn internal(conn: &mut PgConnection) -> Vec<Tuple> {
    let mut cursor = commit_log::ChangeCursor::after(0);
    let mut tuples = Vec::new();
    loop {
        let page = commit_log::changes_page(conn, cursor, 2, 1024)
            .await
            .unwrap();
        assert!(page.gap.is_none());
        if page.entries.is_empty() {
            return tuples;
        }
        for entry in &page.entries {
            let data = match &entry.change {
                commit_log::Change::Event(e) => Some((EventId(e.id), "event")),
                commit_log::Change::Archive(a) => Some((
                    EventId(a.archive.event_id),
                    if a.archive.archived {
                        "archived"
                    } else {
                        "restored"
                    },
                )),
                commit_log::Change::Tombstoned(e) => Some((EventId(e.event_id), "tombstoned")),
                commit_log::Change::PositionComplete => None,
            };
            if let Some((event, change)) = data {
                tuples.push((entry.position, event, change));
            }
        }
        cursor = page.next.unwrap();
    }
}

#[devkit::test]
async fn commit_log_and_confirmed_order_survive_lost_stream(db: devkit::pg::TestDatabase) {
    let nats = devkit::nats::test_nats().unwrap();
    let subjects = Subjects::new(nats.prefix());
    let js = context(async_nats::connect(nats.url()).await.unwrap());
    ensure_stream(
        &mut PgConnection::connect(&db.url(None)).await.unwrap(),
        &js,
        &subjects,
    )
    .await
    .unwrap();
    let mut conn = sinex_store::AdmissionContext::connect(&db.url(None))
        .await
        .unwrap();
    let first = executed("same", "pwd", 1, Class::Open);
    let first_id = first.id;
    admit(&mut conn, batch(vec![first])).await;
    let next = executed("same", "ls", 2, Class::Open);
    let next_id = next.id;
    admit(&mut conn, batch(vec![next])).await;
    lifecycle(&mut conn, Act::Archive, vec![next_id]).await;
    lifecycle(&mut conn, Act::Restore, vec![next_id]).await;
    let expected = internal(&mut conn).await;
    assert_eq!(
        expected.iter().map(|t| t.2).collect::<Vec<_>>(),
        ["event", "event", "archived", "archived", "restored"]
    );
    publish_pending(&mut conn, &js, &subjects).await.unwrap();
    assert_eq!(
        read_wire_all(&js, &subjects)
            .await
            .iter()
            .filter_map(tuple)
            .collect::<Vec<_>>(),
        expected
    );
    let previous = instance(&js, &subjects).await;
    // This broker and database are owned by the synthetic test.
    js.delete_stream(subjects.confirmed_stream()).await.unwrap();
    ensure_stream(
        &mut PgConnection::connect(&db.url(None)).await.unwrap(),
        &js,
        &subjects,
    )
    .await
    .unwrap();
    let current = instance(&js, &subjects).await;
    assert_ne!(previous, current);
    publish(
        &js,
        subjects.confirmed_control(),
        "fixture-control".into(),
        &Confirmed::PositionComplete { commit_seq: 0 },
    )
    .await
    .unwrap()
    .await
    .unwrap();
    let stream_start =
        read_stream_start(&js.get_stream(subjects.confirmed_stream()).await.unwrap()).unwrap();
    assert_eq!(
        stream_start.position,
        commit_log::latest(&mut conn).await.unwrap()
    );
    assert!(stream_start.requires_snapshot);
    assert_eq!(
        publish_pending(&mut conn, &js, &subjects)
            .await
            .unwrap()
            .events,
        0
    );
    let later = executed("later", "echo later", 3, Class::Open);
    let later_id = later.id;
    admit(&mut conn, batch(vec![later])).await;
    assert_eq!(
        publish_pending(&mut conn, &js, &subjects)
            .await
            .unwrap()
            .events,
        1
    );
    assert_eq!(
        read_wire_all(&js, &subjects)
            .await
            .iter()
            .filter_map(tuple)
            .collect::<Vec<_>>(),
        vec![(stream_start.position + 1, later_id, "event")]
    );
    // An old stream instance's sequence must never delete the unrelated new stream slot.
    sqlx::query("INSERT INTO core.event_tombstones(event_id,source,event_type,operation_id,commit_seq,original_commit_seq,class) VALUES($1,'atuin','command.executed',$2,5,1,'open')")
        .bind(first_id.0).bind(uuid::Uuid::now_v7()).execute(&mut conn).await.unwrap();
    assert_eq!(
        erase_tombstoned_in(&mut conn, &js, &subjects, &current)
            .await
            .unwrap(),
        0
    );
    assert!(
        js.get_stream(subjects.confirmed_stream())
            .await
            .unwrap()
            .get_raw_message(1)
            .await
            .is_ok()
    );
}

#[devkit::test]
async fn partial_position_and_marker_rollback_resume(db: devkit::pg::TestDatabase) {
    let nats = devkit::nats::test_nats().unwrap();
    let subjects = Subjects::new(nats.prefix());
    let js = context(async_nats::connect(nats.url()).await.unwrap());
    ensure_stream(
        &mut PgConnection::connect(&db.url(None)).await.unwrap(),
        &js,
        &subjects,
    )
    .await
    .unwrap();
    let mut conn = sinex_store::AdmissionContext::connect(&db.url(None))
        .await
        .unwrap();
    let parent = executed("parent", "pwd", 1, Class::Open);
    let id = parent.id;
    admit(&mut conn, batch(vec![parent])).await;
    let mut child = executed("child", "ls", 2, Class::Open);
    child.origin = Origin::Derived { parents: vec![id] };
    admit(&mut conn, batch(vec![child])).await;
    publish_pending(&mut conn, &js, &subjects).await.unwrap();
    lifecycle(&mut conn, Act::Archive, vec![id]).await;
    let instance = instance(&js, &subjects).await;
    let consumer = consumer_for(&instance);
    let (before, _) = commit_log::consumer_position(&mut conn, &consumer)
        .await
        .unwrap();
    let stream = confirmed_stream(&mut conn, &js, &subjects).await.unwrap();
    let stream_start = read_stream_start(&stream).unwrap();
    let page = commit_log::stream_changes_after_start(
        &mut conn,
        commit_log::ChangeCursor::after(before),
        &stream_start,
        1,
        1024,
    )
    .await
    .unwrap();
    assert_eq!(page.entries.len(), 1);
    assert!(matches!(
        page.entries[0].change,
        commit_log::Change::Archive(_)
    ));
    publish_chunk(
        &mut conn,
        &js,
        &subjects,
        &instance,
        &consumer,
        &page.entries,
        None,
        &mut Published::default(),
    )
    .await
    .unwrap();
    assert_eq!(
        commit_log::consumer_position(&mut conn, &consumer)
            .await
            .unwrap(),
        (before, 0)
    );
    publish_pending(&mut conn, &js, &subjects).await.unwrap();
    let expected = internal(&mut conn).await;
    assert_eq!(
        read_wire_all(&js, &subjects)
            .await
            .iter()
            .filter_map(tuple)
            .collect::<Vec<_>>(),
        expected
    );
    // Model a crash after the broker acknowledged the completion marker and before PG committed.
    commit_log::set_consumer_position(&mut conn, &consumer, before, 0)
        .await
        .unwrap();
    publish_pending(&mut conn, &js, &subjects).await.unwrap();
    assert_eq!(
        read_wire_all(&js, &subjects)
            .await
            .iter()
            .filter_map(tuple)
            .collect::<Vec<_>>(),
        expected
    );
    let latest = sinex_store::query::current_seq(&mut conn).await.unwrap();
    assert!(matches!(
        read_wire_all(&js, &subjects).await.last(),
        Some(Confirmed::PositionComplete { commit_seq }) if *commit_seq == latest
    ));
}

#[devkit::test]
async fn empty_database_is_ready_at_zero_and_nonempty_old_contract_is_refused(
    db: devkit::pg::TestDatabase,
) {
    let nats = devkit::nats::test_nats().unwrap();
    let subjects = Subjects::new(nats.prefix());
    let js = context(async_nats::connect(nats.url()).await.unwrap());
    let mut conn = PgConnection::connect(&db.url(None)).await.unwrap();
    publish_pending(&mut conn, &js, &subjects).await.unwrap();
    assert_eq!(
        read_wire_all(&js, &subjects).await,
        [Confirmed::PositionComplete { commit_seq: 0 }]
    );
    publish_pending(&mut conn, &js, &subjects).await.unwrap();
    assert_eq!(read_wire_all(&js, &subjects).await.len(), 1);
    js.delete_stream(subjects.confirmed_stream()).await.unwrap();
    js.create_stream(stream::Config {
        name: subjects.confirmed_stream(),
        subjects: vec![subjects.confirmed_all()],
        ..Default::default()
    })
    .await
    .unwrap();
    js.publish(subjects.confirmed_control(), "older contract".into())
        .await
        .unwrap()
        .await
        .unwrap();
    assert!(
        ensure_stream(
            &mut PgConnection::connect(&db.url(None)).await.unwrap(),
            &js,
            &subjects
        )
        .await
        .unwrap_err()
        .to_string()
        .starts_with("confirmed_stream_contract_mismatch:")
    );
    assert_eq!(
        js.get_stream(subjects.confirmed_stream())
            .await
            .unwrap()
            .cached_info()
            .state
            .messages,
        1
    );
}

#[devkit::test]
async fn giant_event_metadata_travels_by_id_and_completes(db: devkit::pg::TestDatabase) {
    let nats = devkit::nats::test_nats().unwrap();
    let subjects = Subjects::new(nats.prefix());
    let js = context(async_nats::connect(nats.url()).await.unwrap());
    ensure_stream(
        &mut PgConnection::connect(&db.url(None)).await.unwrap(),
        &js,
        &subjects,
    )
    .await
    .unwrap();
    let mut conn = sinex_store::AdmissionContext::connect(&db.url(None))
        .await
        .unwrap();
    let parent = executed("metadata-parent", "pwd", 1, Class::Open);
    let parent_id = parent.id;
    admit(&mut conn, batch(vec![parent])).await;
    // This valid stored parent list exceeds the broker's message size limit.
    let count = js.client().server_info().max_payload / 36 + 1;
    let parents = vec![parent_id; count];
    let mut event = executed("giant-metadata", "ls", 2, Class::Open);
    let id = event.id;
    event.origin = Origin::Derived {
        parents: parents.clone(),
    };
    admit(&mut conn, batch(vec![event])).await;
    let page = commit_log::after(&mut conn, 1, 1, i64::MAX).await.unwrap();
    assert!(
        serde_json::to_vec(&page.events[0].parents).unwrap().len()
            > js.client().server_info().max_payload
    );
    let transfer = commit_log::changes_page(
        &mut conn,
        commit_log::ChangeCursor::after(1),
        100,
        js.client().server_info().max_payload as i64,
    )
    .await
    .unwrap();
    assert_eq!(
        transfer.entries.len(),
        1,
        "giant metadata is allowed alone and its completion waits for the next page"
    );
    assert!(matches!(
        transfer.entries[0].change,
        commit_log::Change::Event(_)
    ));
    publish_pending(&mut conn, &js, &subjects).await.unwrap();
    let messages = read_wire_all(&js, &subjects).await;
    assert!(matches!(&messages[0], Confirmed::Event(event) if event.id == parent_id));
    assert_eq!(
        messages[1..],
        [
            Confirmed::Withheld(sinex_protocol::WithheldEvent {
                commit_seq: 2,
                event: id,
                event_type: "command.executed".into(),
                class: Class::Open,
                reason: WithheldReason::Size,
            }),
            Confirmed::PositionComplete { commit_seq: 2 }
        ]
    );
    let stored: Vec<uuid::Uuid> = sqlx::query_scalar("SELECT parents FROM core.events WHERE id=$1")
        .bind(id.0)
        .fetch_one(&mut conn)
        .await
        .unwrap();
    assert_eq!(
        stored,
        parents.iter().map(|id| id.0).collect::<Vec<_>>(),
        "reference-only transport preserves complete stored metadata"
    );
}

#[devkit::test]
async fn withheld_events_expose_no_source_key_and_route_by_stored_class(
    db: devkit::pg::TestDatabase,
) {
    let nats = devkit::nats::test_nats().unwrap();
    let subjects = Subjects::new(nats.prefix());
    let js = context(async_nats::connect(nats.url()).await.unwrap());
    ensure_stream(
        &mut PgConnection::connect(&db.url(None)).await.unwrap(),
        &js,
        &subjects,
    )
    .await
    .unwrap();
    let mut conn = sinex_store::AdmissionContext::connect(&db.url(None))
        .await
        .unwrap();
    sinex_store::privacy::add_rule(
        &mut conn,
        &sinex_store::privacy::NewRule {
            name: "configured native ID".into(),
            source: "atuin".into(),
            event_type: Some("command.executed".into()),
            field: Some("/native_id".into()),
            matcher: "pattern".into(),
            matcher_arg: Some("route-secret".into()),
            class: Class::Guarded,
            action: sinex_store::privacy::Action::Mask,
            author: "fixture".into(),
        },
    )
    .await
    .unwrap();
    let event = executed("route-secret", "shell command", 1, Class::Open);
    let original_key = event.equivalence_key.clone().unwrap().0;
    let id = event.id;
    admit(&mut conn, batch(vec![event])).await;
    publish_pending(&mut conn, &js, &subjects).await.unwrap();
    let stream = js.get_stream(subjects.confirmed_stream()).await.unwrap();
    let raw = stream.get_raw_message(1).await.unwrap();
    assert_eq!(
        raw.subject,
        subjects
            .confirmed_event("guarded", "command.executed")
            .into()
    );
    assert!(!String::from_utf8_lossy(&raw.payload).contains("route-secret"));
    let wire: Confirmed = serde_json::from_slice(&raw.payload).unwrap();
    assert_eq!(
        wire,
        Confirmed::Withheld(sinex_protocol::WithheldEvent {
            commit_seq: 1,
            event: id,
            event_type: "command.executed".into(),
            class: Class::Guarded,
            reason: WithheldReason::Class,
        })
    );
    let key: String = sqlx::query_scalar("SELECT equivalence_key FROM core.events WHERE id=$1")
        .bind(id.0)
        .fetch_one(&mut *conn)
        .await
        .unwrap();
    let stored: serde_json::Value =
        sqlx::query_scalar("SELECT payload FROM core.events WHERE id=$1")
            .bind(id.0)
            .fetch_one(&mut *conn)
            .await
            .unwrap();
    assert_ne!(
        stored["native_id"], "route-secret",
        "admission applied the rule to the stored payload"
    );
    assert_ne!(
        key, original_key,
        "a changed payload keeps no key built from the matched value"
    );
    assert!(!key.contains("route-secret"));
}

#[devkit::test]
async fn an_older_stream_contract_is_refused_without_erasing_messages(
    db: devkit::pg::TestDatabase,
) {
    let nats = devkit::nats::test_nats().unwrap();
    let subjects = Subjects::new(nats.prefix());
    let js = context(async_nats::connect(nats.url()).await.unwrap());
    let mut config = stream::Config {
        name: subjects.confirmed_stream(),
        subjects: vec![subjects.confirmed_all()],
        ..Default::default()
    };
    config.metadata.insert(CONTRACT_KEY.into(), "4".into());
    js.create_stream(config).await.unwrap();
    let old = serde_json::json!({"type":"event","equivalence_key":"synthetic-inline-key"});
    js.publish(
        subjects.confirmed_event("open", "fixture.event"),
        serde_json::to_vec(&old).unwrap().into(),
    )
    .await
    .unwrap()
    .await
    .unwrap();
    let mut conn = PgConnection::connect(&db.url(None)).await.unwrap();
    assert!(
        publish_pending(&mut conn, &js, &subjects)
            .await
            .unwrap_err()
            .to_string()
            .starts_with("confirmed_stream_contract_mismatch:")
    );
    let stream = js.get_stream(subjects.confirmed_stream()).await.unwrap();
    assert_eq!(stream.cached_info().state.messages, 1);
    assert_eq!(stream.cached_info().config.metadata[CONTRACT_KEY], "4");
}
