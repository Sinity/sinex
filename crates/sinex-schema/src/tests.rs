use super::*;

/// A payload field named like one of the view's event columns makes the
/// kind's view, and so the whole schema, fail to apply.
#[devkit::test]
fn no_payload_field_shadows_an_event_column() {
    let event_columns = [
        "id",
        "source",
        "ts_orig",
        "ts_quality",
        "ts_coided",
        "record_state",
    ];
    for kind in event_types::ALL {
        let schema = (kind.json_schema)();
        let props = schema.get("properties").and_then(|p| p.as_object());
        for field in props.into_iter().flat_map(|p| p.keys()) {
            assert!(
                !event_columns.contains(&field.as_str()),
                "{} has a payload field {field:?} that shadows an event column",
                kind.event_type
            );
        }
    }
}

#[devkit::test]
fn declarations_are_deterministic_and_named() {
    assert_eq!(declarations(), declarations());
    assert_eq!(declaration_hash().len(), 16);
    let all = declarations().join("\n");
    for table in [
        "core.events",
        "core.live_locations",
        "core.live_equivalence",
        "core.hidden_records",
        "core.commit_log",
        "audit.archived",
        "raw.materials",
        "raw.positions",
        "derivation.dirty",
        "core.admission_stage",
        "core.source_counts",
        "core.excised_materials",
    ] {
        assert!(all.contains(&format!("TABLE {table} (")), "{table}");
    }
    assert!(all.contains("CREATE VIEW event_types.\"command_executed\""));
}

#[devkit::test]
fn each_version_after_0_has_its_upgrade() {
    assert_eq!(i32::try_from(upgrades::upgrades().len()).unwrap(), VERSION);
}

async fn search_version_two(conn: &mut PgConnection) {
    sqlx::raw_sql(include_str!("tests/version_two.sql"))
        .execute(conn)
        .await
        .unwrap();
}

#[devkit::test]
async fn search_upgrade_preserves_populated_text_and_other_rows(db: devkit::pg::TestDatabase) {
    let mut conn = PgConnection::connect(&db.url(None)).await.unwrap();
    let fresh = catalog(&mut conn).await.unwrap();
    search_version_two(&mut conn).await;
    for (position, kind, class) in [
        (1, "command.imported", "open"),
        (2, "journal.entry", "guarded"),
        (3, "lake.observed", "open"),
    ] {
        let id: String = sqlx::query_scalar("INSERT INTO core.events (id,event_type,source,run_id,material_id,anchor,ts_quality,commit_seq,payload,reading_order,record_hash,class) VALUES (uuidv7(),$1,'test',uuidv7(),uuidv7(),0,'unknown',$2,'{}',1,decode(repeat('01',32),'hex'),$3) RETURNING id::text")
            .bind(kind).bind(i64::from(position)).bind(class).fetch_one(&mut conn).await.unwrap();
        sqlx::query("INSERT INTO core.search_text (event_id,text) VALUES ($1::text::uuid,'strasse café café cargo test')")
            .bind(id).execute(&mut conn).await.unwrap();
    }
    conn.execute("INSERT INTO core.search_text (event_id,text) VALUES (uuidv7(),'orphan stays searchable'); INSERT INTO core.consumer_positions (consumer,commit_seq,change_seq) VALUES ('read.search_text',33,4); INSERT INTO raw.positions (binding,cursor,carry,committed_seq) VALUES ('test','{\"offset\":42}','{}',33)").await.unwrap();
    let before: Vec<serde_json::Value> =
        sqlx::query_scalar("SELECT to_jsonb(s) FROM core.search_text s ORDER BY event_id")
            .fetch_all(&mut conn)
            .await
            .unwrap();
    let events: Vec<serde_json::Value> =
        sqlx::query_scalar("SELECT to_jsonb(e) FROM core.events e ORDER BY id")
            .fetch_all(&mut conn)
            .await
            .unwrap();
    assert_eq!(
        apply(&mut conn).await.unwrap(),
        Applied::Upgraded {
            from: 2,
            to: VERSION
        }
    );
    let after: Vec<serde_json::Value> = sqlx::query_scalar(
        "SELECT to_jsonb(s)-'trigram_indexed' FROM core.search_text s ORDER BY event_id",
    )
    .fetch_all(&mut conn)
    .await
    .unwrap();
    assert_eq!(after, before);
    assert_eq!(
        sqlx::query_scalar::<_, serde_json::Value>(
            "SELECT to_jsonb(e) FROM core.events e ORDER BY id"
        )
        .fetch_all(&mut conn)
        .await
        .unwrap(),
        events
    );
    let policy: Vec<(String,bool)> = sqlx::query_as("SELECT e.event_type,s.trigram_indexed FROM core.search_text s JOIN core.events e ON e.id=s.event_id ORDER BY e.event_type").fetch_all(&mut conn).await.unwrap();
    assert_eq!(
        policy,
        [
            ("command.imported".into(), true),
            ("journal.entry".into(), false),
            ("lake.observed".into(), false)
        ]
    );
    assert!(
        sqlx::query_scalar::<_, bool>(
            "SELECT trigram_indexed FROM core.search_text WHERE text='orphan stays searchable'"
        )
        .fetch_one(&mut conn)
        .await
        .unwrap()
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT commit_seq FROM core.consumer_positions WHERE consumer='read.search_text'"
        )
        .fetch_one(&mut conn)
        .await
        .unwrap(),
        33
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT committed_seq FROM raw.positions WHERE binding='test'"
        )
        .fetch_one(&mut conn)
        .await
        .unwrap(),
        33
    );
    assert_eq!(catalog(&mut conn).await.unwrap(), fresh);
    conn.execute("SET ROLE sinex_reader").await.unwrap();
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM core.visible_search_text")
            .fetch_one(&mut conn)
            .await
            .unwrap(),
        2
    );
    assert!(
        sqlx::query("SELECT text FROM core.search_text")
            .execute(&mut conn)
            .await
            .is_err()
    );
}

#[devkit::test]
async fn search_upgrade_refuses_writer_and_rolls_back_failed_index_build(
    db: devkit::pg::TestDatabase,
) {
    let mut conn = PgConnection::connect(&db.url(None)).await.unwrap();
    search_version_two(&mut conn).await;
    let before = catalog(&mut conn).await.unwrap();
    let mut writer = PgConnection::connect(&db.url(None)).await.unwrap();
    sqlx::query("SELECT pg_advisory_lock($1)")
        .bind(WRITER_LOCK)
        .execute(&mut writer)
        .await
        .unwrap();
    assert!(matches!(apply(&mut conn).await, Err(Error::WriterRunning)));
    sqlx::query("SELECT pg_advisory_unlock($1)")
        .bind(WRITER_LOCK)
        .execute(&mut writer)
        .await
        .unwrap();
    conn.execute("CREATE INDEX search_text_trigram_partial ON core.search_text(event_id)")
        .await
        .unwrap();
    assert!(apply(&mut conn).await.is_err());
    conn.execute("DROP INDEX core.search_text_trigram_partial")
        .await
        .unwrap();
    assert_eq!(catalog(&mut conn).await.unwrap(), before);
    assert_eq!(
        sqlx::query_scalar::<_, i32>("SELECT max(version) FROM sinex_schema.applied")
            .fetch_one(&mut conn)
            .await
            .unwrap(),
        2
    );
    assert_eq!(
        apply(&mut conn).await.unwrap(),
        Applied::Upgraded {
            from: 2,
            to: VERSION
        }
    );
}

mod preservation;
