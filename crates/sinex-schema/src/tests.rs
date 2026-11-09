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
        "kind_version",
        "validation",
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
        "core.commit_log",
        "audit.archived",
        "raw.materials",
        "raw.positions",
        "derivation.dirty",
        "core.admission_stage",
        "core.source_counts",
        "raw.material_changes",
        "core.operations",
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
    sqlx::raw_sql(include_str!("tests/version_four.sql"))
        .execute(&mut *conn)
        .await
        .unwrap();
    sqlx::raw_sql(include_str!("tests/version_two.sql"))
        .execute(conn)
        .await
        .unwrap();
}

/// Startup never upgrades: it refuses an older version, names the upgrade
/// command and leaves the store as it was; the explicit apply then upgrades.
#[devkit::test]
async fn startup_refuses_an_older_version_and_leaves_it_unchanged(db: devkit::pg::TestDatabase) {
    let mut conn = PgConnection::connect(&db.url(None)).await.unwrap();
    search_version_two(&mut conn).await;
    let owners = crate::kinds::Owners::default();
    let error = start(&mut conn, &owners).await.unwrap_err();
    assert!(matches!(error, Error::UpgradeNeeded { found: 2 }));
    assert!(error.to_string().contains("sinexd admin upgrade"));
    assert!(matches!(
        require_current(&mut conn).await,
        Err(Error::UpgradeNeeded { found: 2 })
    ));
    let version: Option<i32> = sqlx::query_scalar("SELECT max(version) FROM sinex_schema.applied")
        .fetch_one(&mut conn)
        .await
        .unwrap();
    assert_eq!(version, Some(2));
    assert_eq!(
        apply(&mut conn).await.unwrap(),
        Applied::Upgraded {
            from: 2,
            to: VERSION
        }
    );
    assert_eq!(
        start(&mut conn, &owners).await.unwrap(),
        Applied::Current { version: VERSION }
    );
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
    let events: Vec<serde_json::Value> = sqlx::query_scalar(
        "SELECT to_jsonb(e)-'kind_version'-'validation'-'producer_family'-'producer_identity'-'producer_host'-'producer_frame'-'source_version'-'source_fingerprint' FROM core.events e ORDER BY id",
    )
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
    // The upgrade rewrites no stored row and leaves the index over every row;
    // both wait for the work after start.
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT count(*) FROM core.search_text WHERE NOT trigram_indexed"
        )
        .fetch_one(&mut conn)
        .await
        .unwrap(),
        0
    );
    assert_eq!(
        sqlx::query_scalar::<_, String>("SELECT indexdef FROM pg_indexes WHERE schemaname='core' AND indexname='search_text_trigram'")
            .fetch_one(&mut conn)
            .await
            .unwrap(),
        "CREATE INDEX search_text_trigram ON core.search_text USING gin (text gin_trgm_ops)"
    );
    crate::online::finish(&mut conn).await.unwrap();
    assert_eq!(
        sqlx::query_scalar::<_, serde_json::Value>(
            "SELECT to_jsonb(e)-'kind_version'-'validation'-'producer_family'-'producer_identity'-'producer_host'-'producer_frame'-'source_version'-'source_fingerprint' FROM core.events e ORDER BY id"
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
        sqlx::query_scalar::<_, i64>("SELECT commit_seq FROM core.search_position")
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
    // A later step's mark already exists, so the upgrade fails after the
    // search step and every earlier statement.
    conn.execute("CREATE TABLE sinex_schema.events_feed_pending ()")
        .await
        .unwrap();
    assert!(apply(&mut conn).await.is_err());
    conn.execute("DROP TABLE sinex_schema.events_feed_pending")
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

/// The upgrade's transaction
/// rewrites no event chunk. It writes only the event rows whose output number
/// is not the default.
#[devkit::test]
async fn upgrade_keeps_event_storage(db: devkit::pg::TestDatabase) {
    let mut conn = PgConnection::connect(&db.url(None)).await.unwrap();
    search_version_two(&mut conn).await;
    for (position, output_index) in [(1, 0), (2, 3), (3, 0), (4, i32::from(i16::MAX))] {
        sqlx::query("WITH event AS (INSERT INTO core.events (id,event_type,source,run_id,material_id,anchor,output_index,ts_quality,commit_seq,payload,reading_order,record_hash) VALUES (uuidv7(),'command.imported','test',uuidv7(),uuidv7(),0,$2,'unknown',$1,'{}',1,decode(repeat('01',32),'hex')) RETURNING id) INSERT INTO core.search_text (event_id,text) SELECT id,'cargo test' FROM event")
            .bind(i64::from(position))
            .bind(i16::try_from(output_index).unwrap())
            .execute(&mut conn)
            .await
            .unwrap();
    }
    conn.execute("INSERT INTO core.search_text (event_id,text) VALUES (uuidv7(),'no reading')")
        .await
        .unwrap();
    let storage = "SELECT array_agg(pg_relation_filenode(relation)::bigint ORDER BY relation::text) FROM show_chunks('core.events') relation";
    let before: Vec<i64> = sqlx::query_scalar(storage)
        .fetch_one(&mut conn)
        .await
        .unwrap();
    assert!(!before.is_empty());
    apply(&mut conn).await.unwrap();
    let after: Vec<i64> = sqlx::query_scalar(storage)
        .fetch_one(&mut conn)
        .await
        .unwrap();
    assert_eq!(after, before, "no event chunk is rewritten");
    let written: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM core.events, (SELECT xmin::text AS transaction FROM sinex_schema.applied WHERE version=$1) upgrade WHERE xmin::text=transaction",
    )
    .bind(VERSION)
    .fetch_one(&mut conn)
    .await
    .unwrap();
    assert_eq!(written, 2);
    let ordinals: Vec<i64> =
        sqlx::query_scalar("SELECT output_index FROM core.events ORDER BY commit_seq")
            .fetch_all(&mut conn)
            .await
            .unwrap();
    assert_eq!(ordinals, [0, 3, 0, i64::from(i16::MAX)]);
}

/// Decoding an encoded key returns it, and distinct keys encode distinctly,
/// so the upgrade's key conversion needs no comparison against a copy.
#[devkit::test]
async fn equivalence_key_encoding_round_trips_and_keeps_keys_distinct(
    db: devkit::pg::TestDatabase,
) {
    let mut conn = PgConnection::connect(&db.url(None)).await.unwrap();
    let (changed, merged): (i64, i64) = sqlx::query_as(
        r#"WITH digests AS (SELECT md5(i::text)||md5((i+1)::text) AS digest FROM generate_series(1,500) i),
         keys(key) AS (
           SELECT digest FROM digests UNION SELECT upper(digest) FROM digests
           UNION SELECT 'hash:'||digest FROM digests UNION SELECT 'sealed:'||digest FROM digests
           UNION SELECT 'hash:'||upper(digest) FROM digests UNION SELECT 'sealed:'||left(digest,63) FROM digests
           UNION SELECT digest||chr(10) FROM digests UNION SELECT left(digest,63)||'g' FROM digests
           UNION SELECT 'Hash:'||digest FROM digests UNION SELECT '01'||left(digest,62) FROM digests
           UNION SELECT chr(1)||digest FROM digests UNION SELECT chr(3)||digest FROM digests
           UNION SELECT 'zażółć 🦀 '||digest FROM digests UNION SELECT digest||' 字Ωж' FROM digests
           UNION VALUES (''), ('zażółć 🦀'), ('03a7'), ('hash:'), ('sealed:'))
         SELECT count(*) FILTER (WHERE core.decode_equivalence_key(core.encode_equivalence_key(key)) IS DISTINCT FROM key),
                count(*) - count(DISTINCT core.encode_equivalence_key(key))
         FROM keys"#,
    )
    .fetch_one(&mut conn)
    .await
    .unwrap();
    assert_eq!((changed, merged), (0, 0));
}

mod frame_outcomes;
mod online;
mod preservation;

#[devkit::test]
async fn query_kinds_keep_their_definitions_when_apply_recreates_a_view(
    db: devkit::pg::TestDatabase,
) {
    let mut conn = <PgConnection as Connection>::connect(&db.url(None))
        .await
        .unwrap();
    let before = crate::kinds::all(&mut conn).await.unwrap();
    conn.execute("DROP VIEW event_types.query_saved")
        .await
        .unwrap();
    apply(&mut conn).await.unwrap();
    assert_eq!(crate::kinds::all(&mut conn).await.unwrap(), before);
    conn.execute("SELECT * FROM event_types.query_saved LIMIT 0")
        .await
        .unwrap();
    assert!(
        conn.execute(
            "UPDATE registry.event_types SET payload_schema='{}'::jsonb WHERE name='query.saved'"
        )
        .await
        .is_err()
    );
}

mod kinds;

/// A synthetic downgrade leaves unused column slots; names and definitions remain the contract.
pub(crate) async fn catalog(conn: &mut PgConnection) -> Result<Vec<String>, Error> {
    Ok(super::catalog(conn)
        .await?
        .into_iter()
        .map(|row| match row.split_once(' ') {
            Some((name, rest)) if name.matches('.').count() == 2 => {
                let rest = rest.split_once(' ').map_or(rest, |(_, after)| after);
                format!("{name} {rest}")
            }
            _ => row,
        })
        .collect())
}
