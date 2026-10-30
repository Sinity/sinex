use super::*;

async fn version_two(conn: &mut PgConnection) {
    crate::test_schema::downgrade(conn).await;
    sqlx::raw_sql(include_str!(
        "../../../../sinex-schema/src/tests/version_two.sql"
    ))
    .execute(conn)
    .await
    .unwrap();
}

async fn other_rows(
    conn: &mut PgConnection,
) -> std::collections::BTreeMap<String, serde_json::Value> {
    let compact: bool = sqlx::query_scalar("SELECT data_type='bytea' FROM information_schema.columns WHERE table_schema='core' AND table_name='live_equivalence' AND column_name='equivalence_key'").fetch_one(&mut *conn).await.unwrap();
    let tables: Vec<(String, String)> = sqlx::query_as(
        "SELECT schemaname, tablename FROM pg_tables
         WHERE schemaname IN ('core', 'raw', 'audit', 'privacy', 'derivation')
           AND NOT (schemaname = 'core' AND tablename = 'published')",
    )
    .fetch_all(&mut *conn)
    .await
    .unwrap();
    let mut rows = std::collections::BTreeMap::new();
    for (schema, table) in tables {
        if schema == "core" && table == "replay_refusals" {
            let count: i64 = sqlx::query_scalar("SELECT count(*) FROM core.replay_refusals")
                .fetch_one(&mut *conn)
                .await
                .unwrap();
            assert_eq!(count, 0);
            continue;
        }
        let name = format!("{schema}.{table}");
        let value = if table == "live_equivalence" && compact {
            "to_jsonb(t)||jsonb_build_object('equivalence_key',core.decode_equivalence_key(equivalence_key))"
        } else if matches!(table.as_str(), "events" | "admission_stage") {
            "to_jsonb(t)-'kind_version'-'validation'-'producer_family'-'producer_identity'-'producer_host'-'producer_frame'-'source_version'-'source_fingerprint'"
        } else if table == "event_tombstones" {
            "to_jsonb(t)-'commit_seq'-'original_commit_seq'-'class'"
        } else if table == "ingress_frames" {
            "to_jsonb(t)-'family'-'write_kind_prefixes'-'source_name'-'source_version'-'source_fingerprint'-'producer_host'"
        } else if table == "search_text" {
            "to_jsonb(t)-'trigram_indexed'"
        } else {
            "to_jsonb(t)"
        };
        let predicate = if table == "counters" {
            " WHERE name NOT IN ('blob_changes','excised','privacy_outputs','query_metadata')"
        } else {
            ""
        };
        let sql = format!(
            "SELECT coalesce(jsonb_agg({value} ORDER BY {value}), '[]'::jsonb) FROM \"{schema}\".\"{table}\" t{predicate}"
        );
        rows.insert(
            name,
            sqlx::query_scalar(&sql)
                .fetch_one(&mut *conn)
                .await
                .unwrap(),
        );
    }
    rows
}

#[devkit::test]
async fn upgrade_keeps_every_mapping_including_tombstones_without_commit_rows(
    db: devkit::pg::TestDatabase,
) {
    let mut conn = crate::AdmissionContext::connect(&db.url(None))
        .await
        .unwrap();
    let ids = crate::query::tests::admit_commands(
        &mut conn,
        &[("pwd", "/sample"), ("echo example", "/sample")],
    )
    .await;
    crate::read_models::tests::catch_up_search_text(&mut conn)
        .await
        .unwrap();
    let batch = sinex_model::Batch {
        run: sinex_model::RunId::new(),
        kind: sinex_model::BatchKind::Api {
            actor: "test".into(),
        },
        records: vec![],
        archive: vec![sinex_model::ArchiveRequest::Hide {
            target: sinex_model::EventId(ids[0].1),
            operation: sinex_model::OperationId::new(),
        }],
    };
    let mut tx = conn.begin().await.unwrap();
    tx.admit(&batch, &crate::NoDerivations).await.unwrap();
    tx.commit().await.unwrap();
    drop(conn);
    let mut conn = PgConnection::connect(&db.url(None)).await.unwrap();
    sqlx::raw_sql(
        "INSERT INTO core.blobs(hash, size, status) VALUES ('blake3:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa', 9, 'present');
         INSERT INTO raw.materials(id, blob, length, manifest, parser, parser_version)
           VALUES ('00000000-0000-0000-0000-000000000099', 'blake3:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa', 9, '{}', 'example', 1);
         INSERT INTO raw.failures(material_id, anchor, reason, run_id)
           VALUES ('00000000-0000-0000-0000-000000000099', 3, 'synthetic malformed record', '00000000-0000-0000-0000-000000000098');
         INSERT INTO raw.bindings(binding, source, input, settings) VALUES ('example', 'test.source', 'example', '{}');
         INSERT INTO raw.positions(binding, cursor, carry, committed_seq) VALUES ('example', '{\"offset\":9}', '{}', 4);
         INSERT INTO raw.reader_memory(binding, material_id, memory) VALUES ('example', '00000000-0000-0000-0000-000000000099', decode('010203', 'hex'));
         INSERT INTO privacy.source_classes(source, class, author) VALUES ('test.source', 'guarded', 'example');
         INSERT INTO privacy.rules(id, name, source, matcher, class, action, author)
           VALUES ('00000000-0000-0000-0000-000000000097', 'example', 'test.source', 'all', 'guarded', 'classify', 'example')",
    ).execute(&mut conn).await.unwrap();
    version_two(&mut conn).await;
    sqlx::query("INSERT INTO core.published SELECT md5(i::text)::uuid, i * 3 FROM generate_series(1, 2503) i")
        .execute(&mut conn).await.unwrap();
    let before: Vec<(Uuid, i64)> =
        sqlx::query_as("SELECT event_id, stream_seq FROM core.published ORDER BY event_id")
            .fetch_all(&mut conn)
            .await
            .unwrap();
    tombstone(&mut conn, before[1000].0).await;
    set_consumer_position(&mut conn, "publisher", 4500, 150)
        .await
        .unwrap();
    let tombstones: serde_json::Value =
        sqlx::query_scalar("SELECT jsonb_agg(to_jsonb(t)-'commit_seq'-'original_commit_seq'-'class') FROM core.event_tombstones t")
            .fetch_one(&mut conn)
            .await
            .unwrap();
    let other_before = other_rows(&mut conn).await;
    let mut writer = PgConnection::connect(&db.url(None)).await.unwrap();
    sqlx::query("SELECT pg_advisory_lock($1)")
        .bind(sinex_schema::WRITER_LOCK)
        .execute(&mut writer)
        .await
        .unwrap();
    assert!(matches!(
        sinex_schema::apply(&mut conn).await,
        Err(sinex_schema::Error::WriterRunning)
    ));
    sqlx::query("SELECT pg_advisory_unlock($1)")
        .bind(sinex_schema::WRITER_LOCK)
        .execute(&mut writer)
        .await
        .unwrap();
    assert_eq!(
        sinex_schema::apply(&mut conn).await.unwrap(),
        sinex_schema::Applied::Upgraded {
            from: 2,
            to: sinex_schema::VERSION
        }
    );
    assert_eq!(pairs(&mut conn).await, before);
    let mut other_after = other_rows(&mut conn).await;
    other_after.retain(|name, _| other_before.contains_key(name));
    assert_eq!(
        other_after, other_before,
        "every predecessor table preserves its stored values; new tables have no predecessor rows"
    );
    let unknown: (Option<i64>, Option<i64>, Option<String>) = sqlx::query_as(
        "SELECT commit_seq,original_commit_seq,class FROM core.event_tombstones WHERE event_id=$1",
    )
    .bind(before[1000].0)
    .fetch_one(&mut conn)
    .await
    .unwrap();
    assert_eq!(
        unknown,
        (None, None, None),
        "a publication placement does not invent missing erasure history"
    );
    let edges: Vec<(Uuid, Uuid)> =
        sqlx::query_as("SELECT parent,child FROM core.event_children ORDER BY parent,child")
            .fetch_all(&mut conn)
            .await
            .unwrap();
    let retained:Vec<(Uuid,Uuid)>=sqlx::query_as("SELECT parent,id FROM core.events CROSS JOIN LATERAL unnest(parents) parent ORDER BY parent,id").fetch_all(&mut conn).await.unwrap();
    assert_eq!(edges, retained);
    assert_eq!(
        consumer_position(&mut conn, "publisher").await.unwrap(),
        (4500, 150)
    );
    let after: serde_json::Value =
        sqlx::query_scalar("SELECT jsonb_agg(to_jsonb(t)-'commit_seq'-'original_commit_seq'-'class') FROM core.event_tombstones t")
            .fetch_one(&mut conn)
            .await
            .unwrap();
    assert_eq!(after, tombstones);
    let constraints: String = sqlx::query_scalar("SELECT pg_get_constraintdef(oid) FROM pg_constraint WHERE conrelid='core.published'::regclass AND contype='p'")
        .fetch_one(&mut conn).await.unwrap();
    assert_eq!(
        constraints,
        "PRIMARY KEY (event_id, stream_seq, stream_instance)"
    );
    let erased = excised_on_stream(&mut conn, 10).await.unwrap();
    assert_eq!((erased[0].event_id, erased[0].stream_seq), before[1000]);
    sqlx::query("SET ROLE sinex_writer")
        .execute(&mut conn)
        .await
        .unwrap();
    record_published(&mut conn, &[(Uuid::now_v7(), 99999)])
        .await
        .unwrap();
    forget_published(&mut conn, &[before[1000]]).await.unwrap();
    sqlx::query("RESET ROLE").execute(&mut conn).await.unwrap();
    assert_eq!(pairs(&mut conn).await.len(), before.len());
    let retained_pairs = pairs(&mut conn).await;
    let erased_pair = before[1000];
    record_published_in(&mut conn, "synthetic-native-a", &[erased_pair, erased_pair])
        .await
        .unwrap();
    record_published_in(&mut conn, "synthetic-native-b", &[erased_pair])
        .await
        .unwrap();
    for instance in ["synthetic-native-a", "synthetic-native-b"] {
        let copies = excised_on_stream_in(&mut conn, instance, 10).await.unwrap();
        assert_eq!(
            copies
                .iter()
                .map(|copy| (copy.event_id, copy.stream_seq))
                .collect::<Vec<_>>(),
            [erased_pair],
            "the same event and sequence are distinct placements in each native incarnation"
        );
    }
    forget_published_in(&mut conn, "synthetic-native-a", &[erased_pair])
        .await
        .unwrap();
    assert!(
        excised_on_stream_in(&mut conn, "synthetic-native-a", 10)
            .await
            .unwrap()
            .is_empty()
    );
    let other = excised_on_stream_in(&mut conn, "synthetic-native-b", 10)
        .await
        .unwrap();
    assert_eq!(
        other
            .iter()
            .map(|copy| (copy.event_id, copy.stream_seq))
            .collect::<Vec<_>>(),
        [erased_pair],
        "erasing one incarnation never forgets another's physical copy"
    );
    forget_published_in(&mut conn, "synthetic-native-b", &[erased_pair])
        .await
        .unwrap();
    assert_eq!(
        pairs(&mut conn).await,
        retained_pairs,
        "scoped native erasures preserve every unrelated publication mapping"
    );
    assert_eq!(
        sinex_schema::apply(&mut conn).await.unwrap(),
        sinex_schema::Applied::Current {
            version: sinex_schema::VERSION
        }
    );
}

#[devkit::test]
async fn failed_pair_upgrade_rolls_back_the_constraint_and_every_mapping(
    db: devkit::pg::TestDatabase,
) {
    let mut conn = PgConnection::connect(&db.url(None)).await.unwrap();
    version_two(&mut conn).await;
    sqlx::raw_sql("INSERT INTO core.published VALUES ('00000000-0000-0000-0000-000000000001', 99); CREATE INDEX published_pairs_pkey ON core.consumer_positions(commit_seq)")
        .execute(&mut conn).await.unwrap();
    assert!(sinex_schema::apply(&mut conn).await.is_err());
    let version: i32 = sqlx::query_scalar("SELECT max(version) FROM sinex_schema.applied")
        .fetch_one(&mut conn)
        .await
        .unwrap();
    assert_eq!(version, 2);
    let seq: i64 = sqlx::query_scalar("SELECT stream_seq FROM core.published")
        .fetch_one(&mut conn)
        .await
        .unwrap();
    assert_eq!(seq, 99);
    sqlx::query("DROP INDEX core.published_pairs_pkey")
        .execute(&mut conn)
        .await
        .unwrap();
    sinex_schema::apply(&mut conn).await.unwrap();
    assert_eq!(pairs(&mut conn).await, [(Uuid::from_u128(1), 99)]);
}

#[devkit::test]
async fn empty_publication_table_upgrades_and_accepts_multiple_copies(
    db: devkit::pg::TestDatabase,
) {
    let mut conn = PgConnection::connect(&db.url(None)).await.unwrap();
    version_two(&mut conn).await;
    sinex_schema::apply(&mut conn).await.unwrap();
    assert!(pairs(&mut conn).await.is_empty());
    let id = Uuid::now_v7();
    record_published(&mut conn, &[(id, 8), (id, 9), (id, 8)])
        .await
        .unwrap();
    assert_eq!(pairs(&mut conn).await, [(id, 8), (id, 9)]);
}
