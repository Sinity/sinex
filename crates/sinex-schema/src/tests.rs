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

async fn applied_version(conn: &mut PgConnection) -> Option<i32> {
    sqlx::query_scalar("SELECT max(version) FROM sinex_schema.applied")
        .fetch_one(conn)
        .await
        .unwrap()
}

/// A database at a version this build has no steps from, newer or older, is
/// refused by every entry point and left as it was.
#[devkit::test]
async fn a_version_without_steps_to_this_one_is_refused_unchanged(db: devkit::pg::TestDatabase) {
    let mut conn = PgConnection::connect(&db.url(None)).await.unwrap();
    let owners = crate::kinds::Owners::default();
    let fresh = catalog(&mut conn).await.unwrap();
    for found in [VERSION + 1, upgrades::LANDED_VERSION - 1] {
        sqlx::query("DELETE FROM sinex_schema.applied")
            .execute(&mut conn)
            .await
            .unwrap();
        sqlx::query("INSERT INTO sinex_schema.applied (version) VALUES ($1)")
            .bind(found)
            .execute(&mut conn)
            .await
            .unwrap();
        assert!(
            matches!(require_current(&mut conn).await, Err(Error::Version { found: f }) if f == found)
        );
        assert!(
            matches!(start(&mut conn, &owners).await, Err(Error::Version { found: f }) if f == found)
        );
        assert!(matches!(apply(&mut conn).await, Err(Error::Version { found: f }) if f == found));
        assert_eq!(applied_version(&mut conn).await, Some(found));
        assert_eq!(catalog(&mut conn).await.unwrap(), fresh);
    }
}

/// A forward step runs from its own version and records the next one.
#[devkit::test]
async fn a_forward_step_runs_from_its_version_and_records_the_next(db: devkit::pg::TestDatabase) {
    let mut conn = PgConnection::connect(&db.url(None)).await.unwrap();
    let mut steps = upgrades::STEPS.to_vec();
    steps.push(upgrades::Step {
        name: "synthetic",
        statements: || vec!["CREATE TABLE core.synthetic_step (value integer NOT NULL)".into()],
    });
    let mut tx = conn.begin().await.unwrap();
    run_steps(&mut tx, VERSION, &steps).await.unwrap();
    tx.commit().await.unwrap();
    assert_eq!(applied_version(&mut conn).await, Some(VERSION + 1));
    sqlx::query("INSERT INTO core.synthetic_step VALUES (1)")
        .execute(&mut conn)
        .await
        .unwrap();
}

/// Decoding an encoded key returns it, and distinct keys encode distinctly,
/// so the compact stored form keeps every key and never merges two.
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
