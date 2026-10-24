use super::*;

// Frozen from revision 5cc1532395521f4f6a57632d000bf13ada1bc47d and
// independently checked against its native schema binary. This is not a
// version-1 declaration with selected changes undone.
pub(crate) async fn version_zero(conn: &mut PgConnection) {
    let drop_all = format!(
        "DROP SCHEMA {} CASCADE",
        schemas()
            .iter()
            .map(|s| quote_ident(s))
            .collect::<Vec<_>>()
            .join(",")
    );
    conn.execute(drop_all.as_str()).await.unwrap();
    let fixture: serde_json::Value =
        serde_json::from_str(include_str!("v0-declarations.json")).unwrap();
    for sql in fixture["statements"].as_array().unwrap() {
        conn.execute(sql.as_str().unwrap()).await.unwrap();
    }
    conn.execute("CREATE TABLE sinex_schema.applied(version integer PRIMARY KEY,applied_at timestamptz NOT NULL DEFAULT now()); INSERT INTO sinex_schema.applied(version) VALUES(0)").await.unwrap();
    for row in fixture["registry"].as_array().unwrap() {
        let fields: Vec<String> = serde_json::from_value(row[3].clone()).unwrap();
        sqlx::query("INSERT INTO registry.event_types(name,payload_schema,key_kind,key_fields,replace_rule) VALUES($1,$2,$3,$4,$5)")
            .bind(row[0].as_str().unwrap()).bind(&row[1]).bind(row[2].as_str().unwrap()).bind(fields).bind(row[4].as_str().unwrap()).execute(&mut *conn).await.unwrap();
    }
}

#[tokio::test]
async fn explicit_zero_upgrade_matches_fresh_definitions_and_preserves_legacy_data() {
    let db = devkit::pg::test_database().unwrap();
    let mut conn = PgConnection::connect(&db.url(None)).await.unwrap();
    let fresh = catalog(&mut conn).await.unwrap();
    version_zero(&mut conn).await;
    conn.execute("INSERT INTO core.events(id,event_type,source,run_id,ts_quality,parents,commit_seq,payload,reading_order,record_hash) VALUES('019a1234-1234-7000-8000-000000000001','command.executed','fixture','00000000-0000-0000-0000-000000000002','unknown',ARRAY['019a1234-1234-7000-8000-000000000003'::uuid],1,'{\"command\":\"preserved exact text\"}',1,decode(repeat('ab',32),'hex')); INSERT INTO core.commit_log VALUES(1,'019a1234-1234-7000-8000-000000000001','command.executed'); INSERT INTO privacy.material_keys(material_id,key_ref,class,wrapped_key) VALUES('00000000-0000-0000-0000-000000000004','neutral-key','sealed',decode('00ff10','hex')); INSERT INTO core.blobs(hash,size,status) VALUES('blake3:0000000000000000000000000000000000000000000000000000000000000000',17,'present')").await.unwrap();
    let exact_before: Vec<serde_json::Value> = sqlx::query_scalar("SELECT to_jsonb(e) FROM core.events e UNION ALL SELECT to_jsonb(k)-'role' FROM privacy.material_keys k UNION ALL SELECT to_jsonb(b) FROM core.blobs b UNION ALL SELECT to_jsonb(t) FROM registry.event_types t WHERE name NOT IN ('note.document','forecast.issued','forecast.resolved') ORDER BY 1")
        .fetch_all(&mut conn).await.unwrap();
    // A materialless archived identifier is legal metadata, including after
    // its Event has been excised. Upgrading must not invent an Event for it.
    conn.execute("INSERT INTO audit.archived(event_id,archived_seq,reason,archived_at) VALUES('00000000-0000-0000-0000-000000000001',0,'hide','2026-01-01Z'); UPDATE core.counters SET value=7 WHERE name='commit_seq'").await.unwrap();
    let before: serde_json::Value = sqlx::query_scalar("SELECT to_jsonb(a) FROM audit.archived a")
        .fetch_one(&mut conn)
        .await
        .unwrap();
    assert!(matches!(
        require_current(&mut conn).await,
        Err(Error::Upgrade { found: 0, .. })
    ));
    assert_eq!(
        apply(&mut conn).await.unwrap(),
        Applied::Upgraded {
            from: 0,
            version: VERSION
        }
    );
    assert_eq!(catalog(&mut conn).await.unwrap(), fresh);
    let after: serde_json::Value = sqlx::query_scalar("SELECT to_jsonb(a) FROM audit.archived a")
        .fetch_one(&mut conn)
        .await
        .unwrap();
    assert_eq!(after, before, "current metadata/notice identity stay exact");
    let exact_after: Vec<serde_json::Value> = sqlx::query_scalar("SELECT to_jsonb(e) FROM core.events e UNION ALL SELECT to_jsonb(k)-'role' FROM privacy.material_keys k UNION ALL SELECT to_jsonb(b) FROM core.blobs b UNION ALL SELECT to_jsonb(t) FROM registry.event_types t WHERE name NOT IN ('note.document','forecast.issued','forecast.resolved') ORDER BY 1")
        .fetch_all(&mut conn).await.unwrap();
    assert_eq!(
        exact_after, exact_before,
        "event IDs, payloads, keys and blob references stay exact"
    );
    assert!(sqlx::query_scalar::<_,bool>("SELECT c.persisted_at=e.ts_persisted FROM core.commit_log c JOIN core.events e ON e.id=c.event_id").fetch_one(&mut conn).await.unwrap());
    let baseline: (i64, i64, i64) = sqlx::query_as(
        "SELECT min(commit_seq),max(commit_seq),count(*) FROM audit.archive_changes WHERE baseline",
    )
    .fetch_one(&mut conn)
    .await
    .unwrap();
    assert_eq!(baseline, (8, 8, 2));
    let originals:(i64,String)=sqlx::query_as("SELECT archived_seq,archived_at::text FROM audit.archive_changes WHERE event_id IS NOT NULL").fetch_one(&mut conn).await.unwrap();
    assert_eq!(originals.0, 0);
    assert!(originals.1.starts_with("2026-01-01"));
    assert_eq!(
        apply(&mut conn).await.unwrap(),
        Applied::Current { version: VERSION }
    );
}

#[tokio::test]
async fn unknown_zero_shape_and_active_writer_refuse_without_partial_upgrade() {
    let db = devkit::pg::test_database().unwrap();
    let url = db.url(None);
    let mut conn = PgConnection::connect(&url).await.unwrap();
    version_zero(&mut conn).await;
    let mut writer = PgConnection::connect(&url).await.unwrap();
    sqlx::query("SELECT pg_advisory_lock($1)")
        .bind(WRITER_LOCK)
        .execute(&mut writer)
        .await
        .unwrap();
    assert!(matches!(apply(&mut conn).await, Err(Error::ActiveWriter)));
    sqlx::query("SELECT pg_advisory_unlock($1)")
        .bind(WRITER_LOCK)
        .execute(&mut writer)
        .await
        .unwrap();
    conn.execute("ALTER TABLE core.counters ADD COLUMN unrecognized text")
        .await
        .unwrap();
    let before = catalog(&mut conn).await.unwrap();
    assert!(matches!(apply(&mut conn).await, Err(Error::Shape(0))));
    assert_eq!(catalog(&mut conn).await.unwrap(), before);
    assert_eq!(
        sqlx::query_scalar::<_, i32>("SELECT max(version) FROM sinex_schema.applied")
            .fetch_one(&mut conn)
            .await
            .unwrap(),
        0
    );
}

#[tokio::test]
async fn schema_version_read_holds_admission_compatibility_until_transaction_finishes() {
    let db = devkit::pg::test_database().unwrap();
    let mut admission = PgConnection::connect(&db.url(None)).await.unwrap();
    let mut upgrade = PgConnection::connect(&db.url(None)).await.unwrap();
    let mut tx = admission.begin().await.unwrap();
    require_current(&mut tx).await.unwrap();
    assert!(matches!(
        apply(&mut upgrade).await,
        Err(Error::ActiveWriter)
    ));
    tx.rollback().await.unwrap();
    assert_eq!(
        apply(&mut upgrade).await.unwrap(),
        Applied::Current { version: VERSION }
    );
}

#[tokio::test]
async fn declared_writer_role_can_hold_the_schema_version_lock_before_admission() {
    let db = devkit::pg::test_database().unwrap();
    let mut conn = PgConnection::connect(&db.url(None)).await.unwrap();
    conn.execute("SET ROLE sinex_writer").await.unwrap();
    require_current(&mut conn).await.unwrap();
}

#[tokio::test]
async fn current_schema_apply_releases_its_locks_before_the_connection_is_used_again() {
    let db = devkit::pg::test_database().unwrap();
    let mut idle_owner = PgConnection::connect(&db.url(None)).await.unwrap();
    let mut next_writer = PgConnection::connect(&db.url(None)).await.unwrap();
    assert_eq!(
        apply(&mut idle_owner).await.unwrap(),
        Applied::Current { version: VERSION }
    );
    // No more IO on idle_owner: SQLx's queued rollback on Drop is not enough.
    tokio::time::timeout(
        std::time::Duration::from_secs(2),
        require_current(&mut next_writer),
    )
    .await
    .expect("schema apply returned while still blocking the next writer")
    .unwrap();
}

#[tokio::test]
async fn refused_schema_apply_releases_its_locks_before_the_connection_is_used_again() {
    let db = devkit::pg::test_database().unwrap();
    let mut idle_owner = PgConnection::connect(&db.url(None)).await.unwrap();
    version_zero(&mut idle_owner).await;
    idle_owner
        .execute("ALTER TABLE core.counters ADD COLUMN unrecognized text")
        .await
        .unwrap();
    let mut next_writer = PgConnection::connect(&db.url(None)).await.unwrap();
    assert!(matches!(apply(&mut idle_owner).await, Err(Error::Shape(0))));
    let version: i32 = tokio::time::timeout(
        std::time::Duration::from_secs(2),
        sqlx::query_scalar(
            "SELECT version FROM sinex_schema.applied ORDER BY version DESC LIMIT 1 FOR SHARE",
        )
        .fetch_one(&mut next_writer),
    )
    .await
    .expect("refused schema apply retained its transaction locks")
    .unwrap();
    assert_eq!(version, 0);
}

#[tokio::test]
async fn failed_fresh_creation_releases_ddl_locks_before_the_connection_is_used_again() {
    let db = devkit::pg::test_database().unwrap();
    let mut idle_owner = PgConnection::connect(&db.url(None)).await.unwrap();
    let drop_all = format!(
        "DROP SCHEMA {} CASCADE",
        schemas()
            .iter()
            .map(|s| quote_ident(s))
            .collect::<Vec<_>>()
            .join(",")
    );
    idle_owner.execute(drop_all.as_str()).await.unwrap();
    // Existing audit makes creation fail after core was created in its tx.
    idle_owner.execute("CREATE SCHEMA audit").await.unwrap();
    let mut next_creator = PgConnection::connect(&db.url(None)).await.unwrap();
    let error = apply(&mut idle_owner).await.unwrap_err();
    assert!(
        matches!(&error, Error::Database(_))
            && error
                .to_string()
                .contains("schema \"audit\" already exists"),
        "{error}"
    );
    tokio::time::timeout(
        std::time::Duration::from_secs(2),
        next_creator.execute("CREATE SCHEMA core"),
    )
    .await
    .expect("failed creation retained its uncommitted schema locks")
    .unwrap();
    let audit: bool =
        sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM pg_namespace WHERE nspname='audit')")
            .fetch_one(&mut next_creator)
            .await
            .unwrap();
    assert!(audit, "the pre-existing schema stays intact");
}

#[tokio::test]
async fn explicit_two_to_three_upgrade_preserves_legacy_key_bytes_and_matches_fresh_schema() {
    let db = devkit::pg::test_database().unwrap();
    let mut conn = PgConnection::connect(&db.url(None)).await.unwrap();
    let fresh = catalog(&mut conn).await.unwrap();
    version_zero(&mut conn).await;
    changes::version_two(&mut conn).await;
    conn.execute("INSERT INTO privacy.material_keys(material_id,key_ref,class,wrapped_key) VALUES('00000000-0000-0000-0000-000000000004','legacy-key','sealed',decode(repeat('a1',32),'hex'))").await.unwrap();
    let before: serde_json::Value =
        sqlx::query_scalar("SELECT to_jsonb(k) FROM privacy.material_keys k")
            .fetch_one(&mut conn)
            .await
            .unwrap();
    assert!(matches!(
        require_current(&mut conn).await,
        Err(Error::Upgrade {
            found: 2,
            declared: VERSION
        })
    ));
    assert_eq!(
        apply(&mut conn).await.unwrap(),
        Applied::Upgraded {
            from: 2,
            version: VERSION
        }
    );
    assert_eq!(catalog(&mut conn).await.unwrap(), fresh);
    let after: serde_json::Value =
        sqlx::query_scalar("SELECT to_jsonb(k)-'role' FROM privacy.material_keys k")
            .fetch_one(&mut conn)
            .await
            .unwrap();
    assert_eq!(after, before);
    let role: String = sqlx::query_scalar("SELECT role FROM privacy.material_keys")
        .fetch_one(&mut conn)
        .await
        .unwrap();
    assert_eq!(role, "data-and-record-naming");
    assert_eq!(
        apply(&mut conn).await.unwrap(),
        Applied::Current { version: VERSION }
    );
}

#[tokio::test]
async fn unknown_two_shape_refuses_before_any_key_or_replay_column_changes() {
    let db = devkit::pg::test_database().unwrap();
    let mut conn = PgConnection::connect(&db.url(None)).await.unwrap();
    version_zero(&mut conn).await;
    changes::version_two(&mut conn).await;
    conn.execute("ALTER TABLE raw.positions ADD COLUMN unrecognized text")
        .await
        .unwrap();
    let before = catalog(&mut conn).await.unwrap();
    assert!(matches!(apply(&mut conn).await, Err(Error::Shape(2))));
    assert_eq!(catalog(&mut conn).await.unwrap(), before);
    assert_eq!(
        sqlx::query_scalar::<_, i32>("SELECT max(version) FROM sinex_schema.applied")
            .fetch_one(&mut conn)
            .await
            .unwrap(),
        2
    );
}
