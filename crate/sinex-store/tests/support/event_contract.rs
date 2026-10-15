//! Direct SQL fixtures distinguish database laws; they are not supported producer routes.
use sinex_store::{Error, check_schema};
use sqlx::{Connection, Executor, PgConnection};
use uuid::Uuid;
type Result<T> = std::result::Result<T, Box<dyn std::error::Error>>;

struct Case<'a> {
    id: Uuid,
    material: Option<Uuid>,
    parents: Option<Vec<Option<Uuid>>>,
    producer: Option<Uuid>,
    original: Option<&'a str>,
    quality: Option<&'a str>,
    precision: &'a str,
    until: Option<&'a str>,
    clock: &'a str,
    key: Option<&'a str>,
}
impl Case<'_> {
    fn derived(parent: Uuid) -> Self {
        Self {
            id: Uuid::now_v7(),
            material: None,
            parents: Some(vec![Some(parent)]),
            producer: Some(Uuid::now_v7()),
            original: None,
            quality: Some("unknown"),
            precision: "instant",
            until: None,
            clock: "sinex",
            key: Some("synthetic:derived"),
        }
    }
}
async fn insert_into(
    conn: &mut PgConnection,
    run: Uuid,
    c: &Case<'_>,
    reflection: bool,
) -> std::result::Result<(), sqlx::Error> {
    let sql="INSERT INTO core.events (id,source,event_type,payload,commit_seq,source_material_id,anchor_byte,source_event_ids,producer_version_id,run_id,record_hash,ts_orig,ts_quality,ts_precision,ts_orig_until,ts_clock,equivalence_key) VALUES ($1,'synthetic.schema','synthetic.observed','{}',1000000,$2,CASE WHEN $2::uuid IS NULL THEN NULL ELSE 0 END,$3,$4,$5,$6,$7::text::timestamptz,$8,$9,$10::text::timestamptz,$11,$12)".replace("core.events",if reflection {"reflection.events"} else {"core.events"});
    sqlx::query(&sql)
        .bind(c.id)
        .bind(c.material)
        .bind(&c.parents)
        .bind(c.producer)
        .bind(run)
        .bind(vec![0_u8; 32])
        .bind(c.original)
        .bind(c.quality)
        .bind(c.precision)
        .bind(c.until)
        .bind(c.clock)
        .bind(c.key)
        .execute(conn)
        .await?;
    Ok(())
}
async fn insert(
    conn: &mut PgConnection,
    run: Uuid,
    c: &Case<'_>,
) -> std::result::Result<(), sqlx::Error> {
    insert_into(conn, run, c, false).await
}
async fn denied(conn: &mut PgConnection, run: Uuid, c: &Case<'_>, code: &str) -> Result<()> {
    let error = insert(conn, run, c)
        .await
        .expect_err("invalid event must be refused");
    assert_eq!(
        error.as_database_error().and_then(|e| e.code()).as_deref(),
        Some(code)
    );
    Ok(())
}

pub async fn verify(admin: &str, reader: &str, parent: Uuid, material: Uuid) -> Result<()> {
    let mut conn = PgConnection::connect(admin).await?;
    let material_error=sqlx::query("INSERT INTO raw.source_material_registry (id,material_kind,source_identifier,status,ts_quality,blob_id,total_bytes,durable_bytes) SELECT '00000000-0000-0000-0000-000000000000',material_kind,source_identifier,status,ts_quality,blob_id,total_bytes,durable_bytes FROM raw.source_material_registry WHERE id=$1").bind(material).execute(&mut conn).await.expect_err("non-RFC4122 material UUID must be refused");
    assert_eq!(
        material_error
            .as_database_error()
            .and_then(|e| e.code())
            .as_deref(),
        Some("23514")
    );
    let run: Uuid = sqlx::query_scalar("SELECT run_id FROM core.events WHERE id=$1")
        .bind(parent)
        .fetch_one(&mut conn)
        .await?;
    let mut c = Case::derived(parent);
    c.material = Some(material);
    denied(&mut conn, run, &c, "23514").await?;
    let mut c = Case::derived(parent);
    c.parents = None;
    denied(&mut conn, run, &c, "23514").await?;
    let mut c = Case::derived(parent);
    c.parents = Some(vec![]);
    denied(&mut conn, run, &c, "23514").await?;
    let mut c = Case::derived(parent);
    c.parents = Some(vec![None]);
    denied(&mut conn, run, &c, "23514").await?;
    let mut c = Case::derived(parent);
    c.producer = None;
    denied(&mut conn, run, &c, "23514").await?;
    let mut c = Case::derived(parent);
    c.key = None;
    denied(&mut conn, run, &c, "23514").await?;
    let mut c = Case::derived(parent);
    c.quality = None;
    denied(&mut conn, run, &c, "23502").await?;
    let valid = Uuid::now_v7();
    let mut v4 = *valid.as_bytes();
    v4[6] = (v4[6] & 15) | 0x40;
    let mut non_rfc = *valid.as_bytes();
    non_rfc[8] &= 0x3f;
    for reflection in [false, true] {
        let target = if reflection {
            "reflection.events"
        } else {
            "core.events"
        };
        let predicate:String=sqlx::query_scalar("SELECT pg_get_expr(conbin,conrelid) FROM pg_constraint WHERE conrelid=$1::regclass AND conname='id_is_uuidv7'").bind(target).fetch_one(&mut conn).await?;
        // This text comes only from the named constraint on an owned test table.
        // Evaluate it outside hypertable routing to distinguish false from SQL NULL.
        let predicate_sql = [
            "SELECT ",
            &predicate,
            " FROM (SELECT $1::uuid AS id) AS fixture",
        ]
        .concat();
        let valid_predicate: Option<bool> = sqlx::query_scalar(&predicate_sql)
            .bind(valid)
            .fetch_one(&mut conn)
            .await?;
        assert_eq!(valid_predicate, Some(true));
        for id in [Uuid::nil(), Uuid::from_bytes(v4), Uuid::from_bytes(non_rfc)] {
            let invalid_predicate: Option<bool> = sqlx::query_scalar(&predicate_sql)
                .bind(id)
                .fetch_one(&mut conn)
                .await?;
            assert_eq!(
                invalid_predicate,
                Some(false),
                "{target} constraint must reject invalid IDs without SQL NULL"
            );
            let mut c = Case::derived(parent);
            c.id = id;
            let error = insert_into(&mut conn, run, &c, reflection)
                .await
                .expect_err("both physical stores reject non-v7 IDs including NULL extraction");
            let database = error
                .as_database_error()
                .expect("UUID refusal must be a database error");
            match database.code().as_deref() {
                Some("23514") => assert_eq!(database.constraint(), Some("id_is_uuidv7")),
                // TimescaleDB 2.27.2 may reject during UUID dimension routing,
                // before PostgreSQL reaches the canonical event-row CHECK.
                Some("0A000") => {
                    assert_eq!(database.message(), format!("{id} is not a version 7 UUID"))
                }
                other => panic!("unexpected UUID refusal SQLSTATE: {other:?}"),
            }
            for target in ["core.events", "reflection.events"] {
                // A text predicate cannot itself route an invalid UUID value.
                let sql = "SELECT count(*) FROM core.events WHERE id::text=$1"
                    .replace("core.events", target);
                let survivors: i64 = sqlx::query_scalar(&sql)
                    .bind(id.to_string())
                    .fetch_one(&mut conn)
                    .await?;
                assert_eq!(survivors, 0, "invalid UUID survived in {target}");
            }
        }
    }
    let mut c = Case::derived(parent);
    c.precision = "bounded";
    c.until = Some("2000-01-01T00:00:00Z");
    denied(&mut conn, run, &c, "23514").await?;
    let mut c = Case::derived(parent);
    c.original = Some("2000-01-02T00:00:00Z");
    c.precision = "bounded";
    c.until = Some("2000-01-01T00:00:00Z");
    denied(&mut conn, run, &c, "23514").await?;
    let mut c = Case::derived(parent);
    c.original = Some("2100-01-01T00:00:00Z");
    c.quality = Some("captured");
    denied(&mut conn, run, &c, "23514").await?;
    let mut historical = Case::derived(parent);
    historical.original = Some("1999-01-01T00:00:00Z");
    historical.quality = Some("intrinsic");
    insert(&mut conn, run, &historical).await?;
    let mut external = Case::derived(parent);
    external.original = Some("2100-01-01T00:00:00Z");
    external.quality = Some("captured");
    external.clock = "producer";
    insert(&mut conn, run, &external).await?;
    let mut bounded = Case::derived(parent);
    bounded.original = Some("2000-01-01T00:00:00Z");
    bounded.quality = Some("intrinsic");
    bounded.precision = "bounded";
    bounded.until = bounded.original;
    insert(&mut conn, run, &bounded).await?;

    let count: i64 = sqlx::query_scalar(
        "SELECT parsed_event_count FROM raw.source_material_registry WHERE id=$1",
    )
    .bind(material)
    .fetch_one(&mut conn)
    .await?;
    sqlx::query("INSERT INTO core.events (id,source,event_type,payload,commit_seq,source_material_id,anchor_byte,ordinal,record_hash,run_id,ts_quality) SELECT id,source,event_type,payload,commit_seq,source_material_id,anchor_byte,ordinal,record_hash,run_id,ts_quality FROM core.events WHERE id=$1 ON CONFLICT(id) DO NOTHING").bind(parent).execute(&mut conn).await?;
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT parsed_event_count FROM raw.source_material_registry WHERE id=$1"
        )
        .bind(material)
        .fetch_one(&mut conn)
        .await?,
        count
    );
    let coordinate: Vec<u8> =
        sqlx::query_scalar("SELECT occ_hash FROM core.live_coordinates WHERE event_id=$1")
            .bind(parent)
            .fetch_one(&mut conn)
            .await?;
    let duplicate =
        sqlx::query("INSERT INTO core.live_coordinates (store,occ_hash,event_id) VALUES (0,$1,$2)")
            .bind(coordinate)
            .bind(Uuid::now_v7())
            .execute(&mut conn)
            .await
            .expect_err("duplicate live coordinates must fail");
    assert_eq!(
        duplicate
            .as_database_error()
            .and_then(|e| e.code())
            .as_deref(),
        Some("23505")
    );
    let eq: i64 =
        sqlx::query_scalar("SELECT count(*) FROM core.live_equivalence WHERE event_id=$1")
            .bind(parent)
            .fetch_one(&mut conn)
            .await?;
    assert_eq!(eq, 0);
    let immutable = sqlx::query("UPDATE core.events SET payload='{}' WHERE id=$1")
        .bind(parent)
        .execute(&mut conn)
        .await
        .expect_err("event rows immutable");
    assert_eq!(
        immutable
            .as_database_error()
            .and_then(|e| e.code())
            .as_deref(),
        Some("23514")
    );
    let delete = sqlx::query("DELETE FROM core.events WHERE id=$1")
        .bind(parent)
        .execute(&mut conn)
        .await
        .expect_err("no ordinary physical event delete");
    assert_eq!(
        delete.as_database_error().and_then(|e| e.code()).as_deref(),
        Some("23514")
    );

    for (change, restore) in [
        (
            "ALTER TABLE raw.source_material_registry ALTER COLUMN source_identifier TYPE text COLLATE \"C\"",
            "ALTER TABLE raw.source_material_registry ALTER COLUMN source_identifier TYPE text COLLATE \"default\"",
        ),
        (
            "ALTER INDEX core.events_orig SET (fillfactor=73)",
            "ALTER INDEX core.events_orig RESET (fillfactor)",
        ),
        (
            "ALTER TRIGGER event_guard ON core.events RENAME TO event_guard_fixture_drift",
            "ALTER TRIGGER event_guard_fixture_drift ON core.events RENAME TO event_guard",
        ),
        (
            "ALTER TABLE core.events SET (timescaledb.compress_orderby='id')",
            "ALTER TABLE core.events SET (timescaledb.compress_orderby='ts_orig,id')",
        ),
    ] {
        conn.execute(change).await?;
        let drift = check_schema(admin).await;
        conn.execute(restore).await?;
        assert!(matches!(drift, Err(Error::Schema(_))));
        check_schema(admin).await?;
    }
    let mut read = PgConnection::connect(reader).await?;
    for sql in [
        "ALTER TRIGGER event_guard ON core.events RENAME TO event_guard_fixture_drift",
        "INSERT INTO core.live_coordinates VALUES (0,decode('00000000000000000000000000000000','hex'),'00000000-0000-0000-0000-000000000000')",
        "SELECT compress_chunk(c) FROM show_chunks('core.events') c",
    ] {
        let error = read
            .execute(sql)
            .await
            .expect_err("reader cannot mutate or compress records");
        assert_eq!(
            error.as_database_error().and_then(|e| e.code()).as_deref(),
            Some("42501")
        );
    }
    // Force a real old UUIDv7 chunk, then compress it and compare every column.
    let mut old = Case::derived(parent);
    let mut bytes = *old.id.as_bytes();
    let ms = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)?
        .as_millis() as u64
        - 10 * 86400 * 1000;
    bytes[..6].copy_from_slice(&ms.to_be_bytes()[2..]);
    old.id = Uuid::from_bytes(bytes);
    old.original = Some("1990-01-01T00:00:00Z");
    old.quality = Some("intrinsic");
    insert(&mut conn, run, &old).await?;
    // Reflection has its own hypertable; insert explicit non-generated columns.
    sqlx::query("INSERT INTO reflection.events (id,source,event_type,payload,commit_seq,source_event_ids,producer_version_id,run_id,record_hash,ts_quality,equivalence_key) VALUES ($1,'synthetic.schema','synthetic.observed','{}',1000001,$2,$3,$4,$5,'unknown','synthetic:self')").bind(old.id).bind(vec![parent]).bind(Uuid::now_v7()).bind(run).bind(vec![0_u8;32]).execute(&mut conn).await?;
    let self_before: serde_json::Value =
        sqlx::query_scalar("SELECT to_jsonb(e) FROM reflection.events e WHERE id=$1")
            .bind(old.id)
            .fetch_one(&mut conn)
            .await?;
    let before: serde_json::Value =
        sqlx::query_scalar("SELECT to_jsonb(e) FROM core.events e WHERE id=$1")
            .bind(old.id)
            .fetch_one(&mut conn)
            .await?;
    for table in ["core.events", "reflection.events"] {
        let chunks:Vec<String>=sqlx::query_scalar("SELECT chunk_schema||'.'||chunk_name FROM timescaledb_information.chunks WHERE hypertable_schema||'.'||hypertable_name=$1 AND range_end<now()-interval '7 days'").bind(table).fetch_all(&mut conn).await?;
        assert!(!chunks.is_empty());
        for chunk in chunks {
            sqlx::query("SELECT compress_chunk($1::regclass)")
                .bind(chunk)
                .execute(&mut conn)
                .await?;
        }
        let compressed:i64=sqlx::query_scalar("SELECT count(*) FROM timescaledb_information.chunks WHERE hypertable_schema||'.'||hypertable_name=$1 AND is_compressed").bind(table).fetch_one(&mut conn).await?;
        assert!(compressed > 0);
    }
    let after: serde_json::Value =
        sqlx::query_scalar("SELECT to_jsonb(e) FROM core.live_events e WHERE id=$1")
            .bind(old.id)
            .fetch_one(&mut conn)
            .await?;
    assert_eq!(before, after);
    let self_after: serde_json::Value =
        sqlx::query_scalar("SELECT to_jsonb(e) FROM reflection.events e WHERE id=$1")
            .bind(old.id)
            .fetch_one(&mut conn)
            .await?;
    assert_eq!(self_before, self_after);
    sqlx::query("INSERT INTO audit.archived (id,archived_seq,reason) VALUES ($1,1000002,'replay')")
        .bind(old.id)
        .execute(&mut conn)
        .await?;
    let live: i64 = sqlx::query_scalar("SELECT count(*) FROM core.live_events WHERE id=$1")
        .bind(old.id)
        .fetch_one(&mut conn)
        .await?;
    assert_eq!(live, 0);
    let retained: serde_json::Value =
        sqlx::query_scalar("SELECT to_jsonb(e) FROM core.events e WHERE id=$1")
            .bind(old.id)
            .fetch_one(&mut conn)
            .await?;
    assert_eq!(before, retained);
    let still_compressed:i64=sqlx::query_scalar("SELECT count(*) FROM timescaledb_information.chunks WHERE hypertable_schema='core' AND hypertable_name='events' AND is_compressed").fetch_one(&mut conn).await?;
    assert!(still_compressed > 0);
    check_schema(admin).await?;
    Ok(())
}
