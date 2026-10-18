use super::{WORK_TABLES, rule};
use crate::{WriterPool, current_commit_seq, next_commit_seq};
use serde_json::{Value, json};
use sqlx::{Connection, PgConnection, Row};
use uuid::Uuid;

async fn setup(
    connection: &mut PgConnection,
    ids: &[Uuid],
    snapshot_seq: i64,
) -> crate::Result<()> {
    for statement in WORK_TABLES {
        sqlx::query(statement).execute(&mut *connection).await?;
    }
    sqlx::query("INSERT INTO sinex_shell_nodes(id,source,host,command,command_hash,at_ns,digest,anchor,ekey) SELECT e.id,0,e.host,btrim(e.payload->>'command'),md5(e.payload->>'command'),(extract(epoch FROM e.ts_orig)*1000000000+coalesce(e.ts_orig_tiebreak,0))::numeric,b.blake3,e.anchor_byte,e.equivalence_key FROM core.events e JOIN raw.source_material_registry m ON m.id=e.source_material_id JOIN core.blobs b ON b.id=m.blob_id WHERE e.id=ANY($1)").bind(ids).execute(&mut *connection).await?;
    sqlx::query("INSERT INTO sinex_shell_groups(token,parents,strategy) SELECT id,ARRAY[id],0 FROM sinex_shell_nodes").execute(&mut *connection).await?;
    sqlx::query(
        "UPDATE derivation.shell_progress SET basis=$1,phase='publish',replaced=0 WHERE one",
    )
    .bind(snapshot_seq)
    .execute(&mut *connection)
    .await?;
    Ok(())
}
async fn publish(
    connection: &mut PgConnection,
    producer: Uuid,
    run: Uuid,
    snapshot_seq: i64,
) -> crate::Result<()> {
    let groups = WriterPool::shell_pending_groups(connection).await?;
    WriterPool::shell_publish_batch(connection, Some(producer), run, snapshot_seq, groups).await
}
async fn snapshot(connection: &mut PgConnection) -> crate::Result<Value> {
    sqlx::query_scalar("SELECT jsonb_build_object('counter',(SELECT seq FROM core.commit_counter WHERE one),'canonical',(SELECT count(*) FROM core.events WHERE event_type='command.canonical'),'log',(SELECT count(*) FROM core.commit_log WHERE event_type_id=3),'dirty',(SELECT coalesce(jsonb_agg(to_jsonb(d) ORDER BY day),'[]') FROM derivation.session_dirty d),'frontier',(SELECT coalesce(jsonb_agg(to_jsonb(d) ORDER BY parent_id),'[]') FROM derivation.shell_dirty d))").fetch_one(connection).await.map_err(Into::into)
}

#[tokio::test]
#[ignore = "requires runner-admitted synthetic source rows and isolated PostgreSQL credentials"]
async fn cancelled_publish_batch_rolls_back_and_republishes_with_same_ids()
-> Result<(), Box<dyn std::error::Error>> {
    let url = std::env::var("SINEX_TEST_WRITER_URL")?;
    let mut connection = PgConnection::connect(&url).await?;
    let mut admin = PgConnection::connect(&std::env::var("SINEX_TEST_ADMIN_URL")?).await?;
    assert_eq!(
        sqlx::query_scalar::<_, String>("SELECT current_user::text")
            .fetch_one(&mut connection)
            .await?,
        "sinex_writer"
    );
    let rows = sqlx::query(
        "SELECT id,payload FROM core.live_events WHERE source='terminal.atuin-history' ORDER BY id",
    )
    .fetch_all(&mut connection)
    .await?;
    assert_eq!(rows.len(), 6);
    let ids: Vec<Uuid> = rows.iter().map(|r| r.get("id")).collect();
    let native: Vec<String> = rows
        .iter()
        .map(|r| {
            r.get::<Value, _>("payload")["native_id"]
                .as_str()
                .unwrap()
                .to_owned()
        })
        .collect();
    assert_eq!(
        native,
        (0..6).map(|n| format!("turn-{n}")).collect::<Vec<_>>()
    );
    let run: Uuid = sqlx::query_scalar("SELECT id FROM core.runs ORDER BY started_at DESC LIMIT 1")
        .fetch_one(&mut connection)
        .await?;
    let producer = Uuid::now_v7();
    sqlx::query("INSERT INTO core.producer_versions(id,name,version,run_id,configuration) VALUES($1,'fusion.shell-command',$2,$3,$4)").bind(producer).bind(rule().implementation).bind(run).bind(serde_json::from_str::<Value>(super::RULE_JSON)?).execute(&mut connection).await?;
    let mut snapshot_seq: i64 = current_commit_seq(&mut connection).await?;
    setup(&mut connection, &ids[1..2], snapshot_seq).await?;
    publish(&mut connection, producer, run, snapshot_seq).await?;
    let reused: Uuid = sqlx::query_scalar("SELECT output_id FROM sinex_shell_groups")
        .fetch_one(&mut connection)
        .await?;
    snapshot_seq = current_commit_seq(&mut connection).await?;
    // A parent archived after the snapshot is skipped, as in a resumed pass.
    let mut tx = connection.begin().await?;
    let seq: i64 = next_commit_seq(&mut tx).await?;
    sqlx::query("INSERT INTO audit.archived(id,archived_seq,reason) VALUES($1,$2,'supersede')")
        .bind(ids[0])
        .bind(seq)
        .execute(&mut *tx)
        .await?;
    super::dirty(&mut tx, ids[0]).await?;
    tx.commit().await?;
    setup(&mut connection, &ids, snapshot_seq).await?;
    let before = snapshot(&mut connection).await?;
    let backend: i32 = sqlx::query_scalar("SELECT pg_backend_pid()")
        .fetch_one(&mut connection)
        .await?;
    // Block the batch at its second new insert, after one reuse and one new row.
    sqlx::query("SELECT pg_advisory_lock(71982745)")
        .execute(&mut admin)
        .await?;
    let function = format!(
        "CREATE FUNCTION sinex_schemas.publication_test_hold() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN IF NEW.event_type='command.canonical' AND NEW.source_event_ids @> ARRAY['{}'::uuid] THEN PERFORM pg_advisory_xact_lock(71982745); END IF; RETURN NEW; END $$",
        ids[3]
    );
    sqlx::raw_sql(&function).execute(&mut admin).await?;
    sqlx::raw_sql("CREATE TRIGGER publication_test_hold BEFORE INSERT ON core.events FOR EACH ROW EXECUTE FUNCTION sinex_schemas.publication_test_hold()").execute(&mut admin).await?;
    let task = tokio::spawn(async move {
        let result = publish(&mut connection, producer, run, snapshot_seq).await;
        (connection, result)
    });
    let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(10);
    loop {
        let blocked: bool = sqlx::query_scalar(
            "SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE pid=$1 AND wait_event='advisory')",
        )
        .bind(backend)
        .fetch_one(&mut admin)
        .await?;
        if blocked {
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "publish batch never reached the advisory lock"
        );
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }
    assert_eq!(
        snapshot(&mut admin).await?,
        before,
        "uncommitted batch is visible"
    );
    assert!(
        sqlx::query_scalar::<_, bool>("SELECT pg_cancel_backend($1)")
            .bind(backend)
            .fetch_one(&mut admin)
            .await?
    );
    let (mut connection, result) = task.await?;
    assert!(
        matches!(result,Err(crate::Error::Database(ref e)) if e.as_database_error().and_then(|e|e.code()).as_deref()==Some("57014")),
        "{result:?}"
    );
    assert_eq!(snapshot(&mut connection).await?, before);
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT count(*) FROM sinex_shell_groups WHERE done OR output_id IS NOT NULL"
        )
        .fetch_one(&mut connection)
        .await?,
        0
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT replaced FROM derivation.shell_progress WHERE one")
            .fetch_one(&mut connection)
            .await?,
        0
    );
    sqlx::query("SELECT pg_advisory_unlock(71982745)")
        .execute(&mut admin)
        .await?;
    sqlx::raw_sql("DROP TRIGGER publication_test_hold ON core.events; DROP FUNCTION sinex_schemas.publication_test_hold()").execute(&mut admin).await?;
    publish(&mut connection, producer, run, snapshot_seq).await?;
    let committed = snapshot(&mut connection).await?;
    assert_eq!(committed["canonical"].as_i64(), Some(5));
    assert_eq!(
        committed["counter"].as_i64(),
        Some(before["counter"].as_i64().unwrap() + 4)
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT replaced FROM derivation.shell_progress WHERE one")
            .fetch_one(&mut connection)
            .await?,
        5
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM sinex_shell_groups WHERE done")
            .fetch_one(&mut connection)
            .await?,
        6
    );
    let outputs: Vec<Uuid> = sqlx::query_scalar(
        "SELECT output_id FROM sinex_shell_groups WHERE output_id IS NOT NULL ORDER BY token",
    )
    .fetch_all(&mut connection)
    .await?;
    assert_eq!(outputs[0], reused);
    let canonical: Vec<Value> = sqlx::query_scalar("SELECT to_jsonb(e) FROM core.live_events e WHERE event_type='command.canonical' ORDER BY id").fetch_all(&mut connection).await?;
    assert!(canonical.iter().all(|e| e["source_material_id"].is_null()
        && e["source_event_ids"].as_array().unwrap().len() == 1));
    // A new connection rebuilds the temp tables and reuses every id.
    connection.close().await?;
    let mut connection = PgConnection::connect(&url).await?;
    setup(&mut connection, &ids, snapshot_seq).await?;
    publish(&mut connection, producer, run, snapshot_seq).await?;
    assert_eq!(snapshot(&mut connection).await?, committed);
    let resumed: Vec<Uuid> = sqlx::query_scalar(
        "SELECT output_id FROM sinex_shell_groups WHERE output_id IS NOT NULL ORDER BY token",
    )
    .fetch_all(&mut connection)
    .await?;
    assert_eq!(outputs, resumed);
    println!(
        "PUBLICATION_TURN_PROOF {}",
        json!({"source_ids":ids,"canonical_ids":outputs,"reused_id":reused,"mixed":{"skip":1,"reuse":1,"new":4},"middle_statement_cancelled":true,"whole_turn_rollback":true,"restart_stable":true,"canonical":canonical})
    );
    Ok(())
}
