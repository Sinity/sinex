use super::*;
#[tokio::test]
#[ignore = "needs PostgreSQL"]
async fn new_parser_readmits_unchanged_row_and_keeps_old_event()
-> std::result::Result<(), Box<dyn std::error::Error>> {
    let admin = std::env::var("SINEX_TEST_ADMIN_URL")?;
    let writer = std::env::var("SINEX_TEST_WRITER_URL")?;
    let reader = std::env::var("SINEX_TEST_READER_URL")?;
    let state = std::path::PathBuf::from(std::env::var("SINEX_TEST_STORE_DIR")?);
    crate::bootstrap(&admin).await?;
    let executable = std::fs::read(std::env::current_exe()?)?;
    let store = crate::Store::connect(
        &writer,
        &reader,
        state,
        2,
        crate::RuntimeIdentity {
            service_name: "synthetic-atuin-recipe-proof".into(),
            host: "synthetic-host".into(),
            version: env!("CARGO_PKG_VERSION").into(),
            executable_blake3: format!("blake3:{}", blake3::hash(&executable).to_hex()),
            config_blake3: format!(
                "blake3:{}",
                blake3::hash(b"controlled prior recipe fixture").to_hex()
            ),
        },
        None,
    )
    .await?;
    store.admission.enable_shell().await?;
    let source = json!({"_rowid":1,"id":"fixture-completion-v1","timestamp":1_700_000_000_000_000_000i64,"duration":0,"exit":-1,"command":"synthetic recipe proof","cwd":"/synthetic","session":"same","hostname":"host","deleted_at":null});
    let clock = OffsetDateTime::now_utc();
    let step = |interpretation, seconds| AtuinStep {
        interpretation,
        observation: Uuid::now_v7(),
        observed_at: (clock + time::Duration::seconds(seconds))
            .format(&Rfc3339)
            .unwrap(),
        locator: "synthetic-recipe-fixture".into(),
        metadata: json!({"test_fixture":"old parser interpretation"}),
        rows: vec![source.clone()],
        complete: true,
        unavailable: None,
        privacy_rules: vec![],
    };
    let first = store
        .admission
        .submit_atuin(step(AtuinInterpretation::TestOldParser, 0))
        .await?;
    assert_eq!(first["admitted"], 1);
    let mut conn = sqlx::PgConnection::connect(&reader).await?;
    let snapshot = "SELECT e.id,e.payload,e.record_hash,e.equivalence_key,p.version AS parser FROM core.live_events e JOIN core.producer_versions p ON p.id=e.producer_version_id WHERE e.event_type='command.executed'";
    let old = sqlx::query(snapshot).fetch_one(&mut conn).await?;
    let old_id: Uuid = old.try_get("id")?;
    assert_eq!(old.try_get::<String, _>("parser")?, "atuin-v1");
    assert_eq!(old.try_get::<Value, _>("payload")?["completion"], "running");
    let before = store
        .reader
        .trace(&old_id.to_string(), true, &[])
        .await?
        .unwrap();
    async fn canonical(conn: &mut sqlx::PgConnection, parent: Uuid) -> Result<Uuid> {
        for _ in 0..100 {
            if let Some(id)=sqlx::query_scalar("SELECT id FROM core.live_events WHERE event_type='command.canonical' AND source_event_ids=ARRAY[$1]::uuid[] AND NOT EXISTS(SELECT 1 FROM derivation.shell_dirty) AND EXISTS(SELECT 1 FROM derivation.shell_progress WHERE one AND phase='settled' AND failure IS NULL)").bind(parent).fetch_optional(&mut *conn).await? {return Ok(id)}
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        }
        Err(Error::Invalid("canonical event did not appear"))
    }
    let old_canonical = match canonical(&mut conn, old_id).await {
        Ok(id) => id,
        Err(error) => {
            if store.writer.is_finished() {
                // Report the writer's error rather than the polling timeout.
                store.writer.await??;
            }
            return Err(error.into());
        }
    };
    let corrected = store
        .admission
        .submit_atuin(step(AtuinInterpretation::Current, 1))
        .await?;
    assert_eq!(
        corrected["admitted"], 1,
        "an unchanged row must be readmitted under a new parser"
    );
    let new = sqlx::query(snapshot).fetch_one(&mut conn).await?;
    let new_id: Uuid = new.try_get("id")?;
    assert_ne!(new_id, old_id);
    assert_eq!(
        old.try_get::<Vec<u8>, _>("record_hash")?,
        new.try_get::<Vec<u8>, _>("record_hash")?
    );
    assert_eq!(
        old.try_get::<String, _>("equivalence_key")?,
        new.try_get::<String, _>("equivalence_key")?
    );
    assert_eq!(new.try_get::<String, _>("parser")?, PARSER);
    let payload: Value = new.try_get("payload")?;
    assert_eq!(payload["completion"], "ended");
    assert_eq!(payload["duration_ns"], 0);
    assert_eq!(payload["exit_code"], Value::Null);
    let new_canonical = canonical(&mut conn, new_id).await?;
    assert_ne!(old_canonical, new_canonical);
    let canonical_payload: Value =
        sqlx::query_scalar("SELECT payload FROM core.live_events WHERE id=$1")
            .bind(new_canonical)
            .fetch_one(&mut conn)
            .await?;
    assert_eq!(canonical_payload["completion"], "ended");
    let archive_count: i64 =
        sqlx::query_scalar("SELECT count(*) FROM audit.archived WHERE id=ANY($1)")
            .bind(vec![old_id, old_canonical])
            .fetch_one(&mut conn)
            .await?;
    assert_eq!(archive_count, 2);
    let repeated = store
        .admission
        .submit_atuin(step(AtuinInterpretation::Current, 2))
        .await?;
    assert_eq!(repeated["admitted"], 0);
    assert_eq!(repeated["repeated"], 1);
    let stale = store
        .admission
        .submit_atuin(step(AtuinInterpretation::TestOldParser, -1))
        .await?;
    assert_eq!(
        stale["admitted"], 0,
        "an older reading must not replace a newer one"
    );
    let after = store
        .reader
        .trace(&old_id.to_string(), true, &[])
        .await?
        .unwrap();
    for field in ["material", "anchor", "end", "record"] {
        assert_eq!(before[field], after[field]);
    }
    store.shutdown().await?;
    Ok(())
}
#[test]
fn missing_duration_and_exit_is_end_not_recorded() {
    let row = json!({"id":"unfinished","timestamp":1_700_000_000_000_000_000i64,"duration":-1,"exit":-1,"command":"synthetic","cwd":"/synthetic","session":"same","hostname":"host","deleted_at":null});
    assert_eq!(
        parse(&row).unwrap().payload["completion"],
        "end_not_recorded"
    );
}
#[test]
fn zero_duration_records_an_end_with_unknown_exit() {
    let row = json!({"id":"zero","timestamp":1_700_000_000_000_000_000i64,"duration":0,"exit":-1,"command":"synthetic","cwd":"/synthetic","session":"same","hostname":"host","deleted_at":null});
    let parsed = parse(&row).unwrap();
    assert_eq!(parsed.payload["completion"], "ended");
    assert_eq!(parsed.payload["duration_ns"], 0);
    assert_eq!(parsed.payload["exit_code"], Value::Null);
}
#[test]
fn unfinished_exit_is_unknown_and_malformed_time_is_a_record_failure() {
    let row = json!({"id":"synthetic-1","timestamp":1_700_000_000_123_456_789i64,"duration":-1,"exit":-1,"command":"example","cwd":"/synthetic","session":"s","hostname":"h:u","deleted_at":null});
    let parsed = parse(&row).unwrap();
    assert_eq!(parsed.payload["exit_code"], Value::Null);
    assert_eq!(parsed.payload["completion"], "end_not_recorded");
    assert_eq!(parsed.time, "2023-11-14T22:13:20.123456Z");
    assert_eq!(parsed.nanos, 789);
    let mut bad = row.clone();
    bad["timestamp"] = json!("1700000000000000000");
    assert!(parse(&bad).is_err());
    let mut later = row;
    later["_end_not_recorded"] = json!(true);
    assert_eq!(
        parse(&later).unwrap().payload["completion"],
        "end_not_recorded"
    );
}

fn row(exit: i64, duration: i64) -> Value {
    json!({"id":"signed","timestamp":1_700_000_000_123_456_789i64,"duration":duration,"exit":exit,"command":"synthetic signed status","cwd":"/synthetic","session":"s","hostname":"host:user","deleted_at":null})
}

#[test]
fn exit_status_and_duration_are_parsed_independently() {
    for exit in [i64::MIN, -2, -1, 0, 1, i64::MAX] {
        for duration in [-1, 0, 7, i64::MAX] {
            let parsed = parse(&row(exit, duration)).unwrap();
            assert_eq!(
                parsed.payload["exit_code"],
                if exit == -1 { Value::Null } else { json!(exit) }
            );
            assert_eq!(
                parsed.payload["duration_ns"],
                if duration == -1 {
                    Value::Null
                } else {
                    json!(duration)
                }
            );
            assert_eq!(
                parsed.payload["completion"],
                if duration >= 0 || exit != -1 {
                    "ended"
                } else {
                    "end_not_recorded"
                }
            );
            let encoded = serde_json::to_string(&parsed.payload).unwrap();
            let decoded: Value = serde_json::from_str(&encoded).unwrap();
            assert_eq!(decoded, parsed.payload);
        }
    }
}

#[test]
fn malformed_exit_or_duration_is_a_record_failure() {
    assert!(parse(&row(-2, -2)).is_err());
    for exit in [json!("-2"), json!(null), json!(1.5), json!(u64::MAX)] {
        let mut malformed = row(0, 1);
        malformed["exit"] = exit;
        assert!(parse(&malformed).is_err());
    }
}
