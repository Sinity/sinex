use super::*;
use crate::{AtuinInterpretation, AtuinStep, ReplayCommand, RuntimeIdentity, Store};
use time::{OffsetDateTime, format_description::well_known::Rfc3339};
#[tokio::test]
#[ignore = "needs PostgreSQL"]
async fn exclude_and_undo_keep_stored_rows_and_retries_return_first_outcome()
-> std::result::Result<(), Box<dyn std::error::Error>> {
    let admin = std::env::var("SINEX_TEST_ADMIN_URL")?;
    let writer = std::env::var("SINEX_TEST_WRITER_URL")?;
    let reader = std::env::var("SINEX_TEST_READER_URL")?;
    let root = std::path::PathBuf::from(std::env::var("SINEX_TEST_STORE_DIR")?);
    crate::bootstrap(&admin).await?;
    let executable = std::fs::read(std::env::current_exe()?)?;
    let identity = || RuntimeIdentity {
        service_name: "synthetic-exclusion-contract".into(),
        host: "synthetic".into(),
        version: env!("CARGO_PKG_VERSION").into(),
        executable_blake3: format!("blake3:{}", blake3::hash(&executable)),
        config_blake3: format!("blake3:{}", blake3::hash(b"exclusion-contract")),
    };
    let store = Store::connect(&writer, &reader, root.clone(), 4, identity(), None).await?;
    store.admission.set_replay_profile(Some(vec![])).await?;
    store.admission.enable_shell().await?;
    let clock = OffsetDateTime::now_utc();
    let row = |id: &str| json!({"_rowid":if id=="occurrence-a" {1}else{2},"id":id,"timestamp":1700000000000000000i64,"duration":1000,"exit":0,"command":"cargo test exclusion-fixture","cwd":"/synthetic","session":"same","hostname":"fixture","deleted_at":null});
    let step = |rows, second| AtuinStep {
        interpretation: AtuinInterpretation::Current,
        observation: Uuid::now_v7(),
        observed_at: (clock + time::Duration::seconds(second))
            .format(&Rfc3339)
            .unwrap(),
        locator: "synthetic-exclusion-provider".into(),
        metadata: json!({"fixture":"independent identical commands distinct native occurrence IDs"}),
        rows,
        complete: true,
        unavailable: None,
        privacy_rules: vec![],
    };
    store
        .admission
        .submit_atuin(step(vec![row("occurrence-a")], 0))
        .await?;
    store
        .admission
        .submit_atuin(step(vec![row("occurrence-a"), row("occurrence-b")], 1))
        .await?;
    let mut conn = sqlx::PgConnection::connect(&reader).await?;
    async fn canonical_count(conn: &mut sqlx::PgConnection, expected: i64) -> Result<()> {
        for _ in 0..100 {
            let count: i64 = sqlx::query_scalar(
                "SELECT count(*) FROM core.live_events WHERE event_type='command.canonical'",
            )
            .fetch_one(&mut *conn)
            .await?;
            if count == expected {
                return Ok(());
            }
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        }
        Err(Error::Invalid(
            "canonical event count did not reach the expected value",
        ))
    }
    canonical_count(&mut conn, 2).await?;
    let a=sqlx::query("SELECT id,equivalence_key,source_material_id FROM core.live_events WHERE payload->>'native_id'='occurrence-a' AND event_type='command.executed'").fetch_one(&mut conn).await?;
    let a_id: Uuid = a.try_get("id")?;
    let a_key: String = a.try_get("equivalence_key")?;
    let material: Uuid = a.try_get("source_material_id")?;
    let b_id:Uuid=sqlx::query_scalar("SELECT id FROM core.live_events WHERE payload->>'native_id'='occurrence-b' AND event_type='command.executed'").fetch_one(&mut conn).await?;
    let old_trace = store
        .reader
        .trace(&a_id.to_string(), true, &[])
        .await?
        .unwrap();
    let request = Request {
        reference: format!("sinex:ev/{a_id}"),
        action_id: Uuid::now_v7(),
        expected_key: Some(a_key.clone()),
    };
    let applied = store
        .admission
        .exclude(Action::Exclude, request.clone())
        .await?;
    assert_eq!(applied["state"], "completed");
    assert_eq!(applied["archived"].as_array().unwrap().len(), 2);
    canonical_count(&mut conn, 1).await?;
    let result = store
        .reader
        .query("events where kind = command.executed", None)
        .await?;
    assert_eq!(result["total"], 1);
    assert_eq!(result["rows"][0]["id"], b_id.to_string());
    assert_eq!(
        store
            .admission
            .exclude(Action::Exclude, request.clone())
            .await?,
        applied
    );
    let bound:i64=sqlx::query_scalar("SELECT count(*) FROM core.operations_log o JOIN core.operation_effects e ON e.operation_id=o.id WHERE o.action_id=$1").bind(request.action_id).fetch_one(&mut conn).await?;
    assert_eq!(bound, 1, "a retry must not record a second operation");
    let noop = store
        .admission
        .exclude(
            Action::Exclude,
            Request {
                action_id: Uuid::now_v7(),
                ..request.clone()
            },
        )
        .await?;
    assert_eq!(noop["state"], "noop");
    assert_eq!(noop["exclusion_operation"], applied["operation_id"]);
    assert_eq!(noop["archived"], json!([]));
    let missing_request = Request {
        reference: Uuid::now_v7().to_string(),
        action_id: Uuid::now_v7(),
        expected_key: None,
    };
    let missing = store
        .admission
        .exclude(Action::Exclude, missing_request.clone())
        .await?;
    assert_eq!(missing["state"], "not_found");
    assert_eq!(
        store
            .admission
            .exclude(Action::Exclude, missing_request)
            .await?,
        missing
    );
    let final_trace = store
        .reader
        .trace(&a_id.to_string(), true, &[])
        .await?
        .unwrap();
    for key in ["material", "anchor", "end", "record"] {
        assert_eq!(old_trace[key], final_trace[key]);
    }
    assert_eq!(final_trace["archive"]["reason"], "delete");
    assert!(
        store
            .admission
            .exclude(Action::Undo, request.clone())
            .await
            .is_err()
    );
    let mut changed = row("occurrence-a");
    changed["exit"] = json!(7);
    let poll = store
        .admission
        .submit_atuin(step(vec![changed.clone(), row("occurrence-b")], 2))
        .await?;
    assert_eq!(poll["excluded"], 1);
    assert_eq!(poll["admitted"], 0);
    assert_eq!(poll["rows_read"], 2);
    let query = format!("materials where id = {material}");
    let blocked = store
        .admission
        .replay(ReplayCommand::Start(sinex_model::replay::Start {
            query: query.clone(),
            action_id: Uuid::now_v7(),
            expected_basis: None,
        }))
        .await?;
    assert_eq!(blocked["state"], "completed");
    assert_eq!(blocked["processed"], 1);
    assert_eq!(blocked["admitted"], 0);
    let preview = store
        .admission
        .replay(ReplayCommand::Plan(
            sinex_model::replay::Selection {
                query: query.clone(),
            },
            vec![],
        ))
        .await?;
    // The exclusion survives a restart.
    store.shutdown().await?;
    let store = Store::connect(&writer, &reader, root.clone(), 4, identity(), None).await?;
    store.admission.set_replay_profile(Some(vec![])).await?;
    store.admission.enable_shell().await?;
    assert_eq!(
        store
            .admission
            .exclude(Action::Exclude, request.clone())
            .await?,
        applied
    );
    let undo = Request {
        action_id: Uuid::now_v7(),
        ..request.clone()
    };
    let undone = store.admission.exclude(Action::Undo, undo.clone()).await?;
    assert_eq!(undone["state"], "completed");
    assert_eq!(store.admission.exclude(Action::Undo, undo).await?, undone);
    let absent = store
        .reader
        .query("events where kind = command.executed", None)
        .await?;
    assert_eq!(absent["total"], 1);
    let stale = store
        .admission
        .replay(ReplayCommand::Start(sinex_model::replay::Start {
            query: query.clone(),
            action_id: Uuid::now_v7(),
            expected_basis: preview["basis"].as_str().map(String::from),
        }))
        .await?;
    assert_eq!(stale["state"], "refused");
    assert_eq!(stale["admitted"], 0);
    let restored = store
        .admission
        .replay(ReplayCommand::Start(sinex_model::replay::Start {
            query: query.clone(),
            action_id: Uuid::now_v7(),
            expected_basis: None,
        }))
        .await?;
    assert_eq!(restored["state"], "completed");
    assert_eq!(restored["admitted"], 1);
    canonical_count(&mut conn, 2).await?;
    let restored_id:Uuid=sqlx::query_scalar("SELECT id FROM core.live_events WHERE event_type='command.executed' AND equivalence_key=$1").bind(&a_key).fetch_one(&mut conn).await?;
    assert_ne!(a_id, restored_id);
    let restored_trace = store
        .reader
        .trace(&restored_id.to_string(), true, &[])
        .await?
        .unwrap();
    for key in ["material", "anchor", "end", "record"] {
        assert_eq!(old_trace[key], restored_trace[key]);
    }
    assert_eq!(
        store
            .admission
            .exclude(Action::Exclude, request.clone())
            .await?,
        applied,
        "a retried action returns its first outcome after undo and replay"
    );
    // A new action can exclude the current event through the old event's reference.
    let again = Request {
        action_id: Uuid::now_v7(),
        ..request.clone()
    };
    store
        .admission
        .exclude(Action::Exclude, again.clone())
        .await?;
    store
        .admission
        .exclude(
            Action::Undo,
            Request {
                action_id: Uuid::now_v7(),
                ..again
            },
        )
        .await?;
    let acquisition = store
        .admission
        .submit_atuin(step(vec![changed, row("occurrence-b")], 3))
        .await?;
    assert_eq!(acquisition["admitted"], 1);
    canonical_count(&mut conn, 2).await?;
    let request_hash:String=sqlx::query_scalar("SELECT b.blake3 FROM core.operations_log o JOIN raw.source_material_registry m ON m.id=o.request_ref JOIN core.blobs b ON b.id=m.blob_id WHERE o.action_id=$1").bind(request.action_id).fetch_one(&mut conn).await?;
    let request_hash = request_hash
        .strip_prefix("blake3:")
        .expect("registered BLAKE3 name");
    let backing = root
        .join("blobs")
        .join(&request_hash[..2])
        .join(format!("{request_hash}.zst"));
    let held = backing.with_extension("missing");
    std::fs::rename(&backing, &held)?;
    let unavailable = store
        .admission
        .exclude(Action::Exclude, request.clone())
        .await;
    std::fs::rename(&held, &backing)?;
    assert!(
        unavailable.is_err(),
        "a retry whose stored request is missing must fail"
    );
    let wrong = Request {
        action_id: Uuid::now_v7(),
        expected_key: Some("wrong occurrence key".into()),
        ..request.clone()
    };
    let refused = store
        .admission
        .exclude(Action::Exclude, wrong.clone())
        .await?;
    assert_eq!(refused["state"], "refused");
    assert_eq!(
        store.admission.exclude(Action::Exclude, wrong).await?,
        refused
    );
    let canonical:Uuid=sqlx::query_scalar("SELECT id FROM core.live_events WHERE event_type='command.canonical' AND payload->>'native_id'='occurrence-a'").fetch_one(&mut conn).await?;
    let refusal = store
        .admission
        .exclude(
            Action::Exclude,
            Request {
                reference: canonical.to_string(),
                action_id: Uuid::now_v7(),
                expected_key: None,
            },
        )
        .await?;
    assert_eq!(refusal["state"], "refused");
    assert!(
        refusal["raw_ref"]
            .as_str()
            .unwrap()
            .starts_with("sinex:ev/")
    );
    // Purge and excise rows are inserted directly to check that undo refuses them.
    let mut admin_conn = sqlx::PgConnection::connect(&admin).await?;
    for kind in ["purge", "excise"] {
        sqlx::query("INSERT INTO core.deleted_occurrences(store,source,event_type,equivalence_key,material_id,anchor,kind,operation_id) VALUES(0,$1,'command.executed',$2,$3,0,$4,$5)").bind(crate::ATUIN_SOURCE).bind(&a_key).bind(material).bind(kind).bind(Uuid::parse_str(applied["operation_id"].as_str().unwrap())?).execute(&mut admin_conn).await?;
        let permanent = store
            .admission
            .exclude(
                Action::Undo,
                Request {
                    action_id: Uuid::now_v7(),
                    ..request.clone()
                },
            )
            .await?;
        assert_eq!(permanent["state"], "refused");
        sqlx::query("DELETE FROM core.deleted_occurrences WHERE equivalence_key=$1")
            .bind(&a_key)
            .execute(&mut admin_conn)
            .await?;
    }
    store.admission.set_replay_profile(None).await?;
    assert!(
        store
            .admission
            .exclude(
                Action::Undo,
                Request {
                    action_id: Uuid::now_v7(),
                    ..request.clone()
                }
            )
            .await
            .is_err()
    );
    store.shutdown().await?;
    Ok(())
}
