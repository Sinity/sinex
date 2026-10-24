use super::super::sealed::{CiphertextChange, KeyRotation, PreparedSealed};
use super::*;
use crate::sealed as crypto;
use std::io::{Read, Write};
use std::sync::Arc;

#[tokio::test]
async fn prepared_ciphertext_rekey_keeps_unrelated_facts_and_retires_the_old_capability() {
    let (_db, mut conn) = testdb::fresh().await;
    let store = Store::new();
    let identity = crypto::Identity::generate();
    let old = crypto::new_key().unwrap();
    let fresh = crypto::new_key().unwrap();
    let bytes = b"synthetic_removed_secret\nsynthetic_retained_value\n";
    let kept = bytes.iter().position(|b| *b == b'\n').unwrap() + 1;
    let blob = store.blobs.put_sealed(bytes, &old).unwrap();
    let material = MaterialId::new();
    let input = Uuid::now_v7();
    sqlx::query("INSERT INTO raw.inputs(id,binding,kind,locator,host) VALUES($1,'b','file','synthetic','h')").bind(input).execute(&mut conn).await.unwrap();
    sqlx::query(
        "INSERT INTO raw.bindings(binding,source,input,settings) VALUES('b','s','synthetic','{}')",
    )
    .execute(&mut conn)
    .await
    .unwrap();
    sqlx::query("INSERT INTO core.blobs(hash,size,status) VALUES($1,$2,'present')")
        .bind(&blob)
        .bind(bytes.len() as i64)
        .execute(&mut conn)
        .await
        .unwrap();
    sqlx::query("INSERT INTO raw.materials(id,input_id,blob,durable_bytes,manifest,parser,parser_version) VALUES($1,$2,$3,$4,'{}','lines',1)")
        .bind(material.0).bind(input).bind(&blob).bind(bytes.len() as i64).execute(&mut conn).await.unwrap();
    let wrapped = crypto::wrap(&old, &identity.to_public()).unwrap();
    crypto::store_key(&mut conn, material, &old.key_ref, &wrapped)
        .await
        .unwrap();
    let mut gone = keyed(material, bytes, 0, "gone", 1);
    let mut retained = keyed(material, bytes, kept, "retained", 2);
    crypto::seal_event(&mut gone, &old).unwrap();
    crypto::seal_event(&mut retained, &old).unwrap();
    admit_events(&mut conn, vec![gone.clone(), retained.clone()]).await;
    let metadata: serde_json::Value =
        sqlx::query_scalar("SELECT to_jsonb(e)-'payload' FROM core.events e WHERE id=$1")
            .bind(retained.id.0)
            .fetch_one(&mut conn)
            .await
            .unwrap();
    let mut failures = Vec::new();
    for (anchor, text) in [
        (0, "synthetic_removed_failure"),
        (kept as i64, "synthetic_retained_failure"),
    ] {
        let reason = crypto::seal_value(&serde_json::json!(text), &old)
            .unwrap()
            .to_string();
        let id:i64=sqlx::query_scalar("INSERT INTO raw.failures(material_id,anchor,reason,run_id) VALUES($1,$2,$3,$4) RETURNING id")
            .bind(material.0).bind(anchor).bind(&reason).bind(Uuid::now_v7()).fetch_one(&mut conn).await.unwrap();
        failures.push((id, anchor, reason));
    }
    let cursor = serde_json::json!({"material":material,"through":bytes.len()});
    let carry = crypto::seal_value(&serde_json::Value::Null, &old).unwrap();
    sqlx::query(
        "INSERT INTO raw.positions(binding,cursor,carry,committed_seq) VALUES('b',$1,$2,2)",
    )
    .bind(&cursor)
    .bind(&carry)
    .execute(&mut conn)
    .await
    .unwrap();
    let target = Target::Event(gone.id);
    let mut selected = plan(&mut conn, target).await.unwrap();
    selected.candidate_source = Some("s".into());
    selected.candidate_parser = Some("lines".into());
    selected.candidates = candidate_materials(&mut conn, "s", "lines").await.unwrap();
    let old_envelope = selected.candidates[0].key.clone().unwrap();
    let secrets = crypto::MaterialSecrets::from_data_key(&old)
        .naming_only()
        .with_data_key(&fresh);
    let replacement = KeyEnvelope {
        reference: fresh.key_ref.clone(),
        wrapped: crypto::wrap_secrets(&secrets, &identity.to_public()).unwrap(),
        role: crypto::SecretRole::DataAndRecordNaming.as_str().into(),
        class: "sealed".into(),
    };
    let plain_retained = crypto::unseal_payload(&retained.payload, &old.key).unwrap();
    let mut changes = vec![
        CiphertextChange::Event {
            telemetry: false,
            id: gone.id.0,
            material: Some(material.0),
            old: gone.payload.clone(),
            replacement: None,
        },
        CiphertextChange::Event {
            telemetry: false,
            id: retained.id.0,
            material: Some(material.0),
            old: retained.payload.clone(),
            replacement: Some(crypto::seal_value(&plain_retained, &fresh).unwrap()),
        },
    ];
    for (id, anchor, reason) in failures {
        changes.push(CiphertextChange::Failure {
            id,
            material: material.0,
            anchor,
            old: reason,
            replacement: if anchor == 0 {
                None
            } else {
                Some(
                    crypto::seal_value(&serde_json::json!("synthetic_retained_failure"), &fresh)
                        .unwrap()
                        .to_string(),
                )
            },
        });
    }
    changes.push(CiphertextChange::Position {
        binding: "b".into(),
        cursor: cursor.clone(),
        committed_seq: 2,
        old: carry,
        replacement: crypto::seal_value(&serde_json::Value::Null, &fresh).unwrap(),
    });
    let mut file = tempfile::tempfile().unwrap();
    for change in changes {
        serde_json::to_writer(&mut file, &change).unwrap();
        file.write_all(b"\n").unwrap();
    }
    let metadata_scope = crypto::metadata::Scope::Replay {
        source: "s".into(),
        material,
        parser: "lines".into(),
        parser_version: 1,
        length: bytes.len() as u64,
        whole: false,
        append_end: None,
    };
    let mut replay_ciphertext = Vec::new();
    crypto::metadata::seal_replay(
        &mut replay_ciphertext,
        &identity.to_public(),
        &metadata_scope,
        [crypto::metadata::ErasedRecord {
            purpose: crate::sealed::metadata::RecordPurpose::Active,
            start: 0,
            end: kept as u64,
            raw_digest: crypto::MaterialSecrets::from_data_key(&old)
                .name_raw_record(&bytes[..kept]),
            transition: None,
        }],
        || Ok(()),
    )
    .unwrap();
    let metadata_blob = store.blobs.put(&replay_ciphertext).unwrap();
    let prepared = PreparedSealed {
        replay: vec![super::super::sealed::ReplayReplacement {
            material,
            old_blob: None,
            blob: metadata_blob.clone(),
            length: replay_ciphertext.len() as u64,
        }],
        source: "s".into(),
        rotations: vec![KeyRotation {
            material,
            old: old_envelope,
            replacement,
        }],
        changes: Arc::new(file),
    };
    let mut blanked = bytes.to_vec();
    blank(&mut blanked, &[(0, kept)]);
    let replacement_blob = store.blobs.put_sealed(&blanked, &fresh).unwrap();
    let rewrites = vec![Rewrite {
        material,
        blob: replacement_blob.clone(),
        length: bytes.len() as u64,
        manifest: None,
    }];
    // Fail after material and key repointing, when the final dependent carry
    // UPDATE executes. Retrying uses the same prepared ciphertext custody.
    let mut failed = conn.begin().await.unwrap();
    sqlx::query("CREATE FUNCTION pg_temp.reject_rekey_position() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'synthetic final dependent failure'; END $$").execute(&mut *failed).await.unwrap();
    sqlx::query("CREATE TRIGGER reject_rekey_position BEFORE UPDATE ON raw.positions FOR EACH ROW EXECUTE FUNCTION pg_temp.reject_rekey_position()").execute(&mut *failed).await.unwrap();
    assert!(
        apply_prepared(
            &mut failed,
            target,
            &selected,
            &rewrites,
            OperationId::new(),
            "synthetic",
            &NoDerivations,
            Some(&prepared),
            &|| Ok(())
        )
        .await
        .is_err()
    );
    failed.rollback().await.unwrap();
    assert_eq!(
        candidate_materials(&mut conn, "s", "lines").await.unwrap(),
        selected.candidates
    );
    let unchanged_payload: serde_json::Value =
        sqlx::query_scalar("SELECT payload FROM core.events WHERE id=$1")
            .bind(retained.id.0)
            .fetch_one(&mut conn)
            .await
            .unwrap();
    assert_eq!(unchanged_payload, retained.payload);
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM raw.failures WHERE material_id=$1")
            .bind(material.0)
            .fetch_one(&mut conn)
            .await
            .unwrap(),
        2
    );
    assert!(
        crypto::wrapped_key(&mut conn, &fresh.key_ref)
            .await
            .unwrap()
            .is_none()
    );
    assert!(
        crypto::wrapped_key(&mut conn, &old.key_ref)
            .await
            .unwrap()
            .is_some()
    );
    let mut tx = conn.begin().await.unwrap();
    let applied = apply_prepared(
        &mut tx,
        target,
        &selected,
        &rewrites,
        OperationId::new(),
        "synthetic",
        &NoDerivations,
        Some(&prepared),
        &|| Ok(()),
    )
    .await
    .unwrap();
    tx.commit().await.unwrap();
    assert_eq!(
        (
            applied.events,
            applied.failures_removed,
            applied.keys_destroyed
        ),
        (1, 1, 1)
    );
    let stored_replay: String =
        sqlx::query_scalar("SELECT erased_replay_blob FROM raw.materials WHERE id=$1")
            .bind(material.0)
            .fetch_one(&mut conn)
            .await
            .unwrap();
    assert_eq!(stored_replay, metadata_blob);
    let records = crypto::metadata::open_replay(
        store.blobs.read(&stored_replay).unwrap().as_slice(),
        &identity,
        &metadata_scope,
        || Ok(()),
    )
    .unwrap();
    assert_eq!(records.len(), 1);
    assert_eq!(
        records[0].raw_digest,
        crypto::MaterialSecrets::from_data_key(&old).name_raw_record(&bytes[..kept])
    );
    let after: serde_json::Value =
        sqlx::query_scalar("SELECT to_jsonb(e)-'payload' FROM core.events e WHERE id=$1")
            .bind(retained.id.0)
            .fetch_one(&mut conn)
            .await
            .unwrap();
    assert_eq!(after, metadata);
    let payload: serde_json::Value =
        sqlx::query_scalar("SELECT payload FROM core.events WHERE id=$1")
            .bind(retained.id.0)
            .fetch_one(&mut conn)
            .await
            .unwrap();
    assert_eq!(
        crypto::unseal_payload(&payload, &fresh.key).unwrap(),
        plain_retained
    );
    assert!(crypto::unseal_payload(&payload, &old.key).is_err());
    let wrapped: Vec<u8> =
        sqlx::query_scalar("SELECT wrapped_key FROM privacy.material_keys WHERE material_id=$1")
            .bind(material.0)
            .fetch_one(&mut conn)
            .await
            .unwrap();
    assert_eq!(crypto::unwrap(&wrapped, &identity).unwrap(), fresh.key);
    assert!(
        crypto::wrapped_key(&mut conn, &old.key_ref)
            .await
            .unwrap()
            .is_none()
    );
    let reason: String = sqlx::query_scalar("SELECT reason FROM raw.failures WHERE material_id=$1")
        .bind(material.0)
        .fetch_one(&mut conn)
        .await
        .unwrap();
    assert_eq!(
        crypto::unseal_payload(&crypto::sealed_text(&reason).unwrap(), &fresh.key).unwrap(),
        serde_json::json!("synthetic_retained_failure")
    );
    let position: (serde_json::Value, serde_json::Value, i64) =
        sqlx::query_as("SELECT cursor,carry,committed_seq FROM raw.positions WHERE binding='b'")
            .fetch_one(&mut conn)
            .await
            .unwrap();
    assert_eq!((position.0, position.2), (cursor, 2));
    assert!(
        crypto::unseal_payload(&position.1, &fresh.key)
            .unwrap()
            .is_null()
    );
    let mut reader = store
        .blobs
        .read_sealed(
            &replacement_blob,
            Some(bytes.len() as u64),
            &fresh,
            crate::blobs::SealedReadLimits {
                frame_bytes: crate::blobs::BLOB_FRAME_BYTES,
                index_bytes: 65536,
            },
            || Ok(()),
        )
        .unwrap();
    let mut exact = Vec::new();
    reader.read_to_end(&mut exact).unwrap();
    assert_eq!(exact, blanked);
    assert!(
        sqlx::query("UPDATE core.events SET payload=payload WHERE id=$1")
            .bind(retained.id.0)
            .execute(&mut conn)
            .await
            .is_err()
    );
    finish_deletions(&mut conn, &store.blobs).await.unwrap();
    assert!(!store.blobs.contains(&blob).unwrap());
}
