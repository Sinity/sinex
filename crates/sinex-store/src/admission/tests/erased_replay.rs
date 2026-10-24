use super::*;
use crate::sealed::{self, metadata};

#[tokio::test]
async fn native_replay_ciphertext_pointer_keys_and_progress_share_admission_rollback_and_retry() {
    let (_db, mut conn) = testdb::fresh().await;
    let dir = tempfile::tempdir().unwrap();
    let blobs = crate::Blobs::open(dir.path()).unwrap();
    let key = sealed::new_key().unwrap();
    let identity = sealed::Identity::generate();
    let material = MaterialId::new();
    let body = b"                        \n";
    let blob = blobs.put_sealed(body, &key).unwrap();
    let scope = metadata::Scope::Replay {
        source: "s".into(),
        material,
        parser: "p".into(),
        parser_version: 1,
        length: body.len() as u64,
        whole: false,
        append_end: None,
    };
    let mut ciphertext = Vec::new();
    metadata::seal_replay(
        &mut ciphertext,
        &identity.to_public(),
        &scope,
        [metadata::ErasedRecord {
            start: 0,
            end: body.len() as u64,
            raw_digest: [8; 32],
            transition: None,
        }],
        || Ok(()),
    )
    .unwrap();
    assert_eq!(
        metadata::open_replay(ciphertext.as_slice(), &identity, &scope, || Ok(()))
            .unwrap()
            .len(),
        1
    );
    let replay_blob = blobs.put(&ciphertext).unwrap();
    sqlx::query("INSERT INTO core.operations(id,kind,actor,scope) VALUES($1,'excise','synthetic','{\"sources\":[\"s\"]}')").bind(Uuid::now_v7()).execute(&mut conn).await.unwrap();
    let basis = crate::excision::operations_basis(&mut conn, "s")
        .await
        .unwrap();
    let progress = SourceProgress {
        native_pending: None,
        erased_replay: Some(sinex_model::ErasedReplayPublication {
            material,
            blob: replay_blob.clone(),
            length: ciphertext.len() as u64,
        }),
        excision_basis: Some(basis),
        binding: "b".into(),
        image: None,
        image_revision: None,
        material_keys: vec![sinex_model::WrappedMaterialKey {
            material,
            key_ref: key.key_ref.clone(),
            wrapped: sealed::wrap(&key, &identity.to_public()).unwrap(),
        }],
        input: InputRef {
            kind: "file".into(),
            locator: "synthetic".into(),
            host: "h".into(),
        },
        observation: ObservationFacts {
            changed: true,
            ..Default::default()
        },
        revision: RevisionChange::Appended,
        material: Some(sinex_model::MaterialRef {
            id: material,
            input: None,
            blob: blob.clone(),
            length: body.len() as u64,
            part_index: 0,
            previous_part: None,
            parser: "p".into(),
            parser_version: 1,
            manifest: json!({"role":"records"}),
        }),
        cursor: json!({"through":body.len()}),
        carry: sealed::seal_value(&Value::Null, &key).unwrap(),
    };
    let batch = Batch {
        run: RunId::new(),
        kind: BatchKind::Source(Box::new(progress.clone())),
        records: Vec::new(),
        archive: Vec::new(),
    };
    let mut tx = conn.begin().await.unwrap();
    admit(&mut tx, &batch, &NoDerivations).await.unwrap();
    tx.rollback().await.unwrap();
    for table in [
        "raw.materials",
        "raw.positions",
        "privacy.material_keys",
        "core.blobs",
    ] {
        let n: i64 = sqlx::query_scalar(&format!("SELECT count(*) FROM {table}"))
            .fetch_one(&mut conn)
            .await
            .unwrap();
        assert_eq!(n, 0, "{table} stays atomic");
    }
    assert!(
        blobs.contains(&blob).unwrap() && blobs.contains(&replay_blob).unwrap(),
        "prepared ciphertext custody survives rollback"
    );
    admit_one(&mut conn, &batch).await;
    let retained:(String,i64)=sqlx::query_as("SELECT m.erased_replay_blob,b.size FROM raw.materials m JOIN core.blobs b ON b.hash=m.erased_replay_blob WHERE m.id=$1").bind(material.0).fetch_one(&mut conn).await.unwrap();
    assert_eq!(retained, (replay_blob.clone(), ciphertext.len() as i64));
    assert_eq!(
        crate::positions::load(&mut conn, "b")
            .await
            .unwrap()
            .unwrap()
            .0,
        progress.cursor
    );
    let ingress = Batch {
        kind: BatchKind::Ingress(Box::new(sinex_model::IngressFrame {
            publication: None,
            producer: "s".into(),
            frame: "forged-native-state".into(),
            material: Some(progress),
            refusal: None,
        })),
        ..batch
    };
    let mut tx = conn.begin().await.unwrap();
    assert!(
        admit(&mut tx, &ingress, &NoDerivations)
            .await
            .unwrap_err()
            .to_string()
            .contains("producer protocol")
    );
    tx.rollback().await.unwrap();
}
