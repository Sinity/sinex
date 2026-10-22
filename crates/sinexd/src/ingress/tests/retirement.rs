use super::*;
use sinex_model::event_types::polylogue::{ArchiveUnit, ExcisionPublication, UnitReplacement};
use sinex_protocol::{RetireBatches, Retirement};

const ARCHIVE: &str = "archive-a";
fn unit(seq: i64, kind: &str, key: &str) -> ArchiveUnit {
    ArchiveUnit {
        commit_seq: seq,
        kind: kind.into(),
        key: key.into(),
    }
}
fn manifest(replacements: Vec<UnitReplacement>) -> ExcisionPublication {
    let mut value = json!({"version":1,"replacements":replacements,"retired_batches":[]});
    value.sort_all_objects();
    let digest = format!(
        "blake3:{}",
        blake3::hash(&serde_json::to_vec(&value).unwrap())
    );
    value["digest"] = json!(digest);
    serde_json::from_value(value).unwrap()
}
impl Harness {
    async fn batch(&self, archive: &str, bytes: &[u8]) -> (MaterialId, String) {
        let hash = self.blobs.put(bytes).unwrap();
        let name = format!("batch:{archive}:{}", hash.strip_prefix("blake3:").unwrap());
        let (id, mut frame) = self.material_frame(&name, bytes);
        if let Ingress::Material { material, .. } = &mut frame {
            material.input.locator = name;
            material.parser = "polylogue".into();
            material.manifest = json!({"archive":archive,"media_type":"application/jsonl"});
        }
        let ack = self.send("polylogue", &frame).await;
        assert_eq!(ack.refusal, None);
        (id, hash)
    }
    async fn row(
        &self,
        position: u64,
        unit: &ArchiveUnit,
        material: MaterialId,
        hash: &str,
        publication: Option<ExcisionPublication>,
    ) {
        let payload = json!({"archive":ARCHIVE,"position":position,"local_seq":unit.commit_seq,"unit":unit.kind,"key":unit.key,"batch":hash,"rows":1,"blobs":[],"excision":publication});
        let event = ProducerEvent {
            event_type: "polylogue.rows".into(),
            payload,
            location: Some(RecordLocation {
                material,
                anchor: 0,
            }),
            ordinal: 0,
            parents: vec![],
            reading_order: position as i64,
            class: sinex_model::Class::Open,
        };
        let ack = self
            .send(
                "polylogue",
                &Ingress::Events {
                    frame: format!("rows:{ARCHIVE}:{position}"),
                    events: vec![event],
                    failures: vec![],
                },
            )
            .await;
        assert_eq!(ack.refusal, None);
    }
    async fn retire(&self, producer: &str, proof: &RetireBatches) -> Retirement {
        let reply = self
            .client
            .request(
                self.subjects.retire(producer),
                serde_json::to_vec(proof).unwrap().into(),
            )
            .await
            .unwrap();
        serde_json::from_slice(&reply.payload).unwrap()
    }
    async fn fixture(&self) -> (RetireBatches, MaterialId, String, String) {
        let note = unit(1, "note", "n");
        let (old_material, old) = self.batch(ARCHIVE, b"old private rows\n").await;
        self.row(1, &note, old_material, &old, None).await;
        let (_, new) = self.batch(ARCHIVE, b"new rows\n").await;
        let publication = manifest(vec![UnitReplacement {
            unit: note,
            batch: Some(new.clone()),
            bytes: 9,
            rows: 1,
            blobs: vec![],
        }]);
        let owner = unit(2, "excision", "erase");
        let (m, b) = self.batch(ARCHIVE, b"excision rows\n").await;
        self.row(2, &owner, m, &b, Some(publication.clone())).await;
        (
            RetireBatches {
                archive: ARCHIVE.into(),
                owner,
                digest: publication.digest,
            },
            old_material,
            old,
            new,
        )
    }
}

#[tokio::test]
async fn row_retirement_preserves_descriptors_for_stream_reconstruction_and_retries() {
    let h = Harness::start().await;
    let (proof, _, old, new) = h.fixture().await;
    let mut conn = PgConnection::connect(&h.url).await.unwrap();
    let js = crate::publisher::context(h.client.clone());
    crate::publisher::ensure_stream(&js, &h.subjects)
        .await
        .unwrap();
    crate::publisher::publish_pending(&mut conn, &js, &h.subjects)
        .await
        .unwrap();
    let result = h.retire("polylogue", &proof).await;
    assert_eq!(result.refusal, None);
    assert_eq!(result.retired_materials, 1);
    assert_eq!(result.pending_files, 0);
    assert!(!h.blobs.contains(&old).unwrap());
    assert!(h.blobs.contains(&new).unwrap());
    assert_eq!(
        h.count("SELECT count(*) FROM core.events WHERE event_type='polylogue.rows'")
            .await,
        2
    );
    assert_eq!(h.count("SELECT count(*) FROM core.commit_log").await, 2);
    // Destroy the old stream and reconstruct from the preserved commit log.
    js.delete_stream(h.subjects.confirmed_stream())
        .await
        .unwrap();
    sqlx::query("DELETE FROM core.consumer_positions WHERE consumer='publisher'")
        .execute(&mut conn)
        .await
        .unwrap();
    sqlx::query("DELETE FROM core.published")
        .execute(&mut conn)
        .await
        .unwrap();
    crate::publisher::ensure_stream(&js, &h.subjects)
        .await
        .unwrap();
    crate::publisher::publish_pending(&mut conn, &js, &h.subjects)
        .await
        .unwrap();
    let stream = js.get_stream(h.subjects.confirmed_stream()).await.unwrap();
    let info = stream.cached_info();
    assert_eq!(info.state.messages, 2);
    let result = h.retire("polylogue", &proof).await; // lost reply / all held
    assert_eq!(result.refusal, None);
    assert_eq!(result.pending_files, 0);
}

#[tokio::test]
async fn row_retirement_retry_preserves_newer_material_backing() {
    let h = Harness::start().await;
    let (proof, material, old, _) = h.fixture().await;
    assert_eq!(h.retire("polylogue", &proof).await.refusal, None);
    let newer = h.blobs.put(b"newer material backing\n").unwrap();
    let mut conn = PgConnection::connect(&h.url).await.unwrap();
    sqlx::query("INSERT INTO core.blobs(hash,size,status) VALUES($1,$2,'present')")
        .bind(&newer)
        .bind(23_i64)
        .execute(&mut conn)
        .await
        .unwrap();
    sqlx::query("UPDATE raw.materials SET blob=$2 WHERE id=$1")
        .bind(material.0)
        .bind(&newer)
        .execute(&mut conn)
        .await
        .unwrap();
    let result = h.retire("polylogue", &proof).await;
    assert_eq!(result.refusal, None);
    assert_eq!(result.retired_materials, 0);
    let current: String = sqlx::query_scalar("SELECT blob FROM raw.materials WHERE id=$1")
        .bind(material.0)
        .fetch_one(&mut conn)
        .await
        .unwrap();
    assert_eq!(current, newer);
    assert!(h.blobs.contains(&newer).unwrap());
    assert!(!h.blobs.contains(&old).unwrap());
}

#[tokio::test]
async fn row_retirement_refuses_corrupt_replacement_bytes_before_mutation() {
    for valid_blob in [false, true] {
        let h = Harness::start().await;
        let (proof, material, old, replacement) = h.fixture().await;
        let hex = replacement.strip_prefix("blake3:").unwrap();
        let path = h
            .store_dir
            .join("blobs")
            .join(&hex[..2])
            .join(format!("{hex}.zst"));
        if valid_blob {
            let other = h.blobs.put(b"bad rows\n").unwrap();
            let other_hex = other.strip_prefix("blake3:").unwrap();
            let other_path = h
                .store_dir
                .join("blobs")
                .join(&other_hex[..2])
                .join(format!("{other_hex}.zst"));
            std::fs::copy(other_path, &path).unwrap();
        } else {
            std::fs::write(&path, b"corrupt replacement bytes").unwrap();
        }
        assert!(h.blobs.contains(&replacement).unwrap());
        let result = h.retire("polylogue", &proof).await;
        assert!(result.refusal.is_some(), "corrupt replacement was accepted");
        assert!(h.blobs.contains(&old).unwrap());
        let mut conn = PgConnection::connect(&h.url).await.unwrap();
        let current: String = sqlx::query_scalar("SELECT blob FROM raw.materials WHERE id=$1")
            .bind(material.0)
            .fetch_one(&mut conn)
            .await
            .unwrap();
        assert_eq!(current, old);
        assert_eq!(h.count("SELECT count(*) FROM core.commit_log").await, 2);
        assert_eq!(h.count("SELECT count(*) FROM core.operations").await, 0);
    }
}

#[tokio::test]
async fn completed_row_retirement_does_not_reread_live_replacements() {
    let h = Harness::start().await;
    let (proof, _, old, replacement) = h.fixture().await;
    assert_eq!(h.retire("polylogue", &proof).await.refusal, None);
    assert!(!h.blobs.contains(&old).unwrap());
    let hex = replacement.strip_prefix("blake3:").unwrap();
    let path = h
        .store_dir
        .join("blobs")
        .join(&hex[..2])
        .join(format!("{hex}.zst"));
    let held = h.store_dir.join("held-replacement");
    std::fs::rename(&path, &held).unwrap();
    // Physical cleanup is already complete. A repeat request has no backing
    // change to authorize and must not scan the live batch contents again.
    let retry = h.retire("polylogue", &proof).await;
    std::fs::rename(&held, &path).unwrap();
    assert_eq!(retry.refusal, None);
    assert_eq!(retry.retired_materials, 0);
    assert_eq!(retry.pending_files, 0);
}

#[tokio::test]
async fn row_retirement_refuses_wrong_proofs_and_incomplete_coverage_before_mutation() {
    let h = Harness::start().await;
    let (proof, _, old, _) = h.fixture().await;
    for change in [0, 1, 2, 3] {
        let mut bad = proof.clone();
        match change {
            0 => bad.archive = "other".into(),
            1 => bad.digest = "wrong".into(),
            2 => bad.owner.key = "wrong".into(),
            _ => {}
        }
        assert!(
            h.retire(if change == 3 { "phone" } else { "polylogue" }, &bad)
                .await
                .refusal
                .is_some()
        );
        assert!(h.blobs.contains(&old).unwrap());
    }
    let mut conn = PgConnection::connect(&h.url).await.unwrap();
    sqlx::query("DELETE FROM core.ingress_frames WHERE frame=$1")
        .bind(format!("rows:{ARCHIVE}:1"))
        .execute(&mut conn)
        .await
        .unwrap();
    assert!(h.retire("polylogue", &proof).await.refusal.is_some());
    assert!(h.blobs.contains(&old).unwrap());
}

#[tokio::test]
async fn row_retirement_preserves_material_and_same_revision_aliases() {
    for same_revision in [false, true] {
        let h = Harness::start().await;
        let (proof, material, old, _) = h.fixture().await;
        let alias = Uuid::now_v7();
        let mut conn = PgConnection::connect(&h.url).await.unwrap();
        sqlx::query("INSERT INTO raw.materials(id,input_id,revision_id,blob,durable_bytes,manifest,parser,parser_version) SELECT $2,input_id,CASE WHEN $3 THEN revision_id ELSE NULL END,blob,durable_bytes,manifest,parser,parser_version FROM raw.materials WHERE id=$1").bind(material.0).bind(alias).bind(same_revision).execute(&mut conn).await.unwrap();
        let before:Vec<(String,)> =sqlx::query_as("SELECT blob FROM raw.revision_extents WHERE revision_id=(SELECT revision_id FROM raw.materials WHERE id=$1)").bind(material.0).fetch_all(&mut conn).await.unwrap();
        let result = h.retire("polylogue", &proof).await;
        assert_eq!(result.refusal, None);
        assert_eq!(result.retained_files, 1);
        assert!(h.blobs.contains(&old).unwrap());
        let alias_blob: String = sqlx::query_scalar("SELECT blob FROM raw.materials WHERE id=$1")
            .bind(alias)
            .fetch_one(&mut conn)
            .await
            .unwrap();
        assert_eq!(alias_blob, old);
        if same_revision {
            let after:Vec<(String,)> =sqlx::query_as("SELECT blob FROM raw.revision_extents WHERE revision_id=(SELECT revision_id FROM raw.materials WHERE id=$1)").bind(material.0).fetch_all(&mut conn).await.unwrap();
            assert_eq!(before, after);
            assert_eq!(result.retained_materials, 1);
        }
    }
}

#[tokio::test]
async fn row_retirement_reclaims_successive_replacements_but_keeps_selected_aliases() {
    let h = Harness::start().await;
    let (first, _, old, new) = h.fixture().await;
    assert_eq!(h.retire("polylogue", &first).await.refusal, None);
    let (_, latest) = h.batch(ARCHIVE, b"latest rows\n").await;
    let owner = unit(3, "excision", "erase-again");
    let publication = manifest(vec![UnitReplacement {
        unit: unit(1, "note", "n"),
        batch: Some(latest.clone()),
        bytes: 12,
        rows: 1,
        blobs: vec![],
    }]);
    let (m, b) = h.batch(ARCHIVE, b"second excision\n").await;
    h.row(3, &owner, m, &b, Some(publication.clone())).await;
    let proof = RetireBatches {
        archive: ARCHIVE.into(),
        owner,
        digest: publication.digest,
    };
    assert_eq!(h.retire("polylogue", &proof).await.refusal, None);
    assert!(!h.blobs.contains(&old).unwrap());
    assert!(!h.blobs.contains(&new).unwrap());
    assert!(h.blobs.contains(&latest).unwrap());

    let h = Harness::start().await;
    let (proof, m, old, _) = h.fixture().await;
    h.row(3, &unit(3, "note", "shared"), m, &old, None).await;
    let result = h.retire("polylogue", &proof).await;
    assert_eq!(result.refusal, None);
    assert_eq!(result.retained_files, 1);
    assert!(h.blobs.contains(&old).unwrap());
}

#[tokio::test]
async fn row_retirement_pending_unlinks_recover_and_reregistration_cancels_stale_cleanup() {
    for reregister in [false, true] {
        let h = Harness::start().await;
        let (proof, _, old, replacement) = h.fixture().await;
        let hex = old.strip_prefix("blake3:").unwrap();
        let path = h
            .store_dir
            .join("blobs")
            .join(&hex[..2])
            .join(format!("{hex}.zst"));
        let saved = h.store_dir.join("saved-original");
        std::fs::rename(&path, &saved).unwrap();
        std::fs::create_dir(&path).unwrap(); // deterministic unlink failure
        let result = h.retire("polylogue", &proof).await;
        assert_eq!(result.refusal, None);
        assert_eq!(result.pending_files, 1);
        std::fs::remove_dir(&path).unwrap();
        std::fs::rename(&saved, &path).unwrap();
        let replacement_hex = replacement.strip_prefix("blake3:").unwrap();
        let replacement_path = h
            .store_dir
            .join("blobs")
            .join(&replacement_hex[..2])
            .join(format!("{replacement_hex}.zst"));
        let held = h.store_dir.join("held-live-replacement");
        std::fs::rename(&replacement_path, &held).unwrap();
        let retry = h.retire("polylogue", &proof).await;
        std::fs::rename(&held, &replacement_path).unwrap();
        assert!(
            retry.refusal.is_some(),
            "pending unlink skipped replacement verification"
        );
        assert!(h.blobs.contains(&old).unwrap());
        if reregister {
            h.batch("unrelated-archive", b"old private rows\n").await;
        }
        // A new database connection runs the daemon's startup cleanup path.
        let mut conn = PgConnection::connect(&h.url).await.unwrap();
        let (_, left) = sinex_store::excision::finish_deletions(&mut conn, &h.blobs)
            .await
            .unwrap();
        assert!(left.is_empty());
        let retry = h.retire("polylogue", &proof).await;
        assert_eq!(retry.refusal, None);
        assert_eq!(retry.pending_files, 0);
        assert_eq!(h.blobs.contains(&old).unwrap(), reregister);
        assert_eq!(retry.retained_files, u64::from(reregister));
    }
}
