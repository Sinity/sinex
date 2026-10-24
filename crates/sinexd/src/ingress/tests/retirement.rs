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
        self.row_with_sources(ARCHIVE, position, unit, material, hash, &[], publication)
            .await;
    }
    #[allow(clippy::too_many_arguments)]
    async fn row_with_sources(
        &self,
        archive: &str,
        position: u64,
        unit: &ArchiveUnit,
        material: MaterialId,
        hash: &str,
        sources: &[String],
        publication: Option<ExcisionPublication>,
    ) {
        assert_eq!(
            self.send_row_with_sources(
                archive,
                position,
                unit,
                material,
                hash,
                sources,
                publication
            )
            .await
            .refusal,
            None
        );
    }
    #[allow(clippy::too_many_arguments)]
    async fn send_row_with_sources(
        &self,
        archive: &str,
        position: u64,
        unit: &ArchiveUnit,
        material: MaterialId,
        hash: &str,
        sources: &[String],
        publication: Option<ExcisionPublication>,
    ) -> Acknowledgment {
        let payload = json!({"archive":archive,"position":position,"local_seq":unit.commit_seq,"unit":unit.kind,"key":unit.key,"batch":hash,"rows":1,"blobs":sources,"excision":publication});
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
        self.send(
            "polylogue",
            &Ingress::Events {
                frame: format!("rows:{archive}:{position}"),
                events: vec![event],
                failures: vec![],
            },
        )
        .await
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

#[tokio::test]
async fn decoded_source_unit_tombstones_retire_only_their_superseded_batch() {
    let h = Harness::start().await;
    let key = serde_json::to_string(&json!([
        "input",
        1,
        0,
        12,
        "0".repeat(64),
        1,
        "1".repeat(64)
    ]))
    .unwrap();
    let extracted = unit(1, "extracted_blob", &key);
    let (old_material, old) = h.batch(ARCHIVE, b"decoded source association rows\n").await;
    h.row(1, &extracted, old_material, &old, None).await;
    let (sibling_material, sibling) = h.batch(ARCHIVE, b"unrelated note rows\n").await;
    h.row(
        2,
        &unit(2, "note", "sibling"),
        sibling_material,
        &sibling,
        None,
    )
    .await;
    let publication = manifest(vec![UnitReplacement {
        unit: extracted,
        batch: None,
        bytes: 0,
        rows: 0,
        blobs: vec![],
    }]);
    let owner = unit(3, "excision", "remove-decoded");
    let (material, current) = h.batch(ARCHIVE, b"excision rows\n").await;
    h.row(3, &owner, material, &current, Some(publication.clone()))
        .await;
    let proof = RetireBatches {
        archive: ARCHIVE.into(),
        owner,
        digest: publication.digest,
    };
    for _ in 0..2 {
        let result = h.retire("polylogue", &proof).await;
        assert_eq!(result.refusal, None);
        assert_eq!(result.pending_files, 0);
        assert!(!h.blobs.contains(&old).unwrap());
        assert!(h.blobs.contains(&sibling).unwrap());
        assert!(h.blobs.contains(&current).unwrap());
    }
    assert_eq!(
        h.count("SELECT count(*) FROM core.events WHERE event_type='polylogue.rows'")
            .await,
        3
    );
}

#[tokio::test]
async fn row_retirement_refuses_unknown_unit_kind_without_removing_batches() {
    let h = Harness::start().await;
    let (proof, _, old, current) = h.fixture().await;
    let (material, unknown) = h.batch(ARCHIVE, b"unknown unit rows\n").await;
    h.row(
        3,
        &unit(3, "unknown", "future-unit"),
        material,
        &unknown,
        None,
    )
    .await;
    let result = h.retire("polylogue", &proof).await;
    assert!(
        result
            .refusal
            .as_deref()
            .is_some_and(|r| r.contains("unknown archive unit")),
        "{result:?}"
    );
    for hash in [&old, &current, &unknown] {
        assert!(h.blobs.contains(hash).unwrap());
    }
    assert_eq!(h.count("SELECT count(*) FROM core.operations").await, 0);
}

#[tokio::test]
async fn row_retirement_reclaims_original_and_member_source_bytes() {
    let h = Harness::start().await;
    let original = h.source_bytes("polylogue", b"synthetic original ZIP").await;
    let member = h.source_bytes("polylogue", b"synthetic member").await;
    let sources = vec![original.clone(), member.clone()];
    let note = unit(1, "revision", "source/1");
    let (m, batch) = h.batch(ARCHIVE, b"source rows\n").await;
    h.row_with_sources(ARCHIVE, 1, &note, m, &batch, &sources, None)
        .await;
    let owner = unit(2, "excision", "remove-source");
    let publication = manifest(vec![UnitReplacement {
        unit: note,
        batch: None,
        bytes: 0,
        rows: 0,
        blobs: vec![],
    }]);
    let (m, batch) = h.batch(ARCHIVE, b"removal rows\n").await;
    h.row(2, &owner, m, &batch, Some(publication.clone())).await;
    let proof = RetireBatches {
        archive: ARCHIVE.into(),
        owner,
        digest: publication.digest,
    };
    for _ in 0..2 {
        let result = h.retire("polylogue", &proof).await;
        assert_eq!(result.refusal, None);
        assert_eq!(result.pending_files, 0);
        assert_eq!(result.retained_files, 0);
        for source in &sources {
            assert!(
                !h.blobs.contains(source).unwrap(),
                "source still retained: {source}"
            );
        }
        assert!(h.blobs.contains(&batch).unwrap());
    }
    assert_eq!(
        h.count("SELECT count(*) FROM core.events WHERE event_type='polylogue.rows'")
            .await,
        2
    );
}

impl Harness {
    async fn source_bytes(&self, producer: &str, bytes: &[u8]) -> String {
        let hash = self.fixture_blob(bytes);
        let name = format!("blob:{}", hash.strip_prefix("blake3:").unwrap());
        let (_, mut frame) = self.material_frame(&name, bytes);
        if let Ingress::Material { material, .. } = &mut frame {
            material.input.locator = name;
            material.input.kind = "polylogue".into();
            material.parser = "polylogue".into();
            material.manifest = json!({"archive":ARCHIVE,"media_type":"application/octet-stream"});
        }
        assert_eq!(self.send(producer, &frame).await.refusal, None);
        hash
    }
}

#[tokio::test]
async fn retired_global_source_ack_cannot_authorize_a_new_archive_dependency() {
    let h = Harness::start().await;
    let source = h
        .source_bytes("polylogue", b"retire then reuse source")
        .await;
    let original = unit(1, "revision", "source/1");
    let (m, b) = h.batch(ARCHIVE, b"original source rows\n").await;
    h.row_with_sources(
        ARCHIVE,
        1,
        &original,
        m,
        &b,
        std::slice::from_ref(&source),
        None,
    )
    .await;
    let publication = manifest(vec![UnitReplacement {
        unit: original,
        batch: None,
        bytes: 0,
        rows: 0,
        blobs: vec![],
    }]);
    let owner = unit(2, "excision", "erase");
    let (m, b) = h.batch(ARCHIVE, b"erase source rows\n").await;
    h.row(2, &owner, m, &b, Some(publication.clone())).await;
    let proof = RetireBatches {
        archive: ARCHIVE.into(),
        owner,
        digest: publication.digest,
    };
    assert_eq!(h.retire("polylogue", &proof).await.refusal, None);
    assert!(!h.blobs.contains(&source).unwrap());
    let reply = h
        .client
        .request(
            h.subjects.have("polylogue"),
            serde_json::to_vec(&sinex_protocol::Have {
                frames: vec![format!("blob:{}", source.strip_prefix("blake3:").unwrap())],
            })
            .unwrap()
            .into(),
        )
        .await
        .unwrap();
    let have: Vec<Acknowledgment> = serde_json::from_slice(&reply.payload).unwrap();
    let (m, b) = h.batch("later-archive", b"later source rows\n").await;
    let ack = h
        .send_row_with_sources(
            "later-archive",
            1,
            &unit(1, "revision", "source/1"),
            m,
            &b,
            std::slice::from_ref(&source),
            None,
        )
        .await;
    assert!(
        have[0].refusal.is_some() && ack.refusal.is_some(),
        "retired dependency accepted: have={have:?} new row={ack:?}"
    );
    let (_, frame) = h.material_frame(&have[0].frame, b"retire then reuse source");
    let Ingress::Material { material, .. } = frame else {
        unreachable!()
    };
    let reserve = h
        .send(
            "polylogue",
            &Ingress::Reserve {
                frame: "reserve-retired-source".into(),
                publication: Publication {
                    version: 1,
                    frame: have[0].frame.clone(),
                    class: Default::default(),
                    material: Some(Material::from_frame(&material)),
                    envelope: None,
                },
            },
        )
        .await;
    let complete: Acknowledgment =
        serde_json::from_value(reserve.outcome["complete"].clone()).unwrap();
    assert!(complete.refusal.is_some());
    assert_eq!(complete.material, have[0].material);
    assert_eq!(
        h.count(
            "SELECT count(*) FROM core.ingress_frames WHERE frame LIKE 'blob:%' AND refusal IS NULL"
        )
        .await,
        1
    );
    assert!(!h.blobs.contains(&source).unwrap());
}

#[tokio::test]
async fn row_retirement_preserves_latest_source_references_across_archives() {
    for other_archive in [ARCHIVE, "archive-b"] {
        let h = Harness::start().await;
        let source = h.source_bytes("polylogue", b"shared original bytes").await;
        let (m, b) = h.batch(ARCHIVE, b"original rows\n").await;
        let original = unit(1, "revision", "source/1");
        h.row_with_sources(
            ARCHIVE,
            1,
            &original,
            m,
            &b,
            std::slice::from_ref(&source),
            None,
        )
        .await;
        let (m, b) = h.batch(other_archive, b"shared rows\n").await;
        let same_archive = other_archive == ARCHIVE;
        let position = if same_archive { 2 } else { 1 };
        let shared = unit(position as i64, "revision", "shared/1");
        h.row_with_sources(
            other_archive,
            position,
            &shared,
            m,
            &b,
            std::slice::from_ref(&source),
            None,
        )
        .await;
        let publication = manifest(vec![UnitReplacement {
            unit: original,
            batch: None,
            bytes: 0,
            rows: 0,
            blobs: vec![],
        }]);
        let owner = unit(3, "excision", "remove-original");
        let (m, b) = h.batch(ARCHIVE, b"remove original\n").await;
        h.row(
            if same_archive { 3 } else { 2 },
            &owner,
            m,
            &b,
            Some(publication.clone()),
        )
        .await;
        let proof = RetireBatches {
            archive: ARCHIVE.into(),
            owner,
            digest: publication.digest,
        };
        let first = h.retire("polylogue", &proof).await;
        assert_eq!(first.refusal, None);
        assert!(
            h.blobs.contains(&source).unwrap(),
            "selected shared source removed"
        );
        let publication = manifest(vec![UnitReplacement {
            unit: shared,
            batch: None,
            bytes: 0,
            rows: 0,
            blobs: vec![],
        }]);
        let owner = unit(4, "excision", "remove-shared");
        let (m, b) = h.batch(other_archive, b"remove shared\n").await;
        h.row_with_sources(
            other_archive,
            if same_archive { 4 } else { 2 },
            &owner,
            m,
            &b,
            &[],
            Some(publication),
        )
        .await;
        // The first archive's proof also sees the latest reference selection
        // in the other namespace; no proof digest cache may keep stale refs.
        let result = h.retire("polylogue", &proof).await;
        assert_eq!(result.refusal, None);
        assert!(!h.blobs.contains(&source).unwrap());
    }
}

#[tokio::test]
async fn row_retirement_preserves_foreign_material_with_the_same_source_hash() {
    let h = Harness::start().await;
    let source = h.source_bytes("polylogue", b"two producer copies").await;
    assert_eq!(
        source,
        h.source_bytes("foreign", b"two producer copies").await
    );
    let original = unit(1, "revision", "source/1");
    let (m, b) = h.batch(ARCHIVE, b"source rows\n").await;
    h.row_with_sources(
        ARCHIVE,
        1,
        &original,
        m,
        &b,
        std::slice::from_ref(&source),
        None,
    )
    .await;
    let publication = manifest(vec![UnitReplacement {
        unit: original,
        batch: None,
        bytes: 0,
        rows: 0,
        blobs: vec![],
    }]);
    let owner = unit(2, "excision", "erase");
    let (m, b) = h.batch(ARCHIVE, b"remove source\n").await;
    h.row(2, &owner, m, &b, Some(publication.clone())).await;
    let result = h
        .retire(
            "polylogue",
            &RetireBatches {
                archive: ARCHIVE.into(),
                owner,
                digest: publication.digest,
            },
        )
        .await;
    assert_eq!(result.refusal, None);
    assert!(h.blobs.contains(&source).unwrap());
    assert_eq!(result.retained_files, 1);
    assert_eq!(h.count("SELECT count(*) FROM raw.materials m JOIN raw.inputs i ON i.id=m.input_id WHERE i.binding='ingress:foreign' AND m.blob LIKE 'blake3:%'").await, 1);
}

#[tokio::test]
async fn row_descriptor_refuses_foreign_only_and_replacement_source_dependencies() {
    let h = Harness::start().await;
    let foreign = h.source_bytes("foreign", b"foreign source only").await;
    let (m, b) = h.batch(ARCHIVE, b"rows without owned source\n").await;
    let ack = h
        .send_row_with_sources(
            ARCHIVE,
            1,
            &unit(1, "revision", "source/1"),
            m,
            &b,
            std::slice::from_ref(&foreign),
            None,
        )
        .await;
    assert!(
        ack.refusal
            .as_deref()
            .unwrap()
            .contains("unavailable producer source material")
    );
    let publication = manifest(vec![UnitReplacement {
        unit: unit(1, "revision", "source/1"),
        batch: Some(b.clone()),
        bytes: 26,
        rows: 1,
        blobs: vec![foreign],
    }]);
    let ack = h
        .send_row_with_sources(
            ARCHIVE,
            2,
            &unit(2, "excision", "rewrite"),
            m,
            &b,
            &[],
            Some(publication),
        )
        .await;
    assert!(
        ack.refusal
            .as_deref()
            .unwrap()
            .contains("unavailable producer source material")
    );
    assert_eq!(
        h.count("SELECT count(*) FROM core.events WHERE event_type='polylogue.rows'")
            .await,
        0
    );
}

#[tokio::test]
async fn row_retirement_keeps_source_bytes_reserved_by_a_later_publication() {
    let h = Harness::start().await;
    let bytes = b"late pending source copy";
    let source = h.source_bytes("polylogue", bytes).await;
    let original = unit(1, "revision", "source/1");
    let (m, b) = h.batch(ARCHIVE, b"source rows\n").await;
    h.row_with_sources(
        ARCHIVE,
        1,
        &original,
        m,
        &b,
        std::slice::from_ref(&source),
        None,
    )
    .await;
    let (_, late) = h.material_frame("late-source-copy", bytes);
    let Ingress::Material { material, .. } = &late else {
        unreachable!()
    };
    let declaration = Publication {
        version: 1,
        frame: late.frame().into(),
        class: Default::default(),
        material: Some(Material::from_frame(material)),
        envelope: None,
    };
    spool::stage(
        &h.store_dir,
        "polylogue",
        &declaration,
        Role::Material,
        bytes.as_slice(),
    )
    .unwrap();
    let reserved = h
        .send(
            "polylogue",
            &Ingress::Reserve {
                frame: "reserve-late-source".into(),
                publication: declaration,
            },
        )
        .await;
    assert_eq!(reserved.refusal, None);
    assert!(reserved.outcome["complete"].is_null());
    let publication = manifest(vec![UnitReplacement {
        unit: original,
        batch: None,
        bytes: 0,
        rows: 0,
        blobs: vec![],
    }]);
    let owner = unit(2, "excision", "erase");
    let (m, b) = h.batch(ARCHIVE, b"remove source\n").await;
    h.row(2, &owner, m, &b, Some(publication.clone())).await;
    let proof = RetireBatches {
        archive: ARCHIVE.into(),
        owner,
        digest: publication.digest,
    };
    let result = h.retire("polylogue", &proof).await;
    assert_eq!(result.refusal, None);
    assert_eq!(result.retained_files, 1);
    assert!(h.blobs.contains(&source).unwrap());
    let completed = h.send("polylogue", &late).await;
    assert_eq!(completed.refusal, None);
    assert_eq!(h.retire("polylogue", &proof).await.refusal, None);
    assert!(h.blobs.contains(&source).unwrap());
}

#[tokio::test]
async fn row_retirement_reclaims_sources_from_intermediate_replacements() {
    let h = Harness::start().await;
    let original = h.source_bytes("polylogue", b"original source").await;
    let rewritten = h.source_bytes("polylogue", b"rewritten source").await;
    let target = unit(1, "revision", "source/1");
    let (m, b) = h.batch(ARCHIVE, b"original source rows\n").await;
    h.row_with_sources(
        ARCHIVE,
        1,
        &target,
        m,
        &b,
        std::slice::from_ref(&original),
        None,
    )
    .await;
    let (_, replacement) = h.batch(ARCHIVE, b"replacement source rows\n").await;
    let publication = manifest(vec![UnitReplacement {
        unit: target.clone(),
        batch: Some(replacement),
        bytes: 24,
        rows: 1,
        blobs: vec![rewritten.clone()],
    }]);
    let owner = unit(2, "excision", "rewrite");
    let (m, b) = h.batch(ARCHIVE, b"rewrite descriptor rows\n").await;
    h.row(2, &owner, m, &b, Some(publication.clone())).await;
    let result = h
        .retire(
            "polylogue",
            &RetireBatches {
                archive: ARCHIVE.into(),
                owner,
                digest: publication.digest,
            },
        )
        .await;
    assert_eq!(result.refusal, None);
    assert!(!h.blobs.contains(&original).unwrap());
    assert!(h.blobs.contains(&rewritten).unwrap());
    let publication = manifest(vec![UnitReplacement {
        unit: target,
        batch: None,
        bytes: 0,
        rows: 0,
        blobs: vec![],
    }]);
    let owner = unit(3, "excision", "remove-replacement");
    let (m, b) = h.batch(ARCHIVE, b"removal descriptor rows\n").await;
    h.row(3, &owner, m, &b, Some(publication.clone())).await;
    let result = h
        .retire(
            "polylogue",
            &RetireBatches {
                archive: ARCHIVE.into(),
                owner,
                digest: publication.digest,
            },
        )
        .await;
    assert_eq!(result.refusal, None);
    assert!(!h.blobs.contains(&rewritten).unwrap());
}

#[tokio::test]
async fn source_availability_keeps_arbitrary_generic_material_frame_names() {
    let h = Harness::start().await;
    let addressed_name = format!("blob:{}", "a".repeat(64));
    for name in ["blob:descriptive-name", addressed_name.as_str()] {
        let (_, mut frame) = h.material_frame(name, b"ordinary producer material");
        if let Ingress::Material { material, .. } = &mut frame {
            material.input.locator = name.into();
        }
        let first = h.send("generic", &frame).await;
        assert_eq!(first.refusal, None);
        let retry = h.send("generic", &frame).await;
        assert_eq!(retry, first);
        let response = h
            .client
            .request(
                h.subjects.have("generic"),
                serde_json::to_vec(&sinex_protocol::Have {
                    frames: vec![name.into()],
                })
                .unwrap()
                .into(),
            )
            .await
            .unwrap();
        let have: Vec<Acknowledgment> = serde_json::from_slice(&response.payload).unwrap();
        assert_eq!(have, vec![first]);
    }
}

#[tokio::test]
async fn row_descriptor_refuses_generic_material_roles_with_source_frame_names() {
    let h = Harness::start().await;
    let bytes = b"generic material sharing the source naming convention";
    let hash = h.fixture_blob(bytes);
    let name = format!("blob:{}", hash.strip_prefix("blake3:").unwrap());
    let (_, mut frame) = h.material_frame(&name, bytes);
    if let Ingress::Material { material, .. } = &mut frame {
        material.input.locator = name;
        material.parser = "polylogue".into();
        // A parser name alone does not declare the input's Polylogue role.
        assert_eq!(material.input.kind, "file");
    }
    assert_eq!(h.send("polylogue", &frame).await.refusal, None);
    let (m, b) = h
        .batch(ARCHIVE, b"rows with generic source material\n")
        .await;
    let ack = h
        .send_row_with_sources(
            ARCHIVE,
            1,
            &unit(1, "revision", "source/1"),
            m,
            &b,
            std::slice::from_ref(&hash),
            None,
        )
        .await;
    assert!(
        ack.refusal
            .as_deref()
            .unwrap()
            .contains("unavailable producer source material")
    );
    assert!(h.blobs.contains(&hash).unwrap());
    assert_eq!(
        h.count("SELECT count(*) FROM core.events WHERE event_type='polylogue.rows'")
            .await,
        0
    );
}

#[tokio::test]
async fn row_retirement_refuses_missing_original_source_acknowledgment() {
    let h = Harness::start().await;
    let source = h.source_bytes("polylogue", b"owned original source").await;
    let original = unit(1, "revision", "source/1");
    let (material, old) = h.batch(ARCHIVE, b"original source rows\n").await;
    h.row_with_sources(
        ARCHIVE,
        1,
        &original,
        material,
        &old,
        std::slice::from_ref(&source),
        None,
    )
    .await;
    let publication = manifest(vec![UnitReplacement {
        unit: original,
        batch: None,
        bytes: 0,
        rows: 0,
        blobs: vec![],
    }]);
    let owner = unit(2, "excision", "erase");
    let (material, current) = h.batch(ARCHIVE, b"remove source rows\n").await;
    h.row(2, &owner, material, &current, Some(publication.clone()))
        .await;
    // A legacy descriptor with missing acknowledgment evidence cannot grant
    // authority over source bytes merely by listing their content hash.
    let mut conn = PgConnection::connect(&h.url).await.unwrap();
    sqlx::query("DELETE FROM core.ingress_frames WHERE producer='polylogue' AND frame=$1")
        .bind(format!("blob:{}", source.strip_prefix("blake3:").unwrap()))
        .execute(&mut conn)
        .await
        .unwrap();
    let result = h
        .retire(
            "polylogue",
            &RetireBatches {
                archive: ARCHIVE.into(),
                owner,
                digest: publication.digest,
            },
        )
        .await;
    assert!(result.refusal.as_deref().unwrap().contains("not confirmed"));
    for hash in [&source, &old, &current] {
        assert!(h.blobs.contains(hash).unwrap());
    }
    assert_eq!(h.count("SELECT count(*) FROM core.operations").await, 0);
}

#[tokio::test]
async fn row_retirement_does_not_count_unchanged_source_dependencies_as_retained_cleanup() {
    let h = Harness::start().await;
    let (proof, _, old, current) = h.fixture().await;
    let source = h.source_bytes("polylogue", b"unaffected source body").await;
    let (material, batch) = h.batch(ARCHIVE, b"unaffected source rows\n").await;
    h.row_with_sources(
        ARCHIVE,
        3,
        &unit(3, "revision", "kept/1"),
        material,
        &batch,
        std::slice::from_ref(&source),
        None,
    )
    .await;
    for _ in 0..2 {
        let result = h.retire("polylogue", &proof).await;
        assert_eq!(result.refusal, None);
        assert_eq!(result.retained_files, 0);
        assert_eq!(result.pending_files, 0);
        assert!(!h.blobs.contains(&old).unwrap());
        for hash in [&source, &current, &batch] {
            assert!(h.blobs.contains(hash).unwrap());
        }
    }
}

#[tokio::test]
async fn older_manifest_can_follow_confirmed_newer_owner_without_intermediate_source() {
    let h = Harness::start().await;
    let target = unit(1, "revision", "source/1");
    let latest = unit(3, "excision", "latest");
    let publication = manifest(vec![UnitReplacement {
        unit: target.clone(),
        batch: None,
        bytes: 0,
        rows: 0,
        blobs: vec![],
    }]);
    let (material, batch) = h.batch(ARCHIVE, b"latest owner rows\n").await;
    // The publisher sends the latest suffix first, before filling old gaps.
    h.row(3, &latest, material, &batch, Some(publication.clone()))
        .await;
    let missing_source = h.fixture_blob(b"unpublished intermediate source");
    let unpublished_batch = h.fixture_blob(b"unpublished intermediate rows\n");
    let intermediate = manifest(vec![UnitReplacement {
        unit: target.clone(),
        batch: Some(unpublished_batch),
        bytes: 30,
        rows: 1,
        blobs: vec![missing_source.clone()],
    }]);
    let older = unit(2, "excision", "intermediate");
    let (material, batch) = h.batch(ARCHIVE, b"older owner rows\n").await;
    let ack = h
        .send_row_with_sources(
            ARCHIVE,
            2,
            &older,
            material,
            &batch,
            &[],
            Some(intermediate.clone()),
        )
        .await;
    assert_eq!(
        ack.refusal, None,
        "latest owner already supersedes this source"
    );
    let (original_material, original_batch) = h.batch(ARCHIVE, b"original rows\n").await;
    h.row(1, &target, original_material, &original_batch, None)
        .await;
    let retired = h
        .retire(
            "polylogue",
            &RetireBatches {
                archive: ARCHIVE.into(),
                owner: latest,
                digest: publication.digest,
            },
        )
        .await;
    assert_eq!(retired.refusal, None);
    assert_eq!(retired.pending_files, 0);
    let retry = h
        .send_row_with_sources(
            ARCHIVE,
            2,
            &older,
            material,
            &batch,
            &[],
            Some(intermediate),
        )
        .await;
    assert_eq!(retry, ack, "held acknowledgment changed after retirement");
    assert!(!h.blobs.contains(&missing_source).unwrap());
    assert_eq!(
        h.count("SELECT count(*) FROM core.events WHERE event_type='polylogue.rows'")
            .await,
        3
    );
}

#[tokio::test]
async fn unavailable_replacement_needs_a_newer_matching_acknowledged_owner() {
    for case in [
        "absent",
        "archive",
        "unit",
        "older",
        "same",
        "missing_ack",
        "foreign",
    ] {
        let h = Harness::start().await;
        let target = unit(1, "revision", "source/1");
        let incoming = unit(3, "excision", "incoming");
        if case != "absent" {
            let owner = match case {
                "older" => unit(2, "excision", "older"),
                "same" => incoming.clone(),
                _ => unit(4, "excision", "newer"),
            };
            let replaced = if case == "unit" {
                unit(1, "revision", "other/1")
            } else {
                target.clone()
            };
            let publication = manifest(vec![UnitReplacement {
                unit: replaced,
                batch: None,
                bytes: 0,
                rows: 0,
                blobs: vec![],
            }]);
            let archive = if case == "archive" {
                "other-archive"
            } else {
                ARCHIVE
            };
            let (material, batch) = h.batch(archive, b"unrelated owner rows\n").await;
            if case == "foreign" {
                let event = ProducerEvent {
                    event_type: "polylogue.rows".into(),
                    payload: json!({"archive":archive,"position":4,"local_seq":owner.commit_seq,"unit":owner.kind,"key":owner.key,"batch":batch,"rows":1,"blobs":[],"excision":publication}),
                    location: Some(RecordLocation {
                        material,
                        anchor: 0,
                    }),
                    ordinal: 0,
                    parents: vec![],
                    reading_order: 4,
                    class: Default::default(),
                };
                assert_eq!(
                    h.send(
                        "foreign",
                        &Ingress::Events {
                            frame: "foreign-owner".into(),
                            events: vec![event],
                            failures: vec![]
                        }
                    )
                    .await
                    .refusal,
                    None
                );
            } else {
                h.row_with_sources(archive, 4, &owner, material, &batch, &[], Some(publication))
                    .await;
                if case == "missing_ack" {
                    let mut conn = PgConnection::connect(&h.url).await.unwrap();
                    sqlx::query(
                        "DELETE FROM core.ingress_frames WHERE producer='polylogue' AND frame=$1",
                    )
                    .bind(format!("rows:{archive}:4"))
                    .execute(&mut conn)
                    .await
                    .unwrap();
                }
            }
        }
        let source = h.fixture_blob(b"missing intermediate source");
        let publication = manifest(vec![UnitReplacement {
            unit: target,
            batch: Some(h.fixture_blob(b"unpublished rows\n")),
            bytes: 17,
            rows: 1,
            blobs: vec![source],
        }]);
        let (material, batch) = h.batch(ARCHIVE, b"incoming owner rows\n").await;
        let ack = h
            .send_row_with_sources(
                ARCHIVE,
                3,
                &incoming,
                material,
                &batch,
                &[],
                Some(publication),
            )
            .await;
        assert!(
            ack.refusal
                .as_deref()
                .is_some_and(|r| r.contains("unavailable producer source material")),
            "{case}: {ack:?}"
        );
    }
}
