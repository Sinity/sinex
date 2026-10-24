use super::*;
use crate::ingress::bytes::Point;
use sqlx::Executor;

async fn receiver(h: &Harness) -> Ingest {
    Ingest::new(
        h.subjects.clone(),
        h.writer.clone(),
        PgConnection::connect(&h.url).await.unwrap(),
        sqlx::postgres::PgPoolOptions::new()
            .max_connections(1)
            .connect_lazy(&h.url)
            .unwrap(),
        h.blobs.clone(),
        "h".into(),
    )
    .unwrap()
}
fn publication(frame: &Ingress) -> Publication {
    let Ingress::Material { material, .. } = frame else {
        panic!("material fixture")
    };
    Publication {
        version: 1,
        frame: frame.frame().into(),
        class: sinex_model::Class::Guarded,
        material: Some(Material::from_frame(material)),
        envelope: None,
    }
}
async fn reserve(ingest: &mut Ingest, source: &str, publication: &Publication) {
    let ack = ingest
        .handle(
            source,
            &serde_json::to_vec(&Ingress::Reserve {
                frame: Uuid::now_v7().to_string(),
                publication: publication.clone(),
            })
            .unwrap(),
        )
        .await
        .unwrap();
    assert!(ack.refusal.is_none(), "{ack:?}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cancelled_install_and_receive_keep_their_owned_source_and_publication_guards_until_work_finishes()
 {
    for point in [Point::BeforeInstall, Point::Received] {
        let h = Harness::start().await;
        let source = "cancel-node";
        let bytes = b"exact pending version before cancellation\n";
        let (id, frame) = h.material_frame("cancel-publication", bytes);
        let p = publication(&frame);
        let mut ingest = receiver(&h).await;
        reserve(&mut ingest, source, &p).await;
        if point == Point::BeforeInstall {
            spool::stage(&h.store_dir, source, &p, Role::Material, bytes.as_slice()).unwrap();
        }
        let (entered, mut observed) = tokio::sync::mpsc::unbounded_channel();
        let (release, wait) = std::sync::mpsc::channel();
        let wait = std::sync::Mutex::new(wait);
        ingest.byte_hook = Some(Arc::new(move |at| {
            if at == point {
                entered.send(()).unwrap();
                wait.lock().unwrap().recv().unwrap();
            }
        }));
        let request = if point == Point::Received {
            Ingress::Chunk {
                frame: "cancel-chunk".into(),
                publication: p.frame.clone(),
                role: Role::Material,
                blob: p.material.as_ref().unwrap().bytes.blob.clone(),
                offset: 0,
                total: bytes.len() as u64,
                data: base64::engine::general_purpose::STANDARD.encode(bytes),
            }
        } else {
            frame.clone()
        };
        let request = serde_json::to_vec(&request).unwrap();
        let task = tokio::spawn(async move { ingest.handle(source, &request).await });
        tokio::time::timeout(Duration::from_secs(3), observed.recv())
            .await
            .unwrap()
            .unwrap();
        task.abort();
        assert!(task.await.unwrap_err().is_cancelled());
        // A real prospective purge cannot acquire this source while detached
        // byte work is still paused. Its lock is acquired outside Writer.
        let mut conn = PgConnection::connect(&h.url).await.unwrap();
        let pid: i32 = sqlx::query_scalar("SELECT pg_backend_pid()")
            .fetch_one(&mut conn)
            .await
            .unwrap();
        let lock = tokio::spawn(async move {
            let mut tx = conn.begin().await.unwrap();
            sinex_store::excision::lock_source(&mut tx, source, false)
                .await
                .unwrap();
            tx.rollback().await.unwrap();
        });
        let mut observer = PgConnection::connect(&h.url).await.unwrap();
        tokio::time::timeout(Duration::from_secs(3),async {
            loop {
                let waiting:bool=sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE pid=$1 AND wait_event_type='Lock')")
                    .bind(pid).fetch_one(&mut observer).await.unwrap();
                if waiting {break;}
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
        }).await.expect("source guard escaped detached byte work");
        assert!(!lock.is_finished());
        release.send(()).unwrap();
        tokio::time::timeout(Duration::from_secs(3), lock)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            h.count("SELECT count(*) FROM raw.pending_producer_bytes")
                .await,
            1
        );
        assert_eq!(h.count("SELECT count(*) FROM raw.materials").await, 0);
        // An edited provider cannot replace the frozen body. A new receiver
        // resumes exactly the durable private role or already installed CAS.
        std::fs::write(
            h.fixture_path(&p.material.as_ref().unwrap().bytes.blob),
            b"provider changed",
        )
        .unwrap();
        let mut restarted = receiver(&h).await;
        let ack = restarted
            .handle(source, &serde_json::to_vec(&frame).unwrap())
            .await
            .unwrap();
        assert!(ack.refusal.is_none(), "{ack:?}");
        assert_eq!(ack.material, Some(id));
        assert_eq!(
            h.blobs
                .read(&p.material.as_ref().unwrap().bytes.blob)
                .unwrap(),
            bytes
        );
        assert_eq!(
            h.count("SELECT count(*) FROM raw.pending_producer_bytes")
                .await,
            0
        );
        let repeat = restarted
            .handle(
                source,
                &serde_json::to_vec(&Ingress::Reserve {
                    frame: "after-lost-ack".into(),
                    publication: p.clone(),
                })
                .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(
            repeat.outcome["complete"],
            serde_json::to_value(&ack).unwrap()
        );
        assert!(!spool::directory(&h.store_dir, source, &p).exists());
    }
}

#[tokio::test]
async fn oversized_material_owns_body_and_private_envelope_under_one_final_frame() {
    let h = Harness::start().await;
    let bytes = b"body independent of oversized manifest\n";
    let (id, mut inner) = h.material_frame("large-inner", bytes);
    if let Ingress::Material { material, .. } = &mut inner {
        material.manifest = json!({"large":"x".repeat(1_100_000),"nested":{"z":0,"a":1}});
    }
    let encoded = sinex_protocol::producer_bytes::encode_frame(&inner);
    let mut p = publication(&inner);
    p.frame = "large-final".into();
    p.envelope = Some(Bytes {
        blob: format!("blake3:{}", blake3::hash(&encoded).to_hex()),
        length: encoded.len() as u64,
    });
    assert!(
        serde_json::to_vec(&Ingress::Reserve {
            frame: "reserve".into(),
            publication: p.clone()
        })
        .unwrap()
        .len()
            < 1024
    );
    let frozen = spool::Frozen {
        publication: p.clone(),
        frame: inner,
    };
    spool::freeze(&h.store_dir, "large-node", &frozen).unwrap();
    spool::stage(
        &h.store_dir,
        "large-node",
        &p,
        Role::Material,
        bytes.as_slice(),
    )
    .unwrap();
    spool::stage(
        &h.store_dir,
        "large-node",
        &p,
        Role::Envelope,
        encoded.as_slice(),
    )
    .unwrap();
    assert!(
        !h.blobs
            .contains(&p.envelope.as_ref().unwrap().blob)
            .unwrap()
    );
    let mut ingest = receiver(&h).await;
    reserve(&mut ingest, "large-node", &p).await;
    let ack = ingest
        .handle(
            "large-node",
            &serde_json::to_vec(&frozen.wire_frame()).unwrap(),
        )
        .await
        .unwrap();
    assert!(ack.refusal.is_none(), "{ack:?}");
    assert_eq!(ack.material, Some(id));
    assert_eq!(
        h.blobs
            .read(&p.material.as_ref().unwrap().bytes.blob)
            .unwrap(),
        bytes
    );
    assert!(
        !h.blobs
            .contains(&p.envelope.as_ref().unwrap().blob)
            .unwrap()
    );
    assert!(!spool::directory(&h.store_dir, "large-node", &p).exists());
    assert_eq!(
        spool::load(&h.store_dir, "large-node", &p.frame).unwrap(),
        Some(frozen)
    );
    // Lost final acknowledgment recovery doesn't need either removed role.
    let recovered = ingest
        .handle(
            "large-node",
            &serde_json::to_vec(&Ingress::Reserve {
                frame: "recover-final".into(),
                publication: p.clone(),
            })
            .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(
        recovered.outcome["complete"],
        serde_json::to_value(&ack).unwrap()
    );
    spool::discard(&h.store_dir, "large-node", &p).unwrap();
}

#[tokio::test]
async fn admission_refusal_keeps_exact_installed_custody_and_missing_reservations_do_not_import_public_files()
 {
    let h = Harness::start().await;
    let bytes = b"exact body surviving first admission refusal\n";
    let (_, frame) = h.material_frame("refused-material", bytes);
    let p = publication(&frame);
    let mut ingest = receiver(&h).await;
    reserve(&mut ingest, "refused-node", &p).await;
    spool::stage(
        &h.store_dir,
        "refused-node",
        &p,
        Role::Material,
        bytes.as_slice(),
    )
    .unwrap();
    let mut conn = PgConnection::connect(&h.url).await.unwrap();
    conn.execute("CREATE FUNCTION public.reject_ingress_material() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'synthetic first material refusal'; END $$; CREATE TRIGGER reject_ingress_material BEFORE INSERT ON raw.materials FOR EACH ROW EXECUTE FUNCTION public.reject_ingress_material()").await.unwrap();
    let ack = ingest
        .handle("refused-node", &serde_json::to_vec(&frame).unwrap())
        .await
        .unwrap();
    assert!(
        ack.refusal
            .unwrap()
            .contains("synthetic first material refusal")
    );
    assert_eq!(
        h.blobs
            .read(&p.material.as_ref().unwrap().bytes.blob)
            .unwrap(),
        bytes
    );
    assert_eq!(h.count("SELECT count(*) FROM raw.materials").await, 0);
    assert_eq!(
        h.count(
            "SELECT count(*) FROM raw.pending_producer_bytes WHERE installed AND NOT acknowledged"
        )
        .await,
        1
    );
    // A legacy direct CAS put cannot silently become a supported new owner.
    let (_, mut unreserved) = h.material_frame("unreserved-public", b"legacy public file");
    h.blobs.put(b"legacy public file").unwrap();
    if let Ingress::Material { material, .. } = &mut unreserved {
        material.input.locator = "legacy".into();
    }
    let refused = ingest
        .handle("legacy-node", &serde_json::to_vec(&unreserved).unwrap())
        .await
        .unwrap();
    assert!(
        refused
            .refusal
            .unwrap()
            .contains("durable byte reservation")
    );
    assert_eq!(h.count("SELECT count(*) FROM raw.materials").await, 0);
}

#[tokio::test]
async fn reserved_missing_bytes_are_unavailable_then_resume_the_same_material_without_a_false_ack()
{
    let h = Harness::start().await;
    let bytes = b"frozen body arrives after receiver restart\n";
    let (id, frame) = h.material_frame("missing-frozen", bytes);
    let p = publication(&frame);
    let mut ingest = receiver(&h).await;
    reserve(&mut ingest, "missing-node", &p).await;
    assert!(
        ingest
            .handle("missing-node", &serde_json::to_vec(&frame).unwrap())
            .await
            .unwrap_err()
            .contains("unavailable")
    );
    assert_eq!(h.count("SELECT count(*) FROM raw.pending_producer_bytes WHERE NOT installed AND NOT acknowledged").await,1);
    assert_eq!(h.count("SELECT count(*) FROM raw.materials").await, 0);
    assert!(h.have("missing-node", &["missing-frozen"]).await.is_empty());
    drop(ingest);
    let mut restarted = receiver(&h).await;
    spool::stage(
        &h.store_dir,
        "missing-node",
        &p,
        Role::Material,
        bytes.as_slice(),
    )
    .unwrap();
    let ack = restarted
        .handle("missing-node", &serde_json::to_vec(&frame).unwrap())
        .await
        .unwrap();
    assert!(ack.refusal.is_none());
    assert_eq!(ack.material, Some(id));
    assert_eq!(
        h.blobs
            .read(&p.material.as_ref().unwrap().bytes.blob)
            .unwrap(),
        bytes
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cancelled_queued_material_admission_keeps_guard_until_writer_commits_and_restart_recovers_original_ack()
 {
    let h = Harness::start().await;
    let source = "queued-node";
    let bytes = b"exact queued version\n";
    let (id, frame) = h.material_frame("queued-final", bytes);
    let p = publication(&frame);
    let mut ingest = receiver(&h).await;
    reserve(&mut ingest, source, &p).await;
    spool::stage(&h.store_dir, source, &p, Role::Material, bytes.as_slice()).unwrap();
    let mut blocker = PgConnection::connect(&h.url).await.unwrap();
    let mut blocker = blocker.begin().await.unwrap();
    blocker
        .execute("LOCK TABLE raw.materials IN EXCLUSIVE MODE")
        .await
        .unwrap();
    let frame_bytes = serde_json::to_vec(&frame).unwrap();
    let task = tokio::spawn(async move { ingest.handle(source, &frame_bytes).await });
    let mut observer = PgConnection::connect(&h.url).await.unwrap();
    tokio::time::timeout(Duration::from_secs(3),async {
        loop {
            let installed:bool=sqlx::query_scalar("SELECT installed FROM raw.pending_producer_bytes WHERE producer=$1 AND frame=$2")
                .bind(source).bind(&p.frame).fetch_one(&mut observer).await.unwrap();
            let queued:bool=sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE datname=current_database() AND wait_event_type='Lock' AND query LIKE '%INSERT INTO raw.materials%')")
                .fetch_one(&mut observer).await.unwrap();
            if installed && queued {break;}
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    }).await.expect("Writer did not reach queued material admission");
    task.abort();
    assert!(task.await.unwrap_err().is_cancelled());
    // The running Writer still owns the source guard despite its receiver being gone.
    let mut lock_conn = PgConnection::connect(&h.url).await.unwrap();
    lock_conn.execute("SET lock_timeout='50ms'").await.unwrap();
    let mut lock_tx = lock_conn.begin().await.unwrap();
    assert!(
        sinex_store::excision::lock_source(&mut lock_tx, source, false)
            .await
            .is_err()
    );
    lock_tx.rollback().await.unwrap();
    blocker.rollback().await.unwrap();
    tokio::time::timeout(Duration::from_secs(3),async {
        loop {
            let acked:bool=sqlx::query_scalar("SELECT acknowledged FROM raw.pending_producer_bytes WHERE producer=$1 AND frame=$2")
                .bind(source).bind(&p.frame).fetch_one(&mut observer).await.unwrap();
            if acked {break;}
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    }).await.expect("canceled caller prevented queued commit");
    assert_eq!(h.count("SELECT count(*) FROM raw.materials").await, 1);
    let mut restarted = receiver(&h).await;
    let recovered = restarted
        .handle(
            source,
            &serde_json::to_vec(&Ingress::Reserve {
                frame: "queued-recovery".into(),
                publication: p.clone(),
            })
            .unwrap(),
        )
        .await
        .unwrap();
    let original: Acknowledgment =
        serde_json::from_value(recovered.outcome["complete"].clone()).unwrap();
    assert_eq!(original.material, Some(id));
    assert!(original.refusal.is_none());
    assert_eq!(
        h.blobs
            .read(&p.material.as_ref().unwrap().bytes.blob)
            .unwrap(),
        bytes
    );
    assert_eq!(
        h.count("SELECT count(*) FROM raw.pending_producer_bytes")
            .await,
        0
    );
}

#[tokio::test]
async fn receiver_refuses_corrupt_public_dedup_without_admitting_or_replacing_frozen_source_bytes()
{
    let h = Harness::start().await;
    let bytes = b"original provider bytes survive corrupt destination\n";
    let (_, frame) = h.material_frame("corrupt-final", bytes);
    let p = publication(&frame);
    let mut ingest = receiver(&h).await;
    reserve(&mut ingest, "corrupt-node", &p).await;
    spool::stage(
        &h.store_dir,
        "corrupt-node",
        &p,
        Role::Material,
        bytes.as_slice(),
    )
    .unwrap();
    let name = h.blobs.put(bytes).unwrap();
    let hex = name.strip_prefix("blake3:").unwrap();
    let path = h
        .store_dir
        .join("blobs")
        .join(&hex[..2])
        .join(format!("{hex}.zst"));
    std::fs::write(&path, b"corrupt existing representation").unwrap();
    assert!(
        ingest
            .handle("corrupt-node", &serde_json::to_vec(&frame).unwrap())
            .await
            .is_err()
    );
    assert_eq!(
        std::fs::read(path).unwrap(),
        b"corrupt existing representation"
    );
    let mut original = Vec::new();
    use std::io::Read;
    spool::snapshot(&h.store_dir, "corrupt-node", &p, Role::Material)
        .unwrap()
        .read_to_end(&mut original)
        .unwrap();
    assert_eq!(original, bytes);
    assert_eq!(h.count("SELECT count(*) FROM raw.materials").await, 0);
    assert!(h.have("corrupt-node", &["corrupt-final"]).await.is_empty());
    assert_eq!(
        h.count("SELECT count(*) FROM raw.pending_producer_bytes WHERE NOT acknowledged")
            .await,
        1
    );
}

#[tokio::test]
async fn inline_and_indirect_event_frames_keep_the_original_payload_order_and_reading_hash() {
    let h = Harness::start().await;
    let (one, m1) = h.material_frame("inline-material", b"same record\n");
    let (two, m2) = h.material_frame("indirect-material", b"same record\n");
    assert!(h.send("inline-node", &m1).await.refusal.is_none());
    assert!(h.send("indirect-node", &m2).await.refusal.is_none());
    let mut event = imported(one, 0, "ls");
    event.payload=serde_json::from_str(r#"{"started_at":"2026-10-18T12:00:00Z","n":0,"hostname":"laptop","command":"ls","header_elapsed":0}"#).unwrap();
    let expected = record_hash(&event.event_type, &event.payload);
    let inline = Ingress::Events {
        frame: "inline-events".into(),
        events: vec![event.clone()],
        failures: vec![],
    };
    assert_eq!(h.send("inline-node", &inline).await.admitted.len(), 1);
    event.location = Some(RecordLocation {
        material: two,
        anchor: 0,
    });
    let inner = Ingress::Events {
        frame: "ignored-inner".into(),
        events: vec![event],
        failures: vec![],
    };
    let bytes = sinex_protocol::producer_bytes::encode_frame(&inner);
    let p = Publication {
        version: 1,
        frame: "indirect-events".into(),
        class: sinex_model::Class::Guarded,
        material: None,
        envelope: Some(Bytes {
            blob: format!("blake3:{}", blake3::hash(&bytes).to_hex()),
            length: bytes.len() as u64,
        }),
    };
    let mut ingest = receiver(&h).await;
    reserve(&mut ingest, "indirect-node", &p).await;
    spool::stage(
        &h.store_dir,
        "indirect-node",
        &p,
        Role::Envelope,
        bytes.as_slice(),
    )
    .unwrap();
    let ack = ingest
        .handle(
            "indirect-node",
            &serde_json::to_vec(&Ingress::Indirect {
                frame: p.frame.clone(),
                blob: p.envelope.as_ref().unwrap().blob.clone(),
            })
            .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(ack.admitted.len(), 1);
    let mut conn = PgConnection::connect(&h.url).await.unwrap();
    let hashes:Vec<Vec<u8>>=sqlx::query_scalar("SELECT record_hash FROM core.events WHERE source IN ('inline-node','indirect-node') ORDER BY source")
        .fetch_all(&mut conn).await.unwrap();
    assert_eq!(hashes, vec![expected.to_vec(), expected.to_vec()]);
}

#[tokio::test]
async fn colliding_material_identity_refuses_another_source_body_and_preserves_genuine_repeat() {
    let h = Harness::start().await;
    let original = b"original source body\n";
    let replacement = b"other producer frozen body\n";
    let (id, frame) = h.material_frame("owner-final", original);
    let original_blob = publication(&frame).material.unwrap().bytes.blob;
    assert_eq!(h.send("original-node", &frame).await.material, Some(id));
    let (_, mut collision) = h.material_frame("collision-final", replacement);
    if let Ingress::Material { material, .. } = &mut collision {
        material.id = id;
    }
    let pending = publication(&collision);
    let mut ingest = receiver(&h).await;
    reserve(&mut ingest, "other-node", &pending).await;
    spool::stage(
        &h.store_dir,
        "other-node",
        &pending,
        Role::Material,
        replacement.as_slice(),
    )
    .unwrap();
    let refused = ingest
        .handle("other-node", &serde_json::to_vec(&collision).unwrap())
        .await
        .unwrap();
    assert!(
        refused
            .refusal
            .as_deref()
            .is_some_and(|r| r.contains("different retained facts")),
        "{refused:?}"
    );
    let mut conn = PgConnection::connect(&h.url).await.unwrap();
    assert_eq!(
        sqlx::query_scalar::<_, String>("SELECT blob FROM raw.materials WHERE id=$1")
            .bind(id.0)
            .fetch_one(&mut conn)
            .await
            .unwrap(),
        original_blob
    );
    assert_eq!(h.blobs.read(&original_blob).unwrap(), original);
    assert_eq!(
        h.blobs
            .read(&pending.material.as_ref().unwrap().bytes.blob)
            .unwrap(),
        replacement
    );
    assert_eq!(h.count("SELECT count(*) FROM raw.pending_producer_bytes WHERE producer='other-node' AND installed AND NOT acknowledged").await,1);
    let mut repeat = frame.clone();
    if let Ingress::Material { frame, .. } = &mut repeat {
        *frame = "same-material-repeat".into();
    }
    let ack = h.send("original-node", &repeat).await;
    assert!(ack.refusal.is_none(), "{ack:?}");
    assert_eq!(ack.material, Some(id));
    assert_eq!(h.count("SELECT count(*) FROM raw.materials").await, 1);
}
