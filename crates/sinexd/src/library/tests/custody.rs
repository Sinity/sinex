use super::*;
use sinex_model::event_types::library::{LibraryRecord, Rendition, RenditionRole};

fn preview(content: String) -> LibraryRecord {
    LibraryRecord::Rendition(Rendition {
        rendition: "synthetic-preview".into(),
        item: "synthetic-item".into(),
        revision: 1,
        role: RenditionRole::Preview,
        content,
        media_type: "image/webp".into(),
        width: Some(1),
        height: Some(1),
        duration_ms: None,
        made_from: None,
        made_with: Default::default(),
        by: String::new(),
        at: sinex_model::Timestamp::from_unix_secs(1).unwrap(),
        withdrawn_at: None,
    })
}

#[tokio::test]
async fn preview_publication_retains_its_exact_ready_media_owner() {
    let mut w = world().await;
    let bytes = b"synthetic prepared preview bytes";
    let stage = w.blobs.prepare_plain(bytes.as_slice()).unwrap();
    let name = stage.name();
    assert!(!w.blobs.contains(&name).unwrap());
    let library = Library {
        writer: &w.writer,
        blobs: &w.blobs,
        host: "h",
    };
    library
        .write_staged(
            vec![preview(name.clone())],
            "operator",
            "preview",
            vec![stage],
        )
        .await
        .unwrap();
    assert!(
        w.blobs.contains(&name).unwrap(),
        "preview success discarded its staged media"
    );
    let owners: i64 = sqlx::query_scalar("SELECT count(*) FROM core.event_blobs WHERE blob=$1")
        .bind(&name)
        .fetch_one(&mut w.conn)
        .await
        .unwrap();
    assert_eq!(
        owners, 1,
        "first admission discarded the preview's final byte owner"
    );
    assert_eq!(w.blobs.read(&name).unwrap(), bytes);
    for restart in [false, true] {
        if restart {
            w = w.restart().await;
        }
        let report = crate::replay::run(
            &mut w.conn,
            &w.blobs,
            &w.writer,
            crate::replay::Options {
                scope: crate::replay::Scope {
                    binding: Some("library.request:operator@h".into()),
                    ..Default::default()
                },
                compare: false,
                resume: None,
                actor: "synthetic".into(),
                authority: None,
            },
        )
        .await
        .unwrap();
        assert_eq!(report.admitted, 1);
        let live: i64 = sqlx::query_scalar("SELECT count(*) FROM core.event_blobs b JOIN core.live_equivalence l ON l.event_id=b.event_id WHERE b.blob=$1")
            .bind(&name).fetch_one(&mut w.conn).await.unwrap();
        assert_eq!(live, 1, "replay lost the current rendition citation");
        sqlx::query("UPDATE core.blobs SET status='deleting' WHERE hash=$1")
            .bind(&name)
            .execute(&mut w.conn)
            .await
            .unwrap();
        let (removed, errors) = sinex_store::excision::finish_deletions(&mut w.conn, &w.blobs)
            .await
            .unwrap();
        assert!(removed.is_empty() && errors.is_empty());
        assert_eq!(w.blobs.read(&name).unwrap(), bytes);
    }
}

#[tokio::test]
async fn failed_library_preview_retains_media_custody_and_recovers_prior_request_separately_after_restart()
 {
    use sqlx::Executor;
    let mut w = world().await;
    let original = b"original synthetic preview";
    let stage = w.blobs.prepare_plain(original.as_slice()).unwrap();
    let name = stage.name();
    w.conn.execute("CREATE FUNCTION public.refuse_authored_position() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'synthetic authored refusal'; END $$; CREATE TRIGGER refuse_authored_position BEFORE INSERT ON raw.positions FOR EACH ROW EXECUTE FUNCTION public.refuse_authored_position()").await.unwrap();
    let result = Library {
        writer: &w.writer,
        blobs: &w.blobs,
        host: "h",
    }
    .write_staged(
        vec![preview(name.clone())],
        "operator",
        "preview",
        vec![stage],
    )
    .await;
    assert!(result.unwrap_err().contains("synthetic authored refusal"));
    let ids =
        sinex_store::native_materials::binding_pending(&mut w.conn, "library.request:operator@h")
            .await
            .unwrap();
    assert_eq!(ids.len(), 1);
    let pending = sinex_store::native_materials::load(&mut w.conn, ids[0])
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        pending.cited,
        vec![sinex_model::Cited {
            blob: name.clone(),
            size: original.len() as u64
        }]
    );
    let primary = pending.progress.material.as_ref().unwrap();
    let primary_bytes = w.blobs.read(&primary.blob).unwrap();
    assert!(
        sinex_store::material_bytes::reference(&mut w.conn, pending.material)
            .await
            .unwrap()
            .is_none()
    );
    sqlx::query("UPDATE core.blobs SET status='deleting' WHERE hash=ANY($1)")
        .bind(vec![name.clone(), primary.blob.clone()])
        .execute(&mut w.conn)
        .await
        .unwrap();
    let (deleted, errors) = sinex_store::excision::finish_deletions(&mut w.conn, &w.blobs)
        .await
        .unwrap();
    assert!(deleted.is_empty() && errors.is_empty());
    assert_eq!(w.blobs.read(&name).unwrap(), original);
    assert_eq!(w.blobs.read(&primary.blob).unwrap(), primary_bytes);
    w.conn.execute("DROP TRIGGER refuse_authored_position ON raw.positions; DROP FUNCTION public.refuse_authored_position()").await.unwrap();
    w = w.restart().await;
    let fresh = b"fresh synthetic preview";
    let next = w.blobs.prepare_plain(fresh.as_slice()).unwrap();
    let fresh_name = next.name();
    let result = Library {
        writer: &w.writer,
        blobs: &w.blobs,
        host: "h",
    }
    .write_staged(
        vec![preview(fresh_name.clone())],
        "operator",
        "preview",
        vec![next],
    )
    .await
    .unwrap();
    assert_eq!(result["recovered_prior"], json!([pending.material]));
    assert_ne!(
        result["material"],
        json!(pending.material),
        "fresh request received the prior request identity"
    );
    assert_eq!(result["admitted"], 1);
    assert!(
        sinex_store::native_materials::binding_pending(&mut w.conn, "library.request:operator@h")
            .await
            .unwrap()
            .is_empty()
    );
    for (hash, bytes) in [
        (&name, original.as_slice()),
        (&fresh_name, fresh.as_slice()),
    ] {
        assert_eq!(w.blobs.read(hash).unwrap(), bytes);
        let owners: i64 = sqlx::query_scalar("SELECT count(*) FROM core.event_blobs WHERE blob=$1")
            .bind(hash)
            .fetch_one(&mut w.conn)
            .await
            .unwrap();
        assert_eq!(owners, 1);
    }
    let retained = sinex_store::material_bytes::reference(&mut w.conn, pending.material)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(retained.blob, primary.blob);
    assert_eq!(w.blobs.read(&retained.blob).unwrap(), primary_bytes);
}

#[tokio::test]
async fn library_media_publication_survives_both_cleanup_orders_and_canceled_callers() {
    use sqlx::{Connection, Executor};
    for reservation_first in [false, true] {
        let mut w = world().await;
        let bytes = b"original staged scan media";
        let path = w.root.join("evidence.dat");
        std::fs::write(&path, bytes).unwrap();
        let name = w.blobs.put(bytes).unwrap();
        sqlx::query("INSERT INTO core.blobs(hash,size,status) VALUES($1,$2,'deleting')")
            .bind(&name)
            .bind(bytes.len() as i64)
            .execute(&mut w.conn)
            .await
            .unwrap();
        sqlx::query("SELECT pg_advisory_lock(94711)")
            .execute(&mut w.conn)
            .await
            .unwrap();
        w.conn.execute("CREATE FUNCTION public.pause_authored_publication() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN PERFORM pg_advisory_xact_lock(94711); RETURN NEW; END $$").await.unwrap();
        let trigger = if reservation_first {
            format!(
                "CREATE TRIGGER pause_authored_publication AFTER INSERT ON raw.pending_native_blobs FOR EACH ROW WHEN (NEW.blob='{name}') EXECUTE FUNCTION public.pause_authored_publication()"
            )
        } else {
            "CREATE TRIGGER pause_authored_publication BEFORE INSERT ON raw.pending_native_materials FOR EACH ROW EXECUTE FUNCTION public.pause_authored_publication()".into()
        };
        w.conn.execute(trigger.as_str()).await.unwrap();
        let url = w.url.clone();
        let writer = w.writer.clone();
        let blobs = w.blobs.clone();
        let input = path.display().to_string();
        let caller = tokio::spawn(async move {
            let mut conn = PgConnection::connect(&url).await.unwrap();
            dispatch(
                &mut conn,
                Some(&writer),
                &blobs,
                "h",
                Request::LibraryAdd {
                    path: input,
                    reference: None,
                },
                "operator",
            )
            .await
        });
        tokio::time::timeout(std::time::Duration::from_secs(5), async {
            loop {
                let blocked: bool = sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM pg_locks WHERE locktype='advisory' AND objid=94711 AND NOT granted AND database=(SELECT oid FROM pg_database WHERE datname=current_database()))")
                    .fetch_one(&mut w.conn).await.unwrap();
                if blocked { break; }
                tokio::task::yield_now().await;
            }
        }).await.unwrap();
        let mut clean_conn = PgConnection::connect(&w.url).await.unwrap();
        let clean_blobs = w.blobs.clone();
        let cleanup = tokio::spawn(async move {
            sinex_store::excision::finish_deletions(&mut clean_conn, &clean_blobs)
                .await
                .unwrap()
        });
        let cleanup = if reservation_first {
            tokio::time::timeout(std::time::Duration::from_secs(5), async {
                loop {
                    let blocked: bool = sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE datname=current_database() AND wait_event_type='Lock' AND query LIKE '%core.blobs%' AND pid<>pg_backend_pid())")
                        .fetch_one(&mut w.conn).await.unwrap();
                    if blocked {break;}
                    assert!(!cleanup.is_finished(), "cleanup did not wait for the reserved blob row");
                    tokio::task::yield_now().await;
                }
            }).await.unwrap();
            Some(cleanup)
        } else {
            let (removed, errors) = cleanup.await.unwrap();
            assert_eq!(removed, vec![name.clone()]);
            assert!(errors.is_empty());
            assert!(
                !w.blobs.contains(&name).unwrap(),
                "cleanup must win before reservation in this order"
            );
            None
        };
        // Cancellation cannot drop the owned publication's private stage or guard.
        caller.abort();
        assert!(caller.await.unwrap_err().is_cancelled());
        std::fs::write(&path, b"provider edited while publication was paused").unwrap();
        sqlx::query("SELECT pg_advisory_unlock(94711)")
            .execute(&mut w.conn)
            .await
            .unwrap();
        if let Some(cleanup) = cleanup {
            let (removed, errors) = cleanup.await.unwrap();
            assert!(removed.is_empty() && errors.is_empty());
        }
        tokio::time::timeout(std::time::Duration::from_secs(5), async {
            loop {
                let admitted: bool = sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM core.events WHERE event_type='library.acquisition') AND NOT EXISTS(SELECT 1 FROM raw.pending_native_materials)")
                    .fetch_one(&mut w.conn).await.unwrap();
                if admitted {break;}
                tokio::task::yield_now().await;
            }
        }).await.unwrap();
        assert_eq!(w.blobs.read(&name).unwrap(), bytes);
        let captured: String = sqlx::query_scalar(
            "SELECT payload->>'content' FROM core.events WHERE event_type='library.acquisition'",
        )
        .fetch_one(&mut w.conn)
        .await
        .unwrap();
        assert_eq!(
            captured, name,
            "edited provider replaced the frozen staged media"
        );
        let owners: i64 = sqlx::query_scalar("SELECT count(*) FROM core.event_blobs WHERE blob=$1")
            .bind(&name)
            .fetch_one(&mut w.conn)
            .await
            .unwrap();
        assert_eq!(owners, 1);
    }
}
