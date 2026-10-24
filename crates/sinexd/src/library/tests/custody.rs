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

#[tokio::test]
async fn unsupported_authored_evidence_refuses_before_installing_body_or_media() {
    let mut w = world().await;
    for kind in ["size", "order", "unavailable"] {
        let bytes = format!("synthetic refused media {kind}");
        let stage = w.blobs.prepare_plain(bytes.as_bytes()).unwrap();
        let name = stage.name();
        let mut acquired = sinex_sources::sources::library::request_material(
            &[preview(name.clone())],
            "operator",
            "h",
            "preview",
        );
        let material = acquired.material.as_mut().unwrap();
        material.manifest["cited_blobs"] =
            json!([{"blob":name, "size":bytes.len()+usize::from(kind=="size")}]);
        if kind == "order" {
            material.reading_order = Box::new(|_| sinex_model::ReadingOrder(-1));
        }
        if kind == "unavailable" {
            acquired.observation.unavailable = Some("synthetic unavailable provider".into());
        }
        let result = crate::publication::submit_authored(
            &w.writer,
            &w.blobs,
            Arc::new(sinex_sources::sources::library::request_source()),
            "library.request:operator@h",
            acquired,
            vec![stage],
        )
        .await;
        assert!(result.is_err(), "accepted {kind}");
        assert!(
            !w.blobs.contains(&name).unwrap(),
            "refused evidence installed media"
        );
        let owners: i64 = sqlx::query_scalar("SELECT count(*) FROM raw.pending_native_materials")
            .fetch_one(&mut w.conn)
            .await
            .unwrap();
        let materials: i64 = sqlx::query_scalar("SELECT count(*) FROM raw.materials")
            .fetch_one(&mut w.conn)
            .await
            .unwrap();
        assert_eq!((owners, materials), (0, 0));
    }
}

#[tokio::test]
async fn unrelated_authored_requests_continue_after_excision_while_matching_requests_refuse() {
    use sinex_model::event_types::library::Collection;
    let mut w = world().await;
    let record = |id: &str| {
        LibraryRecord::Collection(Collection {
            collection: id.into(),
            name: format!("synthetic {id}"),
            saved_query: None,
            frozen_from: None,
            by: String::new(),
            at: sinex_model::Timestamp::from_unix_secs(1).unwrap(),
            withdrawn_at: None,
        })
    };
    let selected = record("selected");
    Library {
        writer: &w.writer,
        blobs: &w.blobs,
        host: "h",
    }
    .write(vec![selected.clone()], "operator", "collection")
    .await
    .unwrap();
    let id: uuid::Uuid =
        sqlx::query_scalar("SELECT id FROM core.events WHERE event_type='library.collection'")
            .fetch_one(&mut w.conn)
            .await
            .unwrap();
    crate::excision::excise_with(
        &mut w.conn,
        &w.blobs,
        &w.writer,
        sinex_store::excision::Target::Event(sinex_model::EventId(id)),
        "operator",
    )
    .await
    .unwrap();
    let fresh = Library {
        writer: &w.writer,
        blobs: &w.blobs,
        host: "h",
    }
    .write(vec![record("unrelated")], "operator", "collection")
    .await
    .unwrap();
    assert_eq!(fresh["admitted"], 1);
    let refused = Library {
        writer: &w.writer,
        blobs: &w.blobs,
        host: "h",
    }
    .write(vec![selected], "operator", "collection")
    .await
    .unwrap_err();
    assert!(refused.contains("permanent exclusion"), "{refused}");
    let live: Vec<String> = sqlx::query_scalar("SELECT e.payload->>'collection' FROM core.events e JOIN core.live_equivalence l ON l.event_id=e.id WHERE e.event_type='library.collection'").fetch_all(&mut w.conn).await.unwrap();
    assert_eq!(live, vec!["unrelated"]);
    let pending: i64 = sqlx::query_scalar("SELECT count(*) FROM raw.pending_native_materials")
        .fetch_one(&mut w.conn)
        .await
        .unwrap();
    assert_eq!(pending, 0);
}

fn named_preview(id: &str, content: String) -> LibraryRecord {
    let mut record = preview(content);
    if let LibraryRecord::Rendition(rendition) = &mut record {
        rendition.rendition = id.into();
    }
    record
}

#[tokio::test]
async fn source_bound_media_exclusions_refuse_new_wrappers_but_preserve_unrelated_and_other_source_bytes()
 {
    let mut w = world().await;
    let removed = b"synthetic media selected by a request";
    let stage = w.blobs.prepare_plain(removed.as_slice()).unwrap();
    let name = stage.name();
    Library {
        writer: &w.writer,
        blobs: &w.blobs,
        host: "h",
    }
    .write_staged(
        vec![named_preview("selected", name.clone())],
        "operator",
        "preview",
        vec![stage],
    )
    .await
    .unwrap();
    // An unrelated source legitimately owns the same CAS bytes.
    let path = w.root.join("other-source.dat");
    std::fs::write(&path, removed).unwrap();
    w.ok(Request::LibraryAdd {
        path: path.display().to_string(),
        reference: None,
    })
    .await;
    let id: uuid::Uuid = sqlx::query_scalar("SELECT id FROM core.events WHERE event_type='library.rendition' AND payload->>'rendition'='selected'").fetch_one(&mut w.conn).await.unwrap();
    crate::excision::excise_with(
        &mut w.conn,
        &w.blobs,
        &w.writer,
        sinex_store::excision::Target::Event(sinex_model::EventId(id)),
        "operator",
    )
    .await
    .unwrap();
    assert_eq!(w.blobs.read(&name).unwrap(), removed);
    let stage = w.blobs.prepare_plain(removed.as_slice()).unwrap();
    let refused = Library {
        writer: &w.writer,
        blobs: &w.blobs,
        host: "h",
    }
    .write_staged(
        vec![named_preview("fresh-wrapper", name.clone())],
        "operator",
        "preview",
        vec![stage],
    )
    .await
    .unwrap_err();
    assert!(
        refused.contains("media matches a permanent exclusion"),
        "{refused}"
    );
    let fresh = b"unrelated new preview bytes";
    let stage = w.blobs.prepare_plain(fresh.as_slice()).unwrap();
    let fresh_name = stage.name();
    let accepted = Library {
        writer: &w.writer,
        blobs: &w.blobs,
        host: "h",
    }
    .write_staged(
        vec![named_preview("fresh-unrelated", fresh_name.clone())],
        "operator",
        "preview",
        vec![stage],
    )
    .await
    .unwrap();
    assert_eq!(accepted["admitted"], 1);
    let policy = crate::source_excision::AcquisitionPolicy::load(
        &mut w.conn,
        &w.blobs,
        "library.request",
        None,
    )
    .await
    .unwrap();
    assert!(
        policy
            .check_cited(&[sinex_model::Cited {
                blob: name.clone(),
                size: removed.len() as u64 + 1
            }])
            .is_err(),
        "changed size bypassed source+hash exclusion"
    );
    let other = crate::source_excision::AcquisitionPolicy::load(
        &mut w.conn,
        &w.blobs,
        "library.scan",
        None,
    )
    .await
    .unwrap();
    other
        .check_cited(&[sinex_model::Cited {
            blob: name.clone(),
            size: removed.len() as u64,
        }])
        .unwrap();
    let owners: i64 = sqlx::query_scalar("SELECT count(*) FROM core.event_blobs b JOIN core.events e ON e.id=b.event_id WHERE b.blob=$1 AND e.source='library.scan'").bind(&name).fetch_one(&mut w.conn).await.unwrap();
    assert_eq!(owners, 1);
}

#[tokio::test]
async fn complete_empty_media_facts_allow_new_media_but_missing_history_does_not_guess() {
    use sinex_model::event_types::library::Collection;
    let mut w = world().await;
    let record = LibraryRecord::Collection(Collection {
        collection: "no-media".into(),
        name: "synthetic collection".into(),
        saved_query: None,
        frozen_from: None,
        by: String::new(),
        at: sinex_model::Timestamp::now(),
        withdrawn_at: None,
    });
    Library {
        writer: &w.writer,
        blobs: &w.blobs,
        host: "h",
    }
    .write(vec![record], "operator", "collection")
    .await
    .unwrap();
    let id: uuid::Uuid =
        sqlx::query_scalar("SELECT id FROM core.events WHERE event_type='library.collection'")
            .fetch_one(&mut w.conn)
            .await
            .unwrap();
    crate::excision::excise_with(
        &mut w.conn,
        &w.blobs,
        &w.writer,
        sinex_store::excision::Target::Event(sinex_model::EventId(id)),
        "operator",
    )
    .await
    .unwrap();
    let scope: Value = sqlx::query_scalar("SELECT scope FROM core.operations WHERE kind='excise'")
        .fetch_one(&mut w.conn)
        .await
        .unwrap();
    assert_eq!(
        sinex_store::excision::cited_media(&scope, "library.request").unwrap(),
        Some(Vec::new())
    );
    let stage = w
        .blobs
        .prepare_plain(b"new known media".as_slice())
        .unwrap();
    let name = stage.name();
    Library {
        writer: &w.writer,
        blobs: &w.blobs,
        host: "h",
    }
    .write_staged(
        vec![named_preview("new-media", name)],
        "operator",
        "preview",
        vec![stage],
    )
    .await
    .unwrap();
    // A synthetic historical operation lacks media evidence; no old row is rewritten.
    let mut historical = scope;
    historical.as_object_mut().unwrap().remove("cited_media");
    sqlx::query("INSERT INTO core.operations(id,kind,actor,scope) VALUES($1,'excise','synthetic-legacy',$2)").bind(uuid::Uuid::now_v7()).bind(historical).execute(&mut w.conn).await.unwrap();
    let policy = crate::source_excision::AcquisitionPolicy::load(
        &mut w.conn,
        &w.blobs,
        "library.request",
        None,
    )
    .await
    .unwrap();
    policy.check_cited(&[]).unwrap();
    let stage = w
        .blobs
        .prepare_plain(b"unproven after legacy".as_slice())
        .unwrap();
    let hash = stage.name();
    let refused = Library {
        writer: &w.writer,
        blobs: &w.blobs,
        host: "h",
    }
    .write_staged(
        vec![named_preview("unknown-media", hash.clone())],
        "operator",
        "preview",
        vec![stage],
    )
    .await
    .unwrap_err();
    assert!(refused.contains("historical cited media"), "{refused}");
    assert!(!w.blobs.contains(&hash).unwrap());
}

#[tokio::test]
async fn canceled_media_purge_rolls_back_facts_and_preserves_ready_bytes_and_policy() {
    use sqlx::Connection;
    use std::sync::atomic::{AtomicUsize, Ordering};
    let mut w = world().await;
    let bytes = b"synthetic retained media through rollback";
    let stage = w.blobs.prepare_plain(bytes.as_slice()).unwrap();
    let name = stage.name();
    Library {
        writer: &w.writer,
        blobs: &w.blobs,
        host: "h",
    }
    .write_staged(
        vec![named_preview("cancel-target", name.clone())],
        "operator",
        "preview",
        vec![stage],
    )
    .await
    .unwrap();
    let id: uuid::Uuid =
        sqlx::query_scalar("SELECT id FROM core.events WHERE event_type='library.rendition'")
            .fetch_one(&mut w.conn)
            .await
            .unwrap();
    let target = sinex_store::excision::Target::Event(sinex_model::EventId(id));
    let guard = w
        .writer
        .excision_guard("library.request".into())
        .await
        .unwrap();
    let plan = sinex_store::excision::plan(&mut w.conn, target)
        .await
        .unwrap();
    let mut rewrites = Vec::new();
    for material in &plan.materials {
        let mut raw = w.blobs.read(&material.blob).unwrap();
        for byte in &mut raw {
            if *byte != b'\n' {
                *byte = b' ';
            }
        }
        rewrites.push(sinex_store::excision::Rewrite {
            material: material.id,
            blob: w.blobs.put(&raw).unwrap(),
            length: raw.len() as u64,
            manifest: None,
        });
    }
    let operation = sinex_model::OperationId::new();
    let checks = AtomicUsize::new(0);
    let check = || {
        if checks.fetch_add(1, Ordering::SeqCst) == 0 {
            Ok(())
        } else {
            Err(sinex_store::Error::Invalid(
                "synthetic cancellation after media facts".into(),
            ))
        }
    };
    let mut tx = w.conn.begin().await.unwrap();
    let error = sinex_store::excision::apply_prepared(
        &mut tx,
        target,
        &plan,
        &rewrites,
        operation,
        "operator",
        &sinex_store::NoDerivations,
        None,
        &check,
    )
    .await
    .unwrap_err();
    assert!(error.to_string().contains("synthetic cancellation"));
    let staged: Value = sqlx::query_scalar("SELECT scope FROM core.operations WHERE id=$1")
        .bind(operation.0)
        .fetch_one(&mut *tx)
        .await
        .unwrap();
    assert_eq!(
        sinex_store::excision::cited_media(&staged, "library.request").unwrap(),
        Some(vec![sinex_model::Cited {
            blob: name.clone(),
            size: bytes.len() as u64
        }])
    );
    tx.rollback().await.unwrap();
    guard.release().await.unwrap();
    let absent: bool =
        sqlx::query_scalar("SELECT NOT EXISTS(SELECT 1 FROM core.operations WHERE id=$1)")
            .bind(operation.0)
            .fetch_one(&mut w.conn)
            .await
            .unwrap();
    assert!(absent);
    let policy = crate::source_excision::AcquisitionPolicy::load(
        &mut w.conn,
        &w.blobs,
        "library.request",
        None,
    )
    .await
    .unwrap();
    policy
        .check_cited(&[sinex_model::Cited {
            blob: name.clone(),
            size: bytes.len() as u64,
        }])
        .unwrap();
    assert_eq!(w.blobs.read(&name).unwrap(), bytes);
    let owners: i64 = sqlx::query_scalar("SELECT count(*) FROM core.event_blobs WHERE blob=$1")
        .bind(&name)
        .fetch_one(&mut w.conn)
        .await
        .unwrap();
    assert_eq!(owners, 1);
}

#[tokio::test]
async fn a_legacy_repeat_without_media_ownership_cannot_discard_new_pending_custody() {
    let mut w = world().await;
    let bytes = b"legacy rendition bytes supplied by a fresh request";
    let name = format!("blake3:{}", blake3::hash(bytes));
    let record = preview(name.clone());
    Library {
        writer: &w.writer,
        blobs: &w.blobs,
        host: "h",
    }
    .write(vec![record.clone()], "operator", "legacy")
    .await
    .unwrap();
    let before: Value =
        sqlx::query_scalar("SELECT payload FROM core.events WHERE event_type='library.rendition'")
            .fetch_one(&mut w.conn)
            .await
            .unwrap();
    let stage = w.blobs.prepare_plain(bytes.as_slice()).unwrap();
    let result = Library {
        writer: &w.writer,
        blobs: &w.blobs,
        host: "h",
    }
    .write_staged(vec![record], "operator", "preview", vec![stage])
    .await;
    assert!(result.unwrap_err().contains("ready ownership"));
    assert_eq!(w.blobs.read(&name).unwrap(), bytes);
    let current: Value =
        sqlx::query_scalar("SELECT payload FROM core.events WHERE event_type='library.rendition'")
            .fetch_one(&mut w.conn)
            .await
            .unwrap();
    assert_eq!(before, current, "legacy payload interpretation changed");
    let owners: i64 = sqlx::query_scalar("SELECT count(*) FROM core.event_blobs WHERE blob=$1")
        .bind(&name)
        .fetch_one(&mut w.conn)
        .await
        .unwrap();
    assert_eq!(owners, 0, "old unknown custody was fabricated");
    let ids =
        sinex_store::native_materials::binding_pending(&mut w.conn, "library.request:operator@h")
            .await
            .unwrap();
    assert_eq!(ids.len(), 1);
    assert_eq!(
        sinex_store::native_materials::load(&mut w.conn, ids[0])
            .await
            .unwrap()
            .unwrap()
            .cited,
        vec![sinex_model::Cited {
            blob: name,
            size: bytes.len() as u64
        }]
    );
}

#[tokio::test]
async fn a_legacy_repeat_cannot_borrow_an_unrelated_sources_ready_media_owner() {
    let mut w = world().await;
    let bytes = b"synthetic media shared with an unrelated source";
    let path = w.root.join("other-owner.dat");
    std::fs::write(&path, bytes).unwrap();
    w.ok(Request::LibraryAdd {
        path: path.display().to_string(),
        reference: None,
    })
    .await;
    let (_, name) = acquisition(&mut w.conn, &path).await;
    let record = named_preview("legacy-borrowed-owner", name.clone());
    Library {
        writer: &w.writer,
        blobs: &w.blobs,
        host: "h",
    }
    .write(vec![record.clone()], "operator", "legacy")
    .await
    .unwrap();
    let stage = w.blobs.prepare_plain(bytes.as_slice()).unwrap();
    let result = Library {
        writer: &w.writer,
        blobs: &w.blobs,
        host: "h",
    }
    .write_staged(vec![record], "operator", "preview", vec![stage])
    .await;
    assert!(
        result.is_err(),
        "a repeated event without a citation borrowed another source's ready owner"
    );
    let own:i64=sqlx::query_scalar("SELECT count(*) FROM core.event_blobs b JOIN core.events e ON e.id=b.event_id WHERE e.payload->>'rendition'='legacy-borrowed-owner'").fetch_one(&mut w.conn).await.unwrap();
    assert_eq!(own, 0);
    let (other, _) = acquisition(&mut w.conn, &path).await;
    crate::excision::excise_with(
        &mut w.conn,
        &w.blobs,
        &w.writer,
        sinex_store::excision::Target::Event(sinex_model::EventId(other)),
        "operator",
    )
    .await
    .unwrap();
    assert_eq!(
        w.blobs.read(&name).unwrap(),
        bytes,
        "pending citation custody vanished with the other source's owner"
    );
    let pending =
        sinex_store::native_materials::binding_pending(&mut w.conn, "library.request:operator@h")
            .await
            .unwrap();
    assert_eq!(pending.len(), 1);
}

#[tokio::test]
async fn a_real_repeated_reading_keeps_its_own_citation_and_completes_pending_custody() {
    let mut w = world().await;
    let bytes = b"synthetic repeated media";
    let stage = w.blobs.prepare_plain(bytes.as_slice()).unwrap();
    let name = stage.name();
    let record = named_preview("known-repeat", name.clone());
    Library {
        writer: &w.writer,
        blobs: &w.blobs,
        host: "h",
    }
    .write_staged(vec![record.clone()], "operator", "preview", vec![stage])
    .await
    .unwrap();
    let id: uuid::Uuid =
        sqlx::query_scalar("SELECT id FROM core.events WHERE payload->>'rendition'='known-repeat'")
            .fetch_one(&mut w.conn)
            .await
            .unwrap();
    let stage = w.blobs.prepare_plain(bytes.as_slice()).unwrap();
    let repeated = Library {
        writer: &w.writer,
        blobs: &w.blobs,
        host: "h",
    }
    .write_staged(vec![record], "operator", "preview", vec![stage])
    .await
    .unwrap();
    assert_eq!(repeated["admitted"], 0);
    let owner: uuid::Uuid =
        sqlx::query_scalar("SELECT event_id FROM core.event_blobs WHERE blob=$1")
            .bind(&name)
            .fetch_one(&mut w.conn)
            .await
            .unwrap();
    assert_eq!(
        owner, id,
        "repeat changed the citation's actual reading identity"
    );
    assert!(
        sinex_store::native_materials::binding_pending(&mut w.conn, "library.request:operator@h")
            .await
            .unwrap()
            .is_empty()
    );
    assert_eq!(w.blobs.read(&name).unwrap(), bytes);
}
