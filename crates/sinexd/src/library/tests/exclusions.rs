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
    assert!(refused.contains("matches an excised record"), "{refused}");
    let live: Vec<String> = sqlx::query_scalar("SELECT e.payload->>'collection' FROM core.events e JOIN core.live_equivalence l ON l.event_id=e.id WHERE e.event_type='library.collection'").fetch_all(&mut w.conn).await.unwrap();
    assert_eq!(live, vec!["unrelated"]);
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
    let name = w.blobs.put(removed).unwrap();
    let stored = sinex_model::Cited {
        blob: name.clone(),
        size: removed.len() as u64,
    };
    Library {
        writer: &w.writer,
        blobs: &w.blobs,
        host: "h",
    }
    .write_citing(
        vec![named_preview("selected", name.clone())],
        "operator",
        "preview",
        vec![stored.clone()],
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
    let refused = Library {
        writer: &w.writer,
        blobs: &w.blobs,
        host: "h",
    }
    .write_citing(
        vec![named_preview("fresh-wrapper", name.clone())],
        "operator",
        "preview",
        vec![stored],
    )
    .await
    .unwrap_err();
    assert!(
        refused.contains("cited media was excised from this source"),
        "{refused}"
    );
    let fresh = b"unrelated new preview bytes";
    let fresh_name = w.blobs.put(fresh).unwrap();
    let accepted = Library {
        writer: &w.writer,
        blobs: &w.blobs,
        host: "h",
    }
    .write_citing(
        vec![named_preview("fresh-unrelated", fresh_name.clone())],
        "operator",
        "preview",
        vec![sinex_model::Cited {
            blob: fresh_name.clone(),
            size: fresh.len() as u64,
        }],
    )
    .await
    .unwrap();
    assert_eq!(accepted["admitted"], 1);
    let exclusions = crate::excision::exclusions::Exclusions::load(&mut w.conn, "library.request")
        .await
        .unwrap();
    assert!(
        exclusions
            .check_cited(&[sinex_model::Cited {
                blob: name.clone(),
                size: removed.len() as u64 + 1
            }])
            .is_err(),
        "changed size bypassed source+hash exclusion"
    );
    let other = crate::excision::exclusions::Exclusions::load(&mut w.conn, "library.scan")
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
