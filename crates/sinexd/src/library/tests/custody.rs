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
}
