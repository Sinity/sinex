use super::*;
use sinex_model::event_types::library::{item_id, item_key};

#[devkit::test]
async fn library_open_finds_verified_reference_and_released_copies(db: devkit::pg::TestDatabase) {
    let scratch = tempfile::tempdir().unwrap();
    let scratch_root = std::fs::canonicalize(scratch.path()).unwrap();
    let referenced = scratch_root.join("a-reference");
    let released = scratch_root.join("b-released");
    let content = format!("blake3:{}", blake3::hash(b"same bytes").to_hex());
    let w = world(db).await;
    for (root, reference) in [(&referenced, true), (&released, false)] {
        std::fs::create_dir(root).unwrap();
        std::fs::write(root.join("file.txt"), b"same bytes").unwrap();
        let (_, added) = call_unbounded(
            &w,
            Some(OPERATOR),
            json!({"method":"library-add", "path":root, "reference":reference}),
        )
        .await;
        assert_eq!(added["outcome"], "ok", "{added}");
    }
    let (event,): (uuid::Uuid,) = sqlx::query_as(
        "SELECT id FROM core.events
        WHERE event_type='library.copy' AND payload->>'path'=$1 AND (payload->>'stored')::bool",
    )
    .bind(released.join("file.txt").to_str().unwrap())
    .fetch_one(&w.reads)
    .await
    .unwrap();
    let release = json!({"method":"library-release-copy", "event":event, "content":content});
    let (status, _) = call_unbounded(&w, Some(READ), release.clone()).await;
    assert_eq!(status, StatusCode::FORBIDDEN);
    let (_, retired) = call_unbounded(&w, Some(OPERATOR), release).await;
    assert_eq!(retired["outcome"], "ok", "{retired}");
    assert_eq!(retired["result"]["bytes_removed"], true);
    assert!(
        !sqlx::query_scalar::<_, bool>(
            "SELECT EXISTS(SELECT 1 FROM core.blob_extents WHERE hash=$1)"
        )
        .bind(&content)
        .fetch_one(&w.reads)
        .await
        .unwrap()
    );

    let item = item_id(&item_key("h", released.to_str().unwrap(), "file.txt"));
    let request = json!({"method":"library-open", "item":item});
    let (status, _) = call_unbounded(&w, None, request.clone()).await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);
    let (_, opened) = call_unbounded(&w, Some(READ), request.clone()).await;
    assert_eq!(opened["outcome"], "ok", "{opened}");
    assert_eq!(opened["result"]["verified_copy_on_disk"], true);
    assert_eq!(opened["result"]["path"], json!(referenced.join("file.txt")));
    assert_eq!(opened["result"]["content"], content);

    // The old reading is still in the database. The bytes at its path no
    // longer match, even though its length does, so use the released copy.
    std::fs::write(referenced.join("file.txt"), b"bad! bytes").unwrap();
    let (_, opened) = call_unbounded(&w, Some(READ), request).await;
    assert_eq!(opened["outcome"], "ok", "{opened}");
    assert_eq!(opened["result"]["verified_copy_on_disk"], true);
    assert_eq!(opened["result"]["path"], json!(released.join("file.txt")));
    assert_eq!(opened["result"]["content"], content);
}
