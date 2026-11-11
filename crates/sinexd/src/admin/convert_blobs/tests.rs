use super::*;
use std::os::unix::fs::PermissionsExt;

#[devkit::test]
async fn external_exports_convert_once_and_wrong_bytes_never_get_descriptors(
    db: devkit::pg::TestDatabase,
) {
    let root = tempfile::tempdir().unwrap();
    let source = root.path().join("old");
    std::fs::create_dir(&source).unwrap();
    let exporter = root.path().join("exporter");
    std::fs::write(
        &exporter,
        "#!/bin/sh\nset -eu\ntest \"$1\" = payload\ncat \"$2/$3\"\n",
    )
    .unwrap();
    std::fs::set_permissions(&exporter, std::fs::Permissions::from_mode(0o700)).unwrap();
    let bytes = b"synthetic preserved source";
    let name = hashed_blobs::Hash::of(bytes).to_string();
    std::fs::write(source.join(&name), b"wrong bytes").unwrap();
    let database = db.url(None);
    let mut conn = sqlx::PgConnection::connect(&database).await.unwrap();
    sqlx::query("INSERT INTO core.blobs(hash,size,status) VALUES($1,$2,'present')")
        .bind(&name)
        .bind(bytes.len() as i64)
        .execute(&mut conn)
        .await
        .unwrap();
    let store = root.path().join("store");
    assert!(run(&database, &store, &source, &exporter).await.is_err());
    let missing: bool = sqlx::query_scalar("SELECT core.blob_descriptors($1) IS NULL")
        .bind(&name)
        .fetch_one(&mut conn)
        .await
        .unwrap();
    assert!(missing);
    std::fs::write(source.join(&name), bytes).unwrap();
    run(&database, &store, &source, &exporter).await.unwrap();
    let segment = store.join("segments/current.hb");
    let once = std::fs::read(&segment).unwrap();
    run(&database, &store, &source, &exporter).await.unwrap();
    assert_eq!(std::fs::read(&segment).unwrap(), once);
    assert_eq!(std::fs::read(source.join(name)).unwrap(), bytes);
}
