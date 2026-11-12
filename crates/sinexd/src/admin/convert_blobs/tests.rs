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
        "#!/bin/sh\nset -eu\ntest \"$1\" = payloads\necho called >> \"$2/calls\"\nroot=$2\nshift 2\nfor hash do cat \"$root/$hash\"; done\n",
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
    let calls = std::fs::read(source.join("calls")).unwrap();
    run(&database, &store, &source, &exporter).await.unwrap();
    assert_eq!(std::fs::read(&segment).unwrap(), once);
    assert_eq!(
        std::fs::read(source.join("calls")).unwrap(),
        calls,
        "already converted bytes are not exported again"
    );
    assert_eq!(std::fs::read(source.join(name)).unwrap(), bytes);
}

#[devkit::test]
async fn a_failed_exporter_with_correct_bytes_does_not_commit(db: devkit::pg::TestDatabase) {
    let root = tempfile::tempdir().unwrap();
    let source = root.path().join("old");
    std::fs::create_dir(&source).unwrap();
    let bytes = b"synthetic export before failure";
    let name = hashed_blobs::Hash::of(bytes).to_string();
    std::fs::write(source.join(&name), bytes).unwrap();
    let exporter = root.path().join("exporter");
    std::fs::write(&exporter, "#!/bin/sh\ncat \"$2/$3\"\nexit 7\n").unwrap();
    std::fs::set_permissions(&exporter, std::fs::Permissions::from_mode(0o700)).unwrap();
    let database = db.url(None);
    let mut conn = sqlx::PgConnection::connect(&database).await.unwrap();
    sqlx::query("INSERT INTO core.blobs(hash,size,status) VALUES($1,$2,'present')")
        .bind(&name)
        .bind(bytes.len() as i64)
        .execute(&mut conn)
        .await
        .unwrap();
    let error = run(&database, &root.path().join("store"), &source, &exporter)
        .await
        .unwrap_err();
    assert!(error.to_string().contains("exporter failed"), "{error}");
    let missing: bool = sqlx::query_scalar("SELECT core.blob_descriptors($1) IS NULL")
        .bind(&name)
        .fetch_one(&mut conn)
        .await
        .unwrap();
    assert!(missing);
}

#[devkit::test]
async fn partial_or_trailing_exports_commit_none_of_the_group(db: devkit::pg::TestDatabase) {
    let root = tempfile::tempdir().unwrap();
    let source = root.path().join("old");
    std::fs::create_dir(&source).unwrap();
    let exporter = root.path().join("exporter");
    let ordinary = "#!/bin/sh\nset -eu\nroot=$2\nshift 2\nfor hash do cat \"$root/$hash\"; done\n";
    std::fs::write(&exporter, ordinary).unwrap();
    std::fs::set_permissions(&exporter, std::fs::Permissions::from_mode(0o700)).unwrap();
    let mut inputs = [
        b"first synthetic payload".as_slice(),
        b"second synthetic payload".as_slice(),
    ]
    .map(|bytes| (hashed_blobs::Hash::of(bytes).to_string(), bytes));
    inputs.sort_by(|a, b| a.0.cmp(&b.0));
    let database = db.url(None);
    let mut conn = sqlx::PgConnection::connect(&database).await.unwrap();
    for (name, bytes) in &inputs {
        std::fs::write(source.join(name), bytes).unwrap();
        sqlx::query("INSERT INTO core.blobs(hash,size,status) VALUES($1,$2,'present')")
            .bind(name)
            .bind(bytes.len() as i64)
            .execute(&mut conn)
            .await
            .unwrap();
    }
    let before: Vec<serde_json::Value> =
        sqlx::query_scalar("SELECT to_jsonb(b) FROM core.blobs b ORDER BY hash")
            .fetch_all(&mut conn)
            .await
            .unwrap();
    let store = root.path().join("store");
    // The earlier member is valid; a truncated later member must roll back
    // the whole group, rather than committing the prefix it already read.
    std::fs::write(source.join(&inputs[1].0), b"short").unwrap();
    assert!(run(&database, &store, &source, &exporter).await.is_err());
    std::fs::write(source.join(&inputs[1].0), inputs[1].1).unwrap();
    for body in [
        format!("{ordinary}printf extra\n"),
        format!("{ordinary}exit 7\n"),
    ] {
        std::fs::write(&exporter, body).unwrap();
        assert!(run(&database, &store, &source, &exporter).await.is_err());
    }
    let missing: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM core.blobs WHERE core.blob_descriptors(hash) IS NULL",
    )
    .fetch_one(&mut conn)
    .await
    .unwrap();
    assert_eq!(missing, 2);
    let after: Vec<serde_json::Value> =
        sqlx::query_scalar("SELECT to_jsonb(b) FROM core.blobs b ORDER BY hash")
            .fetch_all(&mut conn)
            .await
            .unwrap();
    assert_eq!(after, before);
    std::fs::write(&exporter, ordinary).unwrap();
    run(&database, &store, &source, &exporter).await.unwrap();
    let blobs = sinex_store::Blobs::open(&store).unwrap();
    blobs.load(&mut conn).await.unwrap();
    for (name, bytes) in inputs {
        assert_eq!(blobs.read(&name).unwrap(), bytes);
        assert_eq!(std::fs::read(source.join(name)).unwrap(), bytes);
    }
}
