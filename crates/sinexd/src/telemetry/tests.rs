use super::*;
use sqlx::Connection;

#[devkit::test]
async fn a_sample_spools_one_line_per_part(db: devkit::pg::TestDatabase) {
    let mut conn = PgConnection::connect(&db.url(None)).await.unwrap();
    let dir = tempfile::tempdir().unwrap();
    let mut writer = SpoolWriter::open(dir.path(), Rotation::default()).unwrap();
    let written = sample(&mut conn, &mut writer).await.unwrap();
    drop(writer);
    let mut lines = Vec::new();
    for entry in std::fs::read_dir(dir.path()).unwrap() {
        let text = std::fs::read_to_string(entry.unwrap().path()).unwrap();
        lines.extend(text.lines().map(str::to_owned));
    }
    assert_eq!(lines.len(), written);
    let samples: Vec<Sample> = lines
        .iter()
        .map(|l| {
            let record: serde_json::Map<String, serde_json::Value> =
                serde_json::from_str(l).unwrap();
            let (_, bytes) = sinex_source_core::drivers::live_stream::received(&record).unwrap();
            serde_json::from_slice(&bytes).unwrap()
        })
        .collect();
    let parts: Vec<&str> = samples.iter().map(|s| s.component.as_str()).collect();
    assert_eq!(parts, ["writer", "publisher", "search index"]);
    assert_eq!(samples[1].measures["behind"], 0.0);
}

#[devkit::test]
async fn sampling_reconnects_after_its_database_connection_dies(db: devkit::pg::TestDatabase) {
    let database = db.url(None);
    let mut conn = PgConnection::connect(&database).await.unwrap();
    let pid: i32 = sqlx::query_scalar("SELECT pg_backend_pid()")
        .fetch_one(&mut conn)
        .await
        .unwrap();
    let mut observer = PgConnection::connect(&database).await.unwrap();
    let terminated: bool = sqlx::query_scalar("SELECT pg_terminate_backend($1)")
        .bind(pid)
        .fetch_one(&mut observer)
        .await
        .unwrap();
    assert!(terminated);
    assert!(conn.ping().await.is_err());
    let dir = tempfile::tempdir().unwrap();
    let mut writer = SpoolWriter::open(dir.path(), Rotation::default()).unwrap();
    assert_eq!(
        sample_reconnecting(&mut conn, &database, &mut writer)
            .await
            .unwrap(),
        3
    );
    let replacement: i32 = sqlx::query_scalar("SELECT pg_backend_pid()")
        .fetch_one(&mut conn)
        .await
        .unwrap();
    assert_ne!(replacement, pid);
    assert_eq!(
        sample_reconnecting(&mut conn, &database, &mut writer)
            .await
            .unwrap(),
        3
    );
    drop(writer);
    let count: usize = std::fs::read_dir(dir.path())
        .unwrap()
        .map(|entry| {
            std::fs::read_to_string(entry.unwrap().path())
                .unwrap()
                .lines()
                .count()
        })
        .sum();
    assert_eq!(
        count, 6,
        "the failed read wrote no partial snapshot or duplicate lines"
    );
}
