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
