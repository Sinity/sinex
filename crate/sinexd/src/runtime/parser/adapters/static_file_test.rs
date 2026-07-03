use super::*;
use std::fs;
use std::io::Write;
use tempfile::Builder;
use tempfile::NamedTempFile;
use xtask::sandbox::prelude::sinex_test;

fn dummy_material_id() -> Id<SourceMaterial> {
    Id::from_uuid(uuid::Uuid::new_v4())
}

fn write_git_head(repo: &std::path::Path, oid: &str) -> xtask::sandbox::TestResult<()> {
    let git_dir = repo.join(".git");
    fs::create_dir_all(git_dir.join("refs/heads"))?;
    fs::write(git_dir.join("HEAD"), "ref: refs/heads/main\n")?;
    fs::write(git_dir.join("refs/heads/main"), format!("{oid}\n"))?;
    Ok(())
}

#[sinex_test]
async fn test_static_file_reads_entire_contents() -> xtask::sandbox::TestResult<()> {
    let mut f = NamedTempFile::new().unwrap();
    f.write_all(b"hello world").unwrap();
    let path = f.path().to_str().unwrap().to_string();

    let adapter = StaticFileAdapter;
    let config = StaticFileConfig { path };
    let mut stream = adapter
        .open(dummy_material_id(), &config, None)
        .await
        .unwrap();

    let record = stream.next().await.unwrap().unwrap();
    assert_eq!(record.bytes, b"hello world");
    assert!(stream.next().await.is_none());
    Ok(())
}

#[sinex_test]
async fn test_static_file_already_processed_returns_empty() -> xtask::sandbox::TestResult<()> {
    let mut f = NamedTempFile::new().unwrap();
    f.write_all(b"data").unwrap();
    let path = f.path().to_str().unwrap().to_string();

    let adapter = StaticFileAdapter;
    let config = StaticFileConfig { path };
    let cursor = Some(StaticFileCursor {
        processed: true,
        state_token: None,
    });
    let mut stream = adapter
        .open(dummy_material_id(), &config, cursor)
        .await
        .unwrap();

    assert!(stream.next().await.is_none());
    Ok(())
}

#[sinex_test]
async fn test_static_file_not_processed_cursor_yields_record() -> xtask::sandbox::TestResult<()>
{
    let mut f = NamedTempFile::new().unwrap();
    f.write_all(b"content").unwrap();
    let path = f.path().to_str().unwrap().to_string();

    let adapter = StaticFileAdapter;
    let config = StaticFileConfig { path };
    let cursor = Some(StaticFileCursor {
        processed: false,
        state_token: None,
    });
    let mut stream = adapter
        .open(dummy_material_id(), &config, cursor)
        .await
        .unwrap();

    assert!(stream.next().await.unwrap().is_ok());
    Ok(())
}

#[sinex_test]
async fn test_static_file_anchor_is_byte_range() -> xtask::sandbox::TestResult<()> {
    let mut f = NamedTempFile::new().unwrap();
    f.write_all(b"abc").unwrap();
    let path = f.path().to_str().unwrap().to_string();

    let adapter = StaticFileAdapter;
    let config = StaticFileConfig { path };
    let mut stream = adapter
        .open(dummy_material_id(), &config, None)
        .await
        .unwrap();
    let record = stream.next().await.unwrap().unwrap();

    assert!(matches!(
        record.anchor,
        MaterialAnchor::ByteRange { start: 0, len: 3 }
    ));
    Ok(())
}

#[sinex_test]
async fn test_static_file_cursor_after() -> xtask::sandbox::TestResult<()> {
    let mut f = NamedTempFile::new().unwrap();
    f.write_all(b"x").unwrap();
    let path = f.path().to_str().unwrap().to_string();

    let adapter = StaticFileAdapter;
    let config = StaticFileConfig { path };
    let mut stream = adapter
        .open(dummy_material_id(), &config, None)
        .await
        .unwrap();
    let record = stream.next().await.unwrap().unwrap();

    let cursor = adapter.cursor_after(&record).unwrap();
    assert!(cursor.processed);
    Ok(())
}

#[sinex_test]
async fn test_static_file_missing_path_returns_error() -> xtask::sandbox::TestResult<()> {
    let adapter = StaticFileAdapter;
    let config = StaticFileConfig {
        path: "/nonexistent/path/file.txt".into(),
    };
    let result = adapter.open(dummy_material_id(), &config, None).await;
    assert!(result.is_err());
    Ok(())
}

#[sinex_test]
async fn test_static_file_empty_file_yields_one_empty_record() -> xtask::sandbox::TestResult<()>
{
    let f = NamedTempFile::new().unwrap();
    let path = f.path().to_str().unwrap().to_string();

    let adapter = StaticFileAdapter;
    let config = StaticFileConfig { path };
    let mut stream = adapter
        .open(dummy_material_id(), &config, None)
        .await
        .unwrap();

    let record = stream.next().await.unwrap().unwrap();
    assert!(record.bytes.is_empty());
    assert!(stream.next().await.is_none());
    Ok(())
}

#[sinex_test]
async fn static_file_directory_yields_path_only_record() -> xtask::sandbox::TestResult<()> {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().to_str().unwrap().to_string();

    let adapter = StaticFileAdapter;
    let config = StaticFileConfig { path: path.clone() };
    let mut stream = adapter
        .open(dummy_material_id(), &config, None)
        .await
        .unwrap();

    let record = stream.next().await.unwrap().unwrap();
    assert!(record.bytes.is_empty());
    assert_eq!(record.logical_path.as_ref().unwrap().as_str(), path);
    assert!(matches!(
        record.anchor,
        MaterialAnchor::ByteRange { start: 0, len: 0 }
    ));
    assert!(stream.next().await.is_none());
    Ok(())
}

#[sinex_test]
async fn static_file_git_directory_reopens_when_head_changes()
-> xtask::sandbox::TestResult<()> {
    let dir = tempfile::tempdir().unwrap();
    write_git_head(dir.path(), "1111111111111111111111111111111111111111")?;
    let path = dir.path().to_str().unwrap().to_string();

    let adapter = StaticFileAdapter;
    let config = StaticFileConfig { path };
    let mut initial_stream = adapter.open(dummy_material_id(), &config, None).await?;
    let initial_record = initial_stream
        .next()
        .await
        .expect("initial git directory record")?;
    let initial_cursor = adapter.cursor_after(&initial_record)?;

    let mut unchanged_stream = adapter
        .open(dummy_material_id(), &config, Some(initial_cursor.clone()))
        .await?;
    assert!(unchanged_stream.next().await.is_none());

    write_git_head(dir.path(), "2222222222222222222222222222222222222222")?;
    let mut changed_stream = adapter
        .open(dummy_material_id(), &config, Some(initial_cursor))
        .await?;
    assert!(
        changed_stream.next().await.is_some(),
        "changed git HEAD should re-open the directory-backed static source"
    );
    Ok(())
}

#[sinex_test]
async fn static_file_legacy_processed_git_cursor_reopens_once()
-> xtask::sandbox::TestResult<()> {
    let dir = tempfile::tempdir().unwrap();
    write_git_head(dir.path(), "3333333333333333333333333333333333333333")?;
    let path = dir.path().to_str().unwrap().to_string();

    let adapter = StaticFileAdapter;
    let config = StaticFileConfig { path };
    let legacy_cursor = StaticFileCursor {
        processed: true,
        state_token: None,
    };

    let mut stream = adapter
        .open(dummy_material_id(), &config, Some(legacy_cursor))
        .await?;
    assert!(
        stream.next().await.is_some(),
        "legacy processed cursors without a git HEAD token need one refresh"
    );
    Ok(())
}

#[sinex_test]
async fn static_file_directory_has_no_input_fingerprint() -> xtask::sandbox::TestResult<()> {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().to_str().unwrap().to_string();

    let adapter = StaticFileAdapter;
    let config = StaticFileConfig { path };

    assert!(adapter.input_fingerprint(&config).unwrap().is_none());
    Ok(())
}

#[sinex_test]
async fn static_file_csv_input_fingerprint_reports_header_shape()
-> xtask::sandbox::TestResult<()> {
    let mut f = Builder::new().suffix(".csv").tempfile().unwrap();
    f.write_all(b"id,name\n1,Alice\n").unwrap();
    let path = f.path().to_str().unwrap().to_string();

    let adapter = StaticFileAdapter;
    let config = StaticFileConfig { path };
    let fingerprint = adapter
        .input_fingerprint(&config)
        .unwrap()
        .expect("CSV static files should expose a structural fingerprint");

    assert_eq!(fingerprint.format, "csv");
    assert_eq!(fingerprint.keys, vec!["id", "name"]);
    assert_eq!(fingerprint.type_map["id"], "integer");
    assert_eq!(fingerprint.type_map["name"], "string");
    Ok(())
}

#[sinex_test]
async fn static_file_json_input_fingerprint_reports_nested_shape()
-> xtask::sandbox::TestResult<()> {
    let mut f = Builder::new().suffix(".json").tempfile().unwrap();
    f.write_all(br#"{"id":1,"profile":{"name":"Alice"}}"#)
        .unwrap();
    let path = f.path().to_str().unwrap().to_string();

    let adapter = StaticFileAdapter;
    let config = StaticFileConfig { path };
    let fingerprint = adapter
        .input_fingerprint(&config)
        .unwrap()
        .expect("JSON static files should expose a structural fingerprint");

    assert_eq!(fingerprint.format, "json");
    assert!(fingerprint.keys.contains(&"/id".to_string()));
    assert!(fingerprint.keys.contains(&"/profile/name".to_string()));
    assert_eq!(fingerprint.type_map["/id"], "integer");
    assert_eq!(fingerprint.type_map["/profile/name"], "string");
    Ok(())
}

#[sinex_test]
async fn static_file_jsonl_input_fingerprint_reports_row_shape()
-> xtask::sandbox::TestResult<()> {
    let mut f = Builder::new().suffix(".jsonl").tempfile().unwrap();
    f.write_all(
        br#"{"id":1,"created_at":"2026-01-01"}
{"id":2,"created_at":"2026-01-02","score":7}
"#,
    )
    .unwrap();
    let path = f.path().to_str().unwrap().to_string();

    let adapter = StaticFileAdapter;
    let config = StaticFileConfig { path };
    let fingerprint = adapter
        .input_fingerprint(&config)
        .unwrap()
        .expect("JSONL static files should expose a structural fingerprint");

    assert_eq!(fingerprint.format, "jsonl");
    assert!(fingerprint.keys.contains(&"/[]/id".to_string()));
    assert!(fingerprint.keys.contains(&"/[]/created_at".to_string()));
    assert!(fingerprint.keys.contains(&"/[]/score".to_string()));
    Ok(())
}

#[sinex_test]
async fn static_file_unknown_extension_has_no_input_fingerprint()
-> xtask::sandbox::TestResult<()> {
    let mut f = Builder::new().suffix(".txt").tempfile().unwrap();
    f.write_all(b"not a structured export").unwrap();
    let path = f.path().to_str().unwrap().to_string();

    let adapter = StaticFileAdapter;
    let config = StaticFileConfig { path };

    assert!(adapter.input_fingerprint(&config).unwrap().is_none());
    Ok(())
}
