use super::*;

use sinex_primitives::ids::Id;
use sinex_primitives::parser::{MaterialAnchor, ParserContext, SourceId, SourceRecord};
use sinex_primitives::temporal::Timestamp;
use std::io::Write;
use tempfile::TempDir;
use xtask::sandbox::prelude::*;

fn make_ctx() -> ParserContext {
    ParserContext {
        source_id: SourceId::from_static("docs-library-index"),
        source_material_id: Id::from_uuid(uuid::Uuid::new_v4()),
        record_anchor: MaterialAnchor::DirectoryEntry {
            path: Utf8PathBuf::from("/test/file.pdf"),
            content_hash: None,
        },
        operation_id: uuid::Uuid::new_v4(),
        job_id: uuid::Uuid::new_v4(),
        host: "test-host".into(),
        acquisition_time: Timestamp::now(),
    }
}

fn make_record(path: &str, bytes: Vec<u8>, content_hash: Option<String>) -> SourceRecord {
    SourceRecord {
        material_id: Id::from_uuid(uuid::Uuid::new_v4()),
        anchor: MaterialAnchor::DirectoryEntry {
            path: Utf8PathBuf::from(path),
            content_hash,
        },
        bytes,
        logical_path: Some(Utf8PathBuf::from(path)),
        source_ts_hint: None,
        metadata: serde_json::Value::Null,
    }
}

// -------------------------------------------------------------------------

#[sinex_test]
async fn parses_pdf_into_one_intent() -> xtask::sandbox::TestResult<()> {
    let dir = TempDir::new().unwrap();
    let file = dir
        .path()
        .join("Aaron Clarey - Bachelor Pad Economics (2013).pdf");
    let mut f = std::fs::File::create(&file).unwrap();
    f.write_all(b"pdf content").unwrap();
    drop(f);

    let path = camino::Utf8PathBuf::from_path_buf(file).unwrap();
    let record = make_record(path.as_str(), b"pdf content".to_vec(), None);
    let ctx = make_ctx();

    let mut parser = DocsLibraryParser;
    let intents = parser.parse_record(record, &ctx).await?;

    assert_eq!(intents.len(), 1, "one intent per file");
    assert_eq!(intents[0].event_source.as_static_str(), "docs-library");
    assert_eq!(intents[0].event_type.as_static_str(), "document.indexed");
    Ok(())
}

#[sinex_test]
async fn payload_contains_core_fields() -> xtask::sandbox::TestResult<()> {
    let dir = TempDir::new().unwrap();
    let file = dir.path().join("test_document.epub");
    let mut f = std::fs::File::create(&file).unwrap();
    f.write_all(b"epub bytes here").unwrap();
    drop(f);

    let path = camino::Utf8PathBuf::from_path_buf(file.clone()).unwrap();
    let record = make_record(path.as_str(), b"epub bytes here".to_vec(), None);
    let ctx = make_ctx();

    let mut parser = DocsLibraryParser;
    let intents = parser.parse_record(record, &ctx).await?;

    let payload = &intents[0].payload;
    assert_eq!(payload["filename"].as_str(), Some("test_document.epub"));
    assert_eq!(payload["extension"].as_str(), Some("epub"));
    assert_eq!(payload["byte_size"].as_u64(), Some(15));
    Ok(())
}

#[sinex_test]
async fn year_hint_extracted_from_filename() -> xtask::sandbox::TestResult<()> {
    let dir = TempDir::new().unwrap();
    let file = dir.path().join("Author Name - Some Book (2018).pdf");
    let mut f = std::fs::File::create(&file).unwrap();
    f.write_all(b"x").unwrap();
    drop(f);

    let path = camino::Utf8PathBuf::from_path_buf(file).unwrap();
    let record = make_record(path.as_str(), b"x".to_vec(), None);
    let ctx = make_ctx();

    let mut parser = DocsLibraryParser;
    let intents = parser.parse_record(record, &ctx).await?;

    let payload = &intents[0].payload;
    assert_eq!(payload["year_hint"].as_u64(), Some(2018));
    Ok(())
}

#[sinex_test]
async fn author_and_title_hints_extracted() -> xtask::sandbox::TestResult<()> {
    let dir = TempDir::new().unwrap();
    let file = dir
        .path()
        .join("Jordan B. Peterson - 12 Rules for Life (2018).pdf");
    let mut f = std::fs::File::create(&file).unwrap();
    f.write_all(b"x").unwrap();
    drop(f);

    let path = camino::Utf8PathBuf::from_path_buf(file).unwrap();
    let record = make_record(path.as_str(), b"x".to_vec(), None);
    let ctx = make_ctx();

    let mut parser = DocsLibraryParser;
    let intents = parser.parse_record(record, &ctx).await?;

    let payload = &intents[0].payload;
    assert_eq!(payload["author_hint"].as_str(), Some("Jordan B. Peterson"),);
    assert_eq!(payload["title_hint"].as_str(), Some("12 Rules for Life"),);
    Ok(())
}

#[sinex_test]
async fn external_id_extracted_from_md5_in_name() -> xtask::sandbox::TestResult<()> {
    // Anna's Archive filename pattern: "... -- e73bc86a05ff4661d735d35f844c9650 -- Anna's Archive.pdf"
    let dir = TempDir::new().unwrap();
    let file = dir
        .path()
        .join("Some Book -- e73bc86a05ff4661d735d35f844c9650 -- Anna's Archive.pdf");
    let mut f = std::fs::File::create(&file).unwrap();
    f.write_all(b"x").unwrap();
    drop(f);

    let path = camino::Utf8PathBuf::from_path_buf(file).unwrap();
    let record = make_record(path.as_str(), b"x".to_vec(), None);
    let ctx = make_ctx();

    let mut parser = DocsLibraryParser;
    let intents = parser.parse_record(record, &ctx).await?;

    let payload = &intents[0].payload;
    assert_eq!(
        payload["external_id"].as_str(),
        Some("e73bc86a05ff4661d735d35f844c9650"),
    );
    Ok(())
}

#[sinex_test]
async fn no_hints_for_bare_filename() -> xtask::sandbox::TestResult<()> {
    let dir = TempDir::new().unwrap();
    let file = dir.path().join("965.jpg");
    let mut f = std::fs::File::create(&file).unwrap();
    f.write_all(b"jpeg").unwrap();
    drop(f);

    let path = camino::Utf8PathBuf::from_path_buf(file).unwrap();
    let record = make_record(path.as_str(), b"jpeg".to_vec(), None);
    let ctx = make_ctx();

    let mut parser = DocsLibraryParser;
    let intents = parser.parse_record(record, &ctx).await?;

    let payload = &intents[0].payload;
    assert!(
        payload["author_hint"].is_null(),
        "no author hint for bare filename"
    );
    assert!(
        payload["title_hint"].is_null(),
        "no title hint for bare filename"
    );
    assert!(
        payload["year_hint"].is_null(),
        "no year hint for bare filename"
    );
    assert!(
        payload["external_id"].is_null(),
        "no external_id for bare filename"
    );
    Ok(())
}

#[sinex_test]
async fn content_hash_preserved_from_anchor() -> xtask::sandbox::TestResult<()> {
    let dir = TempDir::new().unwrap();
    let file = dir.path().join("book.pdf");
    let mut f = std::fs::File::create(&file).unwrap();
    f.write_all(b"bytes").unwrap();
    drop(f);

    let path = camino::Utf8PathBuf::from_path_buf(file).unwrap();
    let hash = "deadbeefdeadbeef".to_string();
    let record = make_record(path.as_str(), b"bytes".to_vec(), Some(hash.clone()));
    let ctx = make_ctx();

    let mut parser = DocsLibraryParser;
    let intents = parser.parse_record(record, &ctx).await?;

    let payload = &intents[0].payload;
    assert_eq!(payload["content_hash"].as_str(), Some(hash.as_str()));
    Ok(())
}

#[sinex_test]
async fn occurrence_key_has_path_and_hash_fields() -> xtask::sandbox::TestResult<()> {
    let dir = TempDir::new().unwrap();
    let file = dir.path().join("book.pdf");
    let mut f = std::fs::File::create(&file).unwrap();
    f.write_all(b"x").unwrap();
    drop(f);

    let path = camino::Utf8PathBuf::from_path_buf(file).unwrap();
    let record = make_record(path.as_str(), b"x".to_vec(), None);
    let ctx = make_ctx();

    let mut parser = DocsLibraryParser;
    let intents = parser.parse_record(record, &ctx).await?;

    let key = intents[0]
        .occurrence_key
        .as_ref()
        .expect("occurrence_key present");
    assert_eq!(key.source_id.as_str(), "docs-library-index");
    let field_names: Vec<&str> = key.fields.iter().map(|(k, _)| k.as_str()).collect();
    assert!(field_names.contains(&"path"), "key has 'path' field");
    assert!(
        field_names.contains(&"content_hash"),
        "key has 'content_hash' field"
    );
    Ok(())
}

#[sinex_test]
async fn rejects_non_directory_entry_anchor() -> xtask::sandbox::TestResult<()> {
    let record = SourceRecord {
        material_id: Id::from_uuid(uuid::Uuid::new_v4()),
        anchor: MaterialAnchor::ByteRange { start: 0, len: 1 },
        bytes: b"x".to_vec(),
        logical_path: None,
        source_ts_hint: None,
        metadata: serde_json::Value::Null,
    };
    let ctx = make_ctx();

    let mut parser = DocsLibraryParser;
    let result = parser.parse_record(record, &ctx).await;
    assert!(result.is_err(), "must error on wrong anchor kind");
    Ok(())
}

// -------------------------------------------------------------------------
// Unit tests for heuristics (no I/O)
// -------------------------------------------------------------------------

#[sinex_test]
async fn extract_year_finds_4digit_year() -> xtask::sandbox::TestResult<()> {
    assert_eq!(extract_year("Book Title (2023)"), Some(2023));
    assert_eq!(extract_year("Title-Publisher (2018)"), Some(2018));
    assert_eq!(extract_year("no year here"), None);
    // Out-of-range years are not matched.
    assert_eq!(extract_year("old 1800 book"), None);
    Ok(())
}

#[sinex_test]
async fn extract_year_ignores_longer_sequences() -> xtask::sandbox::TestResult<()> {
    // 5+ digit numbers should not match.
    assert_eq!(extract_year("12345 is not a year"), None);
    // Exactly 4 digits followed by non-digit is fine.
    assert_eq!(extract_year("prefix2021suffix"), Some(2021));
    Ok(())
}

#[sinex_test]
async fn extract_external_id_finds_32hex() -> xtask::sandbox::TestResult<()> {
    let name = "Some Book -- e73bc86a05ff4661d735d35f844c9650 -- Anna's Archive";
    assert_eq!(
        extract_external_id(name),
        Some("e73bc86a05ff4661d735d35f844c9650".to_string()),
    );
    Ok(())
}

#[sinex_test]
async fn extract_external_id_none_for_short_hex() -> xtask::sandbox::TestResult<()> {
    assert_eq!(extract_external_id("abc123"), None);
    Ok(())
}

#[sinex_test]
async fn extract_author_title_splits_on_separator() -> xtask::sandbox::TestResult<()> {
    let (author, title) = extract_author_title("Jordan B. Peterson - 12 Rules for Life (2018)");
    assert_eq!(author.as_deref(), Some("Jordan B. Peterson"));
    assert_eq!(title.as_deref(), Some("12 Rules for Life"));
    Ok(())
}

#[sinex_test]
async fn extract_author_title_no_separator_returns_none() -> xtask::sandbox::TestResult<()> {
    let (author, title) = extract_author_title("DocumentWithNoSeparator");
    assert!(author.is_none());
    assert!(title.is_none());
    Ok(())
}

#[sinex_test]
async fn extract_author_title_preserves_inner_dashes() -> xtask::sandbox::TestResult<()> {
    // Titles like "The XX Factor - How the Rise ... (2013)" have " - " only once.
    let stem = "Alison Wolf - The XX Factor (2013)";
    let (author, title) = extract_author_title(stem);
    assert_eq!(author.as_deref(), Some("Alison Wolf"));
    assert_eq!(title.as_deref(), Some("The XX Factor"));
    Ok(())
}
