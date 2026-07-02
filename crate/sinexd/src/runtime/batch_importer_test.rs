use super::*;
use std::io::Write;
use std::thread::sleep;
use std::time::Duration;
use tempfile::TempDir;
use xtask::sandbox::sinex_test;

fn create_test_file(dir: &std::path::Path, name: &str, content: &str) {
    let path = dir.join(name);
    let mut file = std::fs::File::create(&path).unwrap();
    file.write_all(content.as_bytes()).unwrap();
}

fn utf8(path: &std::path::Path) -> &Utf8Path {
    Utf8Path::from_path(path).expect("non-utf8 temp path")
}

fn fingerprint_for(path: &std::path::Path) -> ImportedFileFingerprint {
    let metadata = std::fs::metadata(path).unwrap();
    ImportedFileFingerprint {
        size_bytes: metadata.len(),
        modified_unix_ms: metadata
            .modified()
            .ok()
            .and_then(|modified| modified.duration_since(UNIX_EPOCH).ok())
            .map(|duration| duration.as_millis().min(u128::from(u64::MAX)) as u64),
    }
}

#[sinex_test]
async fn scan_finds_new_files() -> TestResult<()> {
    let dir = TempDir::new().unwrap();
    create_test_file(dir.path(), "data1.json", r#"{"key":"val"}"#);
    create_test_file(dir.path(), "data2.json", r#"{"key":"val2"}"#);
    create_test_file(dir.path(), "readme.txt", "ignore me");

    let state = BatchImporterState::default();
    let files = scan_for_new_files(&state, utf8(dir.path()), &[".json"]).unwrap();

    assert_eq!(files.len(), 2);
    assert_eq!(files[0].filename, "data1.json");
    assert_eq!(files[0].change_kind, ImportFileChangeKind::New);
    assert_eq!(files[1].filename, "data2.json");
    assert_eq!(files[1].change_kind, ImportFileChangeKind::New);
    Ok(())
}

#[sinex_test]
async fn scan_skips_unchanged_processed_files() -> TestResult<()> {
    let dir = TempDir::new().unwrap();
    let path1 = dir.path().join("data1.json");
    let path2 = dir.path().join("data2.json");
    create_test_file(dir.path(), "data1.json", "{}");
    create_test_file(dir.path(), "data2.json", "{}");

    let mut state = BatchImporterState::default();
    state.mark_processed(utf8(&path1), fingerprint_for(&path1), 2, 1);

    let files = scan_for_new_files(&state, utf8(dir.path()), &[".json"]).unwrap();
    assert_eq!(files.len(), 1);
    assert_eq!(files[0].filename, "data2.json");
    assert_eq!(files[0].change_kind, ImportFileChangeKind::New);

    state.mark_processed(utf8(&path2), fingerprint_for(&path2), 2, 1);
    let files = scan_for_new_files(&state, utf8(dir.path()), &[".json"]).unwrap();
    assert!(files.is_empty());
    Ok(())
}

#[sinex_test]
async fn scan_empty_directory() -> TestResult<()> {
    let dir = TempDir::new().unwrap();
    let state = BatchImporterState::default();
    let files = scan_for_new_files(&state, utf8(dir.path()), &[]).unwrap();
    assert!(files.is_empty());
    Ok(())
}

#[sinex_test]
async fn scan_no_extension_filter() -> TestResult<()> {
    let dir = TempDir::new().unwrap();
    create_test_file(dir.path(), "data.json", "{}");
    create_test_file(dir.path(), "data.csv", "a,b");
    create_test_file(dir.path(), "notes.txt", "hello");

    let state = BatchImporterState::default();
    let files = scan_for_new_files(&state, utf8(dir.path()), &[]).unwrap();
    assert_eq!(files.len(), 3);
    Ok(())
}

#[sinex_test]
async fn scan_missing_path() -> TestResult<()> {
    let state = BatchImporterState::default();
    let path = Utf8Path::new("/tmp/sinex-test-nonexistent-batch-dir");
    assert!(matches!(
        scan_for_new_files(&state, path, &[]),
        Err(ScanError::PathNotFound(_))
    ));
    Ok(())
}

#[sinex_test]
async fn read_file_content_works() -> TestResult<()> {
    let dir = TempDir::new().unwrap();
    create_test_file(dir.path(), "test.json", r#"{"hello":"world"}"#);

    let path = Utf8PathBuf::from_path_buf(dir.path().join("test.json")).unwrap();
    let file = DiscoveredFile {
        path,
        filename: "test.json".to_string(),
        fingerprint: ImportedFileFingerprint {
            size_bytes: 17,
            modified_unix_ms: None,
        },
        start_offset_bytes: 0,
        start_line_number: 0,
        change_kind: ImportFileChangeKind::New,
    };
    let content = read_file_content(&file).unwrap();
    assert_eq!(
        std::str::from_utf8(&content).unwrap(),
        r#"{"hello":"world"}"#
    );
    Ok(())
}

#[sinex_test]
async fn read_file_lines_works() -> TestResult<()> {
    let dir = TempDir::new().unwrap();
    create_test_file(dir.path(), "lines.txt", "line1\nline2\nline3\n");

    let path = Utf8PathBuf::from_path_buf(dir.path().join("lines.txt")).unwrap();
    let file = DiscoveredFile {
        path,
        filename: "lines.txt".to_string(),
        fingerprint: ImportedFileFingerprint {
            size_bytes: 18,
            modified_unix_ms: None,
        },
        start_offset_bytes: 0,
        start_line_number: 0,
        change_kind: ImportFileChangeKind::New,
    };
    let lines = read_file_lines(&file).unwrap();
    assert_eq!(lines, vec!["line1", "line2", "line3"]);
    Ok(())
}

#[sinex_test]
async fn state_tracks_processed_files() -> TestResult<()> {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("file.json");
    create_test_file(dir.path(), "file.json", "{}");

    let mut state = BatchImporterState::default();
    state.mark_processed(utf8(&path), fingerprint_for(&path), 2, 1);
    assert_eq!(state.total_files_processed, 1);
    assert_eq!(state.total_bytes_processed, 2);
    assert_eq!(state.total_lines_processed, 1);
    assert!(state.file_state(utf8(&path)).is_some());
    Ok(())
}

#[sinex_test]
async fn scan_detects_appended_file() -> TestResult<()> {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("history.ndjson");
    create_test_file(dir.path(), "history.ndjson", "{\"x\":1}\n");

    let mut state = BatchImporterState::default();
    state.mark_processed(utf8(&path), fingerprint_for(&path), 8, 1);

    sleep(Duration::from_millis(5));
    let mut file = std::fs::OpenOptions::new()
        .append(true)
        .open(&path)
        .unwrap();
    writeln!(file, "{{\"x\":2}}").unwrap();

    let files = scan_for_new_files(&state, utf8(dir.path()), &[".ndjson"]).unwrap();
    assert_eq!(files.len(), 1);
    assert_eq!(files[0].change_kind, ImportFileChangeKind::Appended);
    assert_eq!(files[0].start_offset_bytes, 8);
    assert_eq!(files[0].start_line_number, 1);
    Ok(())
}

#[sinex_test]
async fn scan_detects_replaced_file() -> TestResult<()> {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("history.json");
    create_test_file(dir.path(), "history.json", "[1,2,3]");

    let mut state = BatchImporterState::default();
    state.mark_processed(utf8(&path), fingerprint_for(&path), 7, 1);

    sleep(Duration::from_millis(5));
    create_test_file(dir.path(), "history.json", "[]");

    let files = scan_for_new_files(&state, utf8(dir.path()), &[".json"]).unwrap();
    assert_eq!(files.len(), 1);
    assert_eq!(files[0].change_kind, ImportFileChangeKind::Replaced);
    assert_eq!(files[0].start_offset_bytes, 0);
    assert_eq!(files[0].start_line_number, 0);
    Ok(())
}
