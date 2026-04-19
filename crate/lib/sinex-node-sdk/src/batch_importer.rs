use camino::{Utf8Path, Utf8PathBuf};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};
use std::time::UNIX_EPOCH;
use tracing::{debug, info, warn};

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Default)]
pub struct ImportedFileFingerprint {
    pub size_bytes: u64,
    pub modified_unix_ms: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
pub struct ImportedFileState {
    pub fingerprint: ImportedFileFingerprint,
    pub imported_offset_bytes: u64,
    pub imported_line_count: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct BatchImporterState {
    pub files: BTreeMap<String, ImportedFileState>,
    pub scan_roots: BTreeSet<Utf8PathBuf>,
    pub total_files_processed: u64,
    pub total_bytes_processed: u64,
    pub total_lines_processed: u64,
}

impl BatchImporterState {
    #[must_use]
    pub fn new(scan_root: impl Into<Utf8PathBuf>) -> Self {
        let scan_root = scan_root.into();
        let mut scan_roots = BTreeSet::new();
        scan_roots.insert(scan_root);
        Self {
            scan_roots,
            ..Self::default()
        }
    }

    pub fn remember_scan_root(&mut self, scan_root: impl Into<Utf8PathBuf>) {
        self.scan_roots.insert(scan_root.into());
    }

    #[must_use]
    pub fn file_state(&self, path: &Utf8Path) -> Option<&ImportedFileState> {
        self.files.get(path.as_str())
    }

    pub fn mark_processed(
        &mut self,
        path: &Utf8Path,
        fingerprint: ImportedFileFingerprint,
        imported_offset_bytes: u64,
        processed_lines: u64,
    ) {
        self.files.insert(
            path.as_str().to_string(),
            ImportedFileState {
                fingerprint,
                imported_offset_bytes,
                imported_line_count: processed_lines,
            },
        );
        self.total_files_processed = self.total_files_processed.saturating_add(1);
        self.total_bytes_processed = self
            .total_bytes_processed
            .saturating_add(imported_offset_bytes);
        self.total_lines_processed = self.total_lines_processed.saturating_add(processed_lines);
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ImportFileChangeKind {
    New,
    Appended,
    Replaced,
}

#[derive(Debug)]
pub struct DiscoveredFile {
    pub path: Utf8PathBuf,
    pub filename: String,
    pub fingerprint: ImportedFileFingerprint,
    pub start_offset_bytes: u64,
    pub start_line_number: u64,
    pub change_kind: ImportFileChangeKind,
}

#[derive(Debug)]
pub enum ScanError {
    PathNotFound(Utf8PathBuf),
    IoError(std::io::Error),
    InvalidScanRoot(Utf8PathBuf),
}

impl std::fmt::Display for ScanError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::PathNotFound(path) => write!(f, "scan path not found: {path}"),
            Self::IoError(error) => write!(f, "I/O error during scan: {error}"),
            Self::InvalidScanRoot(path) => {
                write!(f, "path is neither a file nor a directory: {path}")
            }
        }
    }
}

impl std::error::Error for ScanError {}

impl From<std::io::Error> for ScanError {
    fn from(error: std::io::Error) -> Self {
        Self::IoError(error)
    }
}

pub fn scan_for_new_files(
    state: &BatchImporterState,
    scan_root: &Utf8Path,
    extensions: &[&str],
) -> Result<Vec<DiscoveredFile>, ScanError> {
    if !scan_root.exists() {
        return Err(ScanError::PathNotFound(scan_root.to_owned()));
    }

    let mut discovered = Vec::new();
    if scan_root.is_file() {
        maybe_collect_file(state, scan_root, extensions, &mut discovered)?;
    } else if scan_root.is_dir() {
        for entry in std::fs::read_dir(scan_root.as_std_path())? {
            let entry = entry?;
            let path = entry.path();
            if !path.is_file() {
                continue;
            }
            let utf8_path = Utf8PathBuf::from_path_buf(path)
                .unwrap_or_else(|path| Utf8PathBuf::from(path.to_string_lossy().to_string()));
            maybe_collect_file(state, &utf8_path, extensions, &mut discovered)?;
        }
    } else {
        return Err(ScanError::InvalidScanRoot(scan_root.to_owned()));
    }

    discovered.sort_by(|a, b| a.path.cmp(&b.path));

    if discovered.is_empty() {
        debug!(scan_root = %scan_root, "No importable file changes detected");
    } else {
        let total_bytes = discovered
            .iter()
            .map(|file| file.fingerprint.size_bytes)
            .sum::<u64>();
        info!(
            scan_root = %scan_root,
            changed_files = discovered.len(),
            total_bytes,
            "Discovered importable file changes"
        );
    }

    Ok(discovered)
}

fn maybe_collect_file(
    state: &BatchImporterState,
    path: &Utf8Path,
    extensions: &[&str],
    discovered: &mut Vec<DiscoveredFile>,
) -> Result<(), ScanError> {
    let filename = if let Some(name) = path.file_name() {
        name.to_string()
    } else {
        warn!(path = %path, "Skipping scan candidate without filename");
        return Ok(());
    };

    if !extensions.is_empty()
        && !extensions.iter().any(|extension| filename.ends_with(extension))
    {
        return Ok(());
    }

    let metadata = std::fs::metadata(path.as_std_path())?;
    let fingerprint = ImportedFileFingerprint {
        size_bytes: metadata.len(),
        modified_unix_ms: metadata
            .modified()
            .ok()
            .and_then(|modified| modified.duration_since(UNIX_EPOCH).ok())
            .map(|duration| duration.as_millis().min(u128::from(u64::MAX)) as u64),
    };

    let previous = state.file_state(path);
    let Some(change_kind) = detect_change_kind(previous, fingerprint) else {
        return Ok(());
    };

    let start_offset_bytes = match (change_kind, previous) {
        (ImportFileChangeKind::Appended, Some(previous)) => previous.imported_offset_bytes,
        _ => 0,
    };
    let start_line_number = match (change_kind, previous) {
        (ImportFileChangeKind::Appended, Some(previous)) => previous.imported_line_count,
        _ => 0,
    };

    discovered.push(DiscoveredFile {
        path: path.to_owned(),
        filename,
        fingerprint,
        start_offset_bytes,
        start_line_number,
        change_kind,
    });
    Ok(())
}

fn detect_change_kind(
    previous: Option<&ImportedFileState>,
    current: ImportedFileFingerprint,
) -> Option<ImportFileChangeKind> {
    let Some(previous) = previous else {
        return Some(ImportFileChangeKind::New);
    };

    if previous.fingerprint == current {
        return None;
    }

    if current.size_bytes >= previous.imported_offset_bytes
        && current.size_bytes >= previous.fingerprint.size_bytes
        && current.modified_unix_ms != previous.fingerprint.modified_unix_ms
    {
        return Some(ImportFileChangeKind::Appended);
    }

    Some(ImportFileChangeKind::Replaced)
}

pub fn read_file_content(file: &DiscoveredFile) -> Result<Vec<u8>, std::io::Error> {
    std::fs::read(file.path.as_std_path())
}

pub fn read_file_lines(file: &DiscoveredFile) -> Result<Vec<String>, std::io::Error> {
    use std::io::BufRead;

    let file_handle = std::fs::File::open(file.path.as_std_path())?;
    let reader = std::io::BufReader::new(file_handle);
    reader.lines().collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;
    use std::thread::sleep;
    use std::time::Duration;
    use tempfile::TempDir;

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

    #[test]
    fn scan_finds_new_files() {
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
    }

    #[test]
    fn scan_skips_unchanged_processed_files() {
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
    }

    #[test]
    fn scan_empty_directory() {
        let dir = TempDir::new().unwrap();
        let state = BatchImporterState::default();
        let files = scan_for_new_files(&state, utf8(dir.path()), &[]).unwrap();
        assert!(files.is_empty());
    }

    #[test]
    fn scan_no_extension_filter() {
        let dir = TempDir::new().unwrap();
        create_test_file(dir.path(), "data.json", "{}");
        create_test_file(dir.path(), "data.csv", "a,b");
        create_test_file(dir.path(), "notes.txt", "hello");

        let state = BatchImporterState::default();
        let files = scan_for_new_files(&state, utf8(dir.path()), &[]).unwrap();
        assert_eq!(files.len(), 3);
    }

    #[test]
    fn scan_missing_path() {
        let state = BatchImporterState::default();
        let path = Utf8Path::new("/tmp/sinex-test-nonexistent-batch-dir");
        assert!(matches!(
            scan_for_new_files(&state, path, &[]),
            Err(ScanError::PathNotFound(_))
        ));
    }

    #[test]
    fn read_file_content_works() {
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
    }

    #[test]
    fn read_file_lines_works() {
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
    }

    #[test]
    fn state_tracks_processed_files() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("file.json");
        create_test_file(dir.path(), "file.json", "{}");

        let mut state = BatchImporterState::default();
        state.mark_processed(utf8(&path), fingerprint_for(&path), 2, 1);
        assert_eq!(state.total_files_processed, 1);
        assert_eq!(state.total_bytes_processed, 2);
        assert_eq!(state.total_lines_processed, 1);
        assert!(state.file_state(utf8(&path)).is_some());
    }

    #[test]
    fn scan_detects_appended_file() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("history.ndjson");
        create_test_file(dir.path(), "history.ndjson", "{\"x\":1}\n");

        let mut state = BatchImporterState::default();
        state.mark_processed(utf8(&path), fingerprint_for(&path), 8, 1);

        sleep(Duration::from_millis(5));
        let mut file = std::fs::OpenOptions::new().append(true).open(&path).unwrap();
        writeln!(file, "{{\"x\":2}}").unwrap();

        let files = scan_for_new_files(&state, utf8(dir.path()), &[".ndjson"]).unwrap();
        assert_eq!(files.len(), 1);
        assert_eq!(files[0].change_kind, ImportFileChangeKind::Appended);
        assert_eq!(files[0].start_offset_bytes, 8);
        assert_eq!(files[0].start_line_number, 1);
    }

    #[test]
    fn scan_detects_replaced_file() {
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
    }
}
