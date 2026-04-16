use camino::{Utf8Path, Utf8PathBuf};
use serde::{Deserialize, Serialize};
use std::collections::BTreeSet;
use tracing::{debug, info};

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct BatchImporterState {
    pub processed_files: BTreeSet<String>,
    pub scan_directory: Option<Utf8PathBuf>,
    pub total_files_processed: u64,
    pub total_bytes_processed: u64,
}

impl BatchImporterState {
    pub fn new(scan_directory: impl Into<Utf8PathBuf>) -> Self {
        Self {
            scan_directory: Some(scan_directory.into()),
            ..Self::default()
        }
    }

    pub fn is_processed(&self, filename: &str) -> bool {
        self.processed_files.contains(filename)
    }

    pub fn mark_processed(&mut self, filename: String, bytes: u64) {
        self.processed_files.insert(filename);
        self.total_files_processed += 1;
        self.total_bytes_processed += bytes;
    }
}

#[derive(Debug)]
pub struct DiscoveredFile {
    pub path: Utf8PathBuf,
    pub filename: String,
    pub size: u64,
}

#[derive(Debug)]
pub enum ScanError {
    DirectoryNotFound(Utf8PathBuf),
    IoError(std::io::Error),
    NotADirectory(Utf8PathBuf),
}

impl std::fmt::Display for ScanError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::DirectoryNotFound(p) => write!(f, "scan directory not found: {p}"),
            Self::IoError(e) => write!(f, "I/O error during scan: {e}"),
            Self::NotADirectory(p) => write!(f, "path is not a directory: {p}"),
        }
    }
}

impl std::error::Error for ScanError {}

impl From<std::io::Error> for ScanError {
    fn from(e: std::io::Error) -> Self {
        Self::IoError(e)
    }
}

pub fn scan_for_new_files(
    state: &BatchImporterState,
    directory: &Utf8Path,
    extensions: &[&str],
) -> Result<Vec<DiscoveredFile>, ScanError> {
    if !directory.exists() {
        return Err(ScanError::DirectoryNotFound(directory.to_owned()));
    }
    if !directory.is_dir() {
        return Err(ScanError::NotADirectory(directory.to_owned()));
    }

    let mut discovered = Vec::new();

    for entry in std::fs::read_dir(directory.as_std_path())? {
        let entry = entry?;
        let path = entry.path();

        if !path.is_file() {
            continue;
        }

        let filename = match path.file_name().and_then(|n| n.to_str()) {
            Some(name) => name.to_string(),
            None => continue,
        };

        if !extensions.is_empty() {
            let matches_extension = extensions.iter().any(|ext| filename.ends_with(ext));
            if !matches_extension {
                continue;
            }
        }

        if state.is_processed(&filename) {
            continue;
        }

        let size = entry.metadata().map_or(0, |m| m.len());
        let utf8_path = Utf8PathBuf::from_path_buf(path)
            .unwrap_or_else(|p| Utf8PathBuf::from(p.to_string_lossy().to_string()));

        discovered.push(DiscoveredFile {
            path: utf8_path,
            filename,
            size,
        });
    }

    discovered.sort_by(|a, b| a.filename.cmp(&b.filename));

    if !discovered.is_empty() {
        info!(
            directory = %directory,
            new_files = discovered.len(),
            total_bytes = discovered.iter().map(|f| f.size).sum::<u64>(),
            "Discovered new files for import"
        );
    } else {
        debug!(directory = %directory, "No new files to import");
    }

    Ok(discovered)
}

pub fn read_file_content(file: &DiscoveredFile) -> Result<Vec<u8>, std::io::Error> {
    std::fs::read(file.path.as_std_path())
}

pub fn read_file_lines(file: &DiscoveredFile) -> Result<Vec<String>, std::io::Error> {
    use std::io::BufRead;
    let f = std::fs::File::open(file.path.as_std_path())?;
    let reader = std::io::BufReader::new(f);
    reader.lines().collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;
    use tempfile::TempDir;

    fn create_test_file(dir: &std::path::Path, name: &str, content: &str) {
        let path = dir.join(name);
        let mut f = std::fs::File::create(&path).unwrap();
        f.write_all(content.as_bytes()).unwrap();
    }

    fn utf8(p: &std::path::Path) -> &Utf8Path {
        Utf8Path::from_path(p).expect("non-utf8 temp path")
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
        assert_eq!(files[1].filename, "data2.json");
    }

    #[test]
    fn scan_skips_processed_files() {
        let dir = TempDir::new().unwrap();
        create_test_file(dir.path(), "data1.json", "{}");
        create_test_file(dir.path(), "data2.json", "{}");

        let mut state = BatchImporterState::default();
        state.mark_processed("data1.json".to_string(), 2);

        let files = scan_for_new_files(&state, utf8(dir.path()), &[".json"]).unwrap();
        assert_eq!(files.len(), 1);
        assert_eq!(files[0].filename, "data2.json");
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
    fn scan_missing_directory() {
        let state = BatchImporterState::default();
        let path = Utf8Path::new("/tmp/sinex-test-nonexistent-batch-dir");
        assert!(matches!(
            scan_for_new_files(&state, path, &[]),
            Err(ScanError::DirectoryNotFound(_))
        ));
    }

    #[test]
    fn read_file_content_works() {
        let dir = TempDir::new().unwrap();
        create_test_file(dir.path(), "test.json", r#"{"hello":"world"}"#);

        let file = DiscoveredFile {
            path: Utf8PathBuf::from_path_buf(dir.path().join("test.json")).unwrap(),
            filename: "test.json".to_string(),
            size: 17,
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

        let file = DiscoveredFile {
            path: Utf8PathBuf::from_path_buf(dir.path().join("lines.txt")).unwrap(),
            filename: "lines.txt".to_string(),
            size: 18,
        };
        let lines = read_file_lines(&file).unwrap();
        assert_eq!(lines, vec!["line1", "line2", "line3"]);
    }

    #[test]
    fn state_tracks_processed_files() {
        let mut state = BatchImporterState::default();
        assert!(!state.is_processed("file.json"));
        state.mark_processed("file.json".to_string(), 100);
        assert!(state.is_processed("file.json"));
        assert_eq!(state.total_files_processed, 1);
        assert_eq!(state.total_bytes_processed, 100);
    }
}
