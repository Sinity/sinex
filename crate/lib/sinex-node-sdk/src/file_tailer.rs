use camino::{Utf8Path, Utf8PathBuf};
use std::io::{BufRead, BufReader, Seek, SeekFrom};
use tracing::{debug, info, warn};

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct FileTailerState {
    pub path: Utf8PathBuf,
    pub byte_offset: u64,
    pub inode: Option<u64>,
    pub lines_read: u64,
}

impl FileTailerState {
    pub fn new(path: impl Into<Utf8PathBuf>) -> Self {
        Self {
            path: path.into(),
            byte_offset: 0,
            inode: None,
            lines_read: 0,
        }
    }

    pub fn from_checkpoint(path: impl Into<Utf8PathBuf>, byte_offset: u64) -> Self {
        Self {
            path: path.into(),
            byte_offset,
            inode: None,
            lines_read: 0,
        }
    }
}

#[derive(Debug)]
pub enum TailError {
    FileNotFound(Utf8PathBuf),
    IoError(std::io::Error),
    FileRotated {
        old_inode: u64,
        new_inode: u64,
        path: Utf8PathBuf,
    },
    FileTruncated {
        old_offset: u64,
        new_size: u64,
        path: Utf8PathBuf,
    },
}

impl std::fmt::Display for TailError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::FileNotFound(p) => write!(f, "file not found: {p}"),
            Self::IoError(e) => write!(f, "I/O error: {e}"),
            Self::FileRotated {
                old_inode,
                new_inode,
                path,
            } => write!(
                f,
                "file rotated: {path} (inode {old_inode} -> {new_inode})"
            ),
            Self::FileTruncated {
                old_offset,
                new_size,
                path,
            } => write!(
                f,
                "file truncated: {path} (offset {old_offset}, new size {new_size})"
            ),
        }
    }
}

impl std::error::Error for TailError {}

impl From<std::io::Error> for TailError {
    fn from(e: std::io::Error) -> Self {
        Self::IoError(e)
    }
}

pub struct TailResult {
    pub lines: Vec<String>,
    pub new_offset: u64,
    pub bytes_read: u64,
}

#[cfg(target_os = "linux")]
fn file_inode(path: &Utf8Path) -> Option<u64> {
    use std::os::unix::fs::MetadataExt;
    std::fs::metadata(path.as_std_path())
        .ok()
        .map(|m| m.ino())
}

#[cfg(not(target_os = "linux"))]
fn file_inode(_path: &Utf8Path) -> Option<u64> {
    None
}

pub fn tail_lines(state: &mut FileTailerState, max_lines: usize) -> Result<TailResult, TailError> {
    let path = &state.path;

    if !path.exists() {
        return Err(TailError::FileNotFound(path.clone()));
    }

    let current_inode = file_inode(path);

    if let (Some(old), Some(new)) = (state.inode, current_inode) {
        if old != new {
            let rotated = TailError::FileRotated {
                old_inode: old,
                new_inode: new,
                path: path.clone(),
            };
            warn!(%rotated, "File rotated, resetting to beginning");
            state.byte_offset = 0;
            state.inode = current_inode;
        }
    }

    let file = std::fs::File::open(path.as_std_path())?;
    let file_len = file.metadata()?.len();

    if file_len < state.byte_offset {
        let truncated = TailError::FileTruncated {
            old_offset: state.byte_offset,
            new_size: file_len,
            path: path.clone(),
        };
        warn!(%truncated, "File truncated, resetting to beginning");
        state.byte_offset = 0;
    }

    if file_len == state.byte_offset {
        return Ok(TailResult {
            lines: Vec::new(),
            new_offset: state.byte_offset,
            bytes_read: 0,
        });
    }

    let mut reader = BufReader::new(file);
    reader.seek(SeekFrom::Start(state.byte_offset))?;

    let mut lines = Vec::new();
    let start_offset = state.byte_offset;

    for _ in 0..max_lines {
        let mut line = String::new();
        let bytes = reader.read_line(&mut line)?;
        if bytes == 0 {
            break;
        }
        state.byte_offset += bytes as u64;

        let trimmed = line.trim_end_matches('\n').trim_end_matches('\r');
        if !trimmed.is_empty() {
            lines.push(trimmed.to_string());
            state.lines_read += 1;
        }
    }

    let bytes_read = state.byte_offset - start_offset;
    state.inode = current_inode;

    if !lines.is_empty() {
        debug!(
            path = %state.path,
            lines = lines.len(),
            bytes = bytes_read,
            offset = state.byte_offset,
            "Tailed new lines"
        );
    }

    Ok(TailResult {
        new_offset: state.byte_offset,
        bytes_read,
        lines,
    })
}

pub fn tail_all_lines(state: &mut FileTailerState) -> Result<TailResult, TailError> {
    tail_lines(state, usize::MAX)
}

pub fn check_file_ready(path: &Utf8Path) -> Result<u64, TailError> {
    if !path.exists() {
        return Err(TailError::FileNotFound(path.to_owned()));
    }
    let meta = std::fs::metadata(path.as_std_path())?;
    info!(path = %path, size = meta.len(), "File ready for tailing");
    Ok(meta.len())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;
    use tempfile::NamedTempFile;

    fn utf8_path(path: &std::path::Path) -> Utf8PathBuf {
        Utf8PathBuf::from_path_buf(path.to_path_buf())
            .unwrap_or_else(|p| panic!("non-utf8 path: {}", p.display()))
    }

    #[test]
    fn tail_empty_file() {
        let file = NamedTempFile::new().unwrap();
        let path = utf8_path(file.path());
        let mut state = FileTailerState::new(&path);
        let result = tail_lines(&mut state, 100).unwrap();
        assert!(result.lines.is_empty());
        assert_eq!(result.bytes_read, 0);
    }

    #[test]
    fn tail_reads_new_lines() {
        let mut file = NamedTempFile::new().unwrap();
        writeln!(file, "line1").unwrap();
        writeln!(file, "line2").unwrap();
        writeln!(file, "line3").unwrap();
        file.flush().unwrap();

        let path = utf8_path(file.path());
        let mut state = FileTailerState::new(&path);
        let result = tail_lines(&mut state, 100).unwrap();

        assert_eq!(result.lines.len(), 3);
        assert_eq!(result.lines[0], "line1");
        assert_eq!(result.lines[1], "line2");
        assert_eq!(result.lines[2], "line3");
        assert_eq!(state.byte_offset, result.new_offset);
    }

    #[test]
    fn tail_resumes_from_offset() {
        let mut file = NamedTempFile::new().unwrap();
        writeln!(file, "line1").unwrap();
        writeln!(file, "line2").unwrap();
        file.flush().unwrap();

        let path = utf8_path(file.path());
        let mut state = FileTailerState::new(&path);
        let _ = tail_lines(&mut state, 100).unwrap();

        writeln!(file, "line3").unwrap();
        writeln!(file, "line4").unwrap();
        file.flush().unwrap();

        let result = tail_lines(&mut state, 100).unwrap();
        assert_eq!(result.lines.len(), 2);
        assert_eq!(result.lines[0], "line3");
        assert_eq!(result.lines[1], "line4");
    }

    #[test]
    fn tail_respects_max_lines() {
        let mut file = NamedTempFile::new().unwrap();
        for i in 0..100 {
            writeln!(file, "line{i}").unwrap();
        }
        file.flush().unwrap();

        let path = utf8_path(file.path());
        let mut state = FileTailerState::new(&path);
        let result = tail_lines(&mut state, 10).unwrap();

        assert_eq!(result.lines.len(), 10);
        assert_eq!(result.lines[0], "line0");
        assert_eq!(result.lines[9], "line9");

        let result2 = tail_lines(&mut state, 10).unwrap();
        assert_eq!(result2.lines.len(), 10);
        assert_eq!(result2.lines[0], "line10");
    }

    #[test]
    fn tail_handles_truncation() {
        let mut file = NamedTempFile::new().unwrap();
        writeln!(file, "long line with lots of content").unwrap();
        file.flush().unwrap();

        let path = utf8_path(file.path());
        let mut state = FileTailerState::new(&path);
        let _ = tail_lines(&mut state, 100).unwrap();

        file.as_file().set_len(0).unwrap();
        writeln!(file, "new").unwrap();
        file.flush().unwrap();

        let result = tail_lines(&mut state, 100).unwrap();
        assert_eq!(result.lines.len(), 1);
        assert_eq!(result.lines[0], "new");
    }

    #[test]
    fn tail_no_new_data_returns_empty() {
        let mut file = NamedTempFile::new().unwrap();
        writeln!(file, "line1").unwrap();
        file.flush().unwrap();

        let path = utf8_path(file.path());
        let mut state = FileTailerState::new(&path);
        let _ = tail_lines(&mut state, 100).unwrap();

        let result = tail_lines(&mut state, 100).unwrap();
        assert!(result.lines.is_empty());
        assert_eq!(result.bytes_read, 0);
    }

    #[test]
    fn check_file_ready_returns_size() {
        let mut file = NamedTempFile::new().unwrap();
        writeln!(file, "hello").unwrap();
        file.flush().unwrap();

        let path = utf8_path(file.path());
        let size = check_file_ready(&path).unwrap();
        assert!(size > 0);
    }

    #[test]
    fn check_file_ready_errors_on_missing() {
        let path = Utf8PathBuf::from("/tmp/sinex-test-nonexistent-12345678.txt");
        assert!(matches!(
            check_file_ready(&path),
            Err(TailError::FileNotFound(_))
        ));
    }
}
