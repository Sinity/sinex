use camino::{Utf8Path, Utf8PathBuf};
use tokio::{
    fs,
    io::{AsyncReadExt, AsyncSeekExt},
};
use tracing::debug;

#[derive(Debug, Clone, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct AppendOnlyFileState {
    #[serde(default)]
    pub offset_bytes: u64,
    #[cfg(unix)]
    #[serde(default)]
    pub inode: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AppendOnlyFileChange {
    Unchanged,
    Rotated { old_inode: u64, new_inode: u64 },
    TruncatedRestarted { previous_offset: u64, new_size: u64 },
    TruncatedAdvancedToEnd { previous_offset: u64, new_size: u64 },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AppendOnlyFileLine {
    pub text: String,
    pub start_offset_bytes: u64,
    pub end_offset_bytes: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AppendOnlyFilePollResult {
    pub file_size: u64,
    pub bytes_consumed: u64,
    pub records: Vec<AppendOnlyFileLine>,
    pub state: AppendOnlyFileState,
    pub change: AppendOnlyFileChange,
}

#[derive(Debug)]
pub enum TailError {
    FileNotFound(Utf8PathBuf),
    IoError(std::io::Error),
}

impl std::fmt::Display for TailError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::FileNotFound(path) => write!(f, "file not found: {path}"),
            Self::IoError(error) => write!(f, "I/O error: {error}"),
        }
    }
}

impl std::error::Error for TailError {}

impl From<std::io::Error> for TailError {
    fn from(error: std::io::Error) -> Self {
        Self::IoError(error)
    }
}

#[cfg(unix)]
fn current_inode(metadata: &std::fs::Metadata) -> u64 {
    use std::os::unix::fs::MetadataExt;

    metadata.ino()
}

async fn read_new_segment(path: &Utf8Path, offset_bytes: u64) -> Result<String, TailError> {
    use std::io::SeekFrom;

    let mut file = fs::File::open(path).await?;
    file.seek(SeekFrom::Start(offset_bytes)).await?;

    let mut buffer = Vec::new();
    file.read_to_end(&mut buffer).await?;
    Ok(String::from_utf8_lossy(&buffer).to_string())
}

fn detect_change(
    state: &mut AppendOnlyFileState,
    file_size: u64,
    metadata: &std::fs::Metadata,
) -> AppendOnlyFileChange {
    #[cfg(unix)]
    {
        let new_inode = current_inode(metadata);
        let change = if file_size < state.offset_bytes {
            match state.inode {
                Some(old_inode) if old_inode != new_inode => {
                    state.offset_bytes = 0;
                    AppendOnlyFileChange::Rotated {
                        old_inode,
                        new_inode,
                    }
                }
                _ => {
                    let previous_offset = state.offset_bytes;
                    state.offset_bytes = file_size;
                    AppendOnlyFileChange::TruncatedAdvancedToEnd {
                        previous_offset,
                        new_size: file_size,
                    }
                }
            }
        } else {
            AppendOnlyFileChange::Unchanged
        };
        state.inode = Some(new_inode);
        change
    }

    #[cfg(not(unix))]
    {
        if file_size < state.offset_bytes {
            let previous_offset = state.offset_bytes;
            state.offset_bytes = 0;
            AppendOnlyFileChange::TruncatedRestarted {
                previous_offset,
                new_size: file_size,
            }
        } else {
            AppendOnlyFileChange::Unchanged
        }
    }
}

pub async fn poll_utf8_lines(
    path: &Utf8Path,
    mut state: AppendOnlyFileState,
) -> Result<AppendOnlyFilePollResult, TailError> {
    let metadata = fs::metadata(path)
        .await
        .map_err(|error| match error.kind() {
            std::io::ErrorKind::NotFound => TailError::FileNotFound(path.to_path_buf()),
            _ => TailError::IoError(error),
        })?;
    let file_size = metadata.len();
    let change = detect_change(&mut state, file_size, &metadata);

    if file_size == state.offset_bytes {
        return Ok(AppendOnlyFilePollResult {
            file_size,
            bytes_consumed: 0,
            records: Vec::new(),
            state,
            change,
        });
    }

    let new_segment = read_new_segment(path, state.offset_bytes).await?;
    if new_segment.is_empty() {
        return Ok(AppendOnlyFilePollResult {
            file_size,
            bytes_consumed: 0,
            records: Vec::new(),
            state,
            change,
        });
    }

    let segment_start_offset = state.offset_bytes;
    let mut records = Vec::new();
    let mut bytes_consumed = 0u64;

    for line in new_segment.split_inclusive('\n') {
        if !line.ends_with('\n') && new_segment.ends_with(line) {
            break;
        }

        let trimmed = line.trim_end_matches('\n').trim_end_matches('\r');
        bytes_consumed += line.len() as u64;
        if !trimmed.is_empty() {
            records.push(AppendOnlyFileLine {
                text: trimmed.to_string(),
                start_offset_bytes: segment_start_offset
                    .saturating_add(bytes_consumed)
                    .saturating_sub(line.len() as u64),
                end_offset_bytes: segment_start_offset.saturating_add(bytes_consumed),
            });
        }
    }

    if bytes_consumed > 0 {
        state.offset_bytes = state.offset_bytes.saturating_add(bytes_consumed);
    }

    if !records.is_empty() || !matches!(change, AppendOnlyFileChange::Unchanged) {
        debug!(
            path = %path,
            file_size,
            bytes_consumed,
            lines = records.len(),
            offset = state.offset_bytes,
            change = ?change,
            "Polled append-only file progress"
        );
    }

    Ok(AppendOnlyFilePollResult {
        file_size,
        bytes_consumed,
        records,
        state,
        change,
    })
}
