use camino::Utf8PathBuf;
use sinex_node_sdk::{AppendOnlyFileChange, AppendOnlyFileState, poll_utf8_lines};
use tempfile::tempdir;
use tokio::{fs, io::AsyncWriteExt};
use xtask::sandbox::prelude::*;

fn utf8_path(path: &std::path::Path) -> Utf8PathBuf {
    Utf8PathBuf::from_path_buf(path.to_path_buf())
        .unwrap_or_else(|path| panic!("non-utf8 path: {}", path.display()))
}

fn lines(result: &sinex_node_sdk::AppendOnlyFilePollResult) -> Vec<&str> {
    result
        .records
        .iter()
        .map(|record| record.text.as_str())
        .collect()
}

#[sinex_test]
async fn poll_preserves_partial_trailing_line(_ctx: TestContext) -> TestResult<()> {
    let dir = tempdir()?;
    let path = utf8_path(&dir.path().join("history.log"));

    fs::write(&path, "echo one\necho two").await?;

    let first = poll_utf8_lines(&path, AppendOnlyFileState::default()).await?;
    assert_eq!(lines(&first), vec!["echo one"]);
    assert_eq!(first.records[0].start_offset_bytes, 0);
    assert_eq!(first.records[0].end_offset_bytes, "echo one\n".len() as u64);
    assert_eq!(first.bytes_consumed, "echo one\n".len() as u64);
    assert_eq!(first.state.offset_bytes, "echo one\n".len() as u64);
    assert!(matches!(first.change, AppendOnlyFileChange::Unchanged));

    let mut file = fs::OpenOptions::new().append(true).open(&path).await?;
    file.write_all(b"\necho three\n").await?;
    file.flush().await?;

    let second = poll_utf8_lines(&path, first.state).await?;
    assert_eq!(lines(&second), vec!["echo two", "echo three"]);
    assert_eq!(
        second.records[0].start_offset_bytes,
        "echo one\n".len() as u64
    );
    assert_eq!(
        second.records[0].end_offset_bytes,
        "echo one\necho two\n".len() as u64
    );
    assert_eq!(
        second.records[1].end_offset_bytes,
        "echo one\necho two\necho three\n".len() as u64
    );
    assert_eq!(
        second.state.offset_bytes,
        "echo one\necho two\necho three\n".len() as u64
    );

    Ok(())
}

#[cfg(unix)]
#[sinex_test]
async fn poll_detects_rotation_and_restarts_from_new_file(_ctx: TestContext) -> TestResult<()> {
    let dir = tempdir()?;
    let path = utf8_path(&dir.path().join("history.log"));
    let rotated = dir.path().join("history.log.1");

    fs::write(&path, "echo one\necho two\n").await?;
    let first = poll_utf8_lines(&path, AppendOnlyFileState::default()).await?;
    assert_eq!(lines(&first), vec!["echo one", "echo two"]);

    fs::rename(&path, &rotated).await?;
    fs::write(&path, "echo rotated\n").await?;

    let second = poll_utf8_lines(&path, first.state).await?;
    assert!(matches!(
        second.change,
        AppendOnlyFileChange::Rotated { .. }
    ));
    assert_eq!(lines(&second), vec!["echo rotated"]);
    assert_eq!(second.state.offset_bytes, "echo rotated\n".len() as u64);

    Ok(())
}

#[cfg(unix)]
#[sinex_test]
async fn poll_advances_to_end_after_same_inode_truncation(_ctx: TestContext) -> TestResult<()> {
    let dir = tempdir()?;
    let path = utf8_path(&dir.path().join("history.log"));

    fs::write(&path, "echo one\necho two\n").await?;
    let first = poll_utf8_lines(&path, AppendOnlyFileState::default()).await?;

    let mut file = fs::OpenOptions::new()
        .write(true)
        .truncate(true)
        .open(&path)
        .await?;
    file.write_all(b"replacement\n").await?;
    file.flush().await?;

    let second = poll_utf8_lines(&path, first.state).await?;
    assert!(matches!(
        second.change,
        AppendOnlyFileChange::TruncatedAdvancedToEnd { .. }
    ));
    assert!(second.records.is_empty());
    assert_eq!(second.state.offset_bytes, "replacement\n".len() as u64);

    Ok(())
}

#[cfg(not(unix))]
#[sinex_test]
async fn poll_restarts_from_beginning_after_truncation(_ctx: TestContext) -> TestResult<()> {
    let dir = tempdir()?;
    let path = utf8_path(&dir.path().join("history.log"));

    fs::write(&path, "echo one\necho two\n").await?;
    let first = poll_utf8_lines(&path, AppendOnlyFileState::default()).await?;

    let mut file = fs::OpenOptions::new()
        .write(true)
        .truncate(true)
        .open(&path)
        .await?;
    file.write_all(b"replacement\n").await?;
    file.flush().await?;

    let second = poll_utf8_lines(&path, first.state).await?;
    assert!(matches!(
        second.change,
        AppendOnlyFileChange::TruncatedRestarted { .. }
    ));
    assert_eq!(lines(&second), vec!["replacement"]);

    Ok(())
}
