use super::{child_running, stream_reader_lines};
use xtask::sandbox::sinex_test;

#[sinex_test]
async fn test_stream_reader_lines_collects_utf8_lines() -> ::xtask::sandbox::TestResult<()> {
    use parking_lot::Mutex;
    use tokio::io::AsyncWriteExt;

    let (reader, mut writer) = tokio::io::duplex(64);
    writer.write_all(b"alpha\nbeta\n").await?;
    drop(writer);

    let collected = std::sync::Arc::new(Mutex::new(Vec::new()));
    let collected_clone = collected.clone();
    stream_reader_lines(reader, "test stdout", move |line| {
        collected_clone.lock().push(line);
    })
    .await?;

    assert_eq!(
        *collected.lock(),
        vec!["alpha".to_string(), "beta".to_string()]
    );
    Ok(())
}

#[sinex_test]
async fn test_stream_reader_lines_surfaces_invalid_utf8() -> ::xtask::sandbox::TestResult<()> {
    use tokio::io::AsyncWriteExt;

    let (reader, mut writer) = tokio::io::duplex(64);
    writer.write_all(&[0xff, b'\n']).await?;
    drop(writer);

    let error = stream_reader_lines(reader, "build stdout", |_| {})
        .await
        .expect_err("invalid utf8 should surface");
    let message = format!("{error:#}");
    assert!(message.contains("failed to read build stdout"));
    assert!(message.contains("valid UTF-8"));
    Ok(())
}

#[sinex_test]
async fn test_child_running_reports_exited_process() -> ::xtask::sandbox::TestResult<()> {
    let mut child = tokio::process::Command::new("sh")
        .arg("-c")
        .arg("exit 0")
        .spawn()?;
    child.wait().await?;

    assert!(!child_running(&mut child, "test child")?);
    Ok(())
}
