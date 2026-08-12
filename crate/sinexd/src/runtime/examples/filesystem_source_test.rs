use super::{FilesystemSource, Utf8PathBuf};
use crate::runtime::exploration::{ExplorationProvider, ExportFormat};
use sinex_primitives::SanitizedPath;
use xtask::sandbox::sinex_test;

#[sinex_test]
async fn example_ingestion_history_is_explicitly_unavailable() -> xtask::sandbox::TestResult<()>
{
    let source = FilesystemSource::new(Vec::<Utf8PathBuf>::new());

    let error = ExplorationProvider::get_ingestion_history(&source, 10)
        .expect_err("example must not report empty ingestion history as success");

    assert!(error.to_string().contains("example source"));
    assert!(error.to_string().contains("filesystem"));
    assert!(error.to_string().contains("ingestion history"));
    Ok(())
}

#[sinex_test]
async fn example_export_is_explicitly_unavailable() -> xtask::sandbox::TestResult<()> {
    let source = FilesystemSource::new(Vec::<Utf8PathBuf>::new());
    let path = SanitizedPath::from_static("/tmp/filesystem-example-export.json");

    let error = ExplorationProvider::export_data(&source, &path, ExportFormat::Json)
        .expect_err("example must not report export success without writing data");

    assert!(error.to_string().contains("example source"));
    assert!(error.to_string().contains("filesystem"));
    assert!(error.to_string().contains("data export"));
    Ok(())
}

/// sinex-t7fx: `FilesystemSourceConfig`'s fields (max_files, include/exclude_extensions,
/// follow_symlinks) are entirely dead. `initialize()` destructures its parsed config as
/// `_config` (discarded) and `scan_directory_simple` takes no config parameter at all --
/// so a config that would restrict the scan to 1 file has no effect on the actual count.
#[sinex_test]
#[ignore = "sinex-t7fx open: FilesystemSourceConfig's max_files/include_extensions/\
            exclude_extensions/follow_symlinks are never threaded into the scan"]
async fn scan_directory_simple_ignores_config_based_filters() -> xtask::sandbox::TestResult<()> {
    let dir = tempfile::tempdir()?;
    tokio::fs::write(dir.path().join("a.txt"), b"x").await?;
    tokio::fs::write(dir.path().join("b.bin"), b"y").await?;
    tokio::fs::write(dir.path().join("c.bin"), b"z").await?;

    let dir_path = Utf8PathBuf::from_path_buf(dir.path().to_path_buf())
        .expect("tempdir path must be valid UTF-8");
    let source = FilesystemSource::new(vec![dir_path.clone()]);
    let checkpoint = crate::runtime::stream::Checkpoint::default();

    // A config restricting to include_extensions=["txt"], max_files=1 would allow only
    // "a.txt" -- but config is never threaded into the scan, so it always counts all 3.
    // scan_directory_simple is private to `super`, reachable from this child test module.
    let count = source
        .scan_directory_simple(&dir_path, &checkpoint, false)
        .await?;

    assert_eq!(
        count, 1,
        "sinex-t7fx: FilesystemSourceConfig's include_extensions/max_files should have \
         restricted this scan to 1 matching file, but scan_directory_simple counted all \
         {count} files -- config fields are dead"
    );
    Ok(())
}
