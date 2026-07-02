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
