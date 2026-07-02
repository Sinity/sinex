use std::io::Write;

use tempfile::TempDir;
use xtask::sandbox::prelude::*;

// -------------------------------------------------------------------------
// Fixtures
// -------------------------------------------------------------------------

/// `document.staging` receives the UTF-8 path of a file to ingest.
/// `/etc/hostname` always exists and has a deterministic MIME type
/// (`text/plain`) so MIME detection produces consistent output.
const DOCUMENT_STAGING_FIXTURE: &[u8] = b"/etc/hostname";

const DOCUMENT_STAGING_CASE: crate::ProductionPathCase = crate::ProductionPathCase::new(
    "document.staging",
    "document.staging",
    crate::AdapterKind::FileDrop,
    DOCUMENT_STAGING_FIXTURE,
    &["document.ingested"],
);

// -------------------------------------------------------------------------
// document.staging
// -------------------------------------------------------------------------

crate::production_path_case_test!(document_staging_obligations, DOCUMENT_STAGING_CASE);

#[sinex_test]
async fn docs_library_index_directory_entry_obligations() -> TestResult<()> {
    let dir = TempDir::new()?;
    let file = dir.path().join("Jane Doe - Practical Notes (2026).pdf");
    let mut handle = std::fs::File::create(&file)?;
    handle.write_all(b"document library fixture")?;
    drop(handle);

    let path = camino::Utf8PathBuf::from_path_buf(file)
        .map_err(|path| color_eyre::eyre::eyre!("fixture path is not UTF-8: {path:?}"))?;
    let failures = crate::_run_case_with_directory_entry(
        "docs-library-index",
        crate::AdapterKind::StaticFile,
        b"document library fixture",
        path.as_str(),
        Some("blake3:test-document-library-fixture"),
        &["document.indexed"],
        crate::ALL_OBLIGATIONS,
    )
    .await;
    assert!(
        failures.is_empty(),
        "docs-library-index obligations failed: {failures:#?}"
    );

    Ok(())
}
