//! Production-path obligation tests for the `document` domain (Wave B).
//!
//! Tests the `document.staging` source unit registered in
//! `sinex_source_worker::sources::document::staging`.

#[cfg(test)]
mod tests {
    use xtask::sandbox::prelude::*;

    /// A minimal fixture for `document.staging`: a UTF-8 file path that
    /// the `DocumentStagingParser` will treat as the document path.
    /// Uses a well-known path that always exists so MIME detection works.
    const DOCUMENT_STAGING_FIXTURE: &[u8] = b"/etc/hostname";

    #[sinex_test]
    async fn document_staging_initial_ingestion(_ctx: TestContext) -> TestResult<()> {
        let failures = crate::_run_case(
            "document.staging",
            crate::AdapterKind::FileDrop,
            DOCUMENT_STAGING_FIXTURE,
            &["document.ingested"],
            crate::ALL_OBLIGATIONS,
        )
        .await;
        assert!(
            failures.is_empty(),
            "document.staging obligations failed: {failures:#?}"
        );
        Ok(())
    }
}
