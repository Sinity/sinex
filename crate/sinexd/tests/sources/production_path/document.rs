//! Production-path obligation tests for the `document` domain (Wave B).
//!
//! Source contracts covered:
//! - `document.staging`     (`FileDrop` + `DocumentStagingParser` → `document.ingested`)
//! - `docs-library-index`   (`DirectoryWalkAdapter` + `DocsLibraryParser` → `document.indexed`)

#[cfg(test)]
#[path = "document_test.rs"]
mod tests;
