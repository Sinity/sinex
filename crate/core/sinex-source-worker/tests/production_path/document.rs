//! Production-path obligation tests for the `document` domain (Wave B).
//!
//! Source units covered:
//! - `document.staging`     (FileDrop + DocumentStagingParser → `document.ingested`)
//! - `docs-library-index`   (DirectoryWalkAdapter + DocsLibraryParser → `document.indexed`)
//!
//! ## Harness note for `docs-library-index`
//!
//! `DocsLibraryParser::parse_record` requires a `MaterialAnchor::DirectoryEntry`
//! to extract the file path and MIME type. The `default_parser_dispatch()` used
//! by `_run_case` always constructs a `ByteRange` anchor, so the behaviour
//! obligations (initial_ingestion, replay, drain, isolation, privacy) cannot be
//! driven through the shared harness today.
//!
//! Instead this file verifies:
//! - The source unit descriptor is registered in the inventory.
//! - The parser is registered and reachable via `find_parser_factory`.
//! - The node factory is registered and reachable via `find_node_factory`.
//!
//! Full behaviour coverage lives in the unit tests inside
//! `crate/core/sinex-source-worker/src/sources/library.rs`.  When the harness
//! gains `DirectoryEntry` anchor support, replace the structural tests below
//! with a `_run_case("docs-library-index", ...)` call.

#[cfg(test)]
mod tests {
    use xtask::sandbox::prelude::*;

    // -------------------------------------------------------------------------
    // Fixtures
    // -------------------------------------------------------------------------

    /// `document.staging` receives the UTF-8 path of a file to ingest.
    /// `/etc/hostname` always exists and has a deterministic MIME type
    /// (`text/plain`) so MIME detection produces consistent output.
    const DOCUMENT_STAGING_FIXTURE: &[u8] = b"/etc/hostname";

    // -------------------------------------------------------------------------
    // document.staging
    // -------------------------------------------------------------------------

    #[sinex_test]
    async fn document_staging_obligations(_ctx: TestContext) -> TestResult<()> {
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

    // -------------------------------------------------------------------------
    // docs-library-index — structural coverage
    //
    // The dispatch harness cannot exercise this parser directly because it
    // requires a DirectoryEntry anchor (see module-level doc).  Until the
    // harness gains that capability, verify descriptor + parser + node-factory
    // registration here and rely on the unit tests in library.rs for behaviour.
    // -------------------------------------------------------------------------

    #[sinex_test]
    async fn docs_library_index_descriptor_registered(_ctx: TestContext) -> TestResult<()> {
        use sinex_primitives::parser::SourceUnitId;
        use sinex_source_worker::registry::SourceUnitRegistry;

        let registry = SourceUnitRegistry::from_inventory();
        let id = SourceUnitId::new("docs-library-index").unwrap();
        let descriptor = registry.find(&id);

        assert!(
            descriptor.is_some(),
            "docs-library-index descriptor must be registered in inventory"
        );

        let d = descriptor.unwrap();
        assert_eq!(d.id, "docs-library-index");
        assert_eq!(d.namespace, "library");

        let event_types: Vec<&str> = d.event_types.iter().map(|(_, t)| *t).collect();
        assert!(
            event_types.contains(&"document.indexed"),
            "docs-library-index must declare document.indexed; got {event_types:?}"
        );

        Ok(())
    }

    #[sinex_test]
    async fn docs_library_index_parser_registered(_ctx: TestContext) -> TestResult<()> {
        use sinex_primitives::parser::SourceUnitId;
        use sinex_source_worker::dispatch::find_parser_factory;

        let id = SourceUnitId::new("docs-library-index").unwrap();
        let factory = find_parser_factory(&id);

        assert!(
            factory.is_some(),
            "docs-library-index must have a parser registered via register_parser!"
        );

        Ok(())
    }

    #[sinex_test]
    async fn docs_library_index_factory_registered(_ctx: TestContext) -> TestResult<()> {
        use sinex_primitives::parser::SourceUnitId;
        use sinex_source_worker::node_factory::find_node_factory;

        let id = SourceUnitId::new("docs-library-index").unwrap();
        let factory = find_node_factory(&id);

        assert!(
            factory.is_some(),
            "docs-library-index must have a node factory registered (adapter-ingestor path)"
        );

        Ok(())
    }
}
