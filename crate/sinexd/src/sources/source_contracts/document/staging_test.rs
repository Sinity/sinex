//! Regression coverage for sinex-audit-docstaging-context-mismatch.
//!
//! `document.staging` declares `PrivacyTier::Secret` and flags `file_path` as
//! `SourcePath`/`PotentiallySensitive`, so downloaded filenames that happen to
//! embed PII-shaped text (e.g. a statement export named after an SSN) must be
//! scanned by the Document-scoped PII recognizers (ssn/phone/pesel/nip/regon
//! in `sinex_primitives::privacy::catalog`). Those recognizers scope
//! themselves to `ProcessingContext::Document` and never `Metadata`; before
//! this fix, `manifest()` and the actual emitted intent stamped `Metadata`
//! while `SourceMeta` declared `Document`, so the mismatch silently exempted
//! `document.staging` events from PII scanning wherever a caller honors the
//! per-event `ProcessingContext` (as the catalog's own `PatternRule::contexts`
//! scoping is designed to be honored).

use super::*;
use sinex_primitives::Uuid;
use sinex_primitives::ids::Id;
use sinex_primitives::parser::MaterialAnchor;
use sinex_primitives::privacy::{CategorySet, PrivacyConfig, PrivacyEngine};

use xtask::sandbox::prelude::sinex_test;

fn test_ctx() -> ParserContext {
    ParserContext {
        source_id: SourceId::from_static("document.staging"),
        source_material_id: Id::new(),
        record_anchor: MaterialAnchor::ByteRange { start: 0, len: 0 },
        operation_id: Uuid::new_v4(),
        job_id: Uuid::new_v4(),
        host: "test-host".into(),
        acquisition_time: Timestamp::now(),
    }
}

fn record_for(path: &str) -> SourceRecord {
    SourceRecord {
        material_id: Id::new(),
        anchor: MaterialAnchor::ByteRange {
            start: 0,
            len: path.len() as u64,
        },
        bytes: path.as_bytes().to_vec(),
        logical_path: None,
        source_ts_hint: None,
        metadata: serde_json::Value::Null,
    }
}

/// A filename embedding a checksum-valid US SSN, exactly the shape the
/// `document.staging` sensitivity hints warn about (`SourcePath` +
/// `PotentiallySensitive` on `file_path`).
const SSN_SHAPED_FILENAME: &str = "/tmp/statements/export-123-45-6789.pdf";

/// The manifest's declared privacy context must match the `SourceMeta`
/// contract declaration (`ProcessingContext::Document`, line 77) instead of
/// silently diverging to `Metadata`.
#[sinex_test]
async fn manifest_declares_document_privacy_context() -> TestResult<()> {
    let parser = DocumentStagingParser;
    assert_eq!(
        parser.manifest().privacy_contexts,
        vec![ProcessingContext::Document],
        "manifest() must agree with SourceMeta's privacy_context = ProcessingContext::Document"
    );
    Ok(())
}

/// The actual emitted `document.ingested` intent must carry the same
/// `ProcessingContext::Document` tag the manifest and `SourceMeta` declare —
/// this is the value that ends up on the persisted event.
#[sinex_test]
async fn emitted_intent_carries_document_privacy_context() -> TestResult<()> {
    let mut parser = DocumentStagingParser;
    let intents = parser
        .parse_record(record_for(SSN_SHAPED_FILENAME), &test_ctx())
        .await
        .unwrap();
    let material_intent = &intents[0];
    assert_eq!(material_intent.event_type.as_str(), "document.ingested");
    assert_eq!(
        material_intent.privacy_context,
        ProcessingContext::Document,
        "parse_record() must stamp the same privacy_context the source declares as Secret-tier"
    );
    Ok(())
}

/// Anti-vacuity: proves the mismatch was a real recognizer bypass, not just
/// a documentation inconsistency.
///
/// The Document-scoped SSN recognizer (`sinex_primitives::privacy::catalog`)
/// fires on `file_path` when the emitted intent's `privacy_context` is
/// `Document`, but — matching exactly what `parse_record()` stamped before
/// this fix — the identical text is NOT redacted under `Metadata`. This test
/// fails against the pre-fix code because `parse_record()` used to stamp
/// `.privacy_context(ProcessingContext::Metadata)`, which this test would
/// have fed into the `Metadata` branch instead, and the assertion that the
/// *actual* emitted context catches the SSN would fail.
#[sinex_test]
async fn document_context_pii_recognizer_catches_ssn_shaped_file_path() -> TestResult<()> {
    let mut parser = DocumentStagingParser;
    let intents = parser
        .parse_record(record_for(SSN_SHAPED_FILENAME), &test_ctx())
        .await
        .unwrap();
    let payload = &intents[0].payload;
    let file_path = payload["file_path"]
        .as_str()
        .expect("file_path is a string");
    assert!(
        file_path.contains("123-45-6789"),
        "fixture path must carry the SSN-shaped text unredacted at the parser boundary \
         (parsers preserve interpreted values; admission owns redaction)"
    );

    // Full builtin PII catalog, as the Document-scoped ssn/phone/pesel/nip/regon
    // recognizers are actually defined (contexts include Document, never Metadata).
    let engine = PrivacyEngine::new(PrivacyConfig {
        builtin_categories: CategorySet::All,
        ..PrivacyConfig::default()
    })
    .expect("engine builds");

    // The context this test actually emitted (post-fix: Document) must get
    // the SSN caught.
    let processed_actual = engine.process(file_path, intents[0].privacy_context);
    assert!(
        !processed_actual.text.contains("123-45-6789"),
        "document.staging's actual emitted privacy_context ({:?}) must be scanned by the \
         Document-scoped SSN recognizer, got unredacted: {:?}",
        intents[0].privacy_context,
        processed_actual.text
    );

    // Control: prove the bypass mechanism the bead describes is real — the
    // identical text under Metadata (what pre-fix parse_record() stamped)
    // is silently skipped by the same recognizer.
    let processed_metadata = engine.process(file_path, ProcessingContext::Metadata);
    assert!(
        processed_metadata.text.contains("123-45-6789"),
        "control check: the SSN recognizer is scoped away from Metadata by design — if this \
         fails, the catalog's contexts scoping changed and this test's premise needs revisiting"
    );

    Ok(())
}

/// sinex-mrp4: a staged record whose path bytes aren't valid UTF-8 is
/// silently treated as "nothing to ingest" (`parse_record` returns
/// `Ok(vec![])`) instead of surfacing an explicit error/warning. From the
/// caller's side this is indistinguishable from "there was genuinely
/// nothing here" — silent data loss from staging/replay, not a visible
/// parse failure.
#[sinex_test]
#[ignore = "sinex-mrp4 open: document.staging silently returns Ok(vec![]) \
            for a record whose path bytes aren't valid UTF-8 instead of \
            surfacing an explicit ParserError"]
async fn non_utf8_staged_path_surfaces_an_error_not_silent_empty() -> TestResult<()> {
    let mut parser = DocumentStagingParser;
    let record = SourceRecord {
        material_id: Id::new(),
        anchor: MaterialAnchor::ByteRange { start: 0, len: 2 },
        bytes: vec![0xff, 0xfe],
        logical_path: None,
        source_ts_hint: None,
        metadata: serde_json::Value::Null,
    };

    let result = parser.parse_record(record, &test_ctx()).await;
    assert!(
        result.is_err(),
        "a non-UTF8 staged path must surface an explicit error, not silently return an \
         empty intent list indistinguishable from 'nothing to ingest': got {result:?}"
    );
    Ok(())
}
