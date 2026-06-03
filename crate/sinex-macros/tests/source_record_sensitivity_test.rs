//! Tests for `#[privacy(sensitivity = "...")]` on `#[derive(SourceRecord)]`.
//!
//! Verifies that the semantic sensitivity-class hint vocabulary (#1611) is
//! parsed onto the generated `FieldSpec` and unioned into the parser manifest's
//! `sensitivity_hints` for policy tooling consumption.

use serde::{Deserialize, Serialize};
use sinex_primitives::parser::MaterialParser;
use sinex_primitives::privacy::SensitivityHint;
use xtask::sandbox::prelude::*;

#[derive(Debug, Clone, Serialize, Deserialize, sinex_macros::SourceRecord)]
#[source_record(
    id = "sensitivity-fixture",
    source_unit_id = "test.sensitivity-fixture",
    input_shape = "json",
    event_type = "test.sensitive"
)]
struct SensitivityFixtureRecord {
    #[source(json_pointer = "/title")]
    #[privacy(sensitivity = "free_text, potentially_sensitive")]
    title: String,

    #[source(json_pointer = "/token")]
    #[privacy(context = "Command", sensitivity = "credential_bearing")]
    token: String,

    #[source(json_pointer = "/path")]
    #[privacy(sensitivity = "source_path")]
    path: String,

    #[source(json_pointer = "/count")]
    count: i64,
}

#[sinex_test]
async fn field_spec_carries_sensitivity_hints() -> TestResult<()> {
    let spec = SensitivityFixtureRecord::parser_spec();

    let title = spec
        .fields
        .iter()
        .find(|f| f.name == "title")
        .expect("title field present");
    assert_eq!(
        title.sensitivity,
        vec![
            SensitivityHint::FreeText,
            SensitivityHint::PotentiallySensitive
        ],
    );

    let token = spec
        .fields
        .iter()
        .find(|f| f.name == "token")
        .expect("token field present");
    assert_eq!(token.sensitivity, vec![SensitivityHint::CredentialBearing]);

    let path = spec
        .fields
        .iter()
        .find(|f| f.name == "path")
        .expect("path field present");
    assert_eq!(path.sensitivity, vec![SensitivityHint::SourcePath]);

    // A field without the attribute has no hints.
    let count = spec
        .fields
        .iter()
        .find(|f| f.name == "count")
        .expect("count field present");
    assert!(count.sensitivity.is_empty());

    Ok(())
}

#[sinex_test]
async fn manifest_unions_sensitivity_hints() -> TestResult<()> {
    let record = SensitivityFixtureRecord {
        title: String::new(),
        token: String::new(),
        path: String::new(),
        count: 0,
    };
    let manifest = record.manifest();

    // The manifest exports the deduplicated union of field-level hints for
    // policy tooling. Order follows first-declaration order.
    assert_eq!(
        manifest.sensitivity_hints,
        vec![
            SensitivityHint::FreeText,
            SensitivityHint::PotentiallySensitive,
            SensitivityHint::CredentialBearing,
            SensitivityHint::SourcePath,
        ],
    );

    Ok(())
}
