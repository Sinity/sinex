use serde_json::json;
use crate::runtime::{InputProvenanceFilter, Transducer};
use xtask::sandbox::sinex_test;

#[sinex_test]
async fn tag_applier_consumes_material_events_only() -> TestResult<()> {
    let automaton = super::TagApplier;

    assert_eq!(
        automaton.input_provenance_filter(),
        InputProvenanceFilter::MaterialOnly
    );
    assert_eq!(automaton.input_event_type(), "*");
    Ok(())
}

#[sinex_test]
async fn test_source_based_tagging() -> TestResult<()> {
    let input = json!({});
    let _ = input;
    Ok(())
}

#[sinex_test]
async fn test_file_extension_rust() -> TestResult<()> {
    let input = json!({"path": "/home/user/main.rs"});
    let _ = input;
    Ok(())
}

#[sinex_test]
async fn test_file_extension_unknown() -> TestResult<()> {
    let input = json!({"path": "/tmp/file.xyz"});
    let _ = input;
    Ok(())
}
