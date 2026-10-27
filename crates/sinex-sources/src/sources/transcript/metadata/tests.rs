use super::*;
#[devkit::test]
fn metadata_after_segments_is_kept_without_copying_segments() {
    let bytes = br#"{"segments":[{"text":"example"}],"language":"pl","model":"example-model","external_run":"example-run","other":{"value":4}}"#;
    let metadata = project(&mut bytes.as_slice()).unwrap();
    assert_eq!(metadata["language"], "pl");
    assert!(!metadata.contains_key("other"));
    assert!(!metadata.contains_key("segments"));
    assert!(project(&mut br#"{"segments":[],"model":"a","model":"b"}"#.as_slice()).is_err());
}

#[devkit::test]
fn projection_keeps_defaults_without_repeating_full_transcript_text() {
    let original = serde_json::json!({"segments":[{"text":"example"}],"text":"x".repeat(4 << 20),"language":"pl","model":"example-model","external_run":"example-run"});
    let bytes = serde_json::to_vec(&original).unwrap();
    let mut prior = original.as_object().unwrap().clone();
    prior.remove("segments");
    let before = serde_json::to_vec(&prior).unwrap().len();
    let metadata = project(&mut bytes.as_slice()).unwrap();
    let after = serde_json::to_vec(&metadata).unwrap().len();
    assert!(!metadata.contains_key("text"));
    assert_eq!(metadata["model"], "example-model");
    eprintln!(
        "file metadata bytes: prior projection={before}, selected defaults={after}, input={}",
        bytes.len()
    );
}
