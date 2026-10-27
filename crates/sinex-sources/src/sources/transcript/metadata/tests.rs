use super::*;
#[devkit::test]
fn metadata_after_segments_is_kept_without_copying_segments() {
    let bytes = br#"{"segments":[{"text":"example"}],"language":"pl","model":"example-model","external_run":"example-run","other":{"value":4}}"#;
    let metadata = project(&mut bytes.as_slice()).unwrap();
    assert_eq!(metadata["language"], "pl");
    assert_eq!(metadata["other"]["value"], 4);
    assert!(!metadata.contains_key("segments"));
    assert!(project(&mut br#"{"segments":[],"model":"a","model":"b"}"#.as_slice()).is_err());
}
