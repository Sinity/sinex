use super::*;
use serde_json::Value as JsonValue;

#[sinex_test]
async fn persist_failure_writes_evidence_bundle_and_summary() -> TestResult<()> {
    let dir = tempfile::tempdir()?;
    let _guard = EnvGuard::set_single("SINEX_TEST_FAIL_DIR", dir.path().as_os_str());

    persist_failure(
        "sample::evidence_failure",
        "assertion exploded",
        FailureContext::None,
    );

    let files = fs::read_dir(dir.path())?.collect::<std::result::Result<Vec<_>, _>>()?;
    let bundle_path = files
        .iter()
        .map(std::fs::DirEntry::path)
        .find(|path| {
            path.file_name()
                .and_then(|name| name.to_str())
                .is_some_and(|name| name.ends_with(".evidence.json"))
        })
        .ok_or_else(|| color_eyre::eyre::eyre!("missing evidence bundle"))?;
    let summary_path = files
        .iter()
        .map(std::fs::DirEntry::path)
        .find(|path| {
            path.file_name()
                .and_then(|name| name.to_str())
                .is_some_and(|name| name.ends_with(".summary.txt"))
        })
        .ok_or_else(|| color_eyre::eyre::eyre!("missing evidence summary"))?;

    let bundle: JsonValue = serde_json::from_slice(&fs::read(&bundle_path)?)?;
    let summary = fs::read_to_string(summary_path)?;

    assert_eq!(bundle["schema_version"], EVIDENCE_SCHEMA_VERSION);
    assert_eq!(bundle["kind"], "sinex.test.evidence");
    assert_eq!(bundle["status"], "failed");
    assert_eq!(bundle["error"], "assertion exploded");
    assert!(summary.contains("assertion exploded"));
    Ok(())
}

#[sinex_test]
#[ignore = "sinex-gp25 open: to_json() silently collapses non-serializable evidence to Null instead of surfacing the serialization error"]
async fn to_json_surfaces_non_serializable_values_instead_of_silent_null() -> TestResult<()> {
    // f64::NAN is one of the few values serde_json genuinely refuses to
    // serialize (JSON has no representation for NaN/Infinity) -- a
    // realistic stand-in for "non-serializable evidence payload" that a
    // postmortem snapshot could plausibly carry (e.g. a computed
    // percentage/ratio that divided by zero).
    let value = to_json(&f64::NAN);
    assert_ne!(
        value,
        JsonValue::Null,
        "to_json() must not silently collapse a non-serializable value (NaN) to Null -- \
         doing so obscures the exact failure mode during postmortem analysis, which is \
         the entire point of capturing this evidence in the first place"
    );
    Ok(())
}
