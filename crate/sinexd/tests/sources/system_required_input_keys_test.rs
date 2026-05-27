//! Required input-key declarations for system JSON-line parsers.

use serde_json::json;
use sinex_node_sdk::parser::{MaterialParser, SourceRecordFingerprint};
use sinex_primitives::{
    parser::SourceUnitId,
    rpc::sources::{CaveatSeverity, caveat_codes},
};
use sinexd::sources::sources::system::{journald::JournaldParser, systemd::SystemdParser};
use xtask::sandbox::prelude::*;

#[sinex_test]
async fn system_json_parsers_declare_required_input_keys() -> TestResult<()> {
    assert_eq!(
        JournaldParser.required_input_keys(),
        vec!["/MESSAGE", "/__CURSOR", "/__REALTIME_TIMESTAMP"]
    );
    assert_eq!(
        SystemdParser.required_input_keys(),
        vec![
            "/ACTIVE_STATE",
            "/MESSAGE",
            "/SUB_STATE",
            "/UNIT_RESULT",
            "/__CURSOR",
            "/__REALTIME_TIMESTAMP",
            "/_SYSTEMD_UNIT",
        ]
    );
    Ok(())
}

#[sinex_test]
async fn journald_required_cursor_removal_blocks_readiness() -> TestResult<()> {
    let before = SourceRecordFingerprint::from_json(&json!({
        "__CURSOR": "s=abc;i=1",
        "__REALTIME_TIMESTAMP": "1700000000000000",
        "MESSAGE": "service started"
    }));
    let after = SourceRecordFingerprint::from_json(&json!({
        "__REALTIME_TIMESTAMP": "1700000000000000",
        "MESSAGE": "service started"
    }));

    let mut drift = SourceRecordFingerprint::diff(
        SourceUnitId::from_static("system.journald"),
        &before,
        &after,
    )
    .expect("removing __CURSOR should produce JSON shape drift");
    drift.required_input_keys = JournaldParser.required_input_keys();

    let caveats = drift.readiness_caveats();

    assert!(caveats.iter().any(|caveat| {
        caveat.code == caveat_codes::PARSER_REQUIRED_FIELD_MISSING
            && caveat.severity == CaveatSeverity::Blocking
            && caveat.message.contains("/__CURSOR")
    }));
    Ok(())
}

#[sinex_test]
async fn systemd_required_unit_removal_blocks_readiness() -> TestResult<()> {
    let before = SourceRecordFingerprint::from_json(&json!({
        "__CURSOR": "s=abc;i=1",
        "__REALTIME_TIMESTAMP": "1700000000000000",
        "_SYSTEMD_UNIT": "example.service",
        "MESSAGE": "Started example.service.",
        "UNIT_RESULT": "success",
        "ACTIVE_STATE": "active",
        "SUB_STATE": "running"
    }));
    let after = SourceRecordFingerprint::from_json(&json!({
        "__CURSOR": "s=abc;i=1",
        "__REALTIME_TIMESTAMP": "1700000000000000",
        "MESSAGE": "Started example.service.",
        "UNIT_RESULT": "success",
        "ACTIVE_STATE": "active",
        "SUB_STATE": "running"
    }));

    let mut drift =
        SourceRecordFingerprint::diff(SourceUnitId::from_static("system.systemd"), &before, &after)
            .expect("removing _SYSTEMD_UNIT should produce JSON shape drift");
    drift.required_input_keys = SystemdParser.required_input_keys();

    let caveats = drift.readiness_caveats();

    assert!(caveats.iter().any(|caveat| {
        caveat.code == caveat_codes::PARSER_REQUIRED_FIELD_MISSING
            && caveat.severity == CaveatSeverity::Blocking
            && caveat.message.contains("/_SYSTEMD_UNIT")
    }));
    Ok(())
}
