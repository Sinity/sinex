//! Required input-key declarations for system JSON-line parsers.

#[path = "required_input_keys_support.rs"]
mod required_input_keys_support;

use required_input_keys_support::{
    assert_required_input_keys, assert_required_key_blocks_readiness,
};
use serde_json::json;
use sinex_primitives::parser::SourceId;
use sinexd::runtime::parser::SourceRecordFingerprint;
use sinexd::sources::source_contracts::system::{journald::JournaldParser, systemd::SystemdParser};
use xtask::sandbox::prelude::*;

#[sinex_test]
async fn system_json_parsers_declare_required_input_keys() -> TestResult<()> {
    assert_required_input_keys(
        JournaldParser,
        &["/MESSAGE", "/__CURSOR", "/__REALTIME_TIMESTAMP"],
    );
    assert_required_input_keys(
        SystemdParser,
        &[
            "/ACTIVE_STATE",
            "/MESSAGE",
            "/SUB_STATE",
            "/UNIT_RESULT",
            "/__CURSOR",
            "/__REALTIME_TIMESTAMP",
            "/_SYSTEMD_UNIT",
        ],
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

    let drift =
        SourceRecordFingerprint::diff(SourceId::from_static("system.journald"), &before, &after)
            .expect("removing __CURSOR should produce JSON shape drift");
    assert_required_key_blocks_readiness(drift, JournaldParser, "/__CURSOR");
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

    let drift =
        SourceRecordFingerprint::diff(SourceId::from_static("system.systemd"), &before, &after)
            .expect("removing _SYSTEMD_UNIT should produce JSON shape drift");
    assert_required_key_blocks_readiness(drift, SystemdParser, "/_SYSTEMD_UNIT");
    Ok(())
}
