use super::*;
use crate::fmt::render_finite_envelope;
use sinex_primitives::views::VIEW_ENVELOPE_SCHEMA_VERSION;
use xtask::sandbox::sinex_test;

#[sinex_test]
async fn gateway_ping_envelope_renders_finite_machine_view() -> xtask::TestResult<()> {
    let envelope = gateway_envelope("sinexctl.runtime.gateway.ping", "pong".to_string());
    let rendered = render_finite_envelope(&envelope, OutputFormat::Json)?
        .expect("json renders finite gateway ping envelope");
    let parsed: serde_json::Value = serde_json::from_str(&rendered)?;

    assert_eq!(parsed["schema_version"], VIEW_ENVELOPE_SCHEMA_VERSION);
    assert_eq!(parsed["source_surface"], "sinexctl.runtime.gateway.ping");
    assert_eq!(parsed["payload"]["response"], "pong");
    assert!(
        parsed.get("caveats").is_none(),
        "gateway ping should not invent caveats for a successful probe"
    );
    Ok(())
}

#[sinex_test]
async fn gateway_version_envelope_renders_finite_machine_view() -> xtask::TestResult<()> {
    let envelope = gateway_envelope("sinexctl.runtime.gateway.version", "0.4.2".to_string());
    let rendered = render_finite_envelope(&envelope, OutputFormat::Json)?
        .expect("json renders finite gateway version envelope");
    let parsed: serde_json::Value = serde_json::from_str(&rendered)?;

    assert_eq!(
        parsed["source_surface"],
        "sinexctl.runtime.gateway.version"
    );
    assert_eq!(parsed["payload"]["response"], "0.4.2");
    Ok(())
}
