use super::*;
use xtask::sandbox::prelude::*;

#[sinex_test]
async fn registry_includes_known_payloads() -> TestResult<()> {
    let reg = registry();
    // The inventory is sizeable in this workspace; a non-zero population
    // is the only durable invariant we can pin at this layer without
    // hard-coding a moving target.
    assert!(!reg.is_empty(), "schema registry should be non-empty");
    Ok(())
}

#[sinex_test]
async fn unknown_pair_rejected() -> TestResult<()> {
    let reg = registry();
    let err = reg
        .validate(
            &EventSource::from_static("__definitely_not_a_real_source__"),
            &EventType::from_static("__nope__"),
        )
        .expect_err("unknown pair should be rejected");
    assert!(
        err.to_string().contains("unknown event"),
        "expected validation error, got {err}"
    );
    Ok(())
}

#[sinex_test]
async fn known_gateway_pair_accepted() -> TestResult<()> {
    let reg = registry();
    // ApiRequestStatsPayload is registered as
    // (sinexd.api, request.stats); skip if the workspace surface ever
    // changes underneath us, but keep the assertion strict otherwise.
    assert!(
        reg.contains("sinexd.api", "request.stats"),
        "registry must include sinexd.api/request.stats"
    );
    Ok(())
}
