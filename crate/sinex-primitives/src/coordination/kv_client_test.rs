use super::*;
use xtask::sandbox::{EnvGuard, sinex_test};

#[sinex_test]
async fn coordination_timing_defaults_match_deployment_contract()
-> ::xtask::sandbox::TestResult<()> {
    let timing = CoordinationTiming::from_overrides(None, None, None);
    assert_eq!(timing.heartbeat_secs, Seconds::from_secs(5));
    assert_eq!(timing.leadership_timeout_secs, Seconds::from_secs(30));
    assert_eq!(timing.handoff_timeout_secs, Seconds::from_secs(10));
    Ok(())
}

#[sinex_test]
async fn coordination_timing_accepts_positive_overrides() -> ::xtask::sandbox::TestResult<()> {
    let timing = CoordinationTiming::from_overrides(Some(7), Some(31), Some(11));
    assert_eq!(timing.heartbeat_secs, Seconds::from_secs(7));
    assert_eq!(timing.leadership_timeout_secs, Seconds::from_secs(31));
    assert_eq!(timing.handoff_timeout_secs, Seconds::from_secs(11));
    Ok(())
}

#[sinex_test]
async fn coordination_timing_rejects_zero_overrides() -> ::xtask::sandbox::TestResult<()> {
    let timing = CoordinationTiming::from_overrides(Some(0), Some(0), Some(0));
    assert_eq!(timing.heartbeat_secs, Seconds::from_secs(5));
    assert_eq!(timing.leadership_timeout_secs, Seconds::from_secs(30));
    assert_eq!(timing.handoff_timeout_secs, Seconds::from_secs(10));
    Ok(())
}

#[sinex_test(serial = true)]
async fn coordination_timing_from_env_accepts_positive_overrides()
-> ::xtask::sandbox::TestResult<()> {
    let mut env = EnvGuard::with_keys(&[
        "SINEX_COORDINATION_HEARTBEAT",
        "SINEX_COORDINATION_TIMEOUT",
        "SINEX_COORDINATION_HANDOFF",
    ]);
    env.set("SINEX_COORDINATION_HEARTBEAT", "7");
    env.set("SINEX_COORDINATION_TIMEOUT", "31");
    env.set("SINEX_COORDINATION_HANDOFF", "11");

    let timing = CoordinationTiming::from_env();

    assert_eq!(timing.heartbeat_secs, Seconds::from_secs(7));
    assert_eq!(timing.leadership_timeout_secs, Seconds::from_secs(31));
    assert_eq!(timing.handoff_timeout_secs, Seconds::from_secs(11));
    Ok(())
}

#[sinex_test(serial = true)]
async fn coordination_timing_from_env_ignores_invalid_overrides()
-> ::xtask::sandbox::TestResult<()> {
    let mut env = EnvGuard::with_keys(&[
        "SINEX_COORDINATION_HEARTBEAT",
        "SINEX_COORDINATION_TIMEOUT",
        "SINEX_COORDINATION_HANDOFF",
    ]);
    env.set("SINEX_COORDINATION_HEARTBEAT", "oops");
    env.set("SINEX_COORDINATION_TIMEOUT", "0");
    env.set("SINEX_COORDINATION_HANDOFF", "-5");

    let timing = CoordinationTiming::from_env();

    assert_eq!(timing.heartbeat_secs, Seconds::from_secs(5));
    assert_eq!(timing.leadership_timeout_secs, Seconds::from_secs(30));
    assert_eq!(timing.handoff_timeout_secs, Seconds::from_secs(10));
    Ok(())
}

#[sinex_test]
async fn parse_leader_id_accepts_valid_utf8() -> ::xtask::sandbox::TestResult<()> {
    let leader =
        CoordinationKvClient::parse_leader_id(b"module-a", "coordination leadership entry")?;
    assert_eq!(leader, "module-a");
    Ok(())
}

#[sinex_test]
async fn parse_leader_id_rejects_invalid_utf8() -> ::xtask::sandbox::TestResult<()> {
    let error =
        CoordinationKvClient::parse_leader_id(&[0xff, 0xfe], "coordination leadership entry")
            .expect_err("invalid leader bytes must fail honestly");
    assert!(
        error
            .to_string()
            .contains("Invalid coordination leadership entry leader ID encoding")
    );
    Ok(())
}

#[sinex_test]
async fn parse_leader_id_rejects_empty_value() -> ::xtask::sandbox::TestResult<()> {
    let error = CoordinationKvClient::parse_leader_id(b"   ", "coordination leadership entry")
        .expect_err("empty leader bytes must fail honestly");
    assert!(
        error
            .to_string()
            .contains("Invalid coordination leadership entry leader ID: value is empty")
    );
    Ok(())
}
