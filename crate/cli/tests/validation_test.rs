use sinex_primitives::temporal::{Duration, Timestamp};
use sinexctl::mcp::{MCP_PROTOCOL_VERSION, assert_read_only_tool_names, tools};
use sinexctl::validation::{parse_time_input, parse_time_input_with_now, validate_time_range};
use xtask::sandbox::prelude::*;

#[sinex_test]
async fn parse_time_input_with_now_handles_relative_and_absolute_inputs() -> TestResult<()> {
    let now = Timestamp::parse_rfc3339("2026-03-24T10:00:00Z")?;

    assert_eq!(
        parse_time_input_with_now("1h", now)?,
        now - Duration::hours(1)
    );
    assert_eq!(
        parse_time_input_with_now("30m", now)?,
        now - Duration::minutes(30)
    );
    assert_eq!(
        parse_time_input_with_now("2026-03-24T09:15:00Z", now)?,
        Timestamp::parse_rfc3339("2026-03-24T09:15:00Z")?
    );
    assert_eq!(
        parse_time_input_with_now("2026-03-24", now)?,
        Timestamp::parse_rfc3339("2026-03-24T00:00:00Z")?
    );

    Ok(())
}

#[sinex_test]
async fn parse_time_input_rejects_invalid_formats() -> TestResult<()> {
    assert!(parse_time_input("not-a-date").is_err());
    assert!(parse_time_input("2026/03/24").is_err());
    Ok(())
}

#[sinex_test]
async fn validate_time_range_rejects_inverted_and_equal_bounds() -> TestResult<()> {
    let now = Timestamp::now();
    let past = Timestamp::new(now.inner() - Duration::hours(1));
    let future = Timestamp::new(now.inner() + Duration::hours(1));

    assert!(validate_time_range(Some(past), Some(now)).is_ok());
    assert!(validate_time_range(Some(now), Some(future)).is_ok());
    assert!(validate_time_range(None, Some(future)).is_ok());
    assert!(validate_time_range(Some(past), None).is_ok());
    assert!(validate_time_range(None, None).is_ok());

    assert!(validate_time_range(Some(future), Some(past)).is_err());
    assert!(validate_time_range(Some(now), Some(now)).is_err());
    Ok(())
}

#[sinex_test]
async fn mcp_lists_first_slice_read_only_tools() -> TestResult<()> {
    let tools = tools();
    let names = tools.iter().map(|tool| tool.name).collect::<Vec<_>>();

    assert_eq!(
        names,
        vec![
            "sinex.search_events",
            "sinex.trace_lineage",
            "sinex.source_readiness"
        ]
    );
    assert_read_only_tool_names()?;
    Ok(())
}

#[sinex_test]
async fn mcp_tool_schemas_are_closed_objects() -> TestResult<()> {
    for tool in tools() {
        assert_eq!(tool.input_schema["type"], "object");
        assert_eq!(tool.input_schema["additionalProperties"], false);
        assert!(tool.input_schema["properties"].is_object());
    }
    Ok(())
}

#[sinex_test]
async fn mcp_protocol_version_is_pinned() -> TestResult<()> {
    assert_eq!(MCP_PROTOCOL_VERSION, "2024-11-05");
    Ok(())
}
