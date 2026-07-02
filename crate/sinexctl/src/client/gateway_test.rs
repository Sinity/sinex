// Tests unwrap optional windows after asserting inputs that must construct
// a replay range.
#![allow(clippy::expect_used)]
use super::*;
use xtask::sandbox::prelude::*;

#[sinex_test]
async fn test_build_replay_time_window_supports_relative_inputs() -> TestResult<()> {
    let now = Timestamp::parse_rfc3339("2025-01-15T12:00:00Z")?;
    let window =
        GatewayClient::build_replay_time_window(Some("24h"), None, now)?.expect("window");

    assert_eq!(window.0.format_rfc3339(), "2025-01-14T12:00:00Z");
    assert_eq!(window.1.format_rfc3339(), "2025-01-15T12:00:00Z");
    Ok(())
}

#[sinex_test]
async fn test_build_replay_time_window_rejects_inverted_range() -> TestResult<()> {
    let now = Timestamp::parse_rfc3339("2025-01-15T12:00:00Z")?;
    let err = GatewayClient::build_replay_time_window(
        Some("2025-01-16T00:00:00Z"),
        Some("2025-01-15T00:00:00Z"),
        now,
    )
    .expect_err("inverted replay window must fail");

    assert!(err.to_string().contains("Invalid time range"));
    Ok(())
}

#[sinex_test]
async fn test_build_replay_time_window_defaults_since_from_until() -> TestResult<()> {
    let now = Timestamp::parse_rfc3339("2025-01-15T12:00:00Z")?;
    let window =
        GatewayClient::build_replay_time_window(None, Some("2025-01-10T08:30:00Z"), now)?
            .expect("window");

    assert_eq!(window.0.format_rfc3339(), "2025-01-09T08:30:00Z");
    assert_eq!(window.1.format_rfc3339(), "2025-01-10T08:30:00Z");
    Ok(())
}

#[sinex_test]
async fn sse_parser_preserves_split_utf8_scalars() -> TestResult<()> {
    let mut state = SseFrameState::default();
    let bytes = "data: ż\n".as_bytes();
    state.push_chunk(&bytes[..7]);
    assert!(state.try_parse_frame().is_none());
    state.push_chunk(&bytes[7..]);
    assert!(state.try_parse_frame().is_none());

    assert_eq!(state.current_data, "ż");
    Ok(())
}

#[sinex_test]
async fn sse_parser_strips_only_single_optional_value_space() -> TestResult<()> {
    assert_eq!(
        parse_sse_field("data:  leading-space-preserved"),
        ("data", " leading-space-preserved")
    );
    assert_eq!(
        parse_sse_field("data: trailing-space-preserved "),
        ("data", "trailing-space-preserved ")
    );
    assert_eq!(parse_sse_field("data:"), ("data", ""));
    Ok(())
}

#[sinex_test]
async fn sse_parser_dispatches_empty_data_frame() -> TestResult<()> {
    let mut state = SseFrameState::default();
    state.push_chunk(b"event: heartbeat\ndata:\n\n");

    assert!(matches!(
        state.try_parse_frame().transpose()?,
        Some(SseClientMessage::Heartbeat)
    ));
    Ok(())
}

#[sinex_test]
async fn sse_parser_handles_crlf_multiline_data() -> TestResult<()> {
    let mut state = SseFrameState::default();
    state.push_chunk(b"data: first\r\ndata: second\r\n");
    assert!(state.try_parse_frame().is_none());

    assert_eq!(state.current_data, "first\nsecond");
    Ok(())
}

#[sinex_test]
async fn sse_parser_ignores_control_and_unknown_fields() -> TestResult<()> {
    let mut state = SseFrameState::default();
    state.push_chunk(
        b": comment\nid: abc\nretry: 1000\nfoo: bar\nevent: unknown\ndata: {}\n\n\
          event: heartbeat\ndata: {}\n\n",
    );

    assert!(matches!(
        state.try_parse_frame().transpose()?,
        Some(SseClientMessage::Heartbeat)
    ));
    Ok(())
}

#[sinex_test]
async fn sse_parser_returns_structured_error_frames() -> TestResult<()> {
    let mut state = SseFrameState::default();
    state.push_chunk(
        br#"event: error
data: {"code":"serialization_error","message":"failed to serialize SSE event payload"}

"#,
    );

    assert!(matches!(
        state.try_parse_frame().transpose()?,
        Some(SseClientMessage::Error { code, message })
            if code == "serialization_error"
                && message == "failed to serialize SSE event payload"
    ));
    Ok(())
}
