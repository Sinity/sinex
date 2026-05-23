//! Production-shaped replay proof: source-worker parse listener receives
//! parse commands via NATS, dispatches to parser, returns acks with event counts.
//!
//! Replaces the fake-DB-write scan-node tests referenced in #1132.

use color_eyre::eyre::eyre;
use sinex_primitives::Uuid;
use sinex_source_worker::dispatch::test_parser_dispatch;
use sinex_source_worker::parse_listener::{
    SourceParseAck, SourceParseCommand, spawn_parse_listener,
};
use xtask::sandbox::prelude::*;

/// Prove that a parse command published over NATS reaches the source-worker
/// parse listener, is dispatched to the parser, and returns an ack.
#[sinex_test]
async fn test_parse_command_round_trip_via_nats(ctx: TestContext) -> TestResult<()> {
    let ctx = ctx.with_nats().shared().await?;

    let (dispatch, calls) = test_parser_dispatch();
    let source_id = "weechat";
    let client = ctx.nats_client();

    let handle = spawn_parse_listener(client.clone(), source_id, dispatch)
        .await
        .map_err(|e| eyre!("spawn failed: {e}"))?;

    let operation_id = Uuid::now_v7();
    let material_id = Uuid::now_v7();

    let cmd = SourceParseCommand {
        operation_id,
        source_id: source_id.to_string(),
        source_material_id: Some(material_id),
        source_version: None,
        executor: "test".to_string(),
    };
    let payload = serde_json::to_vec(&cmd)?;

    let subject = format!("sinex.control.sources.{source_id}.parse");
    let response = client
        .request(subject, payload.into())
        .await
        .map_err(|e| eyre!("NATS request failed: {e}"))?;

    let ack: SourceParseAck = serde_json::from_slice(&response.payload)?;

    assert!(ack.accepted, "parse should be accepted");
    assert!(ack.error.is_none(), "should have no error: {:?}", ack.error);

    let recorded = calls.lock().unwrap();
    assert_eq!(recorded.len(), 1, "dispatch should be called once");
    assert_eq!(recorded[0].0, source_id, "dispatch source_id should match");
    assert_eq!(
        recorded[0].2,
        Some(material_id),
        "dispatch material_id should match"
    );

    handle.abort();
    Ok(())
}

/// Prove that a parse command for a mismatched source_id is rejected.
#[sinex_test]
async fn test_parse_command_rejected_for_mismatched_source(ctx: TestContext) -> TestResult<()> {
    let ctx = ctx.with_nats().shared().await?;

    let (dispatch, calls) = test_parser_dispatch();
    let listener_source = "weechat";
    let client = ctx.nats_client();

    let handle = spawn_parse_listener(client.clone(), listener_source, dispatch)
        .await
        .map_err(|e| eyre!("spawn failed: {e}"))?;

    let cmd = SourceParseCommand {
        operation_id: Uuid::now_v7(),
        source_id: "desktop".to_string(),
        source_material_id: None,
        source_version: None,
        executor: "test".to_string(),
    };
    let payload = serde_json::to_vec(&cmd)?;

    let subject = format!("sinex.control.sources.{listener_source}.parse");
    let response = client
        .request(subject, payload.into())
        .await
        .map_err(|e| eyre!("NATS request failed: {e}"))?;

    let ack: SourceParseAck = serde_json::from_slice(&response.payload)?;

    assert!(
        !ack.accepted,
        "parse should be rejected for mismatched source"
    );
    assert!(ack.error.is_some(), "should have error");
    assert_eq!(
        calls.lock().unwrap().len(),
        0,
        "dispatch should not be called"
    );

    handle.abort();
    Ok(())
}

/// Prove that two concurrent parse commands are both processed independently.
#[sinex_test]
async fn test_concurrent_parse_commands(ctx: TestContext) -> TestResult<()> {
    let ctx = ctx.with_nats().shared().await?;

    let (dispatch, calls) = test_parser_dispatch();
    let source_id = "weechat";
    let client = ctx.nats_client();

    let handle = spawn_parse_listener(client.clone(), source_id, dispatch)
        .await
        .map_err(|e| eyre!("spawn failed: {e}"))?;

    let subject = format!("sinex.control.sources.{source_id}.parse");

    let cmd1 = SourceParseCommand {
        operation_id: Uuid::now_v7(),
        source_id: source_id.to_string(),
        source_material_id: Some(Uuid::now_v7()),
        source_version: None,
        executor: "test".to_string(),
    };
    let cmd2 = SourceParseCommand {
        operation_id: Uuid::now_v7(),
        source_id: source_id.to_string(),
        source_material_id: Some(Uuid::now_v7()),
        source_version: None,
        executor: "test".to_string(),
    };

    let (r1, r2) = tokio::join!(
        client.request(subject.clone(), serde_json::to_vec(&cmd1)?.into()),
        client.request(subject.clone(), serde_json::to_vec(&cmd2)?.into()),
    );

    let ack1: SourceParseAck =
        serde_json::from_slice(&r1.map_err(|e| eyre!("request 1 failed: {e}"))?.payload)?;
    let ack2: SourceParseAck =
        serde_json::from_slice(&r2.map_err(|e| eyre!("request 2 failed: {e}"))?.payload)?;

    assert!(ack1.accepted, "ack1 should be accepted");
    assert!(ack2.accepted, "ack2 should be accepted");
    assert_eq!(calls.lock().unwrap().len(), 2, "both should be dispatched");

    handle.abort();
    Ok(())
}
