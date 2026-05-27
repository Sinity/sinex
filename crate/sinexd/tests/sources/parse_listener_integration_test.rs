//! Production-shaped replay proof: source-worker parse listener receives
//! parse commands via NATS, dispatches to parser, returns acks with event counts.
//!
//! Replaces the fake-DB-write scan-node tests referenced in #1132.

use color_eyre::eyre::eyre;
use sinex_primitives::Uuid;
use sinexd::sources::dispatch::test_parser_dispatch;
use sinexd::sources::parse_listener::{
    SourceParseAck, SourceParseCommand, spawn_parse_listener,
};
use xtask::sandbox::prelude::*;

async fn request_parse_ack(
    client: &async_nats::Client,
    subject: &str,
    cmd: &SourceParseCommand,
) -> TestResult<SourceParseAck> {
    let response = client
        .request(subject.to_string(), serde_json::to_vec(cmd)?.into())
        .await
        .map_err(|e| eyre!("NATS request failed: {e}"))?;
    Ok(serde_json::from_slice(&response.payload)?)
}

/// Prove that parse commands published over NATS reach the source-worker parse
/// listener, dispatch matching sources, reject mismatches, and handle
/// concurrent requests independently.
#[sinex_test]
async fn parse_listener_handles_command_lifecycle(ctx: TestContext) -> TestResult<()> {
    let ctx = ctx.with_nats().shared().await?;

    let (dispatch, calls) = test_parser_dispatch();
    let source_id = "weechat";
    let client = ctx.nats_client();

    let handle = spawn_parse_listener(client.clone(), source_id, dispatch)
        .await
        .map_err(|e| eyre!("spawn failed: {e}"))?;

    let operation_id = Uuid::now_v7();
    let material_id = Uuid::now_v7();
    let subject = format!("sinex.control.sources.{source_id}.parse");

    let cmd = SourceParseCommand {
        operation_id,
        source_id: source_id.to_string(),
        source_material_id: Some(material_id),
        source_version: None,
        executor: "test".to_string(),
    };
    let ack = request_parse_ack(&client, &subject, &cmd).await?;

    assert!(ack.accepted, "parse should be accepted");
    assert!(ack.error.is_none(), "should have no error: {:?}", ack.error);

    let mismatched = SourceParseCommand {
        operation_id: Uuid::now_v7(),
        source_id: "desktop".to_string(),
        source_material_id: None,
        source_version: None,
        executor: "test".to_string(),
    };
    let rejected = request_parse_ack(&client, &subject, &mismatched).await?;

    assert!(
        !rejected.accepted,
        "parse should be rejected for mismatched source"
    );
    assert!(rejected.error.is_some(), "should have error");

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
        request_parse_ack(&client, &subject, &cmd1),
        request_parse_ack(&client, &subject, &cmd2),
    );

    let ack1 = r1?;
    let ack2 = r2?;
    assert!(ack1.accepted, "ack1 should be accepted");
    assert!(ack2.accepted, "ack2 should be accepted");

    let recorded = calls.lock().unwrap();
    assert_eq!(
        recorded.len(),
        3,
        "accepted requests should dispatch; rejected request should not"
    );
    assert_eq!(recorded[0].0, source_id, "dispatch source_id should match");
    assert_eq!(
        recorded[0].2,
        Some(material_id),
        "dispatch material_id should match"
    );

    handle.abort();
    Ok(())
}
