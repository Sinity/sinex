use serde_json::json;
use sinex_db::repositories::{DbPoolExt, EmailMailboxProjectionEvent};
use uuid::Uuid;
use xtask::sandbox::prelude::*;

#[sinex_test]
async fn email_mailbox_projection_merges_message_thread_and_attachment_events(
    ctx: TestContext,
) -> TestResult<()> {
    let repo = ctx.pool().email_mailbox_projections();
    let source_id = "email.mailbox".to_string();
    let mode_id = "source:email.mailbox.mbox-staged".to_string();
    let message_id = "<projection@example.com>";

    repo.upsert_event(EmailMailboxProjectionEvent {
        source_id: source_id.clone(),
        mode_id: mode_id.clone(),
        observed_event_id: Uuid::now_v7(),
        event_type: "email.message.received".to_string(),
        payload: json!({
            "message_id": message_id,
            "folder": "inbox",
            "source_file": "mailbox.mbox",
            "raw_material_id": Uuid::now_v7().to_string(),
            "mailbox_format": "mbox",
            "mbox_byte_start": 128,
            "mbox_byte_end": 512,
            "subject": "Projection fixture",
            "from": ["Sender <sender@example.com>"],
            "to": ["Receiver <receiver@example.com>"],
            "body_bytes": 42,
            "attachment_count": 2
        }),
    })
    .await?;
    repo.upsert_event(EmailMailboxProjectionEvent {
        source_id: source_id.clone(),
        mode_id: mode_id.clone(),
        observed_event_id: Uuid::now_v7(),
        event_type: "email.thread.observed".to_string(),
        payload: json!({
            "thread_key": "thread:projection",
            "thread_root_message_id": message_id,
            "message_id": message_id,
            "folder": "inbox",
            "source_file": "mailbox.mbox",
            "raw_material_id": Uuid::now_v7().to_string(),
            "mailbox_format": "mbox",
            "subject": "Projection fixture",
            "from": ["Sender <sender@example.com>"],
            "to": ["Receiver <receiver@example.com>"]
        }),
    })
    .await?;
    repo.upsert_event(EmailMailboxProjectionEvent {
        source_id: source_id.clone(),
        mode_id: mode_id.clone(),
        observed_event_id: Uuid::now_v7(),
        event_type: "email.attachment.observed".to_string(),
        payload: json!({
            "message_id": message_id,
            "folder": "inbox",
            "source_file": "mailbox.mbox",
            "raw_material_id": Uuid::now_v7().to_string(),
            "mailbox_format": "mbox",
            "attachment_index": 1,
            "material_policy_ref": "operator.email-mailbox.attachment-deferred"
        }),
    })
    .await?;

    let rows = repo.list_current_by_source(&source_id).await?;
    assert_eq!(rows.len(), 1);
    let row = &rows[0];
    assert_eq!(row.mode_id, mode_id);
    assert_eq!(row.message_id.as_deref(), Some(message_id));
    assert_eq!(row.thread_key.as_deref(), Some("thread:projection"));
    assert_eq!(row.body_bytes, 42);
    assert_eq!(row.mbox_byte_start, Some(128));
    assert_eq!(row.mbox_byte_end, Some(512));
    assert_eq!(row.attachment_count, 2);
    assert_eq!(row.attachment_observed_count, 2);
    assert_eq!(
        row.attachment_policy_refs,
        json!(["operator.email-mailbox.attachment-deferred"])
    );

    let summaries = repo.summarize_by_source(&source_id).await?;
    assert_eq!(summaries.len(), 1);
    assert_eq!(summaries[0].message_count, 1);
    assert_eq!(summaries[0].thread_count, 1);
    assert_eq!(summaries[0].body_bytes, 42);
    assert_eq!(summaries[0].attachment_count, 2);
    assert_eq!(summaries[0].attachment_observed_count, 2);
    Ok(())
}
