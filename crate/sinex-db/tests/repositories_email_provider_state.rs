use serde_json::json;
use sinex_db::repositories::{DbPoolExt, EmailProviderStateUpsert};
use sinex_primitives::domain::OperationStatus;
use uuid::Uuid;
use xtask::sandbox::prelude::*;

fn provider_state(operation_id: Uuid, sync_state: &str) -> EmailProviderStateUpsert {
    EmailProviderStateUpsert {
        source_id: "email.mailbox".to_string(),
        mode_id: "source:email.mailbox.imap-scheduled-sync".to_string(),
        provider: "imap".to_string(),
        account_binding_ref: "operator-mailbox:primary".to_string(),
        mailbox_scope: "INBOX".to_string(),
        operation_id,
        result_status: OperationStatus::Success,
        auth_state: "authorized".to_string(),
        network_state: "online".to_string(),
        sync_state: sync_state.to_string(),
        rate_limit_state: Some("none".to_string()),
        runtime_state_ref: "email.provider_runtime.imap".to_string(),
        coverage_ref: "coverage:email.mailbox.imap.provider_runtime".to_string(),
        debt_ref: "debt:email.mailbox.imap.provider_runtime".to_string(),
        cursor_kind: Some("imap_uid".to_string()),
        cursor_value: Some("700:41".to_string()),
        continuity_state: Some("valid".to_string()),
        provider_runtime: json!({
            "provider": "imap",
            "runtime_observation_contract": {
                "auth_state": "authorized",
                "network_state": "online",
                "sync_state": sync_state,
                "rate_limit_state": "none"
            }
        }),
        provider_cursor: Some(json!({
            "provider": "imap",
            "cursor_kind": "imap_uid",
            "cursor_value": "700:41"
        })),
        provider_failure: None,
    }
}

#[sinex_test]
async fn email_provider_state_upsert_keeps_current_scope_row(ctx: TestContext) -> TestResult<()> {
    let repo = ctx.pool().email_provider_states();
    let first_operation_id = Uuid::now_v7();
    let second_operation_id = Uuid::now_v7();

    let first = repo
        .upsert(provider_state(first_operation_id, "completed"))
        .await?;
    let second = repo
        .upsert(provider_state(second_operation_id, "failed"))
        .await?;

    assert_eq!(first.id, second.id);
    assert_eq!(second.operation_id, second_operation_id);
    assert_eq!(second.sync_state, "failed");

    let rows = repo.list_current_by_source("email.mailbox").await?;
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].operation_id, second_operation_id);
    assert_eq!(rows[0].cursor_value.as_deref(), Some("700:41"));
    Ok(())
}
