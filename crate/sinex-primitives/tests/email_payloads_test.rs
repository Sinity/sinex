use sinex_primitives::Timestamp;
use sinex_primitives::events::payloads::email::{
    EmailAuthorizationState, EmailCaptureRuntimeObservedPayload, EmailContinuityState,
    EmailMailboxFormat, EmailMessageReceivedPayload, EmailNetworkState, EmailProviderKind,
    EmailProviderRuntime, EmailRateLimitState, EmailSyncCursorKind, EmailSyncCursorObservedPayload,
    EmailSyncState,
};
use xtask::sandbox::sinex_test;

#[sinex_test]
async fn typed_mailbox_format_serializes_as_package_mode_value() -> xtask::sandbox::TestResult<()> {
    let value = serde_json::to_value(EmailMessageReceivedPayload {
        message_id: Some("m-1@example.com".to_string()),
        date: None,
        from: vec!["Alice <alice@example.com>".to_string()],
        to: vec!["Bob <bob@example.com>".to_string()],
        cc: Vec::new(),
        bcc: Vec::new(),
        subject: Some("Quarterly plan".to_string()),
        in_reply_to: None,
        references: Vec::new(),
        list_id: None,
        folder: Some("INBOX".to_string()),
        source_file: "Maildir/INBOX/cur/1:2,S".to_string(),
        raw_material_id: "material-1".to_string(),
        mailbox_format: EmailMailboxFormat::MaildirStaged,
        maildir_subdir: Some("cur".to_string()),
        maildir_flags: vec!["S".to_string()],
        maildir_stable_filename: Some("1".to_string()),
        mbox_file: None,
        mbox_byte_start: None,
        mbox_byte_end: None,
        size_bytes: 128,
        body_bytes: 32,
        attachment_count: 0,
        provider_material: None,
    })?;

    assert_eq!(value["mailbox_format"], "maildir-staged");
    Ok(())
}

#[sinex_test]
async fn typed_provider_runtime_payloads_keep_provider_coordinates_explicit()
-> xtask::sandbox::TestResult<()> {
    let observed_at = Timestamp::from_unix_timestamp_nanos(1_700_000_000_000_000_000)
        .ok_or_else(|| color_eyre::eyre::eyre!("test timestamp must be valid"))?;
    let cursor = serde_json::to_value(EmailSyncCursorObservedPayload {
        provider: EmailProviderKind::Gmail,
        account_binding_ref: "operator-mailbox:primary".to_string(),
        mailbox_scope: Some("INBOX".to_string()),
        cursor_kind: EmailSyncCursorKind::GmailHistoryId,
        cursor_value: Some("12345".to_string()),
        uidvalidity: None,
        uid: None,
        gmail_history_id: Some("12345".to_string()),
        page_token: None,
        observed_at,
        continuity_state: EmailContinuityState::Current,
        required_action: None,
        caveats: Vec::new(),
    })?;

    assert_eq!(cursor["provider"], "gmail");
    assert_eq!(cursor["cursor_kind"], "gmail-history-id");
    assert_eq!(cursor["continuity_state"], "current");

    let runtime = serde_json::to_value(EmailCaptureRuntimeObservedPayload {
        provider: EmailProviderKind::Imap,
        account_binding_ref: "operator-mailbox:primary".to_string(),
        mode_id: "source:email.mailbox.imap-idle-live".to_string(),
        observed_at,
        provider_runtime: EmailProviderRuntime::IdleLive,
        auth_state: EmailAuthorizationState::Authorized,
        network_state: EmailNetworkState::Online,
        rate_limit_state: Some(EmailRateLimitState::Clear),
        sync_state: EmailSyncState::Syncing,
        pending_messages: Some(2),
        pending_material_bytes: Some(512),
        caveats: Vec::new(),
        actions: vec!["email.mailbox.pause".to_string()],
    })?;

    assert_eq!(runtime["provider"], "imap");
    assert_eq!(runtime["provider_runtime"], "idle-live");
    assert_eq!(runtime["auth_state"], "authorized");
    assert_eq!(runtime["network_state"], "online");
    assert_eq!(runtime["rate_limit_state"], "clear");
    assert_eq!(runtime["sync_state"], "syncing");
    Ok(())
}
