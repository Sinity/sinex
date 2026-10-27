use super::*;
use crate::{EventPayload, KeyKind, ReplaceRule};

#[test]
fn provider_records_have_account_keys_and_unknown_event_times() {
    let gap = HistoryGap { account: "one@example.test".into(), requested_history_id: "184467440737095516169999".into(), status: 404, reason: "history_expired_or_unavailable".into(), observed_at: Timestamp::now() };
    assert_eq!(HistoryGap::KEY, KeyKind::Fields(&["account", "requested_history_id"]));
    assert_eq!(HistoryGap::REPLACE, ReplaceRule::SuppressRepeat);
    assert_eq!(gap.ts_orig(), crate::TsOrig::unknown());
    assert_eq!(ProviderMessage::KEY, KeyKind::Fields(&["account", "provider_message_id", "history_id"]));
    assert_eq!(MailChanged::KEY, KeyKind::Fields(&["account", "history_id"]));
}

#[test]
fn ordered_changes_keep_repeated_labels_and_extra_provider_fields() {
    let change: MessageChange = serde_json::from_value(serde_json::json!({"message":{"id":"m","threadId":"t","labelIds":["INBOX"],"future":true},"labelIds":["a","a","b"],"other":42})).unwrap();
    assert_eq!(change.label_ids, ["a", "a", "b"]);
    assert_eq!(change.message.extra["future"], true);
    assert_eq!(change.extra["other"], 42);
    let schema = MailChanged::payload_schema();
    assert!(schema["properties"]["labels_removed"].is_object());
}
