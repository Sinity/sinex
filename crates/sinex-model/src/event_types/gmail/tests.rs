use super::*;
use crate::{EventPayload, KeyKind, ReplaceRule};

#[test]
fn provider_records_have_account_keys_and_unknown_event_times() {
    let gap = HistoryGap {
        account: "one@example.test".into(),
        requested_history_id: "184467440737095516169999".into(),
        status: 404,
        reason: "history_expired_or_unavailable".into(),
        observed_at: Timestamp::now(),
    };
    assert_eq!(
        HistoryGap::KEY,
        KeyKind::Fields(&["account", "requested_history_id"])
    );
    assert_eq!(HistoryGap::REPLACE, ReplaceRule::SuppressRepeat);
    assert_eq!(gap.ts_orig(), crate::TsOrig::unknown());
    assert_eq!(
        ProviderMessage::KEY,
        KeyKind::Fields(&["account", "provider_message_id", "history_id"])
    );
    assert_eq!(
        MailChanged::KEY,
        KeyKind::Fields(&["account", "history_id"])
    );
}

#[test]
fn ordered_changes_keep_repeated_labels_and_extra_provider_fields() {
    let change: MessageChange = serde_json::from_value(serde_json::json!({"message":{"id":"m","threadId":"t","labelIds":["INBOX"],"future":true},"labelIds":["a","a","b"],"other":42})).unwrap();
    assert_eq!(change.label_ids.as_ref().unwrap(), &["a", "a", "b"]);
    assert_eq!(change.message.extra["future"], true);
    assert_eq!(change.extra["other"], 42);
    let schema = MailChanged::payload_schema();
    assert!(schema["properties"]["labels_removed"].is_object());
}

#[test]
fn absent_label_ids_stay_distinct_from_an_empty_supplied_list() {
    let absent: MessageChange =
        serde_json::from_value(serde_json::json!({"message":{"id":"m"}})).unwrap();
    let empty: MessageChange =
        serde_json::from_value(serde_json::json!({"message":{"id":"m"},"labelIds":[]})).unwrap();
    assert!(absent.label_ids.is_none());
    assert_eq!(empty.label_ids, Some(vec![]));
    assert!(
        serde_json::to_value(&absent)
            .unwrap()
            .get("labelIds")
            .is_none()
    );
}

#[test]
fn repeated_provider_observations_share_a_key_only_within_the_account() {
    let payload = ProviderMessage {
        account: "one@example.test".into(),
        provider_message_id: "message".into(),
        thread_id: "thread".into(),
        history_id: "184467440737095516169999".into(),
        labels: vec![],
        internal_date: None,
        message_id: "rfc@example.test".into(),
    };
    let build = |payload: &ProviderMessage| {
        crate::Event::build(
            payload,
            "communication.gmail",
            crate::Origin::Record {
                location: crate::RecordLocation {
                    material: crate::MaterialId::new(),
                    anchor: 0,
                },
                output_index: 0,
            },
            crate::ReadingOrder(1),
            [0; 32],
            crate::Class::Guarded,
        )
        .unwrap()
    };
    let first = build(&payload);
    let repeated = build(&payload);
    let mut other = payload.clone();
    other.account = "two@example.test".into();
    assert_eq!(first.equivalence_key, repeated.equivalence_key);
    assert_ne!(first.equivalence_key, build(&other).equivalence_key);
    assert_eq!(first.payload["history_id"], "184467440737095516169999");
}
