use super::*;
use crate::{
    Class, EventPayload, KeyKind, MaterialId, Origin, ReadingOrder, RecordLocation, ReplaceRule,
};

fn at(seconds: i64) -> Timestamp {
    Timestamp::from_unix_secs(seconds).unwrap()
}

fn message(text: &str) -> Message {
    Message {
        id: "claude-code:m1".into(),
        session: "claude-code:s1".into(),
        parent: None,
        role: "user".into(),
        written_by: "human".into(),
        prompt_kind: Some("prose".into()),
        at: Some(at(1_790_000_000)),
        model: None,
        text: text.into(),
        usage: Usage::default(),
        cost_nusd: None,
        stop_reason: None,
        duration_ms: None,
        attachments: vec![],
        copies: vec![],
    }
}

fn origin() -> Origin {
    Origin::Record {
        location: RecordLocation {
            material: MaterialId::new(),
            anchor: 0,
        },
        ordinal: 0,
    }
}

#[devkit::test]
fn sessions_messages_and_calls_are_keyed_by_polylogue_ids_and_replaced_when_newer() {
    for (key, replace) in [
        (Session::KEY, Session::REPLACE),
        (Message::KEY, Message::REPLACE),
        (ToolCall::KEY, ToolCall::REPLACE),
        (Turn::KEY, Turn::REPLACE),
    ] {
        assert_eq!(key, KeyKind::Fields(&["id"]));
        assert_eq!(replace, ReplaceRule::ReplaceWhenNewer);
    }
    assert_eq!(Note::REPLACE, ReplaceRule::SuppressRepeat);
    let a = Message::payload_schema();
    assert!(
        a["required"]
            .as_array()
            .unwrap()
            .iter()
            .any(|f| f == "text")
    );
}

#[devkit::test]
fn message_text_is_stored_guarded_whatever_the_producer_asks() {
    assert_eq!(Message::CLASS, Class::Guarded);
    assert_eq!(Session::CLASS, Class::Open);
    let event = crate::Event::build(
        &message("a prompt"),
        "polylogue",
        origin(),
        ReadingOrder(1),
        [0; 32],
        Class::Open,
    )
    .unwrap();
    assert_eq!(event.class, Class::Guarded);
    let sealed = crate::Event::build(
        &message("a prompt"),
        "polylogue",
        origin(),
        ReadingOrder(1),
        [0; 32],
        Class::Sealed,
    )
    .unwrap();
    assert_eq!(sealed.class, Class::Sealed);
}

#[devkit::test]
fn the_same_message_read_twice_has_one_equivalence_key() {
    let build = |text: &str| {
        crate::Event::build(
            &message(text),
            "polylogue",
            origin(),
            ReadingOrder(1),
            [0; 32],
            Class::Guarded,
        )
        .unwrap()
    };
    assert_eq!(
        build("first reading").equivalence_key,
        build("a newer parser's reading").equivalence_key
    );
}
