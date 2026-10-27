use super::*;

// 2024-06-15 10:00 CEST.
const T: i64 = 1_718_438_400;

fn at(secs: i64) -> Timestamp {
    Timestamp::from_unix_secs(T + secs).unwrap()
}

fn parent(secs: i64) -> Event {
    crate::fusion::tests::zsh("x", (T + secs) as f64)
}

fn chat(buffer: &str, nick: &str, secs: i64, kind: ChatLineKind) -> Event {
    crate::fusion::tests::captured(
        &ChatMessage {
            buffer: buffer.into(),
            sent_at: at(secs),
            nick: Some(nick.into()),
            prefix: None,
            text: "hi".into(),
            kind,
        },
        "communication.weechat-log",
        sinex_model::Class::Open,
    )
}

fn mail(id: &str, subject: &str, thread: Option<&str>, from: &str, secs: i64) -> Event {
    derived(
        &MailFused {
            record: id.into(),
            message_id: id.into(),
            subject: Some(subject.into()),
            from: vec![from.into()],
            thread_id: thread.map(str::to_owned),
            sent_at: at(secs),
            observed_by: vec!["communication.imap".into()],
        },
        "mail.fusion",
        [&parent(secs)],
    )
}

fn days(inputs: &[Event]) -> Vec<ConversationDay> {
    ConversationDays
        .compute(
            &json!({"day": "2024-06-15"}),
            inputs,
            &Env::at(at(86_400 * 3)),
        )
        .outputs
        .iter()
        .map(|e| e.payload_as::<ConversationDay>().unwrap())
        .collect()
}

#[devkit::test]
fn messages_group_by_conversation_within_their_provider() {
    let inputs = vec![
        chat("#rust", "ann", 0, ChatLineKind::Message),
        chat("#rust", "bob", 60, ChatLineKind::Action),
        chat("#rust", "carl", 90, ChatLineKind::Join),
        chat("#nix", "ann", 30, ChatLineKind::Message),
        mail("<1>", "Plans", None, "a@x", 100),
        mail("<2>", "RE: Re:  plans", None, "b@x", 200),
        mail("<3>", "Other", Some("t9"), "a@x", 300),
        mail("<4>", "Unrelated subject", Some("t9"), "c@x", 400),
    ];
    let mut got = days(&inputs);
    got.sort_by(|a, b| (&a.channel, &a.conversation).cmp(&(&b.channel, &b.conversation)));
    let summary: Vec<(&str, &str, u64, Vec<&str>)> = got
        .iter()
        .map(|d| {
            (
                d.channel.as_str(),
                d.conversation.as_str(),
                d.messages,
                d.senders.iter().map(String::as_str).collect(),
            )
        })
        .collect();
    assert_eq!(
        summary,
        [
            ("chat", "#nix", 1, vec!["ann"]),
            ("chat", "#rust", 2, vec!["ann", "bob"]),
            ("mail", "plans", 2, vec!["a@x", "b@x"]),
            ("mail", "t9", 2, vec!["a@x", "c@x"]),
        ]
    );
    let rust = got.iter().find(|d| d.conversation == "#rust").unwrap();
    assert_eq!((rust.first_at, rust.last_at), (at(0), at(60)));
}

#[devkit::test]
fn the_result_does_not_depend_on_input_order() {
    let mut inputs = vec![
        chat("#rust", "ann", 0, ChatLineKind::Message),
        mail("<1>", "Plans", None, "a@x", 100),
        chat("#rust", "bob", 60, ChatLineKind::Message),
        mail("<2>", "Re: plans", None, "b@x", 200),
    ];
    let forward = days(&inputs);
    inputs.reverse();
    assert_eq!(days(&inputs), forward);
}
