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

fn mail(id: &str, subject: Option<&str>, thread: Option<&str>, from: &str, secs: i64) -> Event {
    derived(
        &MailFused {
            account: Some("synthetic-account".into()),
            labels: vec![],
            record: id.into(),
            message_id: Some(id.into()),
            subject: subject.map(str::to_owned),
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
            &Env::at(
                at(86_400 * 3),
                sinex_model::time_settings::TimeSettings::new("Europe/Warsaw", 4, 0)
                    .expect("fixture day setting"),
            ),
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
        mail("<1>", Some("Plans"), None, "a@x", 100),
        mail("<2>", Some("RE: Re:  plans"), None, "b@x", 200),
        mail("<3>", Some("Other"), Some("t9"), "a@x", 300),
        mail("<4>", Some("Unrelated subject"), Some("t9"), "c@x", 400),
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
            (
                "mail",
                r#"["synthetic-account","message:<1>"]"#,
                1,
                vec!["a@x"]
            ),
            (
                "mail",
                r#"["synthetic-account","message:<2>"]"#,
                1,
                vec!["b@x"]
            ),
            (
                "mail",
                r#"["synthetic-account","thread:t9"]"#,
                2,
                vec!["a@x", "c@x"]
            ),
        ]
    );
    let rust = got.iter().find(|d| d.conversation == "#rust").unwrap();
    assert_eq!((rust.first_at, rust.last_at), (at(0), at(60)));
}

#[devkit::test]
fn the_result_does_not_depend_on_input_order() {
    let mut inputs = vec![
        chat("#rust", "ann", 0, ChatLineKind::Message),
        mail("<1>", Some("Plans"), None, "a@x", 100),
        chat("#rust", "bob", 60, ChatLineKind::Message),
        mail("<2>", Some("Re: plans"), None, "b@x", 200),
    ];
    let forward = days(&inputs);
    inputs.reverse();
    assert_eq!(days(&inputs), forward);
}

#[devkit::test]
fn mail_threads_require_recorded_ids_and_do_not_collide_with_message_ids() {
    let inputs = vec![
        mail("<1>", Some("Plans"), None, "a@x", 100),
        mail("<2>", Some("RE: Plans"), None, "b@x", 200),
        mail("<3>", None, None, "c@x", 300),
        mail("<4>", Some("Other"), Some("<1>"), "d@x", 400),
        mail("<5>", Some("Plans"), Some(" "), "e@x", 500),
    ];
    let got = days(&inputs);
    let conversations: Vec<&str> = got.iter().map(|day| day.conversation.as_str()).collect();
    assert_eq!(
        conversations,
        [
            r#"["synthetic-account","message:<1>"]"#,
            r#"["synthetic-account","message:<2>"]"#,
            r#"["synthetic-account","message:<3>"]"#,
            r#"["synthetic-account","message:<5>"]"#,
            r#"["synthetic-account","thread:<1>"]"#
        ]
    );
    assert!(got.iter().all(|day| day.messages == 1));
}

#[devkit::test]
fn equal_time_messages_use_record_identity_instead_of_reading_ids() {
    let mut messages = vec![
        chat("#same", "ann", 0, ChatLineKind::Message),
        chat("#same", "bob", 0, ChatLineKind::Message),
    ];
    messages.sort_by_key(crate::derived_event::record_identity);
    let ids = [messages[0].id, messages[1].id];
    messages[0].id = *ids.iter().max().unwrap();
    messages[1].id = *ids.iter().min().unwrap();
    let expected: Vec<_> = messages.iter().map(|message| message.id).collect();
    messages.reverse();
    let output = ConversationDays
        .compute(
            &json!({"day":"2024-06-15"}),
            &messages,
            &Env::at(
                at(86_400),
                sinex_model::time_settings::TimeSettings::new("Europe/Warsaw", 4, 0)
                    .expect("fixture day setting"),
            ),
        )
        .outputs;
    let sinex_model::Origin::Derived { parents } = &output[0].origin else {
        panic!("conversation output must name its messages")
    };
    assert_eq!(parents, &expected);
}

#[devkit::test]
fn provider_account_and_missing_native_ids_never_merge_unrelated_mail() {
    let first = mail("one", None, Some("thread"), "a@example.invalid", 0);
    let mut other = mail("two", None, Some("thread"), "b@example.invalid", 1);
    other.payload["account"] = json!("other-account");
    assert_eq!(days(&[first.clone(), other.clone()]).len(), 2);
    let mut unknown = first;
    unknown.payload["account"] = Value::Null;
    unknown.payload["message_id"] = Value::Null;
    other.payload["account"] = Value::Null;
    other.payload["message_id"] = Value::Null;
    assert_eq!(days(&[unknown, other]).len(), 2);
}
