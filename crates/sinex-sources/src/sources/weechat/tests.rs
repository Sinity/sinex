use super::*;

fn parse(line: &str) -> ChatMessage {
    let p = WeeChatParser {
        zone: jiff::tz::TimeZone::get("Europe/Warsaw").unwrap(),
    };
    p.parse(&line.as_bytes(), &(), &crate::RecordContext::none())
        .unwrap()
        .0
        .remove(0)
}

#[test]
fn local_times_use_the_configured_zone() {
    let m = parse("2024-07-01 12:00:00\t@op\thello\n");
    assert_eq!(
        m.sent_at,
        Timestamp::from_unix_secs(1_719_828_000).unwrap(),
        "CEST is UTC+2"
    );
    assert_eq!(
        (m.kind, m.nick.as_deref(), m.prefix.as_deref()),
        (ChatLineKind::Message, Some("op"), Some("@"))
    );
    let w = parse("2024-01-15 12:00:00\tann\thi\n");
    assert_eq!(
        w.sent_at,
        Timestamp::from_unix_secs(1_705_316_400).unwrap(),
        "CET is UTC+1"
    );
    // The repeated hour at the October change takes the earlier instant.
    let r = parse("2024-10-27 02:30:00\tann\tx\n");
    assert_eq!(r.sent_at, Timestamp::from_unix_secs(1_729_989_000).unwrap());
}

#[test]
fn line_kinds() {
    assert_eq!(
        parse("2024-09-18 23:30:12\t-->\tsinity (~s@h) has joined #c\n").kind,
        ChatLineKind::Join
    );
    let q = parse("2024-09-18 23:30:12\t<--\tann (~a@h) has quit (Ping timeout)\n");
    assert_eq!(
        (q.kind, q.nick.as_deref()),
        (ChatLineKind::Quit, Some("ann"))
    );
    assert_eq!(
        parse("2024-09-18 23:30:12\t<--\tann (~a@h) has left #c\n").kind,
        ChatLineKind::Part
    );
    assert_eq!(
        parse("2024-09-18 23:30:12\t--\tTopic for #c is \"x\"\n").kind,
        ChatLineKind::System
    );
    let a = parse("2024-09-18 23:30:12\t *\tann waves\n");
    assert_eq!(
        (a.kind, a.nick.as_deref()),
        (ChatLineKind::Action, Some("ann"))
    );
    assert_eq!(
        parse("2024-09-18 23:30:12\t=!=\terror\n").kind,
        ChatLineKind::System
    );
}

#[test]
fn empty_lines_are_not_messages() {
    let p = WeeChatParser {
        zone: jiff::tz::TimeZone::UTC,
    };
    assert!(
        p.parse(&&b"\n"[..], &(), &crate::RecordContext::none())
            .unwrap()
            .0
            .is_empty()
    );
}
