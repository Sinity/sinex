use super::*;
use std::net::TcpListener;

const M1: &str = "Message-ID: <1@x>\r\nFrom: a@x\r\nTo: b@x\r\nSubject: one\r\nDate: Mon, 5 Oct 2026 10:00:00 +0000\r\n\r\nhello\r\n";
const M2: &str = "Message-ID: <2@x>\r\nFrom: a@x\r\nTo: b@x\r\nSubject: two\r\nDate: Mon, 5 Oct 2026 11:00:00 +0000\r\n\r\nworld\r\n";

/// A scripted IMAP server: answers LOGIN/AUTHENTICATE, SELECT and UID
/// FETCH from `messages` (uid, body), with IDLE announcing `idle_new`.
fn server(messages: Vec<(u32, &'static str)>, uid_validity: u32, expect_token: bool) -> u16 {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let port = listener.local_addr().unwrap().port();
    std::thread::spawn(move || {
        for stream in listener.incoming().take(4) {
            let mut s = stream.unwrap();
            let mut r = BufReader::new(s.try_clone().unwrap());
            s.write_all(b"* OK fake IMAP ready\r\n").unwrap();
            let mut line = String::new();
            while r.read_line(&mut line).unwrap_or(0) > 0 {
                let (tag, cmd) = line.trim_end().split_once(' ').unwrap();
                let (tag, cmd) = (tag.to_owned(), cmd.to_owned());
                line.clear();
                let reply = if cmd.starts_with("LOGIN") {
                    if !expect_token && cmd.contains("\"secret\"") {
                        format!("{tag} OK logged in\r\n")
                    } else {
                        format!("{tag} NO bad credentials\r\n")
                    }
                } else if cmd.starts_with("AUTHENTICATE XOAUTH2") {
                    let decoded = base64::engine::general_purpose::STANDARD
                        .decode(cmd.rsplit(' ').next().unwrap())
                        .unwrap();
                    if expect_token && String::from_utf8(decoded).unwrap().contains("Bearer tok") {
                        format!("{tag} OK authenticated\r\n")
                    } else {
                        format!("{tag} NO bad token\r\n")
                    }
                } else if cmd.starts_with("SELECT") {
                    let next = messages.iter().map(|(u, _)| u + 1).max().unwrap_or(1);
                    format!(
                        "* {} EXISTS\r\n* OK [UIDVALIDITY {uid_validity}] ok\r\n* OK [UIDNEXT {next}] ok\r\n{tag} OK [READ-WRITE] selected\r\n",
                        messages.len()
                    )
                } else if let Some(range) = cmd.strip_prefix("UID FETCH ") {
                    let from: u32 = range.split(':').next().unwrap().parse().unwrap();
                    let mut out = String::new();
                    for (i, (uid, body)) in messages.iter().enumerate() {
                        if *uid >= from {
                            out.push_str(&format!(
                                "* {} FETCH (UID {uid} BODY[] {{{}}}\r\n{body})\r\n",
                                i + 1,
                                body.len()
                            ));
                        }
                    }
                    out + &format!("{tag} OK fetched\r\n")
                } else if cmd.starts_with("LOGOUT") {
                    let _ = s.write_all(format!("* BYE\r\n{tag} OK bye\r\n").as_bytes());
                    break;
                } else {
                    format!("{tag} BAD unknown\r\n")
                };
                s.write_all(reply.as_bytes()).unwrap();
            }
        }
    });
    port
}

fn mail(port: u16, password: Option<&str>, token: Option<&str>) -> ImapMail {
    let dir = std::env::temp_dir().join(format!("sinex-imap-{}", uuid::Uuid::now_v7()));
    std::fs::create_dir_all(&dir).unwrap();
    let write = |name: &str, v: Option<&str>| {
        v.map(|v| {
            let p = dir.join(name);
            std::fs::write(&p, v).unwrap();
            p
        })
    };
    ImapMail {
        config: ImapConfig {
            host: "127.0.0.1".into(),
            port,
            username: "me@x".into(),
            password_file: write("pw", password),
            token_file: write("tok", token),
            mailbox: "INBOX".into(),
            tls: false,
            batch: 200,
            idle_seconds: 0,
        },
        host: "h".into(),
    }
}

#[test]
fn new_messages_are_fetched_after_the_cursor_and_parse() {
    let port = server(vec![(4, M1), (7, M2)], 99, false);
    let imap = mail(port, Some("secret"), None);
    let a = imap.poll(&Value::Null).unwrap();
    let m = a.material.unwrap();
    assert_eq!(m.records.len(), 2);
    assert_eq!(&m.bytes[m.records[0].0..m.records[0].1], M1.as_bytes());
    assert_eq!(m.manifest["uids"], json!([4, 7]));
    let cursor: Cursor = serde_json::from_value((m.cursor_at)(m.bytes.len())).unwrap();
    assert_eq!((cursor.uid_validity, cursor.uid_next), (Some(99), 8));
    let parsed =
        super::super::mail::parse_message(&m.bytes[m.records[1].0..m.records[1].1]).unwrap();
    assert_eq!(parsed.subject.as_deref(), Some("two"));
    // Nothing after UID 7.
    let again = imap.poll(&serde_json::to_value(&cursor).unwrap()).unwrap();
    assert!(again.material.is_none());
}

#[test]
fn xoauth2_is_used_when_a_token_file_is_set_and_bad_credentials_fail_without_echoing_them() {
    let port = server(vec![(1, M1)], 5, true);
    assert!(
        mail(port, None, Some("tok"))
            .poll(&Value::Null)
            .unwrap()
            .material
            .is_some()
    );
    let port = server(vec![(1, M1)], 5, false);
    let err = match mail(port, Some("wrong-password"), None).poll(&Value::Null) {
        Err(e) => e.to_string(),
        Ok(_) => panic!("bad credentials were accepted"),
    };
    assert!(
        err.contains("login") && !err.contains("wrong-password"),
        "{err}"
    );
}

#[test]
fn a_renumbered_mailbox_is_read_from_the_start() {
    let port = server(vec![(1, M1), (2, M2)], 200, false);
    let old = Cursor {
        uid_validity: Some(100),
        uid_next: 50,
        material: None,
        part_index: 0,
    };
    let m = mail(port, Some("secret"), None)
        .poll(&serde_json::to_value(old).unwrap())
        .unwrap()
        .material
        .unwrap();
    assert_eq!(m.records.len(), 2);
}
