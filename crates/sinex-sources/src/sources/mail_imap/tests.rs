use super::*;
use std::net::TcpListener;

const M1: &str = "Message-ID: <1@x>\r\nFrom: a@x\r\nTo: b@x\r\nSubject: one\r\nDate: Mon, 5 Oct 2026 10:00:00 +0000\r\n\r\nhello\r\n";
const M2: &str = "Message-ID: <2@x>\r\nFrom: a@x\r\nTo: b@x\r\nSubject: two\r\nDate: Mon, 5 Oct 2026 11:00:00 +0000\r\n\r\nworld\r\n";

/// A poll by a reader that remembers no message.
trait PollFresh {
    fn poll_fresh(&self, cursor: &Value) -> Result<Acquired, DriverError>;
}

impl PollFresh for ImapMail {
    fn poll_fresh(&self, cursor: &Value) -> Result<Acquired, DriverError> {
        self.poll(cursor, &Known::new()).map(|p| p.acquired)
    }
}

/// A scripted IMAP server: answers LOGIN/AUTHENTICATE, SELECT and UID
/// FETCH from `messages` (uid, body).
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
                    let range = range.split(' ').next().unwrap();
                    let (from, through) = range.split_once(':').unwrap();
                    let (from, through): (u32, u32) =
                        (from.parse().unwrap(), through.parse().unwrap());
                    let mut out = String::new();
                    for (i, (uid, body)) in messages.iter().enumerate() {
                        if *uid >= from && *uid <= through {
                            out.push_str(&format!(
                                "* {} FETCH (UID {uid} BODY[] {{{}}}\r\n{body})\r\n",
                                i + 1,
                                body.len()
                            ));
                        }
                    }
                    out + &format!("{tag} OK fetched\r\n")
                } else if let Some(range) = cmd.strip_prefix("UID SEARCH UID 1:") {
                    let highest: u32 = range.trim().parse().unwrap();
                    let uids: Vec<String> = messages
                        .iter()
                        .filter(|(uid, _)| *uid <= highest)
                        .map(|(uid, _)| uid.to_string())
                        .collect();
                    format!("* SEARCH {}\r\n{tag} OK searched\r\n", uids.join(" "))
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

#[devkit::test]
fn new_messages_are_fetched_after_the_cursor_and_parse() {
    let port = server(vec![(4, M1), (7, M2)], 99, false);
    let imap = mail(port, Some("secret"), None);
    let a = imap.poll_fresh(&Value::Null).unwrap();
    let m = a.material.unwrap();
    assert_eq!(m.records.len(), 2);
    assert_eq!(&m.bytes[m.records[0].0..m.records[0].1], M1.as_bytes());
    assert_eq!(m.manifest.detail("uids"), &json!([4, 7]));
    let cursor: Cursor = serde_json::from_value((m.cursor_at)(m.bytes.len()).unwrap()).unwrap();
    assert_eq!((cursor.uid_validity, cursor.uid_next), (Some(99), 8));
    let parsed =
        super::super::mail::parse_message(&m.bytes[m.records[1].0..m.records[1].1]).unwrap();
    assert_eq!(parsed.subject.as_deref(), Some("two"));
    // Nothing after UID 7.
    let again = imap
        .poll_fresh(&serde_json::to_value(&cursor).unwrap())
        .unwrap();
    assert!(again.material.is_none());
}

#[devkit::test]
fn xoauth2_is_used_when_a_token_file_is_set_and_bad_credentials_fail_without_echoing_them() {
    let port = server(vec![(1, M1)], 5, true);
    assert!(
        mail(port, None, Some("tok"))
            .poll_fresh(&Value::Null)
            .unwrap()
            .material
            .is_some()
    );
    let port = server(vec![(1, M1)], 5, false);
    let err = match mail(port, Some("wrong-password"), None).poll_fresh(&Value::Null) {
        Err(e) => e.to_string(),
        Ok(_) => panic!("bad credentials were accepted"),
    };
    assert!(
        err.contains("login") && !err.contains("wrong-password"),
        "{err}"
    );
}

#[devkit::test]
fn a_renumbered_mailbox_is_read_from_the_start() {
    let port = server(vec![(1, M1), (2, M2)], 200, false);
    let old = Cursor {
        uid_validity: Some(100),
        uid_next: 50,
        through: None,
        material: None,
        part_index: 0,
        ..Cursor::default()
    };
    let m = mail(port, Some("secret"), None)
        .poll_fresh(&serde_json::to_value(old).unwrap())
        .unwrap()
        .material
        .unwrap();
    assert_eq!(m.records.len(), 2);
}

#[devkit::test]
fn an_empty_examined_window_reaches_the_later_message() {
    let port = server(vec![(201, M1)], 99, false);
    let imap = mail(port, Some("secret"), None);
    let first = imap.poll_fresh(&Value::Null).unwrap();
    assert!(first.material.is_none());
    let next: Cursor = serde_json::from_value(first.cursor_without_material.clone()).unwrap();
    assert_eq!(next.uid_next, 201);
    let second = imap
        .poll_fresh(&first.cursor_without_material)
        .unwrap()
        .material
        .unwrap();
    assert_eq!(second.manifest.detail("uids"), &json!([201]));
    assert_eq!(second.bytes, M1.as_bytes());
}

/// A finite command transcript; joining also propagates server-side assertions.
fn scripted(responses: Vec<(&'static str, String)>) -> (u16, std::thread::JoinHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let port = listener.local_addr().unwrap().port();
    let handle = std::thread::spawn(move || {
        let (mut stream, _) = listener.accept().unwrap();
        stream
            .set_read_timeout(Some(Duration::from_secs(5)))
            .unwrap();
        let mut reader = BufReader::new(stream.try_clone().unwrap());
        let mut idle_tag = String::new();
        stream.write_all(b"* OK fake IMAP ready\r\n").unwrap();
        for (expected, response) in responses {
            let mut line = String::new();
            assert_ne!(
                reader.read_line(&mut line).unwrap(),
                0,
                "expected {expected}"
            );
            let (tag, command) = if line.trim_end() == "DONE" {
                (idle_tag.as_str(), "DONE")
            } else {
                line.trim_end().split_once(' ').unwrap()
            };
            assert_eq!(command, expected);
            stream
                .write_all(response.replace("{tag}", tag).as_bytes())
                .unwrap();
            if command == "IDLE" {
                idle_tag = tag.to_owned();
            }
        }
    });
    (port, handle)
}

fn selected(next: Option<u32>, validity: u32) -> String {
    let next = next
        .map(|n| format!("* OK [UIDNEXT {n}] ok\r\n"))
        .unwrap_or_default();
    format!("* OK [UIDVALIDITY {validity}] ok\r\n{next}{{tag}} OK selected\r\n")
}

#[devkit::test]
fn examined_empty_progress_is_clamped_and_unknown_or_stale_bounds_stay_put() {
    for (boundary, fetch, expected) in [
        (Some(51), "UID FETCH 1:50 (UID BODY.PEEK[])", 51),
        (None, "UID FETCH 1:200 (UID BODY.PEEK[])", 1),
        (Some(1), "UID FETCH 1:200 (UID BODY.PEEK[])", 1),
        (Some(0), "UID FETCH 1:200 (UID BODY.PEEK[])", 1),
    ] {
        let (port, server) = scripted(vec![
            ("LOGIN \"me@x\" \"secret\"", "{tag} OK login\r\n".into()),
            ("SELECT \"INBOX\"", selected(boundary, 99)),
            (fetch, "{tag} OK fetched\r\n".into()),
            ("LOGOUT", "{tag} OK bye\r\n".into()),
        ]);
        let acquired = mail(port, Some("secret"), None)
            .poll_fresh(&Value::Null)
            .unwrap();
        let cursor: Cursor = serde_json::from_value(acquired.cursor_without_material).unwrap();
        assert_eq!(cursor.uid_next, expected, "boundary={boundary:?}");
        server.join().unwrap();
    }
}

#[devkit::test]
fn failed_fetch_returns_no_progress_and_the_same_cursor_can_retry() {
    let previous = json!({"uid_validity": 99, "uid_next": 1, "material": null, "part_index": 0});
    let original = previous.clone();
    let (port, server) = scripted(vec![
        ("LOGIN \"me@x\" \"secret\"", "{tag} OK login\r\n".into()),
        ("SELECT \"INBOX\"", selected(Some(202), 99)),
        (
            "UID FETCH 1:200 (UID BODY.PEEK[])",
            "{tag} NO fetch failed\r\n".into(),
        ),
    ]);
    assert!(
        mail(port, Some("secret"), None)
            .poll_fresh(&previous)
            .is_err()
    );
    server.join().unwrap();
    assert_eq!(previous, original);
    let (port, server) = scripted(vec![
        ("LOGIN \"me@x\" \"secret\"", "{tag} OK login\r\n".into()),
        ("SELECT \"INBOX\"", selected(Some(202), 99)),
        (
            "UID FETCH 1:200 (UID BODY.PEEK[])",
            "{tag} OK fetched\r\n".into(),
        ),
        ("LOGOUT", "{tag} OK bye\r\n".into()),
    ]);
    let acquired = mail(port, Some("secret"), None)
        .poll_fresh(&previous)
        .unwrap();
    assert_eq!(acquired.cursor_without_material["uid_next"], 201);
    server.join().unwrap();
}

#[devkit::test]
fn idle_refreshes_uidnext_and_validity_before_the_next_fetch() {
    for (validity, next, fetch, expected) in [
        (99, 204, "UID FETCH 201:203 (UID BODY.PEEK[])", 204),
        (100, 3, "UID FETCH 1:2 (UID BODY.PEEK[])", 3),
    ] {
        let (port, server) = scripted(vec![
            ("LOGIN \"me@x\" \"secret\"", "{tag} OK login\r\n".into()),
            ("SELECT \"INBOX\"", selected(Some(201), 99)),
            ("IDLE", "+ idling\r\n* 1 EXISTS\r\n".into()),
            ("DONE", "{tag} OK idle ended\r\n".into()),
            ("SELECT \"INBOX\"", selected(Some(next), validity)),
            (fetch, "{tag} OK fetched\r\n".into()),
            ("LOGOUT", "{tag} OK bye\r\n".into()),
        ]);
        let mut imap = mail(port, Some("secret"), None);
        imap.config.idle_seconds = 1;
        let acquired = imap
            .poll_fresh(&json!({"uid_validity":99, "uid_next":201,
            "material":null, "part_index":0}))
            .unwrap();
        assert_eq!(acquired.cursor_without_material["uid_next"], expected);
        assert_eq!(acquired.cursor_without_material["uid_validity"], validity);
        server.join().unwrap();
    }
}

#[devkit::test]
fn returned_uids_allow_progress_without_a_usable_uidnext() {
    for boundary in [None, Some(0), Some(1)] {
        let (port, server) = scripted(vec![
            ("LOGIN \"me@x\" \"secret\"", "{tag} OK login\r\n".into()),
            ("SELECT \"INBOX\"", selected(boundary, 99)),
            (
                "UID FETCH 1:200 (UID BODY.PEEK[])",
                format!(
                    "* 1 FETCH (UID 4 BODY[] {{{}}}\r\n{M1})\r\n{{tag}} OK fetched\r\n",
                    M1.len()
                ),
            ),
            ("LOGOUT", "{tag} OK bye\r\n".into()),
        ]);
        let acquired = mail(port, Some("secret"), None)
            .poll_fresh(&Value::Null)
            .unwrap();
        let material = acquired.material.unwrap();
        assert_eq!(material.bytes, M1.as_bytes());
        assert_eq!(
            (material.cursor_at)(material.bytes.len()).unwrap()["uid_next"],
            5
        );
        server.join().unwrap();
    }
}

#[devkit::test]
fn partial_admission_keeps_unfed_uids_available_for_retry() {
    struct Discard;
    impl crate::drivers::MaterialSink for Discard {
        fn put(&self, bytes: &[u8]) -> Result<String, String> {
            Ok(format!("blake3:{}", blake3::hash(bytes).to_hex()))
        }
        fn put_file(&self, _: &std::path::Path) -> Result<String, String> {
            panic!("IMAP supplies an in-memory answer")
        }
    }
    let port = server(vec![(4, M1), (7, M2)], 99, false);
    let imap = mail(port, Some("secret"), None);
    let acquired = imap.poll_fresh(&Value::Null).unwrap();
    let source = source();
    let prepared = crate::drivers::prepare(
        &source,
        "imap-test",
        sinex_model::RunId::new(),
        acquired,
        Value::Null,
        &Discard,
        crate::Limits {
            max_events: 1,
            max_bytes: usize::MAX,
        },
    )
    .unwrap();
    let batches = prepared
        .batches()
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    assert_eq!(batches.len(), 2);
    let progress = |batch: &sinex_model::Batch| {
        let sinex_model::BatchKind::Source(progress) = &batch.kind else {
            panic!()
        };
        progress.cursor.clone()
    };
    let partial = progress(&batches[0]);
    assert_eq!(partial["uid_next"], 1);
    let repeated = imap.poll_fresh(&partial).unwrap().material.unwrap();
    assert_eq!(repeated.manifest.detail("uids"), &json!([4, 7]));
    let complete = progress(&batches[1]);
    assert_eq!(complete["uid_next"], 8);
    assert!(imap.poll_fresh(&complete).unwrap().material.is_none());
}

#[devkit::test]
fn the_reader_retries_its_held_answer_without_asking_the_server_again() {
    use crate::binding::Reader;
    let port = server(vec![(4, M1), (7, M2)], 99, false);
    let mut reader = ImapReader::new(mail(port, Some("secret"), None));
    let first = reader.poll(&Value::Null).unwrap();
    let material = first.material.unwrap();
    let partial = (material.cursor_at)(material.records[0].1).unwrap();
    // The server answered once; the retry comes from the held answer.
    let again = reader.poll(&partial).unwrap().material.unwrap();
    assert_eq!(again.id, material.id);
    assert_eq!(again.records, material.records[1..]);
    let retry = reader.poll(&Value::Null).unwrap().material.unwrap();
    assert_eq!(retry.id, material.id);
    assert_eq!(retry.records, material.records);
}

#[devkit::test]
fn an_expunged_message_is_removed_where_it_was_read() {
    use crate::binding::Reader;
    let first_port = server(vec![(4, M1), (7, M2)], 99, false);
    let mut reader = ImapReader::new(mail(first_port, Some("secret"), None));
    let first = reader.poll(&Value::Null).unwrap().material.unwrap();
    let admitted = (first.cursor_at)(first.records.last().unwrap().1).unwrap();
    assert!(first.removed.is_empty());
    // UID 4 is gone from the server; nothing is new.
    reader.mail = mail(server(vec![(7, M2)], 99, false), Some("secret"), None);
    let second = reader.poll(&admitted).unwrap().material.unwrap();
    assert!(second.records.is_empty());
    assert_eq!(second.manifest.detail("expunged"), &json!([4]));
    assert_eq!(
        second.removed,
        vec![sinex_model::RecordLocation {
            material: first.id,
            anchor: 0
        }]
    );
    let after: Cursor =
        serde_json::from_value((second.cursor_at)(second.bytes.len()).unwrap()).unwrap();
    assert_eq!(after.uid_next, 8, "the next UID is unchanged");
    // Once admitted, the reader no longer remembers UID 4.
    reader.mail = mail(server(vec![(7, M2)], 99, false), Some("secret"), None);
    let third = reader.poll(&serde_json::to_value(&after).unwrap()).unwrap();
    assert!(third.material.is_none());
    assert_eq!(reader.known.keys().copied().collect::<Vec<_>>(), [7]);
}

#[devkit::test]
fn a_renumbered_mailbox_removes_nothing() {
    let mut known = Known::new();
    known.insert(
        4,
        sinex_model::RecordLocation {
            material: MaterialId::new(),
            anchor: 0,
        },
    );
    let port = server(vec![(1, M2)], 100, false);
    let polled = mail(port, Some("secret"), None)
        .poll(
            &json!({"uid_validity": 99, "uid_next": 8, "part_index": 0}),
            &known,
        )
        .unwrap();
    let m = polled.acquired.material.unwrap();
    assert!(m.removed.is_empty());
    assert_eq!(
        polled.known.unwrap().keys().copied().collect::<Vec<_>>(),
        [1]
    );
}

/// A stored material whose bytes the restore does not read.
struct NoBytes;

impl crate::drain::RecordRead for NoBytes {
    fn length(&self) -> u64 {
        0
    }
    fn read_exact_at(&self, _: u64, _: &mut [u8]) -> std::io::Result<()> {
        Err(std::io::Error::other("the restore reads manifests only"))
    }
}

#[devkit::test]
fn a_restart_remembers_from_saved_memory_and_later_materials() {
    use crate::binding::Reader;
    let older = MaterialId::new();
    let newer = MaterialId::new();
    let mut saved = Known::new();
    for uid in [1, 2, 3] {
        saved.insert(
            uid,
            sinex_model::RecordLocation {
                material: older,
                anchor: u64::from(uid) * 100,
            },
        );
    }
    let memory = memory_of(&saved);
    let manifest = sinex_model::Manifest::from_json(json!({
        "media_type": "message/rfc822", "uid_validity": 99, "uids": [5, 6],
        "expunged": [2], "records": [[0, 10], [10, 20]],
    }));
    let cursor = json!({"uid_validity": 99, "uid_next": 7, "material": newer,
        "part_index": 3, "through": 10});
    let mut reader = ImapReader::new(mail(1, None, None));
    assert_eq!(reader.history_from(&cursor), Some(newer));
    let more = reader
        .restore(
            &cursor,
            crate::binding::StoredMaterial {
                reference: sinex_model::MaterialRef {
                    id: newer,
                    input: None,
                    blob: String::new(),
                    length: 20,
                    part_index: 3,
                    previous_part: Some(older),
                    parser: "rfc5322-mail".into(),
                    parser_version: 1,
                    manifest,
                },
                bytes: std::sync::Arc::new(NoBytes),
                same_revision: true,
            },
        )
        .unwrap();
    assert!(more);
    reader.restore_memory(&memory).unwrap();
    reader.restored().unwrap();
    let uids: Vec<u32> = reader.known.keys().copied().collect();
    // UID 6 lies past the admitted cursor; UID 2 was expunged later.
    assert_eq!(uids, [1, 3, 5]);
    assert_eq!(reader.known[&5].material, newer);
}

#[devkit::test]
fn an_unrepresentable_idle_timeout_fails_before_connecting() {
    let mut reader = mail(0, Some("synthetic"), None);
    reader.config.idle_seconds = u64::MAX;
    let error = Connection::open(&reader.config)
        .err()
        .expect("invalid timeout");
    assert!(error.to_string().contains("idle timeout"), "{error}");
}

#[devkit::test]
fn uid_coordinates_distinguish_equal_messages_and_survive_retries() {
    use crate::Parser;
    use sinex_model::EventPayload;
    const NO_ID: &str = "Subject: identical\r\n\r\nbody\r\n";
    let imap = mail(
        server(vec![(4, NO_ID), (7, NO_ID)], 99, false),
        Some("secret"),
        None,
    );
    let read = || {
        let material = imap.poll_fresh(&Value::Null).unwrap().material.unwrap();
        material
            .records
            .iter()
            .map(|(start, end)| {
                let context = crate::RecordContext {
                    manifest: &material.manifest,
                    member: material.manifest.member_at(*start),
                };
                super::super::mail::MailParser
                    .parse(&&material.bytes[*start..*end], &(), &context)
                    .unwrap()
                    .0
                    .remove(0)
            })
            .collect::<Vec<_>>()
    };
    let first = read();
    let retry = read();
    assert_ne!(first[0].native_id, first[1].native_id);
    assert_eq!(first[0].native_id, retry[0].native_id);
    assert_eq!(first[1].native_id, retry[1].native_id);
    assert_eq!(first[0].message_id, None);
    assert_eq!(
        first[0].key_kind(),
        sinex_model::KeyKind::Fields(&["native_id"])
    );
}
