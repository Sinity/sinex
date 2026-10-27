use super::*;

const MESSAGE: &[u8] = b"From 1234@xxx Tue Mar 05 10:00:00 +0000 2024\n\
X-GM-THRID: 1790000000000000001\n\
X-Gmail-Labels: Inbox,\"Important\",Category Updates\n\
Message-ID: <abc@example.com>\n\
Date: Tue, 05 Mar 2024 10:00:00 +0100\n\
From: Ann Example <ann@example.com>\n\
To: bob@example.com, \"Carl\" <carl@example.com>\n\
Subject: =?UTF-8?Q?Za=C5=BC=C3=B3=C5=82=C4=87?=\n\
References: <a@x> <b@x>\n\
MIME-Version: 1.0\n\
Content-Type: multipart/mixed; boundary=\"b\"\n\
\n\
--b\n\
Content-Type: text/plain; charset=utf-8\n\
\n\
Hello\n\
>From the archive\n\
--b\n\
Content-Type: application/pdf; name=\"r.pdf\"\n\
Content-Disposition: attachment; filename=\"r.pdf\"\n\
Content-Transfer-Encoding: base64\n\
\n\
aGVsbG8=\n\
--b--\n";

#[devkit::test]
fn reads_headers_labels_text_and_attachments() {
    let (m, ()) = MailParser
        .parse(&MESSAGE, &(), &crate::RecordContext::none())
        .unwrap();
    let m = &m[0];
    assert_eq!(m.message_id, "abc@example.com");
    assert_eq!(m.subject.as_deref(), Some("Zażółć"));
    assert_eq!(m.date, Timestamp::from_unix_secs(1_709_629_200).ok());
    assert_eq!(m.from, vec!["Ann Example <ann@example.com>"]);
    assert_eq!(m.to, vec!["bob@example.com", "Carl <carl@example.com>"]);
    assert_eq!(m.labels, vec!["Inbox", "Important", "Category Updates"]);
    assert_eq!(m.thread_id.as_deref(), Some("1790000000000000001"));
    assert_eq!(m.references, vec!["a@x", "b@x"]);
    // The newline before a boundary belongs to the boundary (RFC 2046).
    assert_eq!(m.text.as_deref(), Some("Hello\nFrom the archive"));
    assert_eq!(m.attachments.len(), 1);
    assert_eq!(m.attachments[0].filename.as_deref(), Some("r.pdf"));
    assert_eq!(m.attachments[0].size, 5);
    assert!(!m.html_only);
}

#[devkit::test]
fn a_message_without_id_or_date_is_hashed_and_undated() {
    let (m, ()) = MailParser
        .parse(
            &&b"Subject: x\n\nbody\n"[..],
            &(),
            &crate::RecordContext::none(),
        )
        .unwrap();
    assert!(m[0].message_id.starts_with("blake3:"));
    assert_eq!(m[0].date, None);
}

#[devkit::test]
fn maildir_selects_message_files() {
    let d = maildir_driver(PathBuf::from("/m"), "h".into());
    assert!((d.select)("INBOX/cur/1.host:2,S"));
    assert!((d.select)("new/2.host"));
    assert!(!(d.select)("INBOX/tmp/3"));
    assert!(!(d.select)("dovecot.index"));
}

fn parse(raw: &[u8]) -> MailMessage {
    MailParser
        .parse(&raw, &(), &crate::RecordContext::none())
        .unwrap()
        .0
        .remove(0)
}

#[devkit::test]
fn an_mbox_frames_one_record_per_message_and_unquotes_from_lines() {
    let mbox = b"From a@x Mon Jan  1 00:00:00 2024\n\
Message-ID: <one@x>\n\
Subject: one\n\
\n\
>From the start\n\
>>From twice quoted\n\
\n\
From b@x Mon Jan  1 00:00:01 2024\n\
Message-ID: <two@x>\n\
Subject: two\n\
\n\
body\n";
    let (records, _) = crate::drivers::export_archive::frame_mbox(mbox);
    assert_eq!(records.len(), 2);
    let first = parse(&mbox[records[0].0..records[0].1]);
    assert_eq!(first.message_id, "one@x");
    assert_eq!(
        first.text.as_deref().map(str::trim_end),
        Some("From the start\n>From twice quoted"),
        "one level of quoting is undone"
    );
    assert_eq!(
        parse(&mbox[records[1].0..records[1].1]).subject.as_deref(),
        Some("two")
    );
}

#[devkit::test]
fn html_only_groups_and_an_unreadable_date() {
    let m = parse(
        b"Message-ID: <h@x>\n\
Date: not a date\n\
To: Team: a@x, b@x;\n\
Content-Type: text/html; charset=utf-8\n\
\n\
<p>hi</p>\n",
    );
    assert!(m.html_only);
    assert_eq!(m.text, None);
    assert_eq!(m.date, None, "an unreadable date stays unknown");
    assert_eq!(m.to, vec!["a@x", "b@x"]);
}

#[devkit::test]
fn a_maildir_file_keeps_its_from_lines() {
    let m = parse(b"Message-ID: <m@x>\n\n>From here\n");
    assert_eq!(m.text.as_deref().map(str::trim_end), Some(">From here"));
}

fn walked(
    root: &std::path::Path,
) -> (
    crate::drivers::dir_snapshot::DirSnapshot,
    crate::drivers::dir_snapshot::DirState,
) {
    (
        maildir_driver(root.to_path_buf(), "h".into()),
        crate::drivers::dir_snapshot::DirState::default(),
    )
}

#[devkit::test]
fn a_deleted_maildir_message_is_removed_where_it_was_read() {
    let owned = tempfile::tempdir().unwrap();
    let root = owned.path();
    std::fs::create_dir_all(root.join("cur")).unwrap();
    std::fs::create_dir_all(root.join("new")).unwrap();
    std::fs::write(root.join("cur/1.host:2,S"), "Message-ID: <1@x>\n\none\n").unwrap();
    std::fs::write(root.join("new/2.host"), "Message-ID: <2@x>\n\ntwo\n").unwrap();
    let (driver, mut state) = walked(root);
    let first = driver.poll(&serde_json::Value::Null, &mut state).unwrap();
    let first = first.material.unwrap();
    let path_at = |m: &crate::drivers::NewMaterial, anchor: u64| {
        m.manifest
            .member_at(anchor as usize)
            .map(|member| member.path.clone())
    };
    let admitted = (first.cursor_at)(first.records.last().unwrap().1).unwrap();
    // A flag change renames the file; a deletion removes it.
    std::fs::rename(root.join("cur/1.host:2,S"), root.join("cur/1.host:2,RS")).unwrap();
    std::fs::remove_file(root.join("new/2.host")).unwrap();
    let second = driver
        .poll(&admitted, &mut state)
        .unwrap()
        .material
        .unwrap();
    let mut removed: Vec<Option<String>> = second
        .removed
        .iter()
        .map(|location| {
            assert_eq!(location.material, first.id);
            path_at(&first, location.anchor)
        })
        .collect();
    removed.sort();
    assert_eq!(
        removed,
        [
            Some("cur/1.host:2,S".to_owned()),
            Some("new/2.host".to_owned())
        ]
    );
    // The renamed file is read again; the removal markers read as nothing.
    let chunks = crate::drain(
        &MailParser,
        crate::DrainInput {
            source: "communication.maildir",
            class: Class::Open,
            material: second.id,
            bytes: second.record_bytes(),
            records: &second.records,
            carry: serde_json::Value::Null,
            reading_order: &|_| sinex_model::ReadingOrder(0),
            limits: crate::Limits::default(),
            manifest: &second.manifest,
        },
    )
    .unwrap();
    let events: usize = chunks
        .into_iter()
        .flat_map(|c| c.outcomes)
        .map(|o| match o {
            sinex_model::RecordOutcome::Events { events, .. } => events.len(),
            sinex_model::RecordOutcome::Failure { reason, .. } => panic!("{reason}"),
        })
        .sum();
    assert_eq!(events, 1);
}
