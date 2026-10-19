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

#[test]
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

#[test]
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

#[test]
fn maildir_selects_message_files() {
    let d = maildir_driver(PathBuf::from("/m"), "h".into());
    assert!((d.select)("INBOX/cur/1.host:2,S"));
    assert!((d.select)("new/2.host"));
    assert!(!(d.select)("INBOX/tmp/3"));
    assert!(!(d.select)("dovecot.index"));
}
