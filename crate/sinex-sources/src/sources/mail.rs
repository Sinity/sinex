//! Email: mbox files (Gmail's Takeout export) on `ExportArchive`, and Maildir
//! folders on `DirSnapshot`. One parser reads a raw RFC 5322 message either
//! way; an mbox record starts with its `From ` separator line, which is not
//! part of the message.
//!
//! The `Message-ID` header is the identity, so the same message in a later
//! export of the same source is one record; a message without one is
//! identified by a hash of its raw bytes. An mbox export and a Maildir are two
//! sources (a Gmail export and a local mail store); the same message in both
//! is combined by fusion, not by admission (architecture §3.3 rule 5). Gmail's labels and thread ID
//! are read from its `X-Gmail-Labels` and `X-GM-THRID` headers. The first
//! plain-text part is kept as text; attachments are listed with their size,
//! and their bytes stay in the material.
//!
//! A message deleted from a Maildir is not recorded as removed yet: the
//! vanished file's marker names only its path, not its Message-ID.
use crate::drain::RecordContext;
use crate::drivers::dir_snapshot::DirSnapshot;
use crate::drivers::export_archive::{ExportArchive, Framing, PART_BYTES};
use crate::registry::{Declared, Driver, SourceSpec};
use crate::{Parser, RecordError};
use mailparse::{MailHeaderMap, ParsedMail};
use sinex_model::kinds::communication::{MailAttachment, MailMessage};
use sinex_model::{Class, Timestamp};
use std::path::PathBuf;

pub const MBOX: SourceSpec = SourceSpec {
    name: "communication.mbox",
    driver: Driver::ExportArchive,
    consumer: "mail recall and correspondence history",
    class: Class::Open,
    parser: "rfc5322-mail",
    parser_version: 1,
    event_types: &["mail.message"],
    framer: crate::drivers::export_archive::frame_mbox,
};

pub const MAILDIR: SourceSpec = SourceSpec {
    name: "communication.maildir",
    driver: Driver::DirSnapshot,
    consumer: "mail recall and correspondence history",
    class: Class::Open,
    parser: "rfc5322-mail",
    parser_version: 1,
    event_types: &["mail.message"],
    framer: crate::drivers::export_archive::frame_whole,
};

pub struct MailParser;

impl Parser for MailParser {
    type Record<'a> = &'a [u8];
    type Carry = ();
    type Payload = MailMessage;

    fn parse(&self, record: &&[u8], _: &()) -> Result<(Vec<MailMessage>, ()), RecordError> {
        parse_message(record).map(|m| (vec![m], ()))
    }

    fn parse_record(
        &self,
        record: &&[u8],
        carry: &(),
        context: &RecordContext<'_>,
    ) -> Result<(Vec<MailMessage>, ()), RecordError> {
        // A vanished Maildir file: nothing to read (see the module note).
        if context
            .member
            .is_some_and(|m| m.get("vanished_at_ns").is_some())
        {
            return Ok((vec![], ()));
        }
        self.parse(record, carry)
    }
}

pub fn parse_message(raw: &[u8]) -> Result<MailMessage, RecordError> {
    let message = mbox_unescape(raw);
    let parsed = mailparse::parse_mail(&message)
        .map_err(|e| RecordError::new(format!("not a mail message: {e}")))?;
    let headers = &parsed.headers;
    let one = |name: &str| {
        headers
            .get_first_value(name)
            .map(|v| v.trim().to_owned())
            .filter(|v| !v.is_empty())
    };
    let message_id = one("Message-ID")
        .map(|id| strip_angles(&id))
        .unwrap_or_else(|| format!("blake3:{}", blake3::hash(&message).to_hex()));
    let date = one("Date")
        .and_then(|d| mailparse::dateparse(&d).ok())
        .and_then(|secs| Timestamp::from_unix_secs(secs).ok());
    let addresses = |name: &str| -> Vec<String> {
        headers
            .get_all_headers(name)
            .iter()
            .flat_map(|h| match mailparse::addrparse_header(h) {
                Ok(list) => list.iter().flat_map(address_strings).collect(),
                Err(_) => vec![h.get_value()],
            })
            .collect()
    };
    let ids = |name: &str| -> Vec<String> {
        one(name)
            .map(|v| v.split_whitespace().map(strip_angles).collect())
            .unwrap_or_default()
    };
    let mut text = None;
    let mut html = false;
    let mut attachments = Vec::new();
    walk(&parsed, &mut text, &mut html, &mut attachments)?;
    Ok(MailMessage {
        message_id,
        date,
        from: addresses("From"),
        to: addresses("To"),
        cc: addresses("Cc"),
        subject: one("Subject"),
        in_reply_to: one("In-Reply-To").map(|v| strip_angles(&v)),
        references: ids("References"),
        labels: one("X-Gmail-Labels")
            .map(|v| {
                v.split(',')
                    .map(|l| l.trim().trim_matches('"').to_owned())
                    .filter(|l| !l.is_empty())
                    .collect()
            })
            .unwrap_or_default(),
        thread_id: one("X-GM-THRID"),
        html_only: text.is_none() && html,
        text,
        attachments,
        size: message.len() as u64,
    })
}

fn walk(
    part: &ParsedMail<'_>,
    text: &mut Option<String>,
    html: &mut bool,
    attachments: &mut Vec<MailAttachment>,
) -> Result<(), RecordError> {
    if !part.subparts.is_empty() {
        for sub in &part.subparts {
            walk(sub, text, html, attachments)?;
        }
        return Ok(());
    }
    let disposition = part.get_content_disposition();
    let filename = disposition
        .params
        .get("filename")
        .or_else(|| part.ctype.params.get("name"))
        .cloned();
    let attached =
        disposition.disposition == mailparse::DispositionType::Attachment || filename.is_some();
    let mime = part.ctype.mimetype.to_ascii_lowercase();
    if !attached && mime == "text/plain" && text.is_none() {
        *text = Some(
            part.get_body()
                .map_err(|e| RecordError::new(format!("text part: {e}")))?,
        );
    } else if !attached && mime == "text/html" {
        *html = true;
    } else if attached {
        let size = part
            .get_body_raw()
            .map_err(|e| RecordError::new(format!("attachment: {e}")))?
            .len() as u64;
        attachments.push(MailAttachment {
            filename,
            content_type: mime,
            size,
        });
    }
    Ok(())
}

fn address_strings(addr: &mailparse::MailAddr) -> Vec<String> {
    let single = |s: &mailparse::SingleInfo| match &s.display_name {
        Some(name) => format!("{name} <{}>", s.addr),
        None => s.addr.clone(),
    };
    match addr {
        mailparse::MailAddr::Single(s) => vec![single(s)],
        mailparse::MailAddr::Group(g) => g.addrs.iter().map(single).collect(),
    }
}

fn strip_angles(id: &str) -> String {
    id.trim()
        .trim_start_matches('<')
        .trim_end_matches('>')
        .to_owned()
}

/// The message inside an mbox record: drop the `From ` separator line and
/// undo mboxrd quoting (`>From ` → `From `, one level).
fn mbox_unescape(raw: &[u8]) -> Vec<u8> {
    if !raw.starts_with(b"From ") {
        // A Maildir file: its bytes are the message, `>From ` lines included.
        return raw.to_vec();
    }
    let body = match raw.iter().position(|b| *b == b'\n') {
        Some(i) => &raw[i + 1..],
        None => &[][..],
    };
    let mut out = Vec::with_capacity(body.len());
    for line in body.split_inclusive(|b| *b == b'\n') {
        let quoted = line.iter().take_while(|b| **b == b'>').count();
        if quoted > 0 && line[quoted..].starts_with(b"From ") {
            out.extend_from_slice(&line[1..]);
        } else {
            out.extend_from_slice(line);
        }
    }
    out
}

pub fn mbox_source() -> Declared<MailParser> {
    Declared {
        spec: MBOX,
        parser: MailParser,
    }
}

pub fn maildir_source() -> Declared<MailParser> {
    Declared {
        spec: MAILDIR,
        parser: MailParser,
    }
}

pub fn mbox_driver(path: PathBuf, host: String) -> ExportArchive {
    ExportArchive {
        path,
        host,
        select: |m| m.ends_with(".mbox"),
        framing: Framing::Mbox,
        part_bytes: PART_BYTES,
    }
}

pub fn maildir_driver(root: PathBuf, host: String) -> DirSnapshot {
    DirSnapshot {
        root,
        host,
        select: |p| {
            p.split('/')
                .rev()
                .nth(1)
                .is_some_and(|dir| dir == "cur" || dir == "new")
        },
        part_bytes: 64 << 20,
    }
}

#[cfg(test)]
mod tests {
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
        let (m, ()) = MailParser.parse(&MESSAGE, &()).unwrap();
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
            .parse(&&b"Subject: x\n\nbody\n"[..], &())
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
}
