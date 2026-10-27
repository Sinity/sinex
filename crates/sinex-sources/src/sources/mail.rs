//! Email: mbox files (Gmail's Takeout export) on `ExportArchive`, and Maildir
//! folders on `DirSnapshot`. One parser reads a raw RFC 5322 message either
//! way; an mbox record starts with its `From ` separator line, which is not
//! part of the message.
//!
//! The `Message-ID` header is the identity, so the same message in a later
//! export of the same source is one record; a message without one is identified
//! by a hash of its raw bytes. An mbox export and a Maildir are two sources (a
//! Gmail export and a local mail store); the same message in both is combined
//! by fusion, not by admission. Gmail's labels and thread ID are read from its
//! `X-Gmail-Labels` and `X-GM-THRID` headers. The first plain-text part is kept
//! as text; attachments are listed with their size, and their bytes stay in the
//! material.
//!
//! A message deleted from a Maildir is recorded as removed at the source,
//! where its file was last read: the removed file's marker names only its
//! path, and a message is identified by its Message-ID, so the parser reads
//! the marker as nothing. A flag change renames the file, which reads the
//! message again under its new name and records the old name's reading as
//! removed; the new reading is the newer one. A message kept in two folders
//! and deleted from the one it was last read in is recorded as removed.
use crate::drain::RecordContext;
use crate::drivers::dir_snapshot::DirSnapshot;
use crate::drivers::export_archive::{ExportArchive, Framing, PART_BYTES};
use crate::registry::{DeclaredSource, Driver, SourceSpec};
use crate::{Parser, RecordError};
use mailparse::{MailHeaderMap, ParsedMail};
use sinex_model::event_types::communication::{MailAttachment, MailMessage};
use sinex_model::{Class, Timestamp};
use std::path::PathBuf;

pub const MBOX: SourceSpec = SourceSpec {
    name: "communication.mbox",
    driver: Driver::ExportArchive,
    excisable: false,
    purpose: "mail recall and correspondence history",
    class: Class::Open,
    parser: "rfc5322-mail",
    parser_version: 1,
    event_types: &["mail.message"],
    framer: crate::Framer::export(
        crate::drivers::export_archive::frame_mbox,
        crate::drivers::export_archive::Framing::Mbox,
    ),
};

pub const MAILDIR: SourceSpec = SourceSpec {
    name: "communication.maildir",
    driver: Driver::DirSnapshot,
    excisable: false,
    purpose: "mail recall and correspondence history",
    class: Class::Open,
    parser: "rfc5322-mail",
    parser_version: 2,
    event_types: &["mail.message"],
    framer: crate::Framer::export(
        crate::drivers::export_archive::frame_whole,
        crate::drivers::export_archive::Framing::Whole,
    ),
};

pub struct MailParser;

impl Parser for MailParser {
    type Record<'a> = &'a [u8];
    type Carry = ();
    type Payload = MailMessage;

    fn parse(
        &self,
        record: &&[u8],
        _: &(),
        context: &RecordContext<'_>,
    ) -> Result<(Vec<MailMessage>, ()), RecordError> {
        // A removed Maildir file: its removal is recorded at the source.
        if context
            .member
            .is_some_and(|m| m.details.contains_key("removed_at_ns"))
        {
            return Ok((vec![], ()));
        }
        parse_message(record).map(|m| (vec![m], ()))
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
        // `dateparse` reads text holding no date as the epoch.
        .filter(|secs| *secs != 0)
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

pub fn mbox_source() -> DeclaredSource<MailParser> {
    DeclaredSource {
        spec: MBOX,
        parser: MailParser,
    }
}

pub fn maildir_source() -> DeclaredSource<MailParser> {
    DeclaredSource {
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
        removed_at_source: true,
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

/// `--read mbox=PATH`: each mbox file in PATH.
pub const MBOX_FORMAT: crate::binding::InputFormat = crate::binding::InputFormat {
    name: "mbox",
    specs: || vec![&MBOX],
    plan: Some(|input, settings| {
        Ok(crate::binding::per_file(
            &MBOX,
            input,
            settings,
            false,
            crate::binding::any,
        ))
    }),
    source: |_| Ok(Box::new(mbox_source())),
    reader: Some(|b| {
        Ok(crate::binding::shared(mbox_driver(
            b.input.clone(),
            b.settings.host.clone(),
        )))
    }),
    capture: None,
};

/// `--read maildir=PATH`: the Maildir at PATH, walked as one directory.
pub const MAILDIR_FORMAT: crate::binding::InputFormat = crate::binding::InputFormat {
    name: "maildir",
    specs: || vec![&MAILDIR],
    plan: Some(|input, settings| Ok(crate::binding::single(&MAILDIR, input, settings))),
    source: |_| Ok(Box::new(maildir_source())),
    reader: Some(|b| {
        Ok(crate::binding::walked(maildir_driver(
            b.input.clone(),
            b.settings.host.clone(),
        )))
    }),
    capture: None,
};

#[cfg(test)]
mod tests;
