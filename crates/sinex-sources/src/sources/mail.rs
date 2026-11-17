//! Email: mbox files (Gmail's Takeout export) on `ExportArchive`, and Maildir
//! folders on `DirSnapshot`. One parser reads a raw RFC 5322 message either
//! way; an mbox record starts with its `From ` separator line, which is not
//! part of the message.
//!
//! The account and provider ID identify a message when supplied; otherwise
//! the `Message-ID` header identifies the same message in a later
//! export of the same source is one record; a message without one is identified
//! by its provider ID or record location. An mbox export and a Maildir are two sources (a
//! Gmail export and a local mail store); the same message in both is combined
//! by fusion, not by admission. Gmail's labels and thread ID are read from its
//! `X-Gmail-Labels` and `X-GM-THRID` headers. The first plain-text part is kept
//! as text; attachments are listed with their size, and their bytes stay in the
//! material.
//!
//! A message deleted from a Maildir is recorded as removed at the source,
//! where its file was last read: the removed file's marker names only its
//! path, so the parser reads
//! the marker as nothing. A flag change renames the file, which reads the
//! message again under its new name and records the old name's reading as
//! removed; the new reading is the newer one. A message kept in two folders
//! and deleted from the one it was last read in is recorded as removed.
use mailparse::{MailHeaderMap, ParsedMail};
use sinex_model::Timestamp;
use sinex_model::event_types::communication::{MailAttachment, MailMessage};
use sinex_model::json::JsonExt;
use sinex_source_core::discovery::{DiscoverySpec, Location};
use sinex_source_core::drain::RecordContext;
use sinex_source_core::drivers::dir_snapshot::DirSnapshot;
use sinex_source_core::drivers::export_archive::{ExportArchive, Framing, PART_BYTES};
use sinex_source_core::registry::{Expectation, Origin, Shape, SourceSpec};
use sinex_source_core::{RecordError, RecordResult, Stateless, StatelessParser};
use std::path::PathBuf;

pub const MBOX: SourceSpec = SourceSpec {
    name: "communication.mbox",
    origin: Origin::Input(Shape::ExportArchive),
    expect: Expectation::Export,
    purpose: "mail recall and correspondence history",
    parser: "rfc5322-mail",
    parser_version: 3,
    event_types: &["mail.message"],
    framer: sinex_source_core::Framer::export(
        sinex_source_core::drivers::export_archive::frame_mbox,
        sinex_source_core::drivers::export_archive::Framing::Mbox,
    ),
    ..SourceSpec::DEFAULT
};

pub const MAILDIR: SourceSpec = SourceSpec {
    name: "communication.maildir",
    origin: Origin::Input(Shape::DirSnapshot),
    expect: Expectation::cadence_hours(1),
    purpose: "mail recall and correspondence history",
    parser: "rfc5322-mail",
    parser_version: 6,
    event_types: &["mail.message"],
    framer: sinex_source_core::Framer::export(
        sinex_source_core::drivers::export_archive::frame_whole,
        sinex_source_core::drivers::export_archive::Framing::Whole,
    ),
    ..SourceSpec::DEFAULT
};

pub struct MailParser;

impl StatelessParser for MailParser {
    type Record<'a> = &'a [u8];
    type Payload = MailMessage;

    fn parse(
        &self,
        record: &&[u8],
        context: &RecordContext<'_>,
    ) -> Result<Vec<MailMessage>, RecordError> {
        // A removed Maildir file: its removal is recorded at the source.
        if context
            .member
            .is_some_and(|m| m.details.contains_key("removed_at_ns"))
        {
            return Ok(vec![]);
        }
        let mut message = parse_message(record)?;
        message.native_id = context
            .member
            .and_then(|member| member.details.str("native_id").map(str::to_owned));
        message.account = context.manifest.details.str("account").map(str::to_owned);
        Ok(vec![message])
    }
}

pub fn parse_message(raw: &[u8]) -> Result<MailMessage, RecordError> {
    let message = mbox_unescape(raw);
    // The MIME parser recurses once per nested multipart, and each nesting
    // level needs its own `boundary` parameter, so the count of that word
    // bounds the depth. The parse, the walk over its parts and the drop of
    // the parsed tree run on a stack sized for that depth: no message,
    // however nested, can overflow the daemon's stack.
    let levels = message
        .windows(b"boundary".len())
        .filter(|w| w.eq_ignore_ascii_case(b"boundary"))
        .count();
    let stack = 1024 * 1024 + levels * 64 * 1024;
    stacker::maybe_grow(stack, stack, || read_message(&message))
}

fn read_message(message: &[u8]) -> Result<MailMessage, RecordError> {
    let parsed = mailparse::parse_mail(message).record("not a mail message")?;
    let headers = &parsed.headers;
    let one = |name: &str| {
        headers
            .get_first_value(name)
            .map(|v| v.trim().to_owned())
            .filter(|v| !v.is_empty())
    };
    let message_id = one("Message-ID")
        .map(|id| strip_angles(&id))
        .filter(|id| !id.trim().is_empty());
    let date = one("Date")
        .and_then(|d| mailparse::dateparse(&d).ok())
        // `dateparse` also returns zero for text holding no date. A real
        // epoch date must be a readable RFC 2822 date, rather than guessed.
        .filter(|secs| {
            *secs != 0
                || one("Date").is_some_and(|date| {
                    time::OffsetDateTime::parse(
                        &date,
                        &time::format_description::well_known::Rfc2822,
                    )
                    .is_ok_and(|date| date.unix_timestamp() == 0)
                })
        })
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
        account: None,
        native_id: None,
        message_id,
        date,
        from: addresses("From"),
        to: addresses("To"),
        cc: addresses("Cc"),
        bcc: addresses("Bcc"),
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
        *text = Some(part.get_body().record("text part")?);
    } else if !attached && mime == "text/html" {
        *html = true;
    } else if attached {
        let size = part.get_body_raw().record("attachment")?.len() as u64;
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

pub struct MaildirParser;

impl StatelessParser for MaildirParser {
    type Record<'a> = &'a [u8];
    type Payload = MailMessage;

    fn record_hash(&self, raw: &[u8], context: &RecordContext<'_>) -> [u8; 32] {
        context.hash_with_path(raw)
    }

    fn parse(
        &self,
        record: &&[u8],
        context: &RecordContext<'_>,
    ) -> Result<Vec<MailMessage>, RecordError> {
        let mut messages = MailParser.parse(record, context)?;
        for message in &mut messages {
            if let (Some(member), Some(root), Some(host)) = (
                context.member,
                context.manifest.path.as_deref(),
                context.manifest.detail("host").as_str(),
            ) {
                use base64::Engine;
                use std::os::unix::ffi::{OsStrExt, OsStringExt};
                let path = match member.details.get("native_path_bytes") {
                    Some(path_bytes) => {
                        let bytes = path_bytes
                            .as_str()
                            .and_then(|value| {
                                base64::engine::general_purpose::STANDARD.decode(value).ok()
                            })
                            .record("invalid raw Maildir path bytes")?;
                        PathBuf::from(std::ffi::OsString::from_vec(bytes))
                    }
                    None => PathBuf::from(&member.path),
                };
                if let (Some(filename), Some(folder)) = (
                    path.file_name(),
                    path.parent().and_then(std::path::Path::parent),
                ) {
                    let filename = filename.as_bytes();
                    let end = filename
                        .windows(3)
                        .rposition(|part| part == b":2,")
                        .unwrap_or(filename.len());
                    message.native_id = Some(
                        serde_json::json!([
                            "maildir",
                            host,
                            root,
                            context.manifest.detail("native_path_bytes"),
                            base64::engine::general_purpose::STANDARD
                                .encode(folder.as_os_str().as_bytes()),
                            base64::engine::general_purpose::STANDARD.encode(&filename[..end]),
                        ])
                        .to_string(),
                    );
                }
            }
        }
        Ok(messages)
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
        locate: None,
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

input_format! {
    /// `stage --parser mbox PATH`: each mbox file in PATH.
    pub MBOX_FORMAT = "mbox" {
    specs: [MBOX],
    parser: |_b| Stateless(MailParser),
    discovery: DiscoverySpec::system(&[Location::new("input", "mbox")]),
    plan: per_file,
    reader: shared(mbox_driver),
} }

input_format! {
    /// `stage --parser maildir PATH`: the Maildir at PATH, walked as one directory.
    pub MAILDIR_FORMAT = "maildir" {
    specs: [MAILDIR],
    parser: |_b| Stateless(MaildirParser),
    discovery: DiscoverySpec::system(&[Location::new("input", "maildir")]),
    plan: single,
    reader: walked(maildir_driver),
} }

#[cfg(test)]
mod tests;
