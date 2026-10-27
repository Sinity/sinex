//! Live mail over IMAP: new messages in a mailbox, fetched by UID after the
//! cursor, waiting with IDLE between polls when the server supports it. Gmail
//! and other providers that refuse password logins take an OAuth access token
//! (SASL `XOAUTH2`).
//!
//! Each poll stores one batch of messages as one material: the messages'
//! exact bytes back to back, each one a record, listed in the manifest so a
//! replay finds the same records. The cursor is the mailbox's UIDVALIDITY and
//! the next UID; a changed UIDVALIDITY means the server renumbered the
//! mailbox, so reading starts over (a message already read is then a repeat
//! by its Message-ID).
//!
//! The reader remembers where each message it read is stored, by UID. Each
//! poll asks the server which of those UIDs it still holds (`UID SEARCH`);
//! a message expunged since is recorded as removed at the source, and its
//! UID is listed in the manifest (`expunged`) of the poll's material, which
//! then may hold no message at all. The reader saves what it remembers with
//! the cursor on the cadence of [`crate::drivers::memory_due`] and rebuilds
//! it after a restart from that and the materials after it.
//!
//! Credentials come from files (`password_file`, `token_file`), never from
//! the configuration itself, and never appear in errors.
//!
//! The client is a small synchronous IMAP4rev1 client (RFC 3501): the
//! commands live mail needs (LOGIN or AUTHENTICATE XOAUTH2, SELECT, UID
//! FETCH, IDLE, LOGOUT) over TLS (rustls) or, for tests, plain TCP.
use crate::drivers::{Acquired, DriverError, NewMaterial};
use crate::registry::{DeclaredSource, Driver, SourceSpec};
use base64::Engine;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sinex_model::{Class, InputRef, MaterialId, ObservationFacts, RevisionChange, Timestamp};
use std::collections::{BTreeMap, HashSet};
use std::io::{BufRead, BufReader, Read, Write};
use std::net::TcpStream;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

pub const SPEC: SourceSpec = SourceSpec {
    name: "communication.imap",
    driver: Driver::ApiCursor,
    excisable: false,
    purpose: "mail recall and correspondence history",
    class: Class::Open,
    parser: "rfc5322-mail",
    parser_version: 3,
    event_types: &["mail.message"],
    framer: crate::Framer::export(
        crate::drivers::export_archive::frame_whole,
        crate::drivers::export_archive::Framing::Whole,
    ),
};

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ImapConfig {
    pub host: String,
    #[serde(default = "default_port")]
    pub port: u16,
    pub username: String,
    /// A file holding the password (`LOGIN`).
    #[serde(default)]
    pub password_file: Option<PathBuf>,
    /// A file holding an OAuth access token (`AUTHENTICATE XOAUTH2`); used
    /// instead of the password when set.
    #[serde(default)]
    pub token_file: Option<PathBuf>,
    #[serde(default = "default_mailbox")]
    pub mailbox: String,
    /// Implicit TLS (port 993). Stateless TCP only for tests on loopback.
    #[serde(default = "yes")]
    pub tls: bool,
    /// Messages per material.
    #[serde(default = "default_batch")]
    pub batch: u32,
    /// How long IDLE waits for news before a poll returns empty.
    #[serde(default = "default_idle")]
    pub idle_seconds: u64,
}

fn default_port() -> u16 {
    993
}
fn default_mailbox() -> String {
    "INBOX".into()
}
fn yes() -> bool {
    true
}
fn default_batch() -> u32 {
    200
}
fn default_idle() -> u64 {
    0
}

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct Cursor {
    pub uid_validity: Option<u32>,
    pub uid_next: u32,
    pub material: Option<MaterialId>,
    pub part_index: u32,
    /// How many bytes of `material` were admitted.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub through: Option<usize>,
    /// The part at whose end the reader last saved what it remembers.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub memory_part: Option<u32>,
    /// Messages read or expunged since that part.
    #[serde(default)]
    pub changed_since_memory: u64,
}

/// Where each message the reader read is stored, by UID.
pub type Known = BTreeMap<u32, sinex_model::RecordLocation>;

/// What one poll produced, and what the reader remembers once its material
/// is admitted (`None` when it produced none).
pub struct Polled {
    pub acquired: Acquired,
    pub known: Option<Known>,
}

trait Stream: Read + Write + Send {}
impl<T: Read + Write + Send> Stream for T {}

struct Connection {
    reader: BufReader<Box<dyn Stream>>,
    tag: u32,
}

fn io(context: &str, e: impl std::fmt::Display) -> DriverError {
    DriverError::Io(format!("IMAP {context}: {e}"))
}

/// One untagged or tagged response line, with any literals it carries.
struct Line {
    text: String,
    literals: Vec<Vec<u8>>,
}

impl Connection {
    fn open(cfg: &ImapConfig) -> Result<Self, DriverError> {
        let timeout = cfg
            .idle_seconds
            .max(60)
            .checked_add(30)
            .ok_or_else(|| io("idle timeout", "outside the clock's range"))?;
        let tcp =
            TcpStream::connect((cfg.host.as_str(), cfg.port)).map_err(|e| io("connect", e))?;
        tcp.set_read_timeout(Some(Duration::from_secs(timeout)))
            .map_err(|e| io("connect", e))?;
        let stream: Box<dyn Stream> = if cfg.tls {
            let roots = rustls::RootCertStore {
                roots: webpki_roots::TLS_SERVER_ROOTS.to_vec(),
            };
            let config = rustls::ClientConfig::builder()
                .with_root_certificates(roots)
                .with_no_client_auth();
            let name = rustls::pki_types::ServerName::try_from(cfg.host.clone())
                .map_err(|e| io("server name", e))?;
            let conn =
                rustls::ClientConnection::new(Arc::new(config), name).map_err(|e| io("TLS", e))?;
            Box::new(rustls::StreamOwned::new(conn, tcp))
        } else {
            Box::new(tcp)
        };
        let mut c = Self {
            reader: BufReader::new(stream),
            tag: 0,
        };
        let greeting = c.read_line()?;
        if !greeting.text.starts_with("* OK") && !greeting.text.starts_with("* PREAUTH") {
            return Err(io("greeting", &greeting.text));
        }
        Ok(c)
    }

    /// Read one response line, following `{n}` literals into the next bytes.
    fn read_line(&mut self) -> Result<Line, DriverError> {
        let mut text = String::new();
        let mut literals = Vec::new();
        loop {
            let mut raw = Vec::new();
            let n = self
                .reader
                .read_until(b'\n', &mut raw)
                .map_err(|e| io("read", e))?;
            if n == 0 {
                return Err(io("read", "connection closed"));
            }
            let chunk = String::from_utf8_lossy(&raw)
                .trim_end_matches(['\r', '\n'])
                .to_owned();
            let literal = chunk
                .strip_suffix('}')
                .and_then(|s| s.rsplit_once('{'))
                .and_then(|(_, n)| n.parse::<usize>().ok());
            text.push_str(&chunk);
            match literal {
                Some(len) => {
                    let mut bytes = vec![0; len];
                    self.reader
                        .read_exact(&mut bytes)
                        .map_err(|e| io("read literal", e))?;
                    literals.push(bytes);
                }
                None => return Ok(Line { text, literals }),
            }
        }
    }

    fn send(&mut self, command: &str) -> Result<String, DriverError> {
        self.tag += 1;
        let tag = format!("a{}", self.tag);
        let w = self.reader.get_mut();
        w.write_all(format!("{tag} {command}\r\n").as_bytes())
            .map_err(|e| io("write", e))?;
        w.flush().map_err(|e| io("write", e))?;
        Ok(tag)
    }

    /// Run a command; return its untagged responses, failing on NO/BAD.
    /// `what` names the command in errors (never its arguments: they can hold
    /// credentials).
    fn run(&mut self, command: &str, what: &str) -> Result<Vec<Line>, DriverError> {
        let tag = self.send(command)?;
        let mut out = Vec::new();
        loop {
            let line = self.read_line()?;
            if let Some(rest) = line.text.strip_prefix(&format!("{tag} ")) {
                return if rest.starts_with("OK") {
                    Ok(out)
                } else {
                    Err(io(what, rest))
                };
            }
            out.push(line);
        }
    }
}

fn quote(s: &str) -> String {
    format!("\"{}\"", s.replace('\\', "\\\\").replace('"', "\\\""))
}

fn number_after(text: &str, key: &str) -> Option<u32> {
    let i = text.find(key)? + key.len();
    text[i..]
        .trim_start()
        .split(|c: char| !c.is_ascii_digit())
        .next()?
        .parse()
        .ok()
}

pub struct ImapMail {
    pub config: ImapConfig,
    pub host: String,
}

impl ImapMail {
    fn input(&self) -> InputRef {
        InputRef {
            kind: "imap".into(),
            locator: format!(
                "imap://{}@{}/{}",
                self.config.username, self.config.host, self.config.mailbox
            ),
            host: self.host.clone(),
        }
    }

    fn login(&self, c: &mut Connection) -> Result<(), DriverError> {
        let read = |p: &PathBuf| {
            std::fs::read_to_string(p)
                .map(|s| s.trim().to_owned())
                .map_err(|e| io("credentials", format!("{}: {e}", p.display())))
        };
        if let Some(token_file) = &self.config.token_file {
            let token = read(token_file)?;
            let response = base64::engine::general_purpose::STANDARD.encode(format!(
                "user={}\x01auth=Bearer {token}\x01\x01",
                self.config.username
            ));
            c.run(&format!("AUTHENTICATE XOAUTH2 {response}"), "authenticate")?;
        } else {
            let password = self
                .config
                .password_file
                .as_ref()
                .ok_or_else(|| io("credentials", "neither password_file nor token_file is set"))
                .and_then(read)?;
            c.run(
                &format!(
                    "LOGIN {} {}",
                    quote(&self.config.username),
                    quote(&password)
                ),
                "login",
            )?;
        }
        Ok(())
    }

    /// Fetch the next batch of messages after the cursor, and find which of
    /// the `known` messages were expunged; when nothing changed, wait with
    /// IDLE for `idle_seconds` (when set) and look once more.
    pub fn poll(&self, cursor: &Value, known: &Known) -> Result<Polled, DriverError> {
        let previous: Cursor = if cursor.is_null() {
            Cursor::default()
        } else {
            serde_json::from_value(cursor.clone())
                .map_err(|e| DriverError::Cursor(e.to_string()))?
        };
        let mut c = Connection::open(&self.config)?;
        self.login(&mut c)?;
        let mut selected = c.run(&format!("SELECT {}", quote(&self.config.mailbox)), "select")?;
        let server_next = selected
            .iter()
            .find_map(|l| number_after(&l.text, "UIDNEXT"));
        let selected_validity = selected
            .iter()
            .find_map(|l| number_after(&l.text, "UIDVALIDITY"));
        if server_next.is_some_and(|n| n <= previous.uid_next.max(1))
            && (previous.uid_validity.is_none() || previous.uid_validity == selected_validity)
            && self.config.idle_seconds > 0
        {
            let tag = c.send("IDLE")?;
            let _continuation = c.read_line()?;
            let deadline = std::time::Instant::now()
                .checked_add(Duration::from_secs(self.config.idle_seconds))
                .ok_or_else(|| io("idle deadline", "outside the monotonic clock's range"))?;
            while std::time::Instant::now() < deadline {
                match c.read_line() {
                    Ok(l) if l.text.contains("EXISTS") || l.text.contains("EXPUNGE") => {
                        break;
                    }
                    Ok(_) => {}
                    Err(_) => break,
                }
            }
            let w = c.reader.get_mut();
            let _ = w.write_all(b"DONE\r\n").and_then(|()| w.flush());
            while let Ok(l) = c.read_line() {
                if l.text.starts_with(&tag) {
                    break;
                }
            }
            selected = c.run(&format!("SELECT {}", quote(&self.config.mailbox)), "select")?;
        }
        let uid_validity = selected
            .iter()
            .find_map(|l| number_after(&l.text, "UIDVALIDITY"));
        let server_next = selected
            .iter()
            .find_map(|l| number_after(&l.text, "UIDNEXT"));
        // A renumbered mailbox: start over, and its old UIDs say nothing.
        let renumbered = previous.uid_validity.is_some() && previous.uid_validity != uid_validity;
        let expunged: Vec<u32> = match known.keys().next_back() {
            Some(highest) if !renumbered => {
                let present = search(&mut c, *highest)?;
                known
                    .keys()
                    .filter(|uid| !present.contains(uid))
                    .copied()
                    .collect()
            }
            _ => Vec::new(),
        };
        let start = first_uid(&previous, uid_validity);
        let window_end = start.saturating_add(self.config.batch.max(1) - 1);
        let known_next = server_next.filter(|n| *n > start);
        let end = known_next.map_or(window_end, |n| window_end.min(n - 1));
        let mut messages: Vec<(u32, Vec<u8>)> = Vec::new();
        for line in c.run(
            &format!("UID FETCH {start}:{end} (UID BODY.PEEK[])"),
            "fetch",
        )? {
            if let (Some(uid), Some(body)) = (
                number_after(&line.text, "UID"),
                line.literals.into_iter().next(),
            ) && uid >= start
                && uid <= end
            {
                messages.push((uid, body));
            }
        }
        // Only an advertised assigned range permits skipping absent UIDs.
        // Without it, future messages could still receive those identifiers.
        let examined_next = known_next.map_or(start, |_| end.saturating_add(1));
        let _ = c.run("LOGOUT", "logout");
        messages.sort_by_key(|(uid, _)| *uid);
        let now = Timestamp::now();
        if messages.is_empty() && expunged.is_empty() {
            let next = Cursor {
                uid_validity,
                uid_next: examined_next,
                ..previous
            };
            let acquired = Acquired {
                input: self.input(),
                observation: ObservationFacts {
                    observed_at: Some(now),
                    changed: false,
                    ..Default::default()
                },
                revision: RevisionChange::Same,
                material: None,
                cursor_without_material: serde_json::to_value(next).expect("cursor serializes"),
            };
            return Ok(Polled {
                acquired,
                known: None,
            });
        }
        let last_uid = messages.last().map(|(u, _)| *u);
        let mut bytes = Vec::new();
        let mut records = Vec::new();
        let mut uids = Vec::new();
        for (uid, body) in &messages {
            let start = bytes.len();
            bytes.extend_from_slice(body);
            records.push((start, bytes.len()));
            uids.push(*uid);
        }
        let members: Vec<_> = uids
            .iter()
            .zip(&records)
            .map(|(uid, (start, end))| {
                json!({
                    "path": uid.to_string(), "stream_offset": start, "length": end - start,
                    "native_id": uid_validity.map(|validity| json!([
                        "imap", self.input().locator, validity, uid
                    ]).to_string()),
                })
            })
            .collect();
        let manifest = sinex_model::Manifest::from_json(json!({
            "media_type": "message/rfc822", "members": members,
            "mailbox": self.config.mailbox, "uid_validity": uid_validity, "uids": uids,
            "expunged": expunged,
            "records": records, "read_at_ns": i64::try_from(now.unix_ns()).unwrap_or(i64::MAX),
        }));
        let id = MaterialId::new();
        let removed = expunged
            .iter()
            .filter_map(|uid| known.get(uid))
            .copied()
            .collect();
        let mut remembered = if renumbered {
            Known::new()
        } else {
            known.clone()
        };
        for uid in &expunged {
            remembered.remove(uid);
        }
        for (uid, (record_start, _)) in uids.iter().zip(&records) {
            remembered.insert(
                *uid,
                sinex_model::RecordLocation {
                    material: id,
                    anchor: *record_start as u64,
                },
            );
        }
        let part_index = if previous.material.is_some() {
            previous
                .part_index
                .checked_add(1)
                .ok_or_else(|| DriverError::Cursor("IMAP part index overflow".into()))?
        } else {
            0
        };
        let changed = previous
            .changed_since_memory
            .saturating_add((uids.len() + expunged.len()) as u64);
        let save = crate::drivers::memory_due(
            part_index,
            previous.memory_part,
            changed,
            remembered.len() as u64,
        );
        let next = Cursor {
            uid_validity,
            uid_next: last_uid.map_or(examined_next, |last| {
                examined_next.max(last.saturating_add(1))
            }),
            material: Some(id),
            part_index,
            through: Some(bytes.len()),
            memory_part: if save {
                Some(part_index)
            } else {
                previous.memory_part
            },
            changed_since_memory: if save { 0 } else { changed },
        };
        let length = bytes.len();
        let size = length as u64;
        let cursor_records = records.clone();
        let memory = save.then(|| memory_of(&remembered));
        let acquired = Acquired {
            input: self.input(),
            observation: ObservationFacts {
                observed_at: Some(now),
                size: Some(size),
                changed: true,
                ..Default::default()
            },
            revision: RevisionChange::Snapshot,
            material: Some(NewMaterial {
                id,
                bytes,
                stored: None,
                records,
                part_index,
                previous_part: previous.material,
                reading_order: crate::drivers::reading_order(&manifest),
                manifest,
                cursor_at: Box::new(move |end| {
                    at(&next, start, &cursor_records, end)
                        .map(|cursor| serde_json::to_value(cursor).expect("cursor serializes"))
                }),
                aligned: false,
                removed,
                memory,
                owned_files: Vec::new(),
            }),
            cursor_without_material: Value::Null,
        };
        Ok(Polled {
            acquired,
            known: Some(remembered),
        })
    }
}

/// The UIDs up to `highest` the selected mailbox holds.
fn search(c: &mut Connection, highest: u32) -> Result<HashSet<u32>, DriverError> {
    let mut present = HashSet::new();
    for line in c.run(&format!("UID SEARCH UID 1:{highest}"), "search")? {
        if let Some(rest) = line.text.strip_prefix("* SEARCH") {
            present.extend(
                rest.split_whitespace()
                    .filter_map(|n| n.parse::<u32>().ok()),
            );
        }
    }
    Ok(present)
}

/// What the reader remembers, to save: for each UID in order, the UID (four
/// bytes, little-endian), its material's ID (sixteen) and the record's
/// anchor (eight).
fn memory_of(known: &Known) -> Arc<[u8]> {
    let mut bytes = Vec::with_capacity(known.len() * 28);
    for (uid, location) in known {
        bytes.extend_from_slice(&uid.to_le_bytes());
        bytes.extend_from_slice(location.material.0.as_bytes());
        bytes.extend_from_slice(&location.anchor.to_le_bytes());
    }
    bytes.into()
}

fn read_memory(mut rest: &[u8]) -> Result<Vec<(u32, sinex_model::RecordLocation)>, DriverError> {
    let mut out = Vec::new();
    while !rest.is_empty() {
        if rest.len() < 28 {
            return Err(invalid("the saved IMAP memory is damaged"));
        }
        let (entry, after) = rest.split_at(28);
        rest = after;
        let uid = u32::from_le_bytes(entry[..4].try_into().expect("four bytes"));
        let material = MaterialId(uuid::Uuid::from_bytes(
            entry[4..20].try_into().expect("sixteen bytes"),
        ));
        let anchor = u64::from_le_bytes(entry[20..].try_into().expect("eight bytes"));
        out.push((uid, sinex_model::RecordLocation { material, anchor }));
    }
    Ok(out)
}

pub fn source() -> DeclaredSource<super::mail::MailParser> {
    DeclaredSource {
        spec: SPEC,
        parser: super::mail::MailParser,
    }
}

/// `--read imap=CONFIG.json`: live mail from the account CONFIG describes,
/// the messages new since the last read.
pub const FORMAT: crate::binding::InputFormat = crate::binding::InputFormat {
    name: "imap",
    specs: || vec![&SPEC],
    plan: Some(|input, settings| {
        let config = config(input)?;
        let mut b = crate::binding::Binding::of(&SPEC, input, settings.clone());
        b.name = format!(
            "{}:{}@{}/{}@{}",
            SPEC.name, config.username, config.host, config.mailbox, settings.host
        );
        Ok(vec![b])
    }),
    source: |_| Ok(Box::new(source())),
    reader: Some(|b| {
        Ok(crate::binding::Reading::Polled(Box::new(ImapReader::new(
            ImapMail {
                config: config(&b.input)?,
                host: b.settings.host.clone(),
            },
        ))))
    }),
    capture: None,
};

fn config(path: &std::path::Path) -> Result<ImapConfig, String> {
    serde_json::from_slice(&std::fs::read(path).map_err(|e| format!("{}: {e}", path.display()))?)
        .map_err(|e| format!("{}: {e}", path.display()))
}

pub(super) struct ImapReader {
    pub(super) mail: ImapMail,
    held: Option<(crate::drivers::held::HeldMaterial, Known)>,
    /// Where each message read is stored, as of the admitted cursor.
    pub(super) known: Known,
    /// UIDs a restart already decided from a newer material.
    decided: HashSet<u32>,
}

impl ImapReader {
    pub(super) fn new(mail: ImapMail) -> Self {
        Self {
            mail,
            held: None,
            known: Known::new(),
            decided: HashSet::new(),
        }
    }
}

impl crate::binding::Reader for ImapReader {
    fn history_from(&self, cursor: &Value) -> Option<MaterialId> {
        serde_json::from_value::<Cursor>(cursor.clone())
            .ok()
            .and_then(|c| c.material)
    }

    fn restore(
        &mut self,
        cursor: &Value,
        material: crate::binding::StoredMaterial,
    ) -> Result<bool, DriverError> {
        let cursor: Cursor =
            serde_json::from_value(cursor.clone()).map_err(|e| invalid(&e.to_string()))?;
        let reference = &material.reference;
        let manifest = &reference.manifest;
        // An older numbering of the mailbox says nothing about its UIDs now.
        if manifest.detail("uid_validity").as_u64() != cursor.uid_validity.map(u64::from) {
            return Ok(false);
        }
        let admitted = match (cursor.material, cursor.through) {
            (Some(id), Some(through)) if id == reference.id => through as u64,
            _ => reference.length,
        };
        let uids = |key: &str| -> Result<Vec<u32>, DriverError> {
            serde_json::from_value(manifest.detail(key).clone())
                .map_err(|_| invalid("an IMAP material lists invalid UIDs"))
        };
        for uid in uids("expunged")? {
            self.decided.insert(uid);
        }
        let records = manifest.records.clone().unwrap_or_default();
        for (uid, (start, end)) in uids("uids")?.into_iter().zip(records) {
            if end as u64 <= admitted && self.decided.insert(uid) {
                self.known.insert(
                    uid,
                    sinex_model::RecordLocation {
                        material: reference.id,
                        anchor: start as u64,
                    },
                );
            }
        }
        Ok(true)
    }

    fn restore_memory(&mut self, memory: &[u8]) -> Result<(), DriverError> {
        for (uid, location) in read_memory(memory)? {
            if self.decided.insert(uid) {
                self.known.insert(uid, location);
            }
        }
        Ok(())
    }

    fn restored(&mut self) -> Result<(), DriverError> {
        self.decided = HashSet::new();
        Ok(())
    }

    fn poll(&mut self, cursor: &Value) -> Result<Acquired, DriverError> {
        if let Some((held, _)) = &self.held {
            if !held.admitted(cursor) {
                return held.acquired(cursor);
            }
            let (_, known) = self.held.take().expect("checked");
            self.known = known;
        }
        let polled = self.mail.poll(cursor, &self.known)?;
        let Some(known) = polled.known else {
            return Ok(polled.acquired);
        };
        let held = crate::drivers::held::HeldMaterial::hold(polled.acquired, cursor)?;
        let acquired = held.acquired(cursor)?;
        self.held = Some((held, known));
        Ok(acquired)
    }
}

/// Where a read after `previous` starts: its next UID, or the start when the
/// mailbox was renumbered.
pub(super) fn first_uid(previous: &Cursor, validity: Option<u32>) -> u32 {
    if previous.uid_validity.is_some() && previous.uid_validity != validity {
        1
    } else {
        previous.uid_next.max(1)
    }
}

/// The cursor `through` bytes into an answer whose final cursor is `end`. One
/// that stops inside the answer resumes at the answer's first UID.
pub(super) fn at(
    end: &Cursor,
    first_uid: u32,
    records: &[(usize, usize)],
    through: usize,
) -> Result<Cursor, DriverError> {
    let length = end
        .through
        .ok_or_else(|| invalid("the IMAP final byte end is missing"))?;
    if through > length || through != 0 && !records.iter().any(|r| r.1 == through) {
        return Err(invalid("the IMAP cursor is not at a message boundary"));
    }
    let mut next = end.clone();
    next.through = Some(through);
    if through < length {
        next.uid_next = first_uid;
    }
    Ok(next)
}

fn invalid(reason: &str) -> DriverError {
    DriverError::Cursor(reason.into())
}

#[cfg(test)]
mod tests;
