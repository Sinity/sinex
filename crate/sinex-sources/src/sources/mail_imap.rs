//! Live mail over IMAP (`di7`): new messages in a mailbox, fetched by UID
//! after the cursor, waiting with IDLE between polls when the server supports
//! it. Gmail and other providers that refuse password logins take an OAuth
//! access token (SASL `XOAUTH2`), as the predecessor's client did.
//!
//! Each poll stores one batch of messages as one material: the messages'
//! exact bytes back to back, each one a record, listed in the manifest so a
//! replay finds the same records. The cursor is the mailbox's UIDVALIDITY and
//! the next UID; a changed UIDVALIDITY means the server renumbered the
//! mailbox, so reading starts over (a message already read is then a repeat
//! by its Message-ID).
//!
//! Credentials come from files (`password_file`, `token_file`), never from
//! the configuration itself, and never appear in errors.
//!
//! The client is a small synchronous IMAP4rev1 client (RFC 3501): the
//! commands live mail needs (LOGIN or AUTHENTICATE XOAUTH2, SELECT, UID
//! FETCH, IDLE, LOGOUT) over TLS (rustls) or, for tests, plain TCP.
use crate::drivers::{Acquired, DriverError, NewMaterial};
use crate::registry::{Declared, Driver, SourceSpec};
use base64::Engine;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sinex_model::{Class, InputRef, MaterialId, ObservationFacts, RevisionChange, Timestamp};
use std::io::{BufRead, BufReader, Read, Write};
use std::net::TcpStream;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

pub const SPEC: SourceSpec = SourceSpec {
    name: "communication.imap",
    driver: Driver::ApiCursor,
    consumer: "mail recall and correspondence history",
    class: Class::Open,
    parser: "rfc5322-mail",
    parser_version: 1,
    event_types: &["mail.message"],
    framer: crate::drivers::export_archive::frame_whole,
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
    /// Implicit TLS (port 993). Plain TCP only for tests on loopback.
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
        let tcp =
            TcpStream::connect((cfg.host.as_str(), cfg.port)).map_err(|e| io("connect", e))?;
        tcp.set_read_timeout(Some(Duration::from_secs(cfg.idle_seconds.max(60) + 30)))
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

    /// Fetch the next batch of messages after the cursor; when none is new,
    /// wait with IDLE for `idle_seconds` (when set) and look once more.
    pub fn poll(&self, cursor: &Value) -> Result<Acquired, DriverError> {
        let previous: Cursor = if cursor.is_null() {
            Cursor::default()
        } else {
            serde_json::from_value(cursor.clone())
                .map_err(|e| DriverError::Cursor(e.to_string()))?
        };
        let mut c = Connection::open(&self.config)?;
        self.login(&mut c)?;
        let selected = c.run(&format!("SELECT {}", quote(&self.config.mailbox)), "select")?;
        let uid_validity = selected
            .iter()
            .find_map(|l| number_after(&l.text, "UIDVALIDITY"));
        let server_next = selected
            .iter()
            .find_map(|l| number_after(&l.text, "UIDNEXT"));
        // A renumbered mailbox: start over.
        let start = if previous.uid_validity.is_some() && previous.uid_validity != uid_validity {
            1
        } else {
            previous.uid_next.max(1)
        };
        let mut have_new = server_next.is_none_or(|n| n > start);
        if !have_new && self.config.idle_seconds > 0 {
            let tag = c.send("IDLE")?;
            let _continuation = c.read_line()?;
            let deadline =
                std::time::Instant::now() + Duration::from_secs(self.config.idle_seconds);
            while std::time::Instant::now() < deadline {
                match c.read_line() {
                    Ok(l) if l.text.contains("EXISTS") => {
                        have_new = true;
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
        }
        let mut messages: Vec<(u32, Vec<u8>)> = Vec::new();
        if have_new {
            let end = start.saturating_add(self.config.batch.max(1) - 1);
            for line in c.run(
                &format!("UID FETCH {start}:{end} (UID BODY.PEEK[])"),
                "fetch",
            )? {
                if let (Some(uid), Some(body)) = (
                    number_after(&line.text, "UID"),
                    line.literals.into_iter().next(),
                ) && uid >= start
                {
                    messages.push((uid, body));
                }
            }
        }
        let _ = c.run("LOGOUT", "logout");
        messages.sort_by_key(|(uid, _)| *uid);
        let now = Timestamp::now();
        let Some(last_uid) = messages.last().map(|(u, _)| *u) else {
            let next = Cursor {
                uid_validity,
                uid_next: start,
                ..previous
            };
            return Ok(Acquired {
                input: self.input(),
                observation: ObservationFacts {
                    observed_at: Some(now),
                    changed: false,
                    ..Default::default()
                },
                revision: RevisionChange::Same,
                material: None,
                cursor_unchanged: serde_json::to_value(next).expect("cursor serializes"),
            });
        };
        let mut bytes = Vec::new();
        let mut records = Vec::new();
        let mut uids = Vec::new();
        for (uid, body) in &messages {
            let start = bytes.len();
            bytes.extend_from_slice(body);
            records.push((start, bytes.len()));
            uids.push(*uid);
        }
        let manifest = json!({
            "media_type": "message/rfc822",
            "mailbox": self.config.mailbox, "uid_validity": uid_validity, "uids": uids,
            "records": records, "read_at_ns": i64::try_from(now.unix_ns()).unwrap_or(i64::MAX),
        });
        let id = MaterialId::new();
        let part_index = if previous.material.is_some() {
            previous.part_index + 1
        } else {
            0
        };
        let next = Cursor {
            uid_validity,
            uid_next: last_uid + 1,
            material: Some(id),
            part_index,
        };
        let size = bytes.len() as u64;
        Ok(Acquired {
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
                stored_file: None,
                records,
                part_index,
                previous_part: previous.material,
                reading_order: crate::drivers::reading_order(&manifest),
                manifest,
                cursor_at: Box::new(move |_| {
                    serde_json::to_value(&next).expect("cursor serializes")
                }),
                aligned: false,
                vanished: Vec::new(),
            }),
            cursor_unchanged: Value::Null,
        })
    }
}

pub fn source() -> Declared<super::mail::MailParser> {
    Declared {
        spec: SPEC,
        parser: super::mail::MailParser,
    }
}

#[cfg(test)]
mod tests {
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
                        if expect_token
                            && String::from_utf8(decoded).unwrap().contains("Bearer tok")
                        {
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
}
