//! WeeChat logs (`~/.local/share/weechat/logs/*.weechatlog`, or archived
//! per-buffer folders), on `AppendFile`: one line per message, as
//! `date<TAB>prefix<TAB>text`.
//!
//! WeeChat writes local time without a zone, so the zone is part of the
//! source's setting and applied here; a time in a repeated hour at the autumn
//! change takes the earlier instant. The prefix says what the line is: a nick
//! (with a mode prefix such as `@`), ` *` for an action, `-->` for a join,
//! `<--` for a part or quit, `--` for notices and other client lines.
use crate::drivers::append_file::AppendFile;
use crate::registry::{Declared, Driver, SourceSpec};
use crate::{Parser, RecordError};
use sinex_model::kinds::communication::{ChatLineKind, ChatMessage};
use sinex_model::{Class, Timestamp};
use std::path::PathBuf;

pub const SPEC: SourceSpec = SourceSpec {
    name: "communication.weechat-log",
    driver: Driver::AppendFile,
    consumer: "chat recall and correspondence history",
    class: Class::Open,
    parser: "weechat-log",
    parser_version: 1,
    event_types: &["chat.message"],
    framer,
};

/// The buffer a line belongs to is named by its log's folder (or, for a flat
/// log directory, its file name), from the material's path.
pub struct WeeChatParser {
    /// The zone WeeChat's times were written in.
    pub zone: jiff::tz::TimeZone,
}

const MODES: &[char] = &['@', '+', '%', '~', '&', '!'];

impl Parser for WeeChatParser {
    type Record<'a> = &'a [u8];
    type Carry = ();
    type Payload = ChatMessage;

    fn parse(
        &self,
        record: &&[u8],
        carry: &(),
        context: &crate::RecordContext<'_>,
    ) -> Result<(Vec<ChatMessage>, ()), RecordError> {
        self.line(
            record,
            carry,
            &buffer_of(context.path().unwrap_or_default()),
        )
    }
}

/// `…/#channel/2024-09-18.log` → `#channel`; `…/irc.libera.#c.weechatlog` →
/// `irc.libera.#c`.
pub fn buffer_of(path: &str) -> String {
    let p = std::path::Path::new(path);
    let flat = p.extension().is_some_and(|e| e == "weechatlog");
    let name = if flat {
        p.file_stem()
    } else {
        p.parent().and_then(|d| d.file_name())
    };
    name.map(|n| n.to_string_lossy().into_owned())
        .unwrap_or_default()
}

impl WeeChatParser {
    fn line(
        &self,
        record: &[u8],
        _: &(),
        buffer: &str,
    ) -> Result<(Vec<ChatMessage>, ()), RecordError> {
        let line = std::str::from_utf8(record)
            .map_err(|_| RecordError::new("invalid UTF-8 in the log line"))?;
        let line = line.strip_suffix('\n').unwrap_or(line);
        let line = line.strip_suffix('\r').unwrap_or(line);
        if line.is_empty() {
            return Ok((vec![], ()));
        }
        let mut parts = line.splitn(3, '\t');
        let date = parts.next().unwrap_or_default();
        let prefix = parts
            .next()
            .ok_or_else(|| RecordError::new("no prefix field"))?;
        let text = parts.next().unwrap_or_default().to_owned();
        let civil: jiff::civil::DateTime = date
            .parse()
            .map_err(|e| RecordError::new(format!("invalid time {date:?}: {e}")))?;
        let zoned = self
            .zone
            .to_ambiguous_zoned(civil)
            .compatible()
            .map_err(|e| RecordError::new(format!("time {date:?} in {:?}: {e}", self.zone)))?;
        let sent_at = Timestamp::from_unix_ns(zoned.timestamp().as_nanosecond())
            .map_err(|_| RecordError::new("time out of range"))?;
        let first_word = || text.split([' ', '(']).next().map(str::to_owned);
        let (kind, nick, mode) = match prefix {
            "-->" => (ChatLineKind::Join, first_word(), None),
            "<--" if text.contains(" has quit") => (ChatLineKind::Quit, first_word(), None),
            "<--" => (ChatLineKind::Part, first_word(), None),
            "--" if text.starts_with("Notice(") => (ChatLineKind::Notice, None, None),
            "--" => (ChatLineKind::System, None, None),
            " *" | "*" => (ChatLineKind::Action, first_word(), None),
            p if p.is_empty() || !p.chars().any(char::is_alphanumeric) => {
                (ChatLineKind::System, None, None)
            }
            p => {
                let mode: String = p.chars().take_while(|c| MODES.contains(c)).collect();
                let nick = p[mode.len()..].to_owned();
                (
                    ChatLineKind::Message,
                    Some(nick),
                    (!mode.is_empty()).then_some(mode),
                )
            }
        };
        Ok((
            vec![ChatMessage {
                buffer: buffer.to_owned(),
                sent_at,
                nick,
                prefix: mode,
                text,
                kind,
            }],
            (),
        ))
    }
}

/// The zone named `name` (an IANA name such as `Europe/Warsaw`), or this
/// system's zone.
pub fn zone(name: Option<&str>) -> Result<jiff::tz::TimeZone, String> {
    match name {
        Some(name) => jiff::tz::TimeZone::get(name).map_err(|e| format!("zone {name}: {e}")),
        None => Ok(jiff::tz::TimeZone::system()),
    }
}

pub fn source(zone: jiff::tz::TimeZone) -> Declared<WeeChatParser> {
    Declared {
        spec: SPEC,
        parser: WeeChatParser { zone },
    }
}

/// Complete lines; a partial last line waits for the next poll.
fn framer(bytes: &[u8]) -> (Vec<(usize, usize)>, usize) {
    let mut out = Vec::new();
    let mut start = 0;
    for (i, b) in bytes.iter().enumerate() {
        if *b == b'\n' {
            out.push((start, i + 1));
            start = i + 1;
        }
    }
    (out, start)
}

pub fn driver(path: PathBuf, host: String) -> AppendFile {
    AppendFile { path, host, framer }
}

/// `--source weechat=PATH`: each WeeChat log under PATH, read in the
/// binding's zone (WeeChat writes local times), recorded when it is planned.
pub const KIND: crate::binding::Kind = crate::binding::Kind {
    name: "weechat",
    specs: || vec![&SPEC],
    plan: Some(|input, settings| {
        Ok(crate::binding::per_file(
            &SPEC,
            input,
            &settings.clone().with_zone()?,
            true,
            |p| {
                p.extension()
                    .is_some_and(|e| e == "log" || e == "weechatlog")
            },
        ))
    }),
    source: |b| Ok(Box::new(source(b.settings.zone()?))),
    reader: Some(|b| {
        Ok(crate::binding::appended(
            driver(b.input.clone(), b.settings.host.clone()),
            SPEC.framer,
        ))
    }),
    capture: None,
};

#[cfg(test)]
mod tests;
