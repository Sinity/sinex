//! Authored journal transactions and directives. Acquisition follows local includes;
//! parsing preserves written quantities and never balances or generates postings.
pub mod framing;
mod includes;
mod posting;

use serde::{Deserialize, Serialize};
use sinex_model::{
    Class,
    event_types::finance::{
        IncludedFrom, JournalDirective, JournalImported, JournalRecord, Posting, TransactionPosted,
    },
};
use sinex_source_core::binding::{self, InputFormat};
use sinex_source_core::drivers::export_archive::{ExportArchive, Framing, PART_BYTES};
use sinex_source_core::registry::{DeclaredSource, Driver, SourceSpec};
use sinex_source_core::{Parser, RecordContext, RecordError};
use std::path::PathBuf;

const TRANSACTIONS: Framing = Framing::Custom {
    name: "hledger-transactions",
    frame: framing::frame,
};

pub const SPEC: SourceSpec = SourceSpec {
    name: "finance.hledger",
    driver: Driver::ExportArchive,
    excisable: false,
    purpose: "journal transactions and their postings",
    class: Class::Guarded,
    parser: "hledger",
    parser_version: 1,
    event_types: &[
        "transaction.posted",
        "journal.directive",
        "journal.imported",
    ],
    framer: sinex_source_core::Framer::export(framing::complete, TRANSACTIONS),
};

pub struct HledgerParser {
    pub host: String,
    pub journal: String,
    pub initial_year: Option<i16>,
    pub input_format: String,
    pub included_from: Option<IncludedFrom>,
    pub includes:
        std::collections::BTreeMap<String, Vec<sinex_model::event_types::finance::IncludeTarget>>,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct Carry {
    member: Option<(String, u64)>,
    year: Option<i16>,
    #[serde(with = "counts")]
    seen: imbl::OrdMap<String, u64>,
}

mod counts {
    use serde::{Deserialize, Deserializer, Serialize, Serializer};

    pub(super) fn serialize<S: Serializer>(
        counts: &imbl::OrdMap<String, u64>,
        serializer: S,
    ) -> Result<S::Ok, S::Error> {
        counts
            .iter()
            .collect::<std::collections::BTreeMap<_, _>>()
            .serialize(serializer)
    }

    pub(super) fn deserialize<'de, D: Deserializer<'de>>(
        deserializer: D,
    ) -> Result<imbl::OrdMap<String, u64>, D::Error> {
        Ok(
            std::collections::BTreeMap::<String, u64>::deserialize(deserializer)?
                .into_iter()
                .collect(),
        )
    }
}

impl Parser for HledgerParser {
    type Record<'a> = &'a [u8];
    type Carry = Carry;
    type Payload = JournalRecord;

    fn parse(
        &self,
        record: &&[u8],
        carry: &Carry,
        context: &RecordContext<'_>,
    ) -> Result<(Vec<JournalRecord>, Carry), RecordError> {
        let text = std::str::from_utf8(record)
            .map_err(|_| RecordError::new("journal block is not UTF-8"))?;
        let member = context
            .member
            .map(|m| {
                let index = m
                    .details
                    .get("member_index")
                    .and_then(serde_json::Value::as_u64)
                    .ok_or_else(|| RecordError::new("export member index is missing"))?;
                Ok::<_, RecordError>((m.path.clone(), index))
            })
            .transpose()?;
        let member_duplicate = match context.member {
            Some(member) => member
                .details
                .get("member_duplicate")
                .and_then(serde_json::Value::as_u64)
                .ok_or_else(|| RecordError::new("export member duplicate number is missing"))?,
            None => 0,
        };
        let mut next = if carry.member == member {
            carry.clone()
        } else {
            Carry {
                member: member.clone(),
                year: self.initial_year,
                ..Default::default()
            }
        };
        if next.year.is_none() {
            next.year = self.initial_year;
        }
        let mut lines = text.lines().filter(|line| !line.trim().is_empty());
        let Some(header) = lines.next() else {
            return Ok((vec![], next));
        };
        let hash = blake3::hash(record).to_hex().to_string();
        let duplicate = next.seen.get(&hash).copied().unwrap_or(0);
        next.seen.insert(
            hash.clone(),
            duplicate
                .checked_add(1)
                .ok_or_else(|| RecordError::new("journal duplicate number exceeds u64"))?,
        );
        if self.input_format != "journal" {
            return Ok((
                vec![JournalRecord::Imported(JournalImported {
                    host: self.host.clone(),
                    journal: self.journal.clone(),
                    member: member.as_ref().map(|(path, _)| path.clone()),
                    member_duplicate,
                    block_hash: hash,
                    duplicate,
                    included_from: self.included_from.clone(),
                    format: self.input_format.clone(),
                    text: text.into(),
                })],
                next,
            ));
        }
        let header = directive_body(header.trim_start());
        if !header.starts_with(|c: char| c.is_ascii_digit()) {
            let directive = directive_name(header)?;
            if directive == "Y" || directive == "year" || header.starts_with("apply year ") {
                let value = header
                    .strip_prefix("apply year ")
                    .unwrap_or_else(|| header.trim_start_matches(directive))
                    .trim();
                next.year = Some(
                    split_comment(value)
                        .0
                        .trim()
                        .parse::<i16>()
                        .map_err(|_| RecordError::new("invalid journal year"))?,
                );
            }
            let includes = self
                .includes
                .get(&format!("{hash}:{duplicate}"))
                .cloned()
                .unwrap_or_else(|| {
                    if directive == "include" {
                        vec![sinex_model::event_types::finance::IncludeTarget {
                            path: header
                                .trim_start_matches("include")
                                .split(';')
                                .next()
                                .unwrap()
                                .trim()
                                .into(),
                            reason: Some(
                                "include resolution is unavailable for this captured block".into(),
                            ),
                        }]
                    } else {
                        vec![]
                    }
                });
            return Ok((
                vec![JournalRecord::Directive(JournalDirective {
                    host: self.host.clone(),
                    journal: self.journal.clone(),
                    member: member.as_ref().map(|(path, _)| path.clone()),
                    member_duplicate,
                    block_hash: hash,
                    duplicate,
                    directive: directive.into(),
                    text: text.into(),
                    included_from: self.included_from.clone(),
                    includes,
                })],
                next,
            ));
        }
        let (dates, rest) = header
            .split_once(char::is_whitespace)
            .unwrap_or((header, ""));
        let (date, effective_date) = match dates.split_once('=') {
            Some((date, effective)) => (date, Some(effective)),
            None => (dates, None),
        };
        let resolved_date = resolve_date(date, next.year)?;
        let resolved_effective_date = effective_date
            .map(|date| {
                resolve_date(
                    date,
                    resolved_date
                        .as_deref()
                        .and_then(|date| date.split('-').next()?.parse().ok())
                        .or(next.year),
                )
            })
            .transpose()?
            .flatten();
        let rest = rest.trim_start();
        let (status, rest) = if let Some(rest) = rest.strip_prefix('*') {
            (Some("*".to_owned()), rest.trim_start())
        } else if let Some(rest) = rest.strip_prefix('!') {
            (Some("!".to_owned()), rest.trim_start())
        } else {
            (None, rest)
        };
        let (code, rest) = if let Some(code) = rest.strip_prefix('(') {
            let (code, rest) = code
                .split_once(')')
                .ok_or_else(|| RecordError::new("unfinished transaction code"))?;
            (Some(code.into()), rest.trim_start())
        } else {
            (None, rest)
        };
        let (description, comment) = split_comment(rest);
        let (description, narration) = match description.split_once('|') {
            Some((payee, narration)) => (payee.trim(), Some(narration.trim().to_owned())),
            None => (description.trim(), None),
        };
        let mut postings: Vec<Posting> = Vec::new();
        let mut comments: Vec<String> = comment.into_iter().collect();
        for line in lines {
            if !line.starts_with([' ', '\t']) {
                return Err(RecordError::new("multiple journal blocks in one record"));
            }
            let line = line.trim();
            if let Some(comment) = line.strip_prefix(';').or_else(|| line.strip_prefix('#')) {
                if let Some(posting) = postings.last_mut() {
                    let text = posting.comment.get_or_insert_with(String::new);
                    if !text.is_empty() {
                        text.push('\n');
                    }
                    text.push_str(comment.trim());
                } else {
                    comments.push(comment.trim().into());
                }
                continue;
            }
            postings.push(posting::parse(line)?);
        }
        let payload = TransactionPosted {
            host: self.host.clone(),
            journal: self.journal.clone(),
            member: member.as_ref().map(|(path, _)| path.clone()),
            member_duplicate,
            block_hash: hash,
            duplicate,
            date: date.into(),
            effective_date: effective_date.map(str::to_owned),
            resolved_date,
            resolved_effective_date,
            default_year: next.year,
            code,
            included_from: self.included_from.clone(),
            status,
            description: description.into(),
            narration,
            comment: (!comments.is_empty()).then(|| comments.join("\n")),
            postings,
        };
        Ok((vec![JournalRecord::Transaction(payload)], next))
    }
}

fn directive_body(header: &str) -> &str {
    header.strip_prefix(['!', '@']).unwrap_or(header)
}

fn directive_name(header: &str) -> Result<&str, RecordError> {
    if header.starts_with([';', '#', '*']) {
        return Ok("comment");
    }
    if header.starts_with('~') {
        return Ok("~");
    }
    if header.starts_with('=') {
        return Ok("=");
    }
    if header.starts_with('Y') && header[1..].starts_with(|c: char| c.is_ascii_digit()) {
        return Ok("Y");
    }
    let name = header.split_whitespace().next().unwrap_or("");
    match name {
        "account" | "commodity" | "P" | "D" | "N" | "C" | "alias" | "include" | "Y" | "year"
        | "decimal-mark" | "payee" | "tag" | "apply" | "end" | "assert" | "check" | "comment"
        | "endcomment" | "bucket" | "A" | "capture" | "define" | "eval" | "expr" | "python"
        | "value" => Ok(name),
        _ if name.starts_with("--") => Ok("--"),
        _ => Err(RecordError::new("unknown journal directive")),
    }
}

fn resolve_date(raw: &str, year: Option<i16>) -> Result<Option<String>, RecordError> {
    let separator = raw
        .chars()
        .find(|c| matches!(c, '-' | '/' | '.'))
        .ok_or_else(|| RecordError::new("invalid journal date"))?;
    let fields = raw.split(separator).collect::<Vec<_>>();
    let (year, month, day) = match fields.as_slice() {
        [y, m, d] => (
            Some(
                y.parse::<i16>()
                    .map_err(|_| RecordError::new("invalid journal date"))?,
            ),
            *m,
            *d,
        ),
        [m, d] => (year, *m, *d),
        _ => return Err(RecordError::new("invalid journal date")),
    };
    let month = month
        .parse::<i8>()
        .map_err(|_| RecordError::new("invalid journal date"))?;
    let day = day
        .parse::<i8>()
        .map_err(|_| RecordError::new("invalid journal date"))?;
    // A leap year checks a partial February 29 without inventing a year.
    let date = jiff::civil::Date::new(year.unwrap_or(2000), month, day)
        .map_err(|_| RecordError::new("invalid journal date"))?;
    Ok(year.map(|_| date.to_string()))
}

fn split_comment(raw: &str) -> (&str, Option<String>) {
    let mut quoted = false;
    let mut escaped = false;
    for (at, c) in raw.char_indices() {
        if escaped {
            escaped = false;
            continue;
        }
        if quoted && c == '\\' {
            escaped = true;
            continue;
        }
        if c == '"' {
            quoted = !quoted;
        }
        if c == ';' && !quoted {
            return (&raw[..at], Some(raw[at + 1..].trim().into()));
        }
    }
    (raw, None)
}

pub fn source(host: String, journal: String) -> DeclaredSource<HledgerParser> {
    DeclaredSource {
        spec: SPEC,
        parser: HledgerParser {
            host,
            journal,
            initial_year: None,
            input_format: "journal".into(),
            included_from: None,
            includes: Default::default(),
        },
    }
}

pub fn driver(path: PathBuf, host: String) -> ExportArchive {
    ExportArchive {
        path,
        host,
        select: |_| true,
        framing: TRANSACTIONS,
        part_bytes: PART_BYTES,
    }
}

pub const FORMAT: InputFormat = InputFormat {
    name: "hledger",
    discovery: sinex_source_core::discovery::DiscoverySpec {
        owner: sinex_source_core::discovery::Owner::System,
        locations: &[
            sinex_source_core::discovery::Location {
                root: "environment",
                path: "LEDGER_FILE",
            },
            sinex_source_core::discovery::Location {
                root: "home",
                path: ".hledger.journal",
            },
            sinex_source_core::discovery::Location {
                root: "declared",
                path: "finance",
            },
        ],
        capabilities: &[],
        credentials: &[],
        streams: &[],
        requests: &[],
        request_only: false,
    },
    discover: |format, context| {
        let mut context = context.clone();
        let config = context.sources.entry(SPEC.name.into()).or_default();
        let mut options = config.options.as_object().cloned().unwrap_or_default();
        options.insert("include_home".into(), serde_json::json!(context.home));
        config.options = options.into();
        crate::discovery::discover_format(format, &context)
    },
    specs: || vec![&SPEC],
    plan: Some(includes::plan),
    source: |b| {
        let mut source = source(
            b.settings.host.clone(),
            b.settings
                .option("journal")
                .unwrap_or(&b.input.display().to_string())
                .into(),
        );
        let input_format = b.settings.option("journal_format").unwrap_or("journal");
        if !matches!(input_format, "journal" | "timeclock" | "timedot") {
            return Err("unknown native journal format".into());
        }
        source.parser.input_format = input_format.into();
        source.parser.initial_year = b
            .settings
            .options
            .get("initial_year")
            .and_then(serde_json::Value::as_i64)
            .and_then(|n| i16::try_from(n).ok());
        source.parser.included_from = b
            .settings
            .options
            .get("included_from")
            .filter(|v| !v.is_null())
            .map(|v| serde_json::from_value(v.clone()))
            .transpose()
            .map_err(|e| e.to_string())?;
        source.parser.includes = b
            .settings
            .options
            .get("includes")
            .map(|v| serde_json::from_value(v.clone()))
            .transpose()
            .map_err(|e| e.to_string())?
            .unwrap_or_default();
        Ok(Box::new(source))
    },
    reader: Some(|b| {
        Ok(binding::shared(driver(
            b.input.clone(),
            b.settings.host.clone(),
        )))
    }),
    capture: None,
};

#[cfg(test)]
pub(crate) mod tests;
