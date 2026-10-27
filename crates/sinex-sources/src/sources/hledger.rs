//! Explicit journal subset: ISO dates, statuses, descriptions, comments and
//! ordered postings with decimal quantities. Includes are not traversed.
pub mod framing;

use crate::binding::{self, InputFormat};
use crate::drivers::export_archive::{ExportArchive, Framing, PART_BYTES};
use crate::registry::{DeclaredSource, Driver, SourceSpec};
use crate::{Parser, RecordContext, RecordError};
use serde::{Deserialize, Serialize};
use sinex_model::{
    Class,
    event_types::finance::{Posting, TransactionPosted},
};
use std::path::PathBuf;

pub const SPEC: SourceSpec = SourceSpec {
    name: "finance.hledger",
    driver: Driver::ExportArchive,
    excisable: false,
    purpose: "journal transactions and their postings",
    class: Class::Guarded,
    parser: "hledger",
    parser_version: 1,
    event_types: &["transaction.posted"],
    framer: crate::Framer::export(framing::complete, Framing::HledgerTransactions),
};

pub struct HledgerParser {
    pub host: String,
    pub journal: String,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct Carry {
    member: Option<(String, u64)>,
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
    type Payload = TransactionPosted;

    fn parse(
        &self,
        record: &&[u8],
        carry: &Carry,
        context: &RecordContext<'_>,
    ) -> Result<(Vec<TransactionPosted>, Carry), RecordError> {
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
        let mut next = if carry.member == member {
            carry.clone()
        } else {
            Carry {
                member: member.clone(),
                ..Default::default()
            }
        };
        let mut lines = text.lines().filter(|line| !line.trim().is_empty());
        let Some(header) = lines.next() else {
            return Ok((vec![], next));
        };
        if header.starts_with(';') || header.starts_with('#') {
            if lines.any(|line| !line.trim_start().starts_with([';', '#'])) {
                return Err(RecordError::new(
                    "unsupported journal directive after comment",
                ));
            }
            return Ok((vec![], next));
        }
        let (dates, rest) = header.split_once(char::is_whitespace).ok_or_else(|| {
            RecordError::new("unsupported journal directive or missing transaction description")
        })?;
        let (date, effective_date) = match dates.split_once('=') {
            Some((date, effective)) => (date, Some(effective)),
            None => (dates, None),
        };
        validate_date(date)?;
        if let Some(date) = effective_date {
            validate_date(date)?;
        }
        let rest = rest.trim_start();
        let (status, rest) = if let Some(rest) = rest.strip_prefix('*') {
            (Some("*".to_owned()), rest.trim_start())
        } else if let Some(rest) = rest.strip_prefix('!') {
            (Some("!".to_owned()), rest.trim_start())
        } else {
            (None, rest)
        };
        let (description, comment) = split_comment(rest);
        let (description, narration) = match description.split_once('|') {
            Some((payee, narration)) => (payee.trim(), Some(narration.trim().to_owned())),
            None => (description.trim(), None),
        };
        if description.is_empty() {
            return Err(RecordError::new("transaction description is empty"));
        }
        let mut postings: Vec<Posting> = Vec::new();
        let mut comments: Vec<String> = comment.into_iter().collect();
        for line in lines {
            if !line.starts_with([' ', '\t']) {
                return Err(RecordError::new("multiple journal blocks in one record"));
            }
            let line = line.trim();
            if let Some(comment) = line.strip_prefix(';') {
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
            postings.push(posting(line)?);
        }
        if postings.is_empty() {
            return Err(RecordError::new("transaction has no postings"));
        }
        let hash = blake3::hash(record).to_hex().to_string();
        let duplicate = next.seen.get(&hash).copied().unwrap_or(0);
        next.seen.insert(
            hash.clone(),
            duplicate
                .checked_add(1)
                .ok_or_else(|| RecordError::new("transaction duplicate number exceeds u64"))?,
        );
        let payload = TransactionPosted {
            host: self.host.clone(),
            journal: self.journal.clone(),
            member: member.as_ref().map(|(path, _)| path.clone()),
            member_index: member.as_ref().map(|(_, index)| *index),
            block_hash: hash,
            duplicate,
            date: date.into(),
            effective_date: effective_date.map(str::to_owned),
            status,
            description: description.into(),
            narration,
            comment: (!comments.is_empty()).then(|| comments.join("\n")),
            postings,
        };
        Ok((vec![payload], next))
    }
}

fn validate_date(raw: &str) -> Result<(), RecordError> {
    let valid_shape = raw.len() == 10
        && raw.as_bytes()[4] == b'-'
        && raw.as_bytes()[7] == b'-'
        && raw
            .bytes()
            .enumerate()
            .all(|(i, b)| i == 4 || i == 7 || b.is_ascii_digit());
    if !valid_shape || jiff::civil::Date::strptime("%Y-%m-%d", raw).is_err() {
        return Err(RecordError::new(
            "invalid journal date or unsupported directive; expected YYYY-MM-DD",
        ));
    }
    Ok(())
}

fn split_comment(raw: &str) -> (&str, Option<String>) {
    match raw.split_once(';') {
        Some((body, comment)) => (body, Some(comment.trim().into())),
        None => (raw, None),
    }
}

fn posting(raw: &str) -> Result<Posting, RecordError> {
    let (body, comment) = split_comment(raw);
    let body = body.trim();
    let separator = body.as_bytes().iter().enumerate().find_map(|(i, b)| {
        (*b == b'\t' || (*b == b' ' && body.as_bytes().get(i + 1) == Some(&b' '))).then_some(i)
    });
    let (account, amount) = separator.map_or((body, ""), |i| (body[..i].trim(), body[i..].trim()));
    if account.is_empty()
        || account.starts_with(['(', '[', '*', '!'])
        || account.contains(['=', '@'])
    {
        return Err(RecordError::new(
            "unsupported journal posting account or status",
        ));
    }
    let (amount, currency) = if amount.is_empty() {
        (None, None)
    } else {
        quantity(amount)?
    };
    Ok(Posting {
        account: account.into(),
        amount,
        currency,
        comment,
    })
}

fn quantity(raw: &str) -> Result<(Option<String>, Option<String>), RecordError> {
    let unsupported = || {
        RecordError::new(
            "unsupported journal amount; expected decimal with optional leading or trailing commodity (no assertions or prices)",
        )
    };
    if raw.contains(['=', '@', ',', '(', ')', '"']) {
        return Err(unsupported());
    }
    static AMOUNT: std::sync::LazyLock<regex::Regex> = std::sync::LazyLock::new(|| {
        regex::Regex::new(r"^(?:(?P<leading>[^\s0-9.+\-;|*!]+)\s*)?(?P<amount>[+-]?(?:[0-9]+(?:\.[0-9]*)?|\.[0-9]+))(?:\s*(?P<trailing>[^\s0-9.+\-;|*!]+))?$")
            .expect("fixed journal quantity expression")
    });
    let fields = AMOUNT.captures(raw).ok_or_else(unsupported)?;
    let amount = fields.name("amount").expect("required decimal").as_str();
    let currency = match (fields.name("leading"), fields.name("trailing")) {
        (Some(_), Some(_)) => return Err(unsupported()),
        (leading, trailing) => leading.or(trailing).map(|m| m.as_str()),
    };
    Ok((Some(amount.into()), currency.map(str::to_owned)))
}

pub fn source(host: String, journal: String) -> DeclaredSource<HledgerParser> {
    DeclaredSource {
        spec: SPEC,
        parser: HledgerParser { host, journal },
    }
}

pub fn driver(path: PathBuf, host: String) -> ExportArchive {
    ExportArchive {
        path,
        host,
        select: |_| true,
        framing: Framing::HledgerTransactions,
        part_bytes: PART_BYTES,
    }
}

fn validate(settings: &binding::Settings) -> Result<(), String> {
    match settings.options.get("resolve_includes") {
        None | Some(serde_json::Value::Bool(false)) => Ok(()),
        _ => {
            Err("hledger resolve_includes must be false; include traversal is not approved".into())
        }
    }
}

pub const FORMAT: InputFormat = InputFormat {
    name: "hledger",
    specs: || vec![&SPEC],
    plan: Some(|input, settings| {
        validate(settings)?;
        Ok(binding::single(&SPEC, input, settings))
    }),
    source: |b| {
        validate(&b.settings)?;
        Ok(Box::new(source(
            b.settings.host.clone(),
            b.input.display().to_string(),
        )))
    },
    reader: Some(|b| {
        validate(&b.settings)?;
        Ok(binding::shared(driver(
            b.input.clone(),
            b.settings.host.clone(),
        )))
    }),
    capture: None,
};

#[cfg(test)]
mod tests;
