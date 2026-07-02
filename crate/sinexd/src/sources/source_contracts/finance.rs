//! hledger plain-text journal parser (#1074).
//!
//! Reads `journal_clean` (or any `*.journal` / `*.hledger` file) from
//! `/realm/data/libraries/finance/` and emits one `ledger`/`transaction.posted`
//! event per transaction block.
//!
//! The personal journal uses a `YYYY-MM-DD Payee|Narration` convention on the
//! header line (pipe separates payee from narration). Standard hledger journals
//! with only a single description field are also supported — the pipe split is
//! applied only when the separator is present.
//!
//! Adapter: [`StaticFileAdapter`] — the journal file is a committed snapshot,
//! parsed in one shot. Anchor is a transaction index (`ByteRange { start:
//! tx_index, len: 1 }`), stable across re-imports of the same file content.

use async_trait::async_trait;
use serde::{Deserialize, Serialize};

use crate::runtime::parser::{MaterialParser, ParserError, ParserResult};
use sinex_macros::SourceMeta;
use sinex_primitives::domain::{EventSource, EventType};
use sinex_primitives::events::payloads::LedgerPosting;
use sinex_primitives::parser::{
    InputShapeKind, MaterialAnchor, OccurrenceKey, ParsedEventIntent, ParserContext, ParserId,
    ParserManifest, SourceId, SourceRecord, TimingConfidence, TimingEvidence,
};
use sinex_primitives::privacy::ProcessingContext;
use sinex_primitives::source_contracts::{
    AccessScope, CheckpointFamily, Horizon, OccurrenceIdentity, PrivacyTier, ResourceProfile,
    RetentionPolicy, RunnerPack, RuntimeShape,
};
use sinex_primitives::temporal::Timestamp;

// ---------------------------------------------------------------------------
// Journal line types
// ---------------------------------------------------------------------------

/// A parsed transaction block extracted from the journal text.
#[derive(Debug)]
struct JournalTransaction {
    /// 0-based index in the file (stable anchor across re-imports).
    index: u64,
    date: Timestamp,
    /// Payee / first part of `Payee|Narration`.
    description: String,
    /// Narration after the pipe, if present.
    narration: Option<String>,
    /// Inline comment on the header line (after `;`).
    comment: Option<String>,
    postings: Vec<LedgerPosting>,
}

// ---------------------------------------------------------------------------
// Parser
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct HledgerJournalParserConfig;

#[derive(Debug, Clone, Default, SourceMeta)]
#[source_meta(
    id = "hledger-journal",
    namespace = "finance",
    event_source = "ledger",
    event_type = "transaction.posted",
    adapter = "StaticFileAdapter",
    privacy_tier = PrivacyTier::Sensitive,
    horizons(Horizon::Historical),
    retention = RetentionPolicy::Forever,
    occurrence_identity = OccurrenceIdentity::Uuid5From("(date, description, first_explicit_posting_amount)"),
    access_scope = AccessScope::StagedExport,
    implementation = "sinexd",
    privacy_context = ProcessingContext::Document,
    resource_profile = ResourceProfile::BoundedFile,
    runner_pack = RunnerPack::SinexdSource,
    checkpoint_family = CheckpointFamily::AppendStream,
    runtime_shape = RuntimeShape::OnDemand,
)]
pub struct HledgerJournalParser;

#[async_trait]
impl MaterialParser for HledgerJournalParser {
    type Config = HledgerJournalParserConfig;

    fn manifest(&self) -> ParserManifest {
        ParserManifest {
            parser_id: ParserId::from_static("hledger-journal"),
            parser_version: "1.0.0".into(),
            accepted_input_shapes: vec![InputShapeKind::StaticFile],
            source_id: SourceId::from_static("hledger-journal"),
            declared_event_types: vec![(
                EventSource::from_static("ledger"),
                EventType::from_static("transaction.posted"),
            )],
            privacy_contexts: vec![ProcessingContext::Document],
            sensitivity_hints: Vec::new(),
            description: "Parses an hledger/ledger plain-text journal into one \
                transaction.posted event per transaction block. Supports the \
                personal Payee|Narration pipe convention on the header line."
                .into(),
        }
    }

    async fn parse_record(
        &mut self,
        record: SourceRecord,
        ctx: &ParserContext,
    ) -> ParserResult<Vec<ParsedEventIntent>> {
        let text = std::str::from_utf8(&record.bytes)
            .map_err(|e| ParserError::Parse(format!("journal is not valid UTF-8: {e}")))?;

        let transactions = parse_journal(text)?;
        let mut intents = Vec::with_capacity(transactions.len());
        for tx in &transactions {
            intents.push(build_intent(tx, ctx));
        }
        Ok(intents)
    }
}

// ---------------------------------------------------------------------------
// Core journal parser (line-by-line state machine)
// ---------------------------------------------------------------------------

fn parse_journal(text: &str) -> ParserResult<Vec<JournalTransaction>> {
    let mut transactions: Vec<JournalTransaction> = Vec::new();
    // Current transaction being accumulated.
    let mut current: Option<(String, Vec<String>)> = None; // (header_line, posting_lines)

    for raw_line in text.lines() {
        // Skip file-level comment lines and directives.
        let trimmed = raw_line.trim_start();

        if trimmed.starts_with(';') || trimmed.starts_with("include ") || trimmed.is_empty() {
            // Blank line terminates an open transaction block.
            if raw_line.trim().is_empty()
                && let Some((header, postings)) = current.take()
            {
                transactions.push(build_transaction(
                    transactions.len() as u64,
                    &header,
                    &postings,
                )?);
            }
            continue;
        }

        // Transaction header: starts with a 4-digit year.
        if raw_line.starts_with(|c: char| c.is_ascii_digit()) && looks_like_date(raw_line) {
            // Close any previous block.
            if let Some((header, postings)) = current.take() {
                transactions.push(build_transaction(
                    transactions.len() as u64,
                    &header,
                    &postings,
                )?);
            }
            current = Some((raw_line.to_string(), Vec::new()));
        } else if raw_line.starts_with(' ') || raw_line.starts_with('\t') {
            // Posting line — must belong to an open transaction.
            if let Some((_, ref mut postings)) = current {
                postings.push(raw_line.to_string());
            }
            // Silently ignore a posting with no open transaction
            // (e.g. indented directives at file top).
        }
        // Non-indented non-date lines (directives like `commodity`, `account`) are skipped.
    }

    // Flush the last transaction if the file doesn't end with a blank line.
    if let Some((header, postings)) = current.take() {
        transactions.push(build_transaction(
            transactions.len() as u64,
            &header,
            &postings,
        )?);
    }

    Ok(transactions)
}

/// Returns true when the line starts with a date in `YYYY-MM-DD` form.
fn looks_like_date(line: &str) -> bool {
    let bytes = line.as_bytes();
    bytes.len() >= 10
        && bytes[4] == b'-'
        && bytes[7] == b'-'
        && bytes
            .get(..4)
            .is_some_and(|s| s.iter().all(u8::is_ascii_digit))
        && bytes[5..7].iter().all(u8::is_ascii_digit)
        && bytes[8..10].iter().all(u8::is_ascii_digit)
}

fn build_transaction(
    index: u64,
    header: &str,
    posting_lines: &[String],
) -> ParserResult<JournalTransaction> {
    // Header format: `YYYY-MM-DD[=YYYY-MM-DD][ *|!] Description [; comment]`
    // We parse: date, optional status marker, then description+comment.
    let (date_str, rest) = header.split_once(' ').ok_or_else(|| {
        ParserError::Parse(format!(
            "transaction header has no space after date: {header:?}"
        ))
    })?;

    // Strip an optional auxiliary date `=YYYY-MM-DD` (effective date).
    let date_str = date_str.split('=').next().unwrap_or(date_str);
    let date = parse_date(date_str)?;

    // Strip optional cleared/pending marker (`*` or `!`).
    let rest = rest.trim_start();
    let rest = if rest.starts_with('*') || rest.starts_with('!') {
        rest[1..].trim_start()
    } else {
        rest
    };

    // Split off inline comment.
    let (desc_part, comment) = split_comment(rest);
    let desc_part = desc_part.trim();

    // Split payee|narration.
    let (description, narration) = if let Some((p, n)) = desc_part.split_once('|') {
        (p.trim().to_string(), Some(n.trim().to_string()))
    } else {
        (desc_part.to_string(), None)
    };

    // Parse posting lines.
    let postings = parse_postings(posting_lines)?;

    Ok(JournalTransaction {
        index,
        date,
        description,
        narration,
        comment: comment
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .map(str::to_string),
        postings,
    })
}

/// Splits `"Some text ; comment here"` into `("Some text ", Some(" comment here"))`.
/// Inline semicolons inside quoted strings are not handled (hledger doesn't use quotes here).
fn split_comment(s: &str) -> (&str, Option<&str>) {
    if let Some(pos) = s.find(';') {
        (&s[..pos], Some(&s[pos + 1..]))
    } else {
        (s, None)
    }
}

fn parse_postings(lines: &[String]) -> ParserResult<Vec<LedgerPosting>> {
    let mut postings = Vec::new();
    for line in lines {
        let trimmed = line.trim();
        // Skip pure comment lines within the transaction.
        if trimmed.starts_with(';') || trimmed.is_empty() {
            continue;
        }
        // Strip inline comment from posting line.
        let (body, _comment) = split_comment(trimmed);
        let body = body.trim();

        // An account posting has at least one word (the account name).
        // Amount is optional — the last posting in a balanced transaction
        // may omit it, letting hledger infer it to zero-balance.
        //
        // Hledger separates account from amount with ≥2 spaces.
        let posting = if let Some(sep_pos) = find_amount_separator(body) {
            let account = body[..sep_pos].trim().to_string();
            let amount_str = body[sep_pos..].trim();
            // Amount may start with `=` (balance assertion) — skip those.
            if amount_str.starts_with('=') {
                // Balance assertion — treat as implicit amount.
                LedgerPosting {
                    account,
                    amount: None,
                    currency: None,
                }
            } else {
                let (amount, currency) = parse_amount(amount_str)?;
                LedgerPosting {
                    account,
                    amount: Some(amount),
                    currency: Some(currency),
                }
            }
        } else {
            // No two-space separator → implicit amount.
            LedgerPosting {
                account: body.to_string(),
                amount: None,
                currency: None,
            }
        };
        postings.push(posting);
    }

    if postings.is_empty() {
        return Err(ParserError::Parse(
            "transaction has no postings — at least one posting is required".into(),
        ));
    }

    Ok(postings)
}

/// Finds the position of the two-space (or tab) separator between account and amount.
/// Returns `None` if only one space (no separator) or no separator at all.
fn find_amount_separator(s: &str) -> Option<usize> {
    // Look for "  " (two or more spaces) or a tab.
    let bytes = s.as_bytes();
    for i in 0..bytes.len().saturating_sub(1) {
        if bytes[i] == b' ' && bytes[i + 1] == b' ' {
            return Some(i);
        }
        if bytes[i] == b'\t' {
            return Some(i);
        }
    }
    None
}

/// Parses `"52.97 PLN"` or `"-1541.00 PLN"` or `"50 USD"`.
/// Returns `(amount_string, currency_string)`.
fn parse_amount(s: &str) -> ParserResult<(String, String)> {
    // Amount may be negative. Split on last whitespace to get currency.
    let s = s.trim();
    // Handle commodity-before-amount (e.g. `$ 50`) — not present in this journal,
    // but guard defensively.
    let last_space = s.rfind(' ');
    if let Some(pos) = last_space {
        let left = s[..pos].trim();
        let right = s[pos + 1..].trim();
        // Determine which side is the number.
        if looks_like_number(left) {
            return Ok((left.to_string(), right.to_string()));
        } else if looks_like_number(right) {
            return Ok((right.to_string(), left.to_string()));
        }
    }
    // Fallback: no space — whole string is amount, currency unknown.
    Err(ParserError::Parse(format!(
        "could not parse amount+currency from {s:?}"
    )))
}

fn looks_like_number(s: &str) -> bool {
    let s = s.trim_start_matches('-').trim_start_matches('+');
    !s.is_empty() && s.chars().next().is_some_and(|c| c.is_ascii_digit())
}

/// Parses a `YYYY-MM-DD` date into a [`Timestamp`] at midnight UTC.
fn parse_date(s: &str) -> ParserResult<Timestamp> {
    use time::{Date, PrimitiveDateTime, Time, format_description};
    let fmt = format_description::parse("[year]-[month]-[day]")
        .map_err(|e| ParserError::Parse(format!("internal date format error: {e}")))?;
    let date = Date::parse(s, &fmt)
        .map_err(|e| ParserError::Parse(format!("invalid journal date {s:?}: {e}")))?;
    let dt = PrimitiveDateTime::new(date, Time::MIDNIGHT).assume_utc();
    Ok(Timestamp::new(dt))
}

// ---------------------------------------------------------------------------
// Intent builder
// ---------------------------------------------------------------------------

fn build_intent(tx: &JournalTransaction, ctx: &ParserContext) -> ParsedEventIntent {
    // Compute occurrence key from (date, description, first explicit posting amount).
    // The first posting with an explicit amount is typically the source account.
    let first_amount = tx
        .postings
        .iter()
        .find(|p| p.amount.is_some())
        .and_then(|p| p.amount.as_deref())
        .unwrap_or("0")
        .to_string();

    let occurrence_key = OccurrenceKey {
        source_id: SourceId::from_static("hledger-journal"),
        fields: vec![
            ("date".into(), tx.date.inner().date().to_string()),
            ("description".into(), tx.description.clone()),
            ("first_amount".into(), first_amount),
        ],
    };

    let postings_json: Vec<_> = tx
        .postings
        .iter()
        .map(|p| {
            serde_json::json!({
                "account": p.account,
                "amount": p.amount,
                "currency": p.currency,
            })
        })
        .collect();

    let payload = serde_json::json!({
        "date": tx.date,
        "description": tx.description,
        "narration": tx.narration,
        "postings": postings_json,
        "comment": tx.comment,
    });

    ParsedEventIntent::builder()
        .source_id(ctx.source_id.clone())
        .parser_id(ParserId::from_static("hledger-journal"))
        .parser_version("1.0.0")
        .event_type(EventType::from_static("transaction.posted"))
        .event_source(EventSource::from_static("ledger"))
        .payload(payload)
        .ts_orig(tx.date)
        .timing(TimingEvidence::Intrinsic {
            field: "date".into(),
            confidence: TimingConfidence::Intrinsic,
        })
        .anchor(MaterialAnchor::ByteRange {
            start: tx.index,
            len: 1,
        })
        .occurrence_key(occurrence_key)
        .privacy_context(ProcessingContext::Document)
        .build()
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
#[path = "finance_test.rs"]
mod tests;
