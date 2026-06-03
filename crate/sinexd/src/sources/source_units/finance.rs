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

use crate::node_sdk::parser::{MaterialParser, ParserError, ParserResult, StaticFileAdapter};
use sinex_primitives::domain::{EventSource, EventType};
use sinex_primitives::events::payloads::LedgerPosting;
use sinex_primitives::parser::{
    InputShapeKind, MaterialAnchor, OccurrenceKey, ParsedEventIntent, ParserContext, ParserId,
    ParserManifest, SourceRecord, SourceUnitId, TimingConfidence, TimingEvidence,
};
use sinex_primitives::privacy::ProcessingContext;
use sinex_primitives::proof::{
    CheckpointFamily, Horizon, OccurrenceIdentity, PrivacyTier, RetentionPolicy, RuntimeShape,
    SourceUnitBinding, SourceUnitBuildImpact, SourceUnitDescriptor, SubjectRef,
};
use sinex_primitives::temporal::Timestamp;
use sinex_primitives::{register_source_unit, register_source_unit_binding};

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

#[derive(Debug, Clone, Default)]
pub struct HledgerJournalParser;

#[async_trait]
impl MaterialParser for HledgerJournalParser {
    type Config = HledgerJournalParserConfig;

    fn manifest(&self) -> ParserManifest {
        ParserManifest {
            parser_id: ParserId::from_static("hledger-journal"),
            parser_version: "1.0.0".into(),
            accepted_input_shapes: vec![InputShapeKind::StaticFile],
            source_unit_id: SourceUnitId::from_static("hledger-journal"),
            declared_event_types: vec![(
                EventSource::from_static("ledger"),
                EventType::from_static("transaction.posted"),
            )],
            privacy_contexts: vec![ProcessingContext::Document],
            sensitivity_hints: Vec::new(),
            proof_obligations: vec![
                "timestamp_intrinsic".into(),
                "anchor_tx_index".into(),
                "occurrence_key_date_description_amount".into(),
                "implicit_posting_amount_none".into(),
            ],
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

/// Parses `"40.00 TEST"` or `"-1541.00 TEST"` or `"50 ALT"`.
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
        source_unit_id: SourceUnitId::from_static("hledger-journal"),
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
        .source_unit_id(ctx.source_unit_id.clone())
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
// Source unit descriptor + binding + registration
// ---------------------------------------------------------------------------

register_source_unit! {
    SourceUnitDescriptor {
        id: "hledger-journal",
        namespace: "finance",
        event_types: &[("ledger", "transaction.posted")],
        privacy_tier: PrivacyTier::Sensitive,
        horizons: &[Horizon::Historical],
        retention: RetentionPolicy::Forever,
        proof_obligations: &[
            "timestamp_intrinsic",
            "anchor_tx_index",
            "occurrence_key_date_description_amount",
            "implicit_posting_amount_none",
        ],
        occurrence_identity: OccurrenceIdentity::Uuid5From(
            "(date, description, first_explicit_posting_amount)",
        ),
        access_policy: "personal_finance_data",
    }
}

register_source_unit_binding! {
    SourceUnitBinding::builder(
        SubjectRef::from_static("source_unit:hledger-journal"),
        "hledger-journal",
        "finance",
    )
    .implementation("sinex-source-worker")
    .adapter("StaticFileAdapter")
    .output_event_type("transaction.posted")
    .privacy_context("Document")
    .material_policy("static_export_file")
    .checkpoint_policy("static_file_cursor")
    .resource_shape("file_reader")
    .source_unit_id("hledger-journal")
    .runner_pack("source-worker")
    .checkpoint_family(CheckpointFamily::AppendStream)
    .runtime_shape(RuntimeShape::OnDemand)
    .package_impact("hledger_journal_source_unit")
    .implementation_mode("rust_in_pack:source-worker")
    .build_impact(SourceUnitBuildImpact::ZERO)
    .build()
}

crate::register_adapter_ingestor!(
    source_unit_id: "hledger-journal",
    adapter: StaticFileAdapter,
    parser: HledgerJournalParser,
);

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use sinex_primitives::Uuid;
    use sinex_primitives::ids::Id;

    use xtask::sandbox::prelude::sinex_test;

    // ---------------------------------------------------------------------------
    // Fixture data — synthetic journal entries covering representative syntax.
    // ---------------------------------------------------------------------------

    /// Three transactions:
    /// 1. Simple two-posting TEST transaction with Payee|Narration.
    /// 2. Multi-posting transaction (three postings, different categories).
    /// 3. Transaction with explicit comment on header line.
    const SAMPLE_JOURNAL: &str = "2030-01-05 Example Fuel Station|sample fuel\n\
        \tAssets:Checking:ExampleBank\n\
        \tExpenses:Transport:Fuel                              40.00 TEST\n\
        \n\
        2030-02-10 Example Grocery|groceries\n\
        \tAssets:Checking:ExampleBank\n\
        \tExpenses:Household:Supplies                           12.34 TEST\n\
        \tExpenses:Food:Groceries                             23.45 TEST\n\
        \n\
        2030-03-15 Sample Employer|sample salary ; sample comment\n\
        \tAssets:Checking:ExampleBank                            1234.56 TEST\n\
        \tIncome:Salary                                     -1234.56 TEST\n\
        \n";

    /// Single transaction with an ALT posting (multi-currency).
    const MULTI_CURRENCY: &str = "2030-04-20 Example Exchange|sample asset purchase\n\
        \tAssets:Investments:SampleAsset                                     50 ALT\n\
        \tAssets:Checking:ExampleBank                             -100.50 ALT\n\
        \tExpenses:Unknown                                      0.50 ALT\n\
        \n";

    /// Transaction without a Payee|Narration pipe separator.
    const NO_PIPE: &str = "2030-05-25 Example Marketplace purchase\n\
        \tAssets:Checking:ExampleBank\n\
        \tExpenses:Stuff:Electronics                          99.00 TEST\n\
        \n";

    /// File-level comment + two transactions. Tests that the comment line
    /// is skipped and both transactions are parsed.
    const WITH_FILE_COMMENT: &str = "; vim:filetype=ledger\n\
        \n\
        2030-06-01 Opening balance|init\n\
        \tAssets:Cash                                        =   0.00 TEST\n\
        \tEquity:Opening Balances\n\
        \n\
        2030-06-02 Example Grocery|snacks\n\
        \tAssets:Cash\n\
        \tExpenses:Food:Groceries                              7.89 TEST\n\
        \n";

    fn test_ctx() -> ParserContext {
        ParserContext {
            source_unit_id: SourceUnitId::from_static("hledger-journal"),
            source_material_id: Id::new(),
            record_anchor: MaterialAnchor::ByteRange { start: 0, len: 0 },
            operation_id: Uuid::new_v4(),
            job_id: Uuid::new_v4(),
            host: "test-host".into(),
            acquisition_time: Timestamp::now(),
        }
    }

    fn record_for(bytes: &[u8]) -> SourceRecord {
        SourceRecord {
            material_id: Id::new(),
            anchor: MaterialAnchor::ByteRange {
                start: 0,
                len: bytes.len() as u64,
            },
            bytes: bytes.to_vec(),
            logical_path: None,
            source_ts_hint: None,
            metadata: serde_json::Value::Null,
        }
    }

    // ---------------------------------------------------------------------------
    // Basic happy path
    // ---------------------------------------------------------------------------

    #[sinex_test]
    async fn parses_three_transactions() -> TestResult<()> {
        let mut parser = HledgerJournalParser;
        let intents = parser
            .parse_record(record_for(SAMPLE_JOURNAL.as_bytes()), &test_ctx())
            .await
            .unwrap();
        assert_eq!(intents.len(), 3, "expected 3 transaction intents");
        for intent in &intents {
            assert_eq!(intent.event_source.as_str(), "ledger");
            assert_eq!(intent.event_type.as_str(), "transaction.posted");
        }
        Ok(())
    }

    // ---------------------------------------------------------------------------
    // Payee|Narration pipe split
    // ---------------------------------------------------------------------------

    #[sinex_test]
    async fn pipe_split_preserved_in_payload() -> TestResult<()> {
        let mut parser = HledgerJournalParser;
        let intents = parser
            .parse_record(record_for(SAMPLE_JOURNAL.as_bytes()), &test_ctx())
            .await
            .unwrap();
        let first = &intents[0].payload;
        assert_eq!(first["description"], "Example Fuel Station");
        assert_eq!(first["narration"], "sample fuel");
        Ok(())
    }

    // ---------------------------------------------------------------------------
    // No pipe — narration is None
    // ---------------------------------------------------------------------------

    #[sinex_test]
    async fn no_pipe_narration_is_null() -> TestResult<()> {
        let mut parser = HledgerJournalParser;
        let intents = parser
            .parse_record(record_for(NO_PIPE.as_bytes()), &test_ctx())
            .await
            .unwrap();
        assert_eq!(intents.len(), 1);
        assert_eq!(intents[0].payload["description"], "Example Marketplace purchase");
        assert!(intents[0].payload["narration"].is_null());
        Ok(())
    }

    // ---------------------------------------------------------------------------
    // Implicit posting amount → None
    // ---------------------------------------------------------------------------

    #[sinex_test]
    async fn implicit_posting_amount_is_none() -> TestResult<()> {
        let mut parser = HledgerJournalParser;
        let intents = parser
            .parse_record(record_for(SAMPLE_JOURNAL.as_bytes()), &test_ctx())
            .await
            .unwrap();
        // First transaction's first posting (ExampleBank account) has no amount.
        let postings = intents[0].payload["postings"].as_array().unwrap();
        let example_bank = &postings[0];
        assert_eq!(example_bank["account"], "Assets:Checking:ExampleBank");
        assert!(example_bank["amount"].is_null());
        assert!(example_bank["currency"].is_null());
        Ok(())
    }

    // ---------------------------------------------------------------------------
    // Explicit amount + currency preserved
    // ---------------------------------------------------------------------------

    #[sinex_test]
    async fn explicit_posting_amount_preserved() -> TestResult<()> {
        let mut parser = HledgerJournalParser;
        let intents = parser
            .parse_record(record_for(SAMPLE_JOURNAL.as_bytes()), &test_ctx())
            .await
            .unwrap();
        let postings = intents[0].payload["postings"].as_array().unwrap();
        let fuel = &postings[1];
        assert_eq!(fuel["account"], "Expenses:Transport:Fuel");
        assert_eq!(fuel["amount"], "40.00");
        assert_eq!(fuel["currency"], "TEST");
        Ok(())
    }

    // ---------------------------------------------------------------------------
    // Multi-posting transaction
    // ---------------------------------------------------------------------------

    #[sinex_test]
    async fn multi_posting_transaction() -> TestResult<()> {
        let mut parser = HledgerJournalParser;
        let intents = parser
            .parse_record(record_for(SAMPLE_JOURNAL.as_bytes()), &test_ctx())
            .await
            .unwrap();
        // Second transaction has 3 postings.
        let postings = intents[1].payload["postings"].as_array().unwrap();
        assert_eq!(postings.len(), 3);
        assert_eq!(postings[1]["currency"], "TEST");
        assert_eq!(postings[2]["currency"], "TEST");
        Ok(())
    }

    // ---------------------------------------------------------------------------
    // Multi-currency
    // ---------------------------------------------------------------------------

    #[sinex_test]
    async fn multi_currency_alt_posting() -> TestResult<()> {
        let mut parser = HledgerJournalParser;
        let intents = parser
            .parse_record(record_for(MULTI_CURRENCY.as_bytes()), &test_ctx())
            .await
            .unwrap();
        assert_eq!(intents.len(), 1);
        let postings = intents[0].payload["postings"].as_array().unwrap();
        assert!(postings.iter().all(|p| p["currency"] == "ALT"));
        Ok(())
    }

    // ---------------------------------------------------------------------------
    // Inline header comment
    // ---------------------------------------------------------------------------

    #[sinex_test]
    async fn header_comment_preserved() -> TestResult<()> {
        let mut parser = HledgerJournalParser;
        let intents = parser
            .parse_record(record_for(SAMPLE_JOURNAL.as_bytes()), &test_ctx())
            .await
            .unwrap();
        // Third transaction has "; sample comment" comment.
        assert_eq!(intents[2].payload["comment"], "sample comment");
        Ok(())
    }

    // ---------------------------------------------------------------------------
    // File-level comment skipped; transaction count correct
    // ---------------------------------------------------------------------------

    #[sinex_test]
    async fn file_comment_skipped_two_transactions() -> TestResult<()> {
        let mut parser = HledgerJournalParser;
        let intents = parser
            .parse_record(record_for(WITH_FILE_COMMENT.as_bytes()), &test_ctx())
            .await
            .unwrap();
        assert_eq!(intents.len(), 2);
        Ok(())
    }

    // ---------------------------------------------------------------------------
    // Anchor is transaction index
    // ---------------------------------------------------------------------------

    #[sinex_test]
    async fn anchor_uses_transaction_index() -> TestResult<()> {
        let mut parser = HledgerJournalParser;
        let intents = parser
            .parse_record(record_for(SAMPLE_JOURNAL.as_bytes()), &test_ctx())
            .await
            .unwrap();
        assert!(matches!(
            intents[0].anchor,
            MaterialAnchor::ByteRange { start: 0, len: 1 }
        ));
        assert!(matches!(
            intents[1].anchor,
            MaterialAnchor::ByteRange { start: 1, len: 1 }
        ));
        assert!(matches!(
            intents[2].anchor,
            MaterialAnchor::ByteRange { start: 2, len: 1 }
        ));
        Ok(())
    }

    // ---------------------------------------------------------------------------
    // Occurrence key
    // ---------------------------------------------------------------------------

    #[sinex_test]
    async fn occurrence_key_fields_and_order() -> TestResult<()> {
        let mut parser = HledgerJournalParser;
        let intents = parser
            .parse_record(record_for(SAMPLE_JOURNAL.as_bytes()), &test_ctx())
            .await
            .unwrap();
        let key = intents[0].occurrence_key.as_ref().unwrap();
        assert_eq!(key.fields[0].0, "date");
        assert_eq!(key.fields[1].0, "description");
        assert_eq!(key.fields[2].0, "first_amount");
        // The first explicit amount for the first transaction is "40.00" (Fuel posting).
        assert_eq!(key.fields[2].1, "40.00");
        Ok(())
    }

    // ---------------------------------------------------------------------------
    // Invalid date surfaces a parse error
    // ---------------------------------------------------------------------------

    #[sinex_test]
    async fn invalid_date_errors() -> TestResult<()> {
        let bad = "2017-99-99 Bad date\n\
            \tAssets:Checking\n\
            \tExpenses:Unknown                                  1.00 TEST\n\
            \n";
        let mut parser = HledgerJournalParser;
        let err = parser
            .parse_record(record_for(bad.as_bytes()), &test_ctx())
            .await
            .unwrap_err()
            .to_string();
        assert!(err.contains("invalid journal date"), "got: {err}");
        Ok(())
    }

    // ---------------------------------------------------------------------------
    // Timestamp year/month/day
    // ---------------------------------------------------------------------------

    #[sinex_test]
    async fn timestamp_matches_journal_date() -> TestResult<()> {
        let mut parser = HledgerJournalParser;
        let intents = parser
            .parse_record(record_for(SAMPLE_JOURNAL.as_bytes()), &test_ctx())
            .await
            .unwrap();
        let ts = intents[0].ts_orig.inner();
        assert_eq!(ts.year(), 2030);
        assert_eq!(ts.month() as u8, 1);
        assert_eq!(ts.day(), 5);
        Ok(())
    }
}
