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
        source_id: SourceId::from_static("hledger-journal"),
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
