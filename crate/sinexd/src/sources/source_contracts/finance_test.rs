use super::*;
use sinex_primitives::Uuid;
use sinex_primitives::ids::Id;

use xtask::sandbox::prelude::sinex_test;

// ---------------------------------------------------------------------------
// Fixture data — representative subset of real journal entries.
// ---------------------------------------------------------------------------

/// Three transactions:
/// 1. Simple two-posting PLN transaction with Payee|Narration.
/// 2. Multi-posting transaction (three postings, different categories).
/// 3. Transaction with explicit comment on header line.
const SAMPLE_JOURNAL: &str = "2017-08-05 BP Buczkowice|LPG\n\
    \tAssets:Checking:Revolut\n\
    \tExpenses:Transport:Fuel                              52.97 PLN\n\
    \n\
    2017-10-20 Zabka|liquid and food\n\
    \tAssets:Checking:Revolut\n\
    \tExpenses:Consumable:Vaping                           10.99 PLN\n\
    \tExpenses:Consumable:Food                             10.98 PLN\n\
    \n\
    2021-01-07 JBR|Wynagrodzenie za 12/2020 ; paycheck\n\
    \tAssets:Checking:Revolut                            3273.12 PLN\n\
    \tIncome:Salary                                     -3273.12 PLN\n\
    \n";

/// Single transaction with a USD posting (multi-currency).
const MULTI_CURRENCY: &str = "2020-05-01 Kraken|ETH purchase\n\
    \tAssets:Crypto:ETH                                     50 USD\n\
    \tAssets:Checking:Revolut                             -50.68 USD\n\
    \tExpenses:Unknown                                      0.68 USD\n\
    \n";

/// Transaction without a Payee|Narration pipe separator.
const NO_PIPE: &str = "2019-03-15 Allegro purchase\n\
    \tAssets:Checking:Revolut\n\
    \tExpenses:Stuff:Electronics                          299.00 PLN\n\
    \n";

/// File-level comment + two transactions. Tests that the comment line
/// is skipped and both transactions are parsed.
const WITH_FILE_COMMENT: &str = "; vim:filetype=ledger\n\
    \n\
    2018-01-01 Opening balance|init\n\
    \tAssets:Cash                                        =   0.00 PLN\n\
    \tEquity:Opening Balances\n\
    \n\
    2018-01-02 Zabka|snacks\n\
    \tAssets:Cash\n\
    \tExpenses:Consumable:Food                              8.50 PLN\n\
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
    assert_eq!(first["description"], "BP Buczkowice");
    assert_eq!(first["narration"], "LPG");
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
    assert_eq!(intents[0].payload["description"], "Allegro purchase");
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
    // First transaction's first posting (Revolut account) has no amount.
    let postings = intents[0].payload["postings"].as_array().unwrap();
    let revolut = &postings[0];
    assert_eq!(revolut["account"], "Assets:Checking:Revolut");
    assert!(revolut["amount"].is_null());
    assert!(revolut["currency"].is_null());
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
    assert_eq!(fuel["amount"], "52.97");
    assert_eq!(fuel["currency"], "PLN");
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
    assert_eq!(postings[1]["currency"], "PLN");
    assert_eq!(postings[2]["currency"], "PLN");
    Ok(())
}

// ---------------------------------------------------------------------------
// Multi-currency
// ---------------------------------------------------------------------------

#[sinex_test]
async fn multi_currency_usd_posting() -> TestResult<()> {
    let mut parser = HledgerJournalParser;
    let intents = parser
        .parse_record(record_for(MULTI_CURRENCY.as_bytes()), &test_ctx())
        .await
        .unwrap();
    assert_eq!(intents.len(), 1);
    let postings = intents[0].payload["postings"].as_array().unwrap();
    assert!(postings.iter().all(|p| p["currency"] == "USD"));
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
    // Third transaction has "; paycheck" comment.
    assert_eq!(intents[2].payload["comment"], "paycheck");
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
    // The first explicit amount for the first transaction is "52.97" (Fuel posting).
    assert_eq!(key.fields[2].1, "52.97");
    Ok(())
}

// ---------------------------------------------------------------------------
// Invalid date surfaces a parse error
// ---------------------------------------------------------------------------

#[sinex_test]
async fn invalid_date_errors() -> TestResult<()> {
    let bad = "2017-99-99 Bad date\n\
        \tAssets:Checking\n\
        \tExpenses:Unknown                                  1.00 PLN\n\
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
    assert_eq!(ts.year(), 2017);
    assert_eq!(ts.month() as u8, 8);
    assert_eq!(ts.day(), 5);
    Ok(())
}
