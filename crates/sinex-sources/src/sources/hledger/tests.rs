use super::*;
use sinex_model::{EventPayload, Member};
pub(crate) mod intake;

const TRANSACTION: &[u8] = b"2030-01-05=2030-01-06 * Example|sample ; header\n  Assets:Example\n  Expenses:Example  -40.00000000000000000001 TEST ; posting\n\n";

fn parser() -> HledgerParser {
    source("example-host".into(), "example.journal".into()).parser
}

fn transaction(input: &[u8]) -> TransactionPosted {
    let (mut records, _) = parser()
        .parse(&input, &Carry::default(), &RecordContext::none())
        .unwrap();
    let JournalRecord::Transaction(record) = records.remove(0) else {
        panic!("transaction")
    };
    record
}

#[devkit::test]
fn civil_dates_status_comments_implicit_amounts_and_precision_survive() {
    let event = transaction(TRANSACTION);
    assert_eq!(event.date, "2030-01-05");
    assert_eq!(event.effective_date.as_deref(), Some("2030-01-06"));
    assert_eq!(event.resolved_date.as_deref(), Some("2030-01-05"));
    assert_eq!(event.status.as_deref(), Some("*"));
    assert_eq!(event.description, "Example");
    assert_eq!(event.narration.as_deref(), Some("sample"));
    assert_eq!(event.comment.as_deref(), Some("header"));
    assert_eq!(event.postings[0].amount, None);
    assert_eq!(event.postings[0].currency, None);
    assert_eq!(
        event.postings[1].amount.as_deref(),
        Some("-40.00000000000000000001")
    );
    assert_eq!(event.postings[1].comment.as_deref(), Some("posting"));
    assert_eq!(event.ts_orig().at, None);
}

#[devkit::test]
fn equal_blocks_are_numbered_across_other_records_and_restart() {
    let (_, carry) = parser()
        .parse(&TRANSACTION, &Carry::default(), &RecordContext::none())
        .unwrap();
    let changed = String::from_utf8(TRANSACTION.to_vec())
        .unwrap()
        .replace("Expenses:Example", "Expenses:Other");
    let (third, carry) = parser()
        .parse(&changed.as_bytes(), &carry, &RecordContext::none())
        .unwrap();
    let saved = serde_json::to_vec(&carry).unwrap();
    let (second, _) = parser()
        .parse(
            &TRANSACTION,
            &serde_json::from_slice(&saved).unwrap(),
            &RecordContext::none(),
        )
        .unwrap();
    let JournalRecord::Transaction(second) = &second[0] else {
        panic!()
    };
    assert_eq!(second.duplicate, 1);
    let JournalRecord::Transaction(third) = &third[0] else {
        panic!()
    };
    assert_ne!(second.block_hash, third.block_hash);
    let member = Member {
        path: "second.journal".into(),
        details: serde_json::from_value(serde_json::json!({"member_index":1,"member_duplicate":0}))
            .unwrap(),
        ..Default::default()
    };
    let context = RecordContext {
        manifest: RecordContext::none().manifest,
        member: Some(&member),
    };
    let (fourth, _) = parser().parse(&TRANSACTION, &carry, &context).unwrap();
    let JournalRecord::Transaction(fourth) = &fourth[0] else {
        panic!()
    };
    assert_eq!(fourth.duplicate, 0);
    assert_eq!(fourth.member.as_deref(), Some("second.journal"));
}

#[devkit::test]
fn comments_directives_and_rules_are_records_and_keep_their_bytes() {
    for input in [
        "; example\n",
        "  ; example\n\t# another comment\n",
        "# comment\n",
        "comment\nfree text\n\nend comment\n",
        "account Assets:Example\n  type: A\n",
        "commodity 1,000.00 TEST\n",
        "P 2030/1/2 TEST 2 OTHER\n",
        "D 1.00 TEST\n",
        "alias A = B\n",
        "~ monthly\n  Assets:Example  10 TEST\n",
        "= expr\n  Assets:Example  *0.1\n",
        "include other.journal\n",
        "decimal-mark ,\n",
    ] {
        let records = parser()
            .parse(&input.as_bytes(), &Carry::default(), &RecordContext::none())
            .unwrap()
            .0;
        assert_eq!(records.len(), 1);
        let JournalRecord::Directive(record) = &records[0] else {
            panic!("{input}")
        };
        assert_eq!(record.text, input);
    }
}

#[devkit::test]
fn all_date_separators_partial_dates_years_codes_and_empty_descriptions() {
    for date in ["2030-1-5", "2030/01/05", "2030.1.05", "1/5", "01.05"] {
        let text = format!("{date} ! (sample-1) Payee|memo\n  Assets:Example\n");
        let event = transaction(text.as_bytes());
        assert_eq!(event.date, date);
        assert_eq!(event.code.as_deref(), Some("sample-1"));
        assert_eq!(event.resolved_date.is_some(), date.starts_with("2030"));
    }
    let mut carry = Carry::default();
    for input in ["Y2032\n", "02/29=03/01\n  Assets:Example\n"] {
        let (records, next) = parser()
            .parse(&input.as_bytes(), &carry, &RecordContext::none())
            .unwrap();
        carry = next;
        if let JournalRecord::Transaction(record) = &records[0] {
            assert_eq!(record.resolved_date.as_deref(), Some("2032-02-29"));
            assert_eq!(
                record.resolved_effective_date.as_deref(),
                Some("2032-03-01")
            );
            assert!(record.description.is_empty());
        }
    }
}

#[devkit::test]
fn postings_preserve_status_virtual_accounts_prices_assertions_and_exact_numbers() {
    let event = transaction(b"2030/1/5 Example\n  * (Assets:Example)  1.000,2300 \"TEST 2\" @@ 4.50 OTHER ==* 9,000 TEST\n  ! [Expenses:Example]  = 10 TEST\n  Assets:Example  -$1,234.567890 @ 2 TEST\n");
    let first = &event.postings[0];
    assert_eq!(first.account, "Assets:Example");
    assert_eq!(first.virtual_account.as_deref(), Some("()"));
    assert_eq!(first.status.as_deref(), Some("*"));
    assert_eq!(first.amount.as_deref(), Some("1.000,2300"));
    assert_eq!(first.currency.as_deref(), Some("\"TEST 2\""));
    assert_eq!(first.price.as_ref().unwrap().operator, "@@");
    assert_eq!(first.price.as_ref().unwrap().amount, "4.50");
    assert_eq!(first.assertion.as_ref().unwrap().operator, "==*");
    assert_eq!(event.postings[1].amount, None);
    assert_eq!(event.postings[1].virtual_account.as_deref(), Some("[]"));
    assert_eq!(event.postings[2].amount.as_deref(), Some("-1,234.567890"));
    for amount in [
        ".5",
        "-.50",
        "+.00",
        "1.",
        "001.00",
        "1,000,000",
        "1 000 000.00",
        "1\u{a0}000,00",
        "9,99,99,999.00",
    ] {
        let value = posting::parse(&format!("Assets:Example  {amount} TEST")).unwrap();
        assert_eq!(value.amount.as_deref(), Some(amount));
    }
}

#[devkit::test]
fn malformed_records_fail_and_authored_imbalances_stay_authored() {
    for input in [
        "2030-02-30 Bad\n  Assets:Example\n",
        "2030-01-01=bad Bad\n  Assets:Example\n",
        "2030-01-01 Number\n  Assets:Example  1.2.3 TEST\n",
        "2030-01-01 Number\n  Assets:Example  1garbage TEST\n",
        "unknown-directive value\n",
    ] {
        assert!(
            parser()
                .parse(&input.as_bytes(), &Carry::default(), &RecordContext::none())
                .is_err(),
            "{input}"
        );
    }
    assert!(
        parser()
            .parse(
                &b"\xff".as_slice(),
                &Carry::default(),
                &RecordContext::none()
            )
            .is_err()
    );
    let event =
        transaction(b"2030-01-01 Example\n  Assets:Example  1 TEST\n  Expenses:Example  3 OTHER\n");
    assert_eq!(event.postings.len(), 2);
    assert_eq!(event.postings[1].amount.as_deref(), Some("3"));
}

#[devkit::test]
fn indented_comments_quoted_semicolons_and_attached_commodities() {
    let event = transaction(b"2030-01-01 Example\n  ; header detail\n  Assets:Example  TEST-12.00 ; first\n  ; second\n  Expenses:Example  12.00\"TEST;2\"");
    assert_eq!(event.comment.as_deref(), Some("header detail"));
    assert_eq!(event.postings[0].amount.as_deref(), Some("-12.00"));
    assert_eq!(event.postings[0].comment.as_deref(), Some("first\nsecond"));
    assert_eq!(event.postings[1].currency.as_deref(), Some("\"TEST;2\""));
}

#[devkit::test]
fn streaming_export_ranges_equal_slice_even_for_oversized_blocks() {
    let input = format!(
        "2030-01-01 Café\n  Assets:Example\n  Expenses:Example  1 TEST ; {}\n\ninclude other.journal\n2030-01-02 Last\n  Assets:Example",
        "x".repeat(200_000)
    );
    assert_eq!(
        SPEC.framer
            .ranges(&mut std::io::Cursor::new(input.as_bytes()))
            .unwrap(),
        framing::complete(input.as_bytes()).0
    );
}

#[devkit::test]
fn shared_intake_restart_and_replay_keep_duplicate_transactions_and_directives() {
    let input = [TRANSACTION, TRANSACTION, b"include other.journal\n"].concat();
    let events = intake::export_restart(
        &input,
        driver("example.journal".into(), "example-host".into()),
        &source("example-host".into(), "example.journal".into()),
        3,
        0,
    );
    assert_eq!(events[0].payload["duplicate"], 0);
    assert_eq!(events[1].payload["duplicate"], 1);
    assert_ne!(events[0].equivalence_key, events[1].equivalence_key);
    assert_eq!(events[2].event_type, "journal.directive");
}

fn archive(names: &[&str]) -> Vec<u8> {
    let mut builder = tar::Builder::new(Vec::new());
    for name in names {
        let bytes = [TRANSACTION, TRANSACTION].concat();
        let mut header = tar::Header::new_gnu();
        header.set_size(bytes.len() as u64);
        header.set_mode(0o600);
        header.set_cksum();
        builder
            .append_data(&mut header, *name, bytes.as_slice())
            .unwrap();
    }
    builder.into_inner().unwrap()
}

#[devkit::test]
fn archived_members_reset_counts_and_reordering_preserves_keys() {
    let parse = |names: &[&str]| {
        let mut driver = driver("example.tar".into(), "example-host".into());
        driver.part_bytes = 8;
        intake::export_restart(
            &archive(names),
            driver,
            &source("example-host".into(), "example.tar".into()),
            4,
            0,
        )
    };
    let events = parse(&["same.journal", "same.journal"]);
    assert_eq!(
        events
            .iter()
            .map(|e| e.payload["duplicate"].as_u64().unwrap())
            .collect::<Vec<_>>(),
        [0, 1, 0, 1]
    );
    assert_ne!(events[0].equivalence_key, events[2].equivalence_key);
    let keys = |names: &[&str]| {
        let mut keys = parse(names)
            .iter()
            .map(|e| e.equivalence_key.clone())
            .collect::<Vec<_>>();
        keys.sort();
        keys
    };
    assert_eq!(
        keys(&["first.journal", "second.journal"]),
        keys(&["second.journal", "first.journal"])
    );
}

#[devkit::test]
fn parser_carry_measurement_preserves_nonadjacent_duplicates_without_a_record_limit() {
    let mut carry = Carry::default();
    let mut measurements = vec![];
    for n in 0..1000 {
        let text = format!("2030-01-01 Example {n}\n  Assets:Example\n");
        carry = parser()
            .parse(&text.as_bytes(), &carry, &RecordContext::none())
            .unwrap()
            .1;
        if [0, 9, 99, 999].contains(&n) {
            measurements.push((n + 1, serde_json::to_vec(&carry).unwrap().len()));
        }
    }
    println!("journal parser carry (distinct records, serialized bytes): {measurements:?}");
    assert_eq!(carry.seen.len(), 1000);
    assert!(measurements.windows(2).all(|pair| pair[1].1 > pair[0].1));
}
