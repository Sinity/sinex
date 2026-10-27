use super::*;
use sinex_model::{EventPayload, Member};

const TRANSACTION: &[u8] = b"2030-01-05=2030-01-06 * Example|sample ; header\n  Assets:Example\n  Expenses:Example  -40.00000000000000000001 TEST ; posting\n\n";

fn parser() -> HledgerParser {
    HledgerParser {
        host: "example-host".into(),
        journal: "example.journal".into(),
    }
}

#[devkit::test]
fn civil_dates_status_comments_implicit_amounts_and_precision_survive() {
    let (events, _) = parser()
        .parse(&TRANSACTION, &Carry::default(), &RecordContext::none())
        .unwrap();
    let event = &events[0];
    assert_eq!(event.date, "2030-01-05");
    assert_eq!(event.effective_date.as_deref(), Some("2030-01-06"));
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
fn equal_blocks_are_numbered_and_later_postings_change_identity() {
    let (first, carry) = parser()
        .parse(&TRANSACTION, &Carry::default(), &RecordContext::none())
        .unwrap();
    let saved = serde_json::to_vec(&carry).unwrap();
    let (second, _) = parser()
        .parse(
            &TRANSACTION,
            &serde_json::from_slice(&saved).unwrap(),
            &RecordContext::none(),
        )
        .unwrap();
    assert_eq!(first[0].duplicate, 0);
    assert_eq!(second[0].duplicate, 1);
    let changed = String::from_utf8(TRANSACTION.to_vec())
        .unwrap()
        .replace("Expenses:Example", "Expenses:Other");
    let (third, _) = parser()
        .parse(&changed.as_bytes(), &carry, &RecordContext::none())
        .unwrap();
    assert_ne!(first[0].block_hash, third[0].block_hash);
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
    assert_eq!(fourth[0].duplicate, 0);
    assert_eq!(fourth[0].member.as_deref(), Some("second.journal"));
}

#[devkit::test]
fn three_transactions_and_commodity_positions() {
    let input = b"; example\n\n2030-01-01 First\n  Assets:Example\n  Expenses:Example  TEST 10.50\n\n2030-01-02 ! Second|two\n  Assets:Example  -2 TEST\n  Expenses:Example  1 TEST\n  Expenses:Other  1 TEST\n\n2030-01-03 Third\n  Assets:Example  0\n  Expenses:Example";
    let mut carry = Carry::default();
    let mut all = vec![];
    for (start, end) in framing::complete(input).0 {
        let (events, next) = parser()
            .parse(&&input[start..end], &carry, &RecordContext::none())
            .unwrap();
        carry = next;
        all.extend(events);
    }
    assert_eq!(all.len(), 3);
    assert_eq!(all[0].postings[1].amount.as_deref(), Some("10.50"));
    assert_eq!(all[0].postings[1].currency.as_deref(), Some("TEST"));
    assert_eq!(all[0].narration, None);
    assert_eq!(all[1].status.as_deref(), Some("!"));
    assert_eq!(all[1].postings.len(), 3);
    assert_eq!(all[2].postings[0].currency, None);
}

#[devkit::test]
fn unsupported_syntax_and_invalid_dates_fail_visibly() {
    for input in [
        "include other.journal\n",
        "account Assets:Example\n",
        "~ monthly\n  Assets:Example\n",
        "= expr\n  Assets:Example\n",
        "2030-02-30 Bad\n  Assets:Example\n",
        "2030-01-01=bad Bad\n  Assets:Example\n",
        "2030-01-01 Empty\n",
        "2030-01-01 Assertion\n  Assets:Example  = 1 TEST\n",
        "2030-01-01 Price\n  Assets:Example  1 TEST @ 2 TEST\n",
        "2030-01-01 Number\n  Assets:Example  1.2.3 TEST\n",
        "2030-01-01 Number\n  Assets:Example  1garbage TEST\n",
        "2030-01-01 Virtual\n  (Assets:Example)\n",
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
    let mut settings = binding::Settings::new("example-host");
    settings.options = serde_json::json!({"resolve_includes":true});
    assert!((FORMAT.plan.unwrap())(std::path::Path::new("example.journal"), &settings).is_err());
}

#[devkit::test]
fn streaming_export_ranges_equal_slice_even_for_oversized_blocks() {
    let input = format!(
        "2030-01-01 Café\n  Assets:Example\n  Expenses:Example  1 TEST ; {}\n\ninclude other.journal\n2030-01-02 Last\n  Assets:Example",
        "x".repeat(200_000)
    );
    let actual = SPEC
        .framer
        .ranges(&mut std::io::Cursor::new(input.as_bytes()))
        .unwrap();
    assert_eq!(actual, framing::complete(input.as_bytes()).0);
}

#[path = "intake_tests.rs"]
mod intake;

#[devkit::test]
fn shared_intake_restart_inside_a_part_and_replay_keep_duplicate_transactions() {
    let input = [TRANSACTION, TRANSACTION, b"include other.journal\n"].concat();
    let events = intake::export_restart(
        &input,
        driver("example.journal".into(), "example-host".into()),
        &source("example-host".into(), "example.journal".into()),
        2,
        1,
    );
    assert_eq!(events[0].payload["duplicate"], 0);
    assert_eq!(events[1].payload["duplicate"], 1);
    assert_ne!(events[0].equivalence_key, events[1].equivalence_key);
}

#[devkit::test]
fn archived_members_reset_duplicate_counts_and_small_parts_keep_whole_transactions() {
    let mut builder = tar::Builder::new(Vec::new());
    for name in ["same.journal", "same.journal"] {
        let bytes = [TRANSACTION, TRANSACTION].concat();
        let mut header = tar::Header::new_gnu();
        header.set_size(bytes.len() as u64);
        header.set_mode(0o600);
        header.set_cksum();
        builder
            .append_data(&mut header, name, bytes.as_slice())
            .unwrap();
    }
    let input = builder.into_inner().unwrap();
    let mut driver = driver("example.tar".into(), "example-host".into());
    driver.part_bytes = 8;
    let events = intake::export_restart(
        &input,
        driver,
        &source("example-host".into(), "example.tar".into()),
        4,
        0,
    );
    assert_eq!(
        events
            .iter()
            .map(|e| e.payload["duplicate"].as_u64().unwrap())
            .collect::<Vec<_>>(),
        [0, 1, 0, 1]
    );
    assert_ne!(events[0].equivalence_key, events[2].equivalence_key);
}

#[devkit::test]
fn indented_comments_keep_their_posting_and_attached_commodities_keep_precision() {
    let input = b"2030-01-01 Example\n  ; header detail\n  Assets:Example  TEST-12.00 ; first\n  ; second\n  Expenses:Example  12.00TEST";
    let payload = parser()
        .parse(&input.as_slice(), &Carry::default(), &RecordContext::none())
        .unwrap()
        .0
        .remove(0);
    assert_eq!(payload.comment.as_deref(), Some("header detail"));
    assert_eq!(payload.postings[0].amount.as_deref(), Some("-12.00"));
    assert_eq!(
        payload.postings[0].comment.as_deref(),
        Some("first\nsecond")
    );
    assert_eq!(payload.postings[1].currency.as_deref(), Some("TEST"));
}

#[devkit::test]
fn decimal_quantities_keep_written_fraction_and_signs() {
    for amount in [".5", "-.50", "+.00", "1.", "001.00"] {
        let text = format!("Assets:Example  {amount} TEST");
        let value = posting(&text).unwrap();
        assert_eq!(value.amount.as_deref(), Some(amount));
        assert_eq!(value.currency.as_deref(), Some("TEST"));
    }
}

#[devkit::test]
fn indented_prefix_comments_emit_no_transactions() {
    let input = b"  ; example\n\t# another comment\n";
    let (events, carry) = parser()
        .parse(&input.as_slice(), &Carry::default(), &RecordContext::none())
        .unwrap();
    assert!(events.is_empty());
    assert!(carry.seen.is_empty());
}

#[devkit::test]
fn reordered_distinct_archive_members_keep_transaction_keys() {
    fn archive(names: &[&str]) -> Vec<u8> {
        let mut builder = tar::Builder::new(Vec::new());
        for name in names {
            let mut header = tar::Header::new_gnu();
            header.set_size(TRANSACTION.len() as u64);
            header.set_mode(0o600);
            header.set_cksum();
            builder
                .append_data(&mut header, *name, TRANSACTION)
                .unwrap();
        }
        builder.into_inner().unwrap()
    }
    let parse = |bytes: Vec<u8>| {
        let events = intake::export_restart(
            &bytes,
            driver("example.tar".into(), "example-host".into()),
            &source("example-host".into(), "example.tar".into()),
            2,
            0,
        );
        let mut keys = events
            .iter()
            .map(|event| event.equivalence_key.clone())
            .collect::<Vec<_>>();
        keys.sort();
        keys
    };
    assert_eq!(
        parse(archive(&["first.journal", "second.journal"])),
        parse(archive(&["second.journal", "first.journal"]))
    );
}
