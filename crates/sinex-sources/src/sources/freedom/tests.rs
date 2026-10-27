use super::*;
use sinex_model::EventPayload;

#[devkit::test]
fn daily_domain_records_keep_zero_and_do_not_invent_an_instant() {
    let bytes = br#"{"stat_2025-02-01":{"example.org":0,"other.example":123},"stat_2025-02-02":{"example.org":7}}"#;
    let (records, consumed) = frame(bytes, true);
    assert_eq!(consumed, bytes.len());
    assert_eq!(records.len(), 2);
    let events: Vec<DomainUsage> = records
        .into_iter()
        .flat_map(|(start, end)| {
            FreedomParser
                .parse(&&bytes[start..end], &(), &RecordContext::none())
                .unwrap()
                .0
        })
        .collect();
    assert_eq!(events.len(), 3);
    assert_eq!(events[0].day, "2025-02-01");
    assert_eq!(events[0].domain, "example.org");
    assert_eq!(events[0].seconds, 0);
    assert_eq!(events[1].seconds, 123);
    assert_eq!(events[2].day, "2025-02-02");
    assert_eq!(events[2].ts_orig(), sinex_model::TsOrig::unknown());
}

#[devkit::test]
fn malformed_days_and_values_are_record_failures() {
    for record in [
        r#""stat_2025-02-30":{"example.org":1}"#,
        r#""2025-02-01":{"example.org":1}"#,
        r#""stat_2025-02-01":{"example.org":-1}"#,
        r#""stat_2025-02-01":{"example.org":null}"#,
        r#""stat_2025-02-01":{"example.org":"7"}"#,
    ] {
        assert!(
            FreedomParser
                .parse(&record.as_bytes(), &(), &RecordContext::none())
                .is_err(),
            "{record}"
        );
    }
}

#[devkit::test]
fn a_partial_day_waits_for_its_complete_record() {
    let bytes = br#"{"stat_2025-02-01":{"example.org":2},"stat_2025-02-02":{"example.org":7}}"#;
    let first_end = bytes.iter().position(|byte| *byte == b'}').unwrap() + 1;
    let (first, consumed) = frame(&bytes[..first_end + 10], false);
    assert_eq!(first.len(), 1);
    let (rest, _) = frame(&bytes[consumed..], true);
    assert_eq!(rest.len(), 1);
    let record = &bytes[consumed + rest[0].0..consumed + rest[0].1];
    assert_eq!(
        FreedomParser
            .parse(&record, &(), &RecordContext::none())
            .unwrap()
            .0[0]
            .day,
        "2025-02-02"
    );
}
