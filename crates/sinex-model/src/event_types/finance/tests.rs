use super::*;
use crate::{
    Class, Event, EventPayload, MaterialId, Origin, ReadingOrder, RecordLocation, TimeQuality,
};

#[devkit::test]
fn transaction_key_keeps_duplicate_and_host_and_time_unknown() {
    let value = serde_json::json!({"host":"example-host", "journal":"example.journal", "member":null, "member_duplicate":0, "block_hash":"example-hash", "duplicate":0, "date":"2030-01-01", "effective_date":null, "status":null, "description":"Example", "narration":null, "comment":null, "postings":[{"account":"Assets:Example", "amount":"12345678901234567890.001", "currency":"TEST", "comment":null}]});
    let payload: TransactionPosted = serde_json::from_value(value.clone()).unwrap();
    assert_eq!(serde_json::to_value(&payload).unwrap(), value);
    let build = |p: &TransactionPosted| {
        Event::build(
            p,
            "finance.hledger",
            Origin::Record {
                location: RecordLocation {
                    material: MaterialId::new(),
                    anchor: 0,
                },
                output_index: 0,
            },
            ReadingOrder(1),
            [1; 32],
            Class::Guarded,
        )
        .unwrap()
    };
    let a = build(&payload);
    assert_eq!(a.ts_orig.quality, TimeQuality::Unknown);
    assert_eq!(a.ts_orig.at, None);
    assert_eq!(a.equivalence_key, build(&payload).equivalence_key);
    let mut changed = payload.clone();
    changed.duplicate = 1;
    assert_ne!(a.equivalence_key, build(&changed).equivalence_key);
    changed = payload;
    changed.host = "second-host".into();
    assert_ne!(a.equivalence_key, build(&changed).equivalence_key);
    assert!(TransactionPosted::payload_schema().is_object());
}
