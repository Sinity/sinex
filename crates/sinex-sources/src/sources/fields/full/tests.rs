use super::*;
use serde_json::json;

fn read(value: Value) -> Result<Option<Timestamp>, RecordError> {
    let Value::Object(object) = json!({ "ts": value }) else {
        unreachable!()
    };
    FieldReader::new(object).unix_time("ts")
}

#[devkit::test]
fn unix_time_keeps_nanoseconds_from_text() {
    let at = read(json!("1731000000.123456789")).unwrap().unwrap();
    assert_eq!(at.unix_ns(), 1_731_000_000_123_456_789);
    let at = read(json!(" 1731000000 ")).unwrap().unwrap();
    assert_eq!(at.unix_ns(), 1_731_000_000_000_000_000);
    // A JSON number is read from the digits it was written with, not from
    // whole microseconds of a float.
    let at = read(json!(1731000000.123456)).unwrap().unwrap();
    assert_eq!(at.unix_ns(), 1_731_000_000_123_456_000);
    let at = read(json!(1.7e9)).unwrap().unwrap();
    assert_eq!(at.unix_ns(), 1_700_000_000_000_000_000);
    assert_eq!(read(Value::Null).unwrap(), None);
    for refused in [json!("soon"), json!("1e300"), json!(true)] {
        assert!(read(refused.clone()).is_err(), "{refused}");
    }
}
