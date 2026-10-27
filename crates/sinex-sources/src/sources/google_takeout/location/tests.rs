use super::*;
use serde_json::json;

#[devkit::test]
fn old_and_new_fix_shapes() {
    let (out, ()) = LocationParser
        .parse(
            json!({"timestampMs": "1398348676787", "latitudeE7": 497031663,
                "longitudeE7": 190826466, "accuracy": 3097})
            .as_object()
            .unwrap(),
            &(),
            &sinex_source_core::RecordContext::none(),
        )
        .unwrap();
    assert!((out[0].latitude - 49.7031663).abs() < 1e-9);
    assert_eq!(out[0].at, fields::from_unix_ms(1_398_348_676_787).unwrap());
    assert_eq!(
        (out[0].device.as_ref(), out[0].accuracy_m),
        (None, Some(3097.0))
    );
    let (out, ()) = LocationParser
        .parse(
            json!({"timestamp": "2022-01-02T03:04:05.678Z", "latitudeE7": 1,
                "longitudeE7": 2, "deviceTag": 12345, "source": "WIFI"})
            .as_object()
            .unwrap(),
            &(),
            &sinex_source_core::RecordContext::none(),
        )
        .unwrap();
    assert_eq!(out[0].device.as_deref(), Some("12345"));
    assert_eq!(out[0].extra["source"], json!("WIFI"));
}
