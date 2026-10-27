use super::*;
use serde_json::json;

fn one(v: Value) -> PurchaseObserved {
    PurchasesParser
        .parse(
            v.as_object().unwrap(),
            &(),
            &sinex_source_core::RecordContext::none(),
        )
        .unwrap()
        .0
        .remove(0)
}

#[devkit::test]
fn every_wrapper_reads_its_own_time() {
    let i = one(
        json!({"install": {"doc": {"documentType": "Android Apps", "title": "Facebook"},
        "firstInstallationTime": "2014-08-03T13:48:39.862Z", "deviceAttribute": {"deviceDisplayName": "Phone"}}}),
    );
    assert_eq!(i.record_kind, "install");
    assert!(i.at.is_some());
    assert_eq!(i.native_id, "Android Apps:Facebook@Phone");
    let o = one(
        json!({"orderHistory": {"orderId": "GPA.1", "creationTime": "2019-02-11T07:30:57.084Z"}}),
    );
    assert_eq!(o.native_id, "GPA.1");
    let l = one(
        json!({"libraryDoc": {"doc": {"documentType": "Book", "title": "S"}, "acquisitionTime": "2013-08-21T15:36:24.789Z"}}),
    );
    assert!(l.at.is_some());
    let s = one(json!({"userSetting": {"userCountryCode": ""}}));
    assert_eq!(s.at, None);
    assert!(s.native_id.starts_with("blake3:"));
}

#[devkit::test]
fn order_files() {
    let o = one(
        json!({"merchantOrderId": "1049", "creationTime": {"usecSinceEpochUtc": "1565375213000000",
        "granularity": "MICROSECOND"}, "transactionMerchant": {"name": "Shop"}, "lineItem": []}),
    );
    assert_eq!(o.record_kind, "order");
    assert_eq!(o.native_id, "Shop:1049");
    assert_eq!(
        o.at,
        Some(fields::from_unix_us(1_565_375_213_000_000).unwrap())
    );
}
