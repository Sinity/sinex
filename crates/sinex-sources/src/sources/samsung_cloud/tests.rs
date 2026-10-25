use super::*;
use serde_json::json;

fn run(path: &str, rows: &[&[u8]]) -> Vec<SamsungEvent> {
    let manifest = json!({"path": path});
    let context = RecordContext {
        manifest: &manifest,
        member: None,
    };
    let mut carry = Carry::default();
    let mut out = Vec::new();
    for row in rows {
        let (events, next) = SamsungCloudParser.parse(row, &carry, &context).unwrap();
        out.extend(events);
        carry = next;
    }
    out
}

#[devkit::test]
fn health_tables_become_health_records() {
    let out = run(
        "/x/samsungcloud_gk000066110879_20260329_access/Step Count/Step Count(3).csv",
        &[
            "\u{feff}create_time,time_offset,end_time,count,start_time,datauuid\n".as_bytes(),
            b"2026-03-12 17:23,3600000,2026-03-12 17:24,21,2026-03-12 17:23,e03a\n",
        ],
    );
    let SamsungEvent::Health(h) = &out[0] else {
        panic!()
    };
    assert_eq!((h.category(), h.native_id.as_str()), ("Step Count", "e03a"));
    assert_eq!(h.offset_minutes, Some(60));
    assert_eq!(
        serde_json::to_value(h.values()).unwrap()["count"],
        json!(21)
    );
    assert_eq!(h.start, fields::rfc3339("2026-03-12T17:23:00Z").ok());
}

#[devkit::test]
fn other_tables_keep_every_column_with_json_decoded() {
    let out = run(
        "/x/samsungcloud_gk000066110887_20260329_access/Alarm/Alarm_1.csv",
        &[
            b"ITEM_DATA,TIMESTAMP,VALUE,KEY\n",
            br#""",1682494230881,"""{\""_id\"":\""1\"",\""active\"":\""0\""}""",abc"#,
        ],
    );
    let SamsungEvent::Account(a) = &out[0] else {
        panic!()
    };
    assert_eq!(
        (a.service.as_str(), a.table.as_str(), a.native_id.as_str()),
        ("samsungcloud", "Alarm", "abc")
    );
    assert_eq!(a.values["VALUE"]["active"], json!("0"));
    assert_eq!(a.at, fields::from_unix_ms(1_682_494_230_881).ok());
    assert!(!is_table(Path::new(
        "/x/samsungcloud_gk1/Samsung Pass/Samsung Pass.csv"
    )));
    assert!(is_table(Path::new("/x/SmartThings_gk1/a.csv")));
}

#[devkit::test]
fn health_unknown_columns_and_categories_remain_captured() {
    for category in ["Step Count", "New Health Category"] {
        let path = format!("/x/samsungcloud_gk1/{category}/{category}.csv");
        let out = run(
            &path,
            &[
                b"datauuid,count,new_column\n",
                b"synthetic-id,0042,{\"raw\":true}\n",
            ],
        );
        let SamsungEvent::Health(record) = &out[0] else {
            panic!("not health")
        };
        let wire = serde_json::to_value(record).unwrap();
        assert_eq!(wire["values"]["count"], json!("0042"));
        assert_eq!(wire["values"]["new_column"], json!({"raw":true}));
        if category == "Step Count" {
            assert_eq!(wire["uninterpreted_fields"], json!(["/values/new_column"]));
        } else {
            assert_eq!(wire["uninterpreted"], true);
        }
    }
}

#[devkit::test]
fn malformed_known_health_fields_are_record_failures() {
    let manifest = json!({"path":"/x/samsungcloud_gk1/Step Count/Step Count.csv"});
    let context = RecordContext {
        manifest: &manifest,
        member: None,
    };
    let (_, carry) = SamsungCloudParser
        .parse(&&b"datauuid,count\n"[..], &Carry::default(), &context)
        .unwrap();
    assert!(
        SamsungCloudParser
            .parse(&&b"synthetic-id,[]\n"[..], &carry, &context)
            .is_err()
    );
}

#[devkit::test]
fn nul_cleanup_and_numeric_times_keep_the_previous_normalization() {
    let out = run(
        "/x/samsungcloud_gk1/Step Count/Step Count.csv",
        &[
            b"datauuid,start_time,time_offset,count\n",
            b"synthetic-id,123,3600000,2\0\n",
        ],
    );
    let SamsungEvent::Health(record) = &out[0] else {
        panic!("not health")
    };
    assert!(record.start.is_none());
    assert_eq!(record.offset_minutes, Some(60));
    let wire = serde_json::to_value(record).unwrap();
    assert_eq!(
        wire["values"],
        json!({"datauuid":"synthetic-id","start_time":123,"time_offset":3600000,"count":2,"count__nul_removed":true})
    );
}

#[devkit::test]
fn generic_account_tables_report_uninterpreted_without_changing_values_or_keys() {
    let rows = run(
        "/x/samsungcloud_gk1/Synthetic Table/Synthetic Table.csv",
        &[
            b"KEY,TIMESTAMP,VALUE,padded,number,empty\n",
            b"synthetic-id,1767225600000,[],0042,9007199254740993,\n",
        ],
    );
    let SamsungEvent::Account(account) = &rows[0] else {
        panic!("not account")
    };
    let output = serde_json::to_value(account).unwrap();
    assert_eq!(account.native_id, "synthetic-id");
    assert_eq!(account.at, fields::rfc3339("2026-01-01T00:00:00Z").ok());
    assert_eq!(output["uninterpreted"], true);
    assert_eq!(
        output["values"],
        json!({"KEY":"synthetic-id","TIMESTAMP":1767225600000_i64,"VALUE":[],"padded":"0042","number":9007199254740993_i64})
    );
}

#[devkit::test]
fn unkeyed_account_rows_keep_the_original_row_hash_identity() {
    let data = b"synthetic,[]\n";
    let rows = run(
        "/x/SmartThings_gk1/Synthetic Table/Synthetic Table.csv",
        &[b"label,VALUE\n", data],
    );
    let SamsungEvent::Account(account) = &rows[0] else {
        panic!("not account")
    };
    assert_eq!(
        account.native_id,
        format!("blake3:{}", blake3::hash(data).to_hex())
    );
    assert!(account.at.is_none());
    assert_eq!(
        serde_json::to_value(account).unwrap()["uninterpreted"],
        true
    );
}
