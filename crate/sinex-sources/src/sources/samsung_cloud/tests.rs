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
        let (events, next) = SamsungCloudParser
            .parse(row, &carry, &context)
            .unwrap();
        out.extend(events);
        carry = next;
    }
    out
}

#[test]
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
    assert_eq!(
        (h.category.as_str(), h.native_id.as_str()),
        ("Step Count", "e03a")
    );
    assert_eq!(h.offset_minutes, Some(60));
    assert_eq!(h.values["count"], json!(21));
    assert_eq!(h.start, fields::rfc3339("2026-03-12T17:23:00Z").ok());
}

#[test]
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
