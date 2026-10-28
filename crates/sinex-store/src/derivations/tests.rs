use super::*;

#[devkit::test]
fn ten_thousand_field_values_share_one_paged_read() {
    let windows = (0..10_000)
        .map(|index| Window {
            kinds: vec![Kind {
                event_type: "synthetic.reading".into(),
                contains: serde_json::json!({"item": format!("item-{index}")}),
            }],
            start: OffsetDateTime::UNIX_EPOCH,
            end: OffsetDateTime::UNIX_EPOCH + time::Duration::days(1),
            before: vec![],
            after: vec![],
        })
        .collect();
    let reader = Windows::new(windows, 100, 4096);
    assert_eq!(reader.values.len(), 1);
    assert_eq!(reader.values[0].open.len(), 10_000);
    assert!(reader.groups.is_empty());
    assert!(reader.alone.is_empty());
}
