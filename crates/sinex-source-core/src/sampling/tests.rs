use super::*;

#[test]
fn selection_survives_saved_rule_and_capture_identity() {
    let rule = SamplingRule::new(10, "measured continuous input rate".into()).unwrap();
    let saved: SamplingRule = serde_json::from_value(serde_json::to_value(&rule).unwrap()).unwrap();
    let capture = Uuid::from_u128(1);
    let selected: Vec<_> = (0..10000)
        .filter(|n| rule.selects(capture, *n, true, false).unwrap())
        .collect();
    assert_eq!(
        selected,
        (0..10000)
            .filter(|n| saved.selects(capture, *n, true, false).unwrap())
            .collect::<Vec<_>>()
    );
    assert!((800..1200).contains(&selected.len()));
    for n in 0..10000 {
        assert!(rule.selects(capture, n, false, false).unwrap());
        assert!(rule.selects(capture, n, true, true).unwrap());
    }
}

#[test]
fn rejects_unknown_rules_and_accepts_the_full_rate_range() {
    assert!(SamplingRule::new(1, "test".into()).is_err());
    assert!(SamplingRule::new(2, " ".into()).is_err());
    assert!(SamplingRule::new(u64::MAX, "test".into()).is_ok());
    let mut rule = SamplingRule::new(2, "test".into()).unwrap();
    rule.version = 2;
    assert!(rule.selects(Uuid::nil(), 0, false, true).is_err());
}
