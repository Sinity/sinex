use super::ClockSkewState;

#[test]
fn clock_skew_state_requires_both_original_and_advanced_epochs() {
    assert!(!ClockSkewState::default().skew_was_injected());

    let mut skew = ClockSkewState {
        original_epoch: Some(1000),
        ..ClockSkewState::default()
    };
    assert!(!skew.skew_was_injected());

    skew.advanced_epoch = Some(4600);
    assert!(skew.skew_was_injected());
}
