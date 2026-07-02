use super::NetworkPartitionState;

#[test]
fn network_partition_state_requires_injection_before_heal_counts() {
    assert!(!NetworkPartitionState::default().partition_was_injected());
    assert!(!NetworkPartitionState::default().partition_was_healed());

    let healed_without_injection = NetworkPartitionState {
        injected: false,
        healed: true,
    };
    assert!(!healed_without_injection.partition_was_injected());
    assert!(!healed_without_injection.partition_was_healed());

    let injected_only = NetworkPartitionState {
        injected: true,
        healed: false,
    };
    assert!(injected_only.partition_was_injected());
    assert!(!injected_only.partition_was_healed());

    let healed = NetworkPartitionState {
        injected: true,
        healed: true,
    };
    assert!(healed.partition_was_injected());
    assert!(healed.partition_was_healed());
}
