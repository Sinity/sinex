use std::collections::BTreeMap;

use sqlx::types::Uuid;

use super::{
    MaterialOccurrenceKey, material_occurrence_count_regressions, missing_baseline_event_count,
};

fn uuid(value: u128) -> Uuid {
    Uuid::from_u128(value)
}

#[test]
fn missing_baseline_event_count_detects_lost_pre_fault_events() {
    let kept = uuid(1);
    let lost = uuid(2);
    let extra = uuid(3);

    assert_eq!(
        missing_baseline_event_count(&[kept, lost], &[kept, extra]),
        1
    );
}

#[test]
fn missing_baseline_event_count_allows_added_recovery_events() {
    let baseline = uuid(1);
    let recovered = uuid(2);

    assert_eq!(
        missing_baseline_event_count(&[baseline], &[baseline, recovered]),
        0
    );
}

fn occurrence(id: u128, anchor_byte: i64) -> MaterialOccurrenceKey {
    MaterialOccurrenceKey {
        source_material_id: uuid(id),
        anchor_byte,
        offset_start: None,
        offset_end: None,
        offset_kind: None,
    }
}

#[test]
fn material_occurrence_regressions_detect_replayed_duplicate_anchors() {
    let stable = occurrence(1, 10);
    let duplicated = occurrence(2, 20);
    let mut baseline = BTreeMap::new();
    baseline.insert(stable.clone(), 1);
    baseline.insert(duplicated.clone(), 1);

    let mut current = BTreeMap::new();
    current.insert(stable, 1);
    current.insert(duplicated.clone(), 2);
    current.insert(occurrence(3, 30), 1);

    assert_eq!(
        material_occurrence_count_regressions(&baseline, &current),
        vec![(duplicated, 1, 2)]
    );
}

#[test]
fn material_occurrence_regressions_allow_new_occurrences() {
    let baseline_key = occurrence(1, 10);
    let mut baseline = BTreeMap::new();
    baseline.insert(baseline_key.clone(), 1);

    let mut current = BTreeMap::new();
    current.insert(baseline_key, 1);
    current.insert(occurrence(2, 20), 3);

    assert!(material_occurrence_count_regressions(&baseline, &current).is_empty());
}
