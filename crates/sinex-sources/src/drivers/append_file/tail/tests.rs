use super::*;

#[test]
fn newest_first_tail_preserves_duplicate_locations_and_erased_members() {
    let material = MaterialId::new();
    let location = |anchor| RecordLocation { material, anchor };
    let mut tail = AppendTail::default();
    assert!(tail.push_raw(location(8), b"same").unwrap());
    let member = ErasedMember::from_archive(
        location(4),
        4,
        [7; 32],
        std::sync::Arc::new(|_| Ok([7; 32])),
    )
    .unwrap();
    assert!(tail.push_erased(member));
    assert!(tail.push_raw(location(0), b"same").unwrap());
    let mut state = tail.into_state().unwrap();
    let expected = std::collections::VecDeque::from([location(0), location(8)]);
    assert_eq!(state.records[blake3::hash(b"same").as_bytes()], expected);
    assert_eq!(
        state.erased_remaining().collect::<Vec<_>>(),
        vec![location(4)]
    );
    assert!(state.take(b"same"));
    assert!(state.take(b"same"));
    assert!(!state.take(b"same"));
}

#[test]
fn completed_tail_obeys_live_record_and_byte_limits_without_retaining_raw_bodies() {
    let material = MaterialId::new();
    let mut tail = AppendTail::default();
    for anchor in (0..STATE_RECORDS as u64).rev() {
        assert!(
            tail.push_raw(RecordLocation { material, anchor }, b"x")
                .unwrap()
        );
    }
    assert!(
        !tail
            .push_raw(
                RecordLocation {
                    material,
                    anchor: 1000
                },
                b"x"
            )
            .unwrap()
    );
    assert_eq!(tail.into_state().unwrap().order.len(), STATE_RECORDS);
    let large = vec![b'x'; STATE_TAIL + 1];
    let mut tail = AppendTail::default();
    assert!(
        tail.push_raw(
            RecordLocation {
                material,
                anchor: 1
            },
            &large
        )
        .unwrap()
    );
    assert!(
        !tail
            .push_raw(
                RecordLocation {
                    material,
                    anchor: 0
                },
                b"x"
            )
            .unwrap()
    );
    let state = tail.into_state().unwrap();
    assert_eq!(state.order.len(), 1);
    assert_eq!(state.bytes, large.len());
    let mut tail = AppendTail::default();
    assert!(
        tail.push_raw(
            RecordLocation {
                material,
                anchor: 0
            },
            b""
        )
        .is_err()
    );
    assert!(
        tail.push_raw(
            RecordLocation {
                material,
                anchor: u64::MAX
            },
            b"x"
        )
        .is_err()
    );
}
