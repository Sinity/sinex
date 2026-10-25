use super::*;
use crate::drivers::append_file::tests::SMALL;

#[devkit::test]
fn newest_first_tail_preserves_duplicate_locations() {
    let material = MaterialId::new();
    let location = |anchor| RecordLocation { material, anchor };
    let mut tail = AppendTail::bounded(SMALL);
    assert!(tail.push_raw(location(8), b"same").unwrap());
    assert!(tail.push_raw(location(0), b"same").unwrap());
    let mut state = tail.into_state().unwrap();
    let expected = std::collections::VecDeque::from([location(0), location(8)]);
    assert_eq!(state.records[blake3::hash(b"same").as_bytes()], expected);
    assert!(state.take(b"same"));
    assert!(state.take(b"same"));
    assert!(!state.take(b"same"));
}

#[devkit::test]
fn completed_tail_obeys_live_record_and_byte_limits_without_keeping_raw_bodies() {
    let material = MaterialId::new();
    let mut tail = AppendTail::bounded(SMALL);
    for anchor in (0..SMALL.records as u64).rev() {
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
    assert_eq!(tail.into_state().unwrap().order.len(), SMALL.records);
    let large = vec![b'x'; SMALL.bytes + 1];
    let mut tail = AppendTail::bounded(SMALL);
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
    let mut tail = AppendTail::bounded(SMALL);
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
