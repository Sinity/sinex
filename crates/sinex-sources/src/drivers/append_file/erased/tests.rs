use super::*;
use std::sync::atomic::{AtomicBool, Ordering};
fn matcher(live: Arc<AtomicBool>) -> RawMatcher {
    Arc::new(move |raw| {
        if !live.load(Ordering::Acquire) {
            return Err(DriverError::Io("synthetic scoped matcher expired".into()));
        }
        Ok(*blake3::keyed_hash(&[77; 32], raw).as_bytes())
    })
}
fn member(material: MaterialId, anchor: usize, raw: &[u8], matching: RawMatcher) -> ErasedMember {
    ErasedMember::from_archive(
        RecordLocation {
            material,
            anchor: anchor as u64,
        },
        raw.len(),
        *blake3::keyed_hash(&[77; 32], raw).as_bytes(),
        matching,
    )
    .unwrap()
}
#[test]
fn rewritten_erased_occurrence_is_consumed_once_while_extra_equal_record_stays_new() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("input");
    let a = b": 1700000000:0;duplicate\n";
    let b = b": 1700000000:1;duplicate\n";
    let c = b": 1700000000:2;duplicate\n";
    let original = [a.as_slice(), b.as_slice()].concat();
    std::fs::write(&path, &original).unwrap();
    let driver = AppendFile {
        path: path.clone(),
        host: "h".into(),
        framer: Framer::incremental(lines),
    };
    let captured = driver
        .poll(&Value::Null, &mut AppendState::default())
        .unwrap();
    let m = captured.material.unwrap();
    let end = (m.cursor_at)(original.len()).unwrap();
    let mut redacted = original.clone();
    redacted[..a.len() - 1].fill(b' ');
    let mut restarted = AppendState::rebuild([(m.id, redacted.as_slice())], driver.framer);
    restarted
        .remember_erased(member(m.id, 0, a, matcher(Arc::new(AtomicBool::new(true)))))
        .unwrap();
    let rewritten = [a.as_slice(), a.as_slice(), b.as_slice(), c.as_slice()].concat();
    std::fs::write(&path, &rewritten).unwrap();
    let acquired = driver.poll(&end, &mut restarted).unwrap();
    let material = acquired.material.unwrap();
    assert!(material.aligned);
    assert_eq!(
        material.records,
        vec![
            (a.len(), a.len() * 2),
            (a.len() * 2 + b.len(), rewritten.len())
        ]
    );
    assert!(
        material.vanished.is_empty(),
        "the already-counted survivor keeps its old occurrence"
    );
}
#[test]
fn expired_matching_and_ambiguous_decreased_multiplicity_refuse_before_state_consumption() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("input");
    let raw = b"same\n";
    std::fs::write(&path, [raw.as_slice(), raw.as_slice()].concat()).unwrap();
    let driver = AppendFile {
        path: path.clone(),
        host: "h".into(),
        framer: Framer::incremental(lines),
    };
    let mut state = AppendState::default();
    let captured = driver.poll(&Value::Null, &mut state).unwrap();
    let m = captured.material.unwrap();
    let cursor = (m.cursor_at)(m.bytes.len()).unwrap();
    let live = Arc::new(AtomicBool::new(true));
    state
        .remember_erased(member(m.id, 0, raw, matcher(live.clone())))
        .unwrap();
    let before = state.clone();
    std::fs::write(&path, raw).unwrap();
    live.store(false, Ordering::Release);
    assert!(
        driver
            .poll(&cursor, &mut state)
            .err()
            .unwrap()
            .to_string()
            .contains("matcher expired")
    );
    assert_eq!(state, before);
    live.store(true, Ordering::Release);
    assert!(
        driver
            .poll(&cursor, &mut state)
            .err()
            .unwrap()
            .to_string()
            .contains("cannot distinguish")
    );
    assert_eq!(state, before);
}
#[test]
fn erased_members_share_the_existing_tail_byte_and_count_limits() {
    let mut state = AppendState::default();
    let id = MaterialId::new();
    let matching = matcher(Arc::new(AtomicBool::new(true)));
    for i in 0..1000 {
        let raw = format!("{i}\n");
        state
            .remember_erased(member(id, i * 8, raw.as_bytes(), matching.clone()))
            .unwrap();
    }
    assert_eq!(state.order.len(), STATE_RECORDS);
    assert!(state.bytes <= STATE_TAIL);
    let remembered = state
        .erased
        .values()
        .map(|g| g.locations.len())
        .sum::<usize>();
    assert_eq!(remembered, state.order.len());
    assert!(state.erased_remaining().all(|location| location.anchor > 0));
}
