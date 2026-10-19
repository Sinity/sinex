use super::*;
use std::io::Write;

fn temp(name: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!("sinex-append-{}", uuid::Uuid::now_v7()));
    std::fs::create_dir_all(&dir).unwrap();
    dir.join(name)
}

fn records(a: &Acquired) -> Vec<String> {
    let m = a.material.as_ref().unwrap();
    m.records
        .iter()
        .map(|(s, e)| String::from_utf8(m.bytes[*s..*e].to_vec()).unwrap())
        .collect()
}

#[test]
fn a_cursor_between_two_batches_reads_on_though_the_file_is_unchanged() {
    let path = temp("h");
    std::fs::write(&path, b"a\nb\nc\n").unwrap();
    let d = AppendFile {
        path: path.clone(),
        host: "h".into(),
        framer: lines,
    };
    let first = d.poll(&Value::Null, &mut AppendMemory::default()).unwrap();
    let m = first.material.unwrap();
    // The first batch (one record) committed; the process died before the next.
    let between = (m.cursor_at)(2);
    let after = (m.cursor_at)(m.bytes.len());
    let again = d.poll(&between, &mut AppendMemory::default()).unwrap();
    assert_eq!(records(&again), vec!["b\n", "c\n"]);
    assert!(
        d.poll(&after, &mut AppendMemory::default())
            .unwrap()
            .material
            .is_none(),
        "a cursor covering the whole read skips the unchanged file"
    );
}

#[test]
fn appends_read_only_new_complete_records() {
    let path = temp("h");
    std::fs::write(&path, b"a\nb\npart").unwrap();
    let d = AppendFile {
        path: path.clone(),
        host: "h".into(),
        framer: lines,
    };
    let mut memory = AppendMemory::default();
    let first = d.poll(&Value::Null, &mut memory).unwrap();
    assert_eq!(records(&first), vec!["a\n", "b\n"]);
    let m = first.material.as_ref().unwrap();
    let cursor = (m.cursor_at)(m.bytes.len());
    assert_eq!(cursor["offset"], json!(4));

    // Unchanged stat: nothing read.
    let same = d.poll(&cursor, &mut memory).unwrap();
    assert!(same.material.is_none() && !same.observation.changed);

    std::fs::OpenOptions::new()
        .append(true)
        .open(&path)
        .unwrap()
        .write_all(b"ial\nc\n")
        .unwrap();
    let next = d.poll(&cursor, &mut memory).unwrap();
    assert_eq!(next.revision, RevisionChange::Appended);
    assert_eq!(records(&next), vec!["partial\n", "c\n"]);
    let n = next.material.as_ref().unwrap();
    assert_eq!(n.part_index, 1);
    assert_eq!(n.previous_part, Some(m.id));
    assert_eq!((n.reading_order)(0), sinex_model::ReadingOrder(4));
    assert!(!n.aligned);
}

#[test]
fn a_rewrite_reads_only_records_the_previous_revision_did_not_hold() {
    let path = temp("h");
    std::fs::write(&path, b"a\nb\nc\n").unwrap();
    let d = AppendFile {
        path: path.clone(),
        host: "h".into(),
        framer: lines,
    };
    let mut memory = AppendMemory::default();
    let first = d.poll(&Value::Null, &mut memory).unwrap();
    let m = first.material.unwrap();
    let cursor = (m.cursor_at)(m.bytes.len());

    // zsh expired the duplicate "b" and appended "dd": a rewrite.
    std::fs::write(&path, b"a\nc\ndd\n").unwrap();
    // A restart in between: the memory is rebuilt from the stored bytes.
    let mut rebuilt = AppendMemory::rebuild([(m.id, m.bytes.as_slice())], lines);
    assert_eq!(rebuilt, memory);
    let again = d.poll(&cursor, &mut rebuilt).unwrap();
    assert_eq!(again.revision, RevisionChange::Rewritten);
    assert_eq!(records(&again), vec!["dd\n"]);
    let n = again.material.as_ref().unwrap();
    assert!(n.aligned);
    assert_eq!(n.part_index, 0, "a rewrite starts a new chain");
    assert_eq!(
        n.vanished,
        vec![RecordLocation {
            material: m.id,
            anchor: 2
        }],
        "the dropped \"b\" is reported where it was read"
    );
    assert_eq!(n.manifest["records"], json!([[4, 7]]));
    assert_eq!(
        crate::drivers::stored_records(&n.manifest, &n.bytes, lines),
        vec![(4, 7)]
    );
    // The cursor covers the whole rewritten file, skipped records included.
    let after = (n.cursor_at)(n.bytes.len());
    assert_eq!(after["offset"], json!(7));
    assert!(d.poll(&after, &mut rebuilt).unwrap().material.is_none());
}

#[test]
fn rewritten_prefix_is_read_again_as_a_new_revision() {
    let path = temp("h");
    std::fs::write(&path, b"a\nb\n").unwrap();
    let d = AppendFile {
        path: path.clone(),
        host: "h".into(),
        framer: lines,
    };
    let mut memory = AppendMemory::default();
    let first = d.poll(&Value::Null, &mut memory).unwrap();
    let m = first.material.unwrap();
    let cursor = (m.cursor_at)(m.bytes.len());
    std::fs::write(&path, b"x\ny\nz\n").unwrap();
    let again = d.poll(&cursor, &mut memory).unwrap();
    assert_eq!(again.revision, RevisionChange::Rewritten);
    assert_eq!(records(&again), vec!["x\n", "y\n", "z\n"]);
    assert_eq!(again.material.unwrap().part_index, 0);
}

#[test]
fn missing_file_is_unavailable_not_empty() {
    let d = AppendFile {
        path: temp("absent"),
        host: "h".into(),
        framer: lines,
    };
    let a = d.poll(&Value::Null, &mut AppendMemory::default()).unwrap();
    assert!(a.observation.unavailable.is_some());
    assert!(a.material.is_none());
}

/// A small part, so a small file spans many.
const PART_SIZE: u64 = 4096;

#[test]
fn a_file_larger_than_a_part_is_read_in_bounded_parts_covering_every_record() {
    let path = temp("h");
    let mut text = String::new();
    for i in 0..5000 {
        text.push_str(&format!("record-{i:05}\n"));
    }
    // One record longer than a part is read whole, never refused.
    text.push_str(&format!("{}\n", "y".repeat(3 * PART_SIZE as usize)));
    std::fs::write(&path, text.as_bytes()).unwrap();
    let d = AppendFile {
        path: path.clone(),
        host: "h".into(),
        framer: lines,
    };
    let mut memory = AppendMemory::default();
    let mut cursor = Value::Null;
    let mut read = Vec::new();
    let mut polls = 0;
    loop {
        let a = d.poll_in_parts(&cursor, &mut memory, PART_SIZE).unwrap();
        let Some(m) = &a.material else { break };
        polls += 1;
        assert!(
            m.bytes.len() as u64 <= PART_SIZE || m.records.len() == 1,
            "a part holds at most PART_SIZE bytes unless one record is longer"
        );
        read.extend(records(&a));
        cursor = (m.cursor_at)(m.bytes.len());
    }
    assert!(polls > 10, "read in parts, not at once ({polls})");
    assert_eq!(read.concat(), text);
}

#[test]
fn the_memory_keeps_a_bounded_tail_of_the_newest_records() {
    let mut memory = AppendMemory::default();
    let mut first = None;
    for m in 0..100 {
        let id = MaterialId::new();
        first.get_or_insert(id);
        for i in 0..10 {
            let mut r = vec![b'r'; 1000];
            r.extend_from_slice(format!("{m}-{i}\n").as_bytes());
            memory.insert(&r, id, i * 1010);
        }
    }
    // 1000 records of ~1 KB: held by bytes, the newest only.
    assert!(memory.bytes <= MEMORY_TAIL, "{} bytes kept", memory.bytes);
    assert!(memory.bytes + 1010 > MEMORY_TAIL);
    let first = first.unwrap();
    assert!(
        memory
            .records
            .values()
            .flatten()
            .all(|l| l.material != first),
        "the oldest records are gone"
    );
    let held: usize = memory.records.values().map(Vec::len).sum();
    assert_eq!(held, memory.order.len());
}

#[test]
fn short_records_are_bounded_by_count_within_one_material() {
    let mut memory = AppendMemory::default();
    let id = MaterialId::new();
    for i in 0..1000 {
        memory.insert(format!("{i}\n").as_bytes(), id, i * 4);
    }
    // 1000 records of 2-4 bytes in one material: far under the byte tail,
    // held by count.
    assert_eq!(memory.order.len(), MEMORY_RECORDS);
    let held: usize = memory.records.values().map(Vec::len).sum();
    assert_eq!(held, MEMORY_RECORDS);
    assert!(memory.take(b"999\n"), "the newest record is held");
    assert!(!memory.take(b"0\n"), "the oldest is not");
}
