use super::*;

fn vault() -> PathBuf {
    let d = std::env::temp_dir().join(format!("sinex-dir-{}", uuid::Uuid::now_v7()));
    std::fs::create_dir_all(d.join("sub")).unwrap();
    std::fs::write(d.join("a.md"), "alpha").unwrap();
    std::fs::write(d.join("sub/b.md"), "beta").unwrap();
    std::fs::write(d.join("skip.png"), "x").unwrap();
    d
}

fn driver(root: PathBuf, part_bytes: usize) -> DirSnapshot {
    DirSnapshot {
        root,
        host: "h".into(),
        select: |p| p.ends_with(".md"),
        part_bytes,
    }
}

fn paths(a: &Acquired) -> Vec<String> {
    a.material.as_ref().map_or(vec![], |m| {
        m.manifest["members"]
            .as_array()
            .unwrap()
            .iter()
            .map(|x| x["path"].as_str().unwrap().to_owned())
            .collect()
    })
}

#[test]
fn reads_changed_files_and_records_vanished_ones() {
    let root = vault();
    let d = driver(root.clone(), 1 << 20);
    let mut memory = DirMemory::default();
    let first = d.poll(&Value::Null, &mut memory).unwrap();
    assert_eq!(paths(&first), vec!["a.md", "sub/b.md"]);
    let m = first.material.as_ref().unwrap();
    assert_eq!(&m.bytes[m.records[1].0..m.records[1].1], b"beta");
    let cursor = (m.cursor_at)(m.bytes.len());

    assert!(d.poll(&cursor, &mut memory).unwrap().material.is_none());

    std::fs::write(root.join("a.md"), "alpha 2").unwrap();
    std::fs::remove_file(root.join("sub/b.md")).unwrap();
    let next = d.poll(&cursor, &mut memory).unwrap();
    assert_eq!(paths(&next), vec!["a.md", "sub/b.md"]);
    let m = next.material.as_ref().unwrap();
    assert_eq!(
        &m.bytes[m.records[1].0..m.records[1].1],
        &vanished_marker("sub/b.md")[..]
    );
    assert!(m.manifest["members"][1].get("vanished_at_ns").is_some());
    assert_eq!(m.manifest["records"].as_array().unwrap().len(), 2);
}

#[test]
fn a_walk_cut_by_the_part_size_does_not_record_vanished_files() {
    let root = vault();
    let mut memory = DirMemory::default();
    let small = driver(root.clone(), 6);
    let first = small.poll(&Value::Null, &mut memory).unwrap();
    assert_eq!(paths(&first), vec!["a.md"]);
    assert_eq!(
        first.material.as_ref().unwrap().manifest["scan_complete"],
        json!(false)
    );
    let m = first.material.as_ref().unwrap();
    let second = small.poll(&(m.cursor_at)(0), &mut memory).unwrap();
    assert_eq!(paths(&second), vec!["sub/b.md"]);
}

#[test]
fn a_touched_but_unchanged_file_is_skipped() {
    let root = vault();
    let d = driver(root.clone(), 1 << 20);
    let mut memory = DirMemory::default();
    d.poll(&Value::Null, &mut memory).unwrap();
    let f = std::fs::File::options()
        .append(true)
        .open(root.join("a.md"))
        .unwrap();
    f.set_modified(std::time::SystemTime::now() + std::time::Duration::from_secs(5))
        .unwrap();
    assert!(
        d.poll(&Value::Null, &mut memory)
            .unwrap()
            .material
            .is_none()
    );
}
