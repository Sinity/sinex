use super::*;

fn predecessor(files: &[(&str, &[u8], i128)]) -> sinex_model::Manifest {
    let mut end = 0;
    let mut members = Vec::new();
    let mut records = Vec::new();
    for (path, bytes, modified) in files {
        let start = end + 1;
        end = start + bytes.len();
        members.push(
            json!({"path":path,"stream_offset":start,"length":bytes.len(),
            "mtime_ns":modified.to_string(),"hash":blake3::hash(bytes).to_hex().to_string()}),
        );
        records.push((start, end));
    }
    sinex_model::Manifest::from_json(json!({"media_type":"application/x-sinex-file-bundle",
        "scan_complete":true,"members":members,"records":records}))
}

fn cursor(material: MaterialId, through: usize) -> Value {
    json!({"material":material,"through":through,"part_index":7,"memory_part":3,
        "changed_since_memory":4,"last_complete_scan_ns":1})
}

fn old_memory(path: &[u8], size: u64, time: i128, hash: [u8; 32]) -> Vec<u8> {
    let mut bytes = (path.len() as u32).to_le_bytes().to_vec();
    bytes.extend(path);
    bytes.push(1);
    bytes.extend(size.to_le_bytes());
    bytes.extend(time.to_le_bytes());
    bytes.extend(hash);
    bytes
}

#[devkit::test]
fn predecessor_facts_preserve_mtime_and_exact_admitted_record_boundary() {
    let manifest = predecessor(&[("a.md", b"", -7), ("b.md", b"beta", 42)]);
    let mut state = DirState::default();
    state.remember_members(&manifest, 1).unwrap();
    let file = state.files[Path::new("a.md")].unwrap();
    assert_eq!(file.size, 0);
    assert_eq!(file.modified_ns, Some(-7));
    assert_eq!(file.changed_ns, None);
    assert_eq!(file.hash, *blake3::hash(b"").as_bytes());
    assert!(!state.files.contains_key(Path::new("b.md")));
    assert!(state.checkpoint_pending);
    assert_eq!(manifest.records.unwrap(), [(1, 1), (2, 6)]);
}

#[devkit::test]
fn untagged_cache_times_require_the_immutable_anchor_format() {
    let hash = *blake3::hash(b"alpha").as_bytes();
    let bytes = old_memory(b"a.md", 5, -17, hash);
    let old = predecessor(&[("a.md", b"alpha", -17)]);
    let mut modern = old.clone();
    modern.media_type = Some("application/x-tar".into());
    for (anchor, modified, changed) in [(&old, Some(-17), None), (&modern, None, Some(-17))] {
        let mut state = DirState::default();
        state.remember_saved(&bytes, Some(anchor), 6).unwrap();
        let facts = state.files[Path::new("a.md")].unwrap();
        assert_eq!((facts.modified_ns, facts.changed_ns), (modified, changed));
        let current = restore::memory_of(&state.files, &state.locations, 6);
        let mut restarted = DirState::default();
        restarted.remember_saved(&current, None, 6).unwrap();
        assert_eq!(restarted.files, state.files);
        assert!(!restarted.checkpoint_pending);
    }
    assert!(
        DirState::default()
            .remember_saved(&bytes, None, 6)
            .unwrap_err()
            .to_string()
            .contains("immutable")
    );
    assert!(
        DirState::default()
            .remember_saved(&bytes, Some(&sinex_model::Manifest::default()), 6)
            .is_err()
    );
}

#[devkit::test]
fn unchanged_predecessor_input_checkpoints_without_new_material_and_retries_until_ack() {
    let dir = tempfile::tempdir().unwrap();
    std::fs::write(dir.path().join("a.md"), b"alpha").unwrap();
    let manifest = predecessor(&[("a.md", b"alpha", -17)]);
    let material = MaterialId::new();
    let committed = cursor(material, 6);
    let mut state = DirState::default();
    state.remember_members(&manifest, 6).unwrap();
    state.finish_remembering();
    let d = driver(dir.path().to_owned(), 4096);
    let first = d.poll(&committed, &mut state).unwrap();
    assert!(first.material.is_none());
    let memory = first.memory.as_ref().unwrap();
    assert_eq!(memory.material, material);
    assert_eq!(
        first.cursor_without_material["material"],
        committed["material"]
    );
    assert_eq!(
        first.cursor_without_material["through"],
        committed["through"]
    );
    let facts = state.files[Path::new("a.md")].unwrap();
    assert_eq!(facts.modified_ns, Some(-17));
    assert_eq!(
        facts.changed_ns,
        Some(tar::change_time(
            &dir.path().join("a.md").metadata().unwrap()
        ))
    );
    let source = crate::test_parser::source();
    let prepared = super::super::super::prepare(
        &source,
        "synthetic",
        sinex_model::RunId::new(),
        first,
        Value::Null,
        &Refuse,
        crate::Limits::default(),
    )
    .unwrap();
    let batches: Vec<_> = prepared
        .batches()
        .unwrap()
        .collect::<Result<_, _>>()
        .unwrap();
    assert_eq!(batches.len(), 1);
    let sinex_model::BatchKind::Source(progress) = &batches[0].kind else {
        panic!("source checkpoint")
    };
    assert!(progress.material.is_none());
    assert!(batches[0].records.is_empty());
    assert_eq!(progress.memory.as_ref().unwrap().material, material);
    // Lost acknowledgement must not clear the cache upgrade in the running reader.
    assert!(d.poll(&committed, &mut state).unwrap().memory.is_some());
    // A restart after the database committed needs no second observation/material.
    let mut restarted = DirState::default();
    restarted
        .remember_saved(&progress.memory.as_ref().unwrap().bytes, None, 6)
        .unwrap();
    restarted.finish_remembering();
    let quiet = d.poll(&progress.cursor, &mut restarted).unwrap();
    assert!(quiet.material.is_none());
    assert!(quiet.memory.is_none());
}

#[devkit::test]
fn newer_tar_facts_win_over_older_predecessor_members() {
    let dir = tempfile::tempdir().unwrap();
    std::fs::write(dir.path().join("a.md"), b"modern").unwrap();
    let current = driver(dir.path().to_owned(), 4096)
        .poll(&Value::Null, &mut DirState::default())
        .unwrap();
    let modern = current.material.unwrap();
    let old = predecessor(&[("a.md", b"older", 11), ("b.md", b"beta", 12)]);
    let mut state = DirState::default();
    state.remember_members(&modern.manifest, u64::MAX).unwrap();
    state.remember_members(&old, u64::MAX).unwrap();
    assert_eq!(
        state.files[Path::new("a.md")].unwrap().hash,
        *blake3::hash(b"modern").as_bytes()
    );
    assert!(state.files[Path::new("a.md")].unwrap().changed_ns.is_some());
    assert_eq!(state.files[Path::new("b.md")].unwrap().changed_ns, None);
}

#[devkit::test]
fn changed_predecessor_bytes_use_current_tar_and_original_removal_anchors_survive() {
    let dir = tempfile::tempdir().unwrap();
    std::fs::write(dir.path().join("a.md"), b"newer").unwrap();
    let old = predecessor(&[("a.md", b"older", 11), ("b.md", b"beta", 12)]);
    let material = MaterialId::new();
    let mut state = DirState::default();
    state.remember_members(&old, u64::MAX).unwrap();
    state.remember_locations(&old, u64::MAX, material).unwrap();
    state.finish_remembering();
    let mut d = driver(dir.path().to_owned(), 4096);
    d.removed_at_source = true;
    let next = d.poll(&cursor(material, 11), &mut state).unwrap();
    let next = next.material.unwrap();
    assert_eq!(
        next.manifest.media_type.as_deref(),
        Some("application/x-tar")
    );
    assert_eq!(record(&next, 0), b"newer");
    assert_eq!(
        next.removed,
        [RecordLocation {
            material,
            anchor: 7
        }]
    );
}

#[devkit::test]
fn malformed_predecessor_facts_and_ranges_are_refused() {
    let good = predecessor(&[("a.md", b"alpha", 11), ("b.md", b"beta", 12)]);
    for (key, value) in [
        ("hash", json!("not-a-hash")),
        ("mtime_ns", json!("not-a-time")),
    ] {
        let mut bad = good.clone();
        bad.members[0].details.insert(key.into(), value);
        let mut state = DirState::default();
        assert!(state.remember_members(&bad, u64::MAX).is_err());
        assert!(
            state.remember_members(&bad, u64::MAX).is_err(),
            "a retry cannot forget malformed evidence"
        );
    }
    let mut overlap = good.clone();
    overlap.records.as_mut().unwrap()[1].0 = 5;
    assert!(
        DirState::default()
            .remember_members(&overlap, u64::MAX)
            .is_err()
    );
    let mut ambiguous = good.clone();
    ambiguous.members[1].path = ambiguous.members[0].path.clone();
    assert!(
        DirState::default()
            .remember_members(&ambiguous, u64::MAX)
            .is_err()
    );
    let mut unknown = good;
    unknown.details.remove("scan_complete");
    assert!(
        DirState::default()
            .remember_members(&unknown, u64::MAX)
            .is_err()
    );
}

#[devkit::test]
fn only_the_checkpoint_cursor_acknowledges_a_pending_cache_upgrade() {
    let dir = tempfile::tempdir().unwrap();
    std::fs::write(dir.path().join("a.md"), b"alpha").unwrap();
    let old = predecessor(&[("a.md", b"alpha", -17)]);
    let bytes = old_memory(b"a.md", 5, -17, *blake3::hash(b"alpha").as_bytes());
    let crate::Reading::Polled(mut reader) =
        crate::binding::walked(driver(dir.path().to_owned(), 4096))
    else {
        panic!("walked reader")
    };
    let material = MaterialId::new();
    let anchor = stored_part(material, old, Some(6)).reference;
    reader
        .restore_memory(&bytes, Some(&anchor), Some(6))
        .unwrap();
    reader.restored().unwrap();
    let prior = cursor(material, 6);
    let first = reader.poll(&prior).unwrap();
    assert!(first.material.is_none());
    assert!(first.memory.is_some());
    reader.committed(&prior).unwrap();
    let retry = reader.poll(&prior).unwrap();
    assert!(
        retry.memory.is_some(),
        "an old or partial ACK cannot clear the upgrade"
    );
    reader.committed(&retry.cursor_without_material).unwrap();
    let quiet = reader.poll(&retry.cursor_without_material).unwrap();
    assert!(quiet.material.is_none());
    assert!(quiet.memory.is_none());
}

struct MetadataOnly(u64);
impl crate::drain::RecordRead for MetadataOnly {
    fn length(&self) -> u64 {
        self.0
    }
    fn read_exact_at(&self, _: u64, _: &mut [u8]) -> std::io::Result<()> {
        panic!("directory restoration must use metadata, not material payloads")
    }
}

fn stored_part(
    id: MaterialId,
    manifest: sinex_model::Manifest,
    admitted: Option<u64>,
) -> crate::StoredMaterial {
    let length = manifest.records.as_ref().unwrap().last().unwrap().1 as u64;
    crate::StoredMaterial {
        admitted,
        reference: sinex_model::MaterialRef {
            id,
            input: None,
            blob: "synthetic".into(),
            length,
            part_index: 0,
            previous_part: None,
            parser: "synthetic-notes".into(),
            parser_version: 1,
            manifest,
        },
        bytes: Arc::new(MetadataOnly(length)),
        same_revision: true,
    }
}

#[devkit::test]
fn older_metadata_fills_only_saved_file_locations_within_its_admitted_prefix() {
    let dir = tempfile::tempdir().unwrap();
    std::fs::write(dir.path().join("a.md"), b"alpha").unwrap();
    std::fs::write(dir.path().join("b.md"), b"beta").unwrap();
    let current = MaterialId::new();
    let older = MaterialId::new();
    let anchor = predecessor(&[("a.md", b"alpha", -17), ("b.md", b"beta", -18)]);
    let mut memory = old_memory(b"a.md", 5, -17, *blake3::hash(b"alpha").as_bytes());
    memory.extend(old_memory(
        b"b.md",
        4,
        -18,
        *blake3::hash(b"beta").as_bytes(),
    ));
    let mut d = driver(dir.path().to_owned(), 4096);
    d.removed_at_source = true;
    let crate::Reading::Polled(mut reader) = crate::binding::walked(d) else {
        panic!("walked reader")
    };
    let committed = cursor(current, 6);
    assert!(
        reader
            .restore(&committed, stored_part(current, anchor, Some(6)))
            .unwrap()
    );
    let snapshot_id = MaterialId::new();
    let snapshot = stored_part(
        snapshot_id,
        predecessor(&[("a.md", b"alpha", -17)]),
        Some(6),
    );
    assert!(
        reader
            .restore_memory(&memory, Some(&snapshot.reference), Some(6))
            .unwrap()
    );
    assert!(reader.restore(&committed, snapshot).unwrap());
    let earlier = predecessor(&[("b.md", b"beta", -18), ("obsolete.md", b"gone", -19)]);
    assert!(
        !reader
            .restore(&committed, stored_part(older, earlier, Some(10)))
            .unwrap()
    );
    reader.restored().unwrap();
    let upgraded = reader.poll(&committed).unwrap();
    assert!(
        upgraded.material.is_none(),
        "older metadata must not resurrect obsolete files"
    );
    reader.committed(&upgraded.cursor_without_material).unwrap();
    std::fs::remove_file(dir.path().join("b.md")).unwrap();
    let removed = reader
        .poll(&upgraded.cursor_without_material)
        .unwrap()
        .material
        .unwrap();
    assert_eq!(
        removed.removed,
        [RecordLocation {
            material: older,
            anchor: 1
        }]
    );
}

#[devkit::test]
fn missing_or_contradictory_part_boundaries_are_diagnosed() {
    let dir = tempfile::tempdir().unwrap();
    let part = predecessor(&[("a.md", b"alpha", -17)]);
    for through in [None, Some(7)] {
        let crate::Reading::Polled(mut reader) =
            crate::binding::walked(driver(dir.path().to_owned(), 4096))
        else {
            panic!("walked reader")
        };
        assert!(
            reader
                .restore(
                    &Value::Null,
                    stored_part(MaterialId::new(), part.clone(), through)
                )
                .unwrap_err()
                .to_string()
                .contains("admitted-prefix evidence")
        );
    }
}

#[devkit::test]
fn predecessor_observations_follow_the_existing_part_target_without_duplicate_material() {
    let dir = tempfile::tempdir().unwrap();
    std::fs::write(dir.path().join("a.md"), b"alpha").unwrap();
    std::fs::write(dir.path().join("b.md"), b"beta").unwrap();
    let part = predecessor(&[("a.md", b"alpha", -17), ("b.md", b"beta", -18)]);
    let mut state = DirState::default();
    state.remember_members(&part, u64::MAX).unwrap();
    state.finish_remembering();
    let d = driver(dir.path().to_owned(), 6);
    let prior = cursor(MaterialId::new(), 11);
    let first = d.poll(&prior, &mut state).unwrap();
    assert!(first.material.is_none());
    assert!(state.files[Path::new("a.md")].unwrap().changed_ns.is_some());
    assert!(state.files[Path::new("b.md")].unwrap().changed_ns.is_none());
    let second = d.poll(&first.cursor_without_material, &mut state).unwrap();
    assert!(second.material.is_none());
    assert!(state.files[Path::new("b.md")].unwrap().changed_ns.is_some());
    assert_eq!(second.cursor_without_material["through"], prior["through"]);
}

#[devkit::test]
fn a_checkpoint_behind_the_admitted_prefix_keeps_the_newer_suffix_facts() {
    for removed in [false, true] {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join("a.md"), b"alpha").unwrap();
        if !removed {
            std::fs::write(dir.path().join("b.md"), b"newer").unwrap();
        }
        let files = HashMap::from([
            (
                PathBuf::from("a.md"),
                Some(FileFacts {
                    size: 5,
                    hash: *blake3::hash(b"alpha").as_bytes(),
                    modified_ns: Some(-17),
                    changed_ns: None,
                }),
            ),
            (
                PathBuf::from("b.md"),
                Some(FileFacts {
                    size: 5,
                    hash: *blake3::hash(b"older").as_bytes(),
                    modified_ns: Some(-18),
                    changed_ns: None,
                }),
            ),
        ]);
        let memory = restore::memory_of(&files, &HashMap::new(), 6);
        let mut manifest = predecessor(&[("a.md", b"alpha", -17), ("b.md", b"newer", -16)]);
        if removed {
            manifest.members[1]
                .details
                .insert("removed_at_ns".into(), json!("1"));
        }
        let material = MaterialId::new();
        let anchor = stored_part(material, manifest, Some(12)).reference;
        let crate::Reading::Polled(mut reader) =
            crate::binding::walked(driver(dir.path().to_owned(), 4096))
        else {
            panic!("walked reader")
        };
        assert!(
            !reader
                .restore_memory(&memory, Some(&anchor), Some(12))
                .unwrap()
        );
        reader.restored().unwrap();
        let polled = reader.poll(&cursor(material, 12)).unwrap();
        assert!(
            polled.material.is_none(),
            "suffix facts must override saved facts, including a removal"
        );
        assert_eq!(polled.cursor_without_material["through"], 12);
    }
}

#[devkit::test]
fn exact_checkpoint_prefix_does_not_apply_unadmitted_suffix_and_beyond_prefix_is_refused() {
    let dir = tempfile::tempdir().unwrap();
    std::fs::write(dir.path().join("a.md"), b"alpha").unwrap();
    std::fs::write(dir.path().join("b.md"), b"older").unwrap();
    let files = HashMap::from([
        (
            PathBuf::from("a.md"),
            Some(FileFacts {
                size: 5,
                hash: *blake3::hash(b"alpha").as_bytes(),
                modified_ns: Some(-17),
                changed_ns: None,
            }),
        ),
        (
            PathBuf::from("b.md"),
            Some(FileFacts {
                size: 5,
                hash: *blake3::hash(b"older").as_bytes(),
                modified_ns: Some(-18),
                changed_ns: None,
            }),
        ),
    ]);
    let memory = restore::memory_of(&files, &HashMap::new(), 6);
    let material = MaterialId::new();
    let anchor = stored_part(
        material,
        predecessor(&[("a.md", b"alpha", -17), ("b.md", b"newer", -16)]),
        Some(6),
    )
    .reference;
    let crate::Reading::Polled(mut reader) =
        crate::binding::walked(driver(dir.path().to_owned(), 4096))
    else {
        panic!("walked reader")
    };
    assert!(
        !reader
            .restore_memory(&memory, Some(&anchor), Some(6))
            .unwrap()
    );
    reader.restored().unwrap();
    assert!(
        reader
            .poll(&cursor(material, 6))
            .unwrap()
            .material
            .is_none()
    );
    let crate::Reading::Polled(mut reader) =
        crate::binding::walked(driver(dir.path().to_owned(), 4096))
    else {
        panic!("walked reader")
    };
    assert!(
        reader
            .restore_memory(&memory, Some(&anchor), Some(5))
            .unwrap_err()
            .to_string()
            .contains("beyond")
    );
}
