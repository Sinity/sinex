use super::*;

fn zsh() -> (Snapshot, Fixture) {
    let first = Uuid::from_u128(1);
    let second = Uuid::from_u128(2);
    let input = Uuid::from_u128(10);
    let a = b": 1600000000:0;printf '%s' \\\nline 0\n".to_vec();
    let b = b": 1600000001:0;cmd 1\n".to_vec();
    let bytes = [a.clone(), b.clone()].concat();
    let mut counts = Vec::new();
    for (at, command) in [
        (1600000000i64, "printf '%s' \nline 0"),
        (1600000001, "cmd 1"),
    ] {
        counts.push((
            at,
            blake3::derive_key("Sinex zsh command occurrence v1", command.as_bytes()),
            1,
        ));
    }
    let parts = HashMap::from([
        (
            first,
            Part {
                id: first,
                input,
                index: 0,
                previous: None,
                manifest: json!({"path":"synthetic","host":HOST,"file_offset":0,"media_type":"text/plain"}),
                bytes: a.clone(),
            },
        ),
        (
            second,
            Part {
                id: second,
                input,
                index: 1,
                previous: Some(first),
                manifest: json!({"path":"synthetic","host":HOST,"file_offset":a.len(),"previous_through":a.len(),"media_type":"text/plain"}),
                bytes: b.clone(),
            },
        ),
    ]);
    let snapshot = Snapshot {
        scope: json!({"locator":"synthetic","host":HOST,"source":{"name":"terminal.zsh-history","version":3}}),
        input,
        cursor: json!({"material":second,"part_index":1,"through":b.len(),"offset":bytes.len(),"size":bytes.len(),"inode":7,"mtime_ns":1,"ctime_ns":2,"looked_ns":3,"window":blake3::hash(&bytes).to_hex().to_string()}),
        carry: json!({"latest":1600000001,"counts":counts}),
        parts,
        facts: vec![
            Fact {
                material: first,
                anchor: 0,
                hash: *blake3::hash(&a).as_bytes(),
                payload: json!({"command":"printf '%s' \nline 0","hostname":HOST,"n":0}),
                order: 0,
            },
            Fact {
                material: second,
                anchor: 0,
                hash: *blake3::hash(&b).as_bytes(),
                payload: json!({"command":"cmd 1","hostname":HOST,"n":0}),
                order: 0,
            },
        ],
        failures: BTreeSet::new(),
        memory: None,
    };
    (
        snapshot,
        Fixture::Zsh {
            records: 2,
            bytes,
            inode: 7,
            mtime: 1,
            ctime: 2,
        },
    )
}
fn atuin() -> (Snapshot, Fixture) {
    let material = Uuid::from_u128(1);
    let input = Uuid::from_u128(10);
    let mut bytes = Vec::new();
    let mut facts = Vec::new();
    let mut memory = Vec::new();
    for i in 0..2i64 {
        let row = json!({"_rowid":i+1,"id":format!("a{i:06}"),"timestamp":1_600_000_000_000_000_000i64+i*1_000_000_000,"duration":1000,"exit":0,"command":format!("atuin {i}"),"cwd":"/","session":"s","hostname":"h","deleted_at":null});
        let mut line = serde_json::to_vec(&row).unwrap();
        memory.extend((i + 1).to_le_bytes());
        memory.extend(blake3::hash(&line).as_bytes());
        line.push(b'\n');
        facts.push(Fact{material,anchor:bytes.len() as u64,hash:*blake3::hash(&line).as_bytes(),payload:json!({"native_id":format!("a{i:06}"),"command":format!("atuin {i}"),"duration_ns":1000,"exit_code":0}),order:1});
        bytes.extend(line);
    }
    let snapshot = Snapshot {
        scope: json!({"locator":"synthetic","host":HOST,"source":{"name":"terminal.atuin-history","version":1}}),
        input,
        cursor: json!({"material":material,"part_index":0,"through":bytes.len(),"database_generation":"crash:38:7","max_rowid":2,"memory_part":0,"changed_since_memory":0,"last_order":2,"last_full_pass_ns":3}),
        carry: Value::Null,
        parts: HashMap::from([(
            material,
            Part {
                id: material,
                input,
                index: 0,
                previous: None,
                manifest: json!({"path":"synthetic","host":HOST,"database_generation":"crash:38:7","media_type":"application/x-ndjson","table":"history"}),
                bytes,
            },
        )]),
        facts,
        failures: BTreeSet::new(),
        memory: Some((material, memory)),
    };
    (
        snapshot,
        Fixture::Atuin {
            rows: 2,
            generation: "crash:38:7".into(),
        },
    )
}
#[devkit::test]
fn valid_different_part_layouts_have_equal_proven_progress() {
    let (split, fixture) = zsh();
    let mut whole = split.clone();
    let first = Uuid::from_u128(1);
    let second = Uuid::from_u128(2);
    let cut = whole.parts[&first].bytes.len();
    let tail = whole.parts.remove(&second).unwrap();
    whole
        .parts
        .get_mut(&first)
        .unwrap()
        .bytes
        .extend(tail.bytes);
    whole.cursor["material"] = json!(first);
    whole.cursor["part_index"] = json!(0);
    whole.cursor["through"] = json!(whole.parts[&first].bytes.len());
    whole.facts[1].material = first;
    whole.facts[1].anchor = cut as u64;
    assert_eq!(
        validate(&split, &fixture).unwrap(),
        validate(&whole, &fixture).unwrap()
    );
    let (rows, fixture) = atuin();
    assert!(validate(&rows, &fixture).is_ok());
}
#[devkit::test]
fn every_consumed_grain_requires_authority_even_with_a_valid_frontier() {
    let (mut snapshot, fixture) = zsh();
    snapshot.facts.remove(0);
    assert!(
        validate(&snapshot, &fixture)
            .unwrap_err()
            .contains("no event")
    );
    let (mut snapshot, fixture) = zsh();
    snapshot.facts[0].hash = [0; 32];
    assert!(
        validate(&snapshot, &fixture).is_err(),
        "anchor alone is insufficient"
    );
}
#[devkit::test]
fn wrong_material_input_source_or_generation_cannot_normalize() {
    let (snapshot, fixture) = zsh();
    for field in ["material", "input", "source", "host", "generation"] {
        let mut bad = snapshot.clone();
        match field {
            "material" => bad.cursor["material"] = json!(Uuid::from_u128(99)),
            "input" => bad.parts.get_mut(&Uuid::from_u128(1)).unwrap().input = Uuid::from_u128(99),
            "source" => bad.scope["source"]["version"] = json!(99),
            "host" => bad.scope["host"] = json!("another-host"),
            _ => bad.cursor["inode"] = json!(99),
        }
        assert!(validate(&bad, &fixture).is_err(), "{field}");
    }
}
#[devkit::test]
fn inside_outside_record_and_broken_chain_cuts_are_refused() {
    let (snapshot, fixture) = zsh();
    for field in [
        "inside", "outside", "link", "cut", "gap", "overlap", "cycle",
    ] {
        let mut bad = snapshot.clone();
        match field {
            "inside" => {
                bad.cursor["through"] = json!(bad.parts[&Uuid::from_u128(2)].bytes.len() - 1)
            }
            "outside" => {
                bad.cursor["through"] = json!(bad.parts[&Uuid::from_u128(2)].bytes.len() + 1)
            }
            "link" => {
                bad.parts.get_mut(&Uuid::from_u128(2)).unwrap().previous = Some(Uuid::from_u128(99))
            }
            "cut" => {
                bad.parts.get_mut(&Uuid::from_u128(2)).unwrap().manifest["previous_through"] =
                    json!(1)
            }
            "gap" => {
                bad.parts.get_mut(&Uuid::from_u128(2)).unwrap().manifest["file_offset"] =
                    json!(bad.parts[&Uuid::from_u128(1)].bytes.len() + 1)
            }
            "overlap" => {
                bad.parts.get_mut(&Uuid::from_u128(2)).unwrap().manifest["file_offset"] = json!(0)
            }
            _ => {
                bad.parts.get_mut(&Uuid::from_u128(1)).unwrap().previous = Some(Uuid::from_u128(2))
            }
        }
        assert!(validate(&bad, &fixture).is_err(), "{field}");
    }
}
#[devkit::test]
fn same_maximum_does_not_hide_a_missing_native_row() {
    let (mut bad, fixture) = atuin();
    let material = Uuid::from_u128(1);
    let bytes = &mut bad.parts.get_mut(&material).unwrap().bytes;
    let first = bytes.iter().position(|b| *b == b'\n').unwrap() + 1;
    bytes.drain(..first);
    bad.cursor["through"] = json!(bytes.len());
    bad.facts.remove(0);
    bad.facts[0].anchor = 0;
    assert!(
        validate(&bad, &fixture)
            .unwrap_err()
            .contains("missing rows")
    );
}
#[devkit::test]
fn memory_and_full_carry_are_checked_independently() {
    let (snapshot, fixture) = atuin();
    for field in ["bytes", "owner", "part", "counter"] {
        let mut bad = snapshot.clone();
        match field {
            "bytes" => bad.memory.as_mut().unwrap().1[9] ^= 1,
            "owner" => bad.memory.as_mut().unwrap().0 = Uuid::from_u128(99),
            "part" => bad.cursor["memory_part"] = json!(99),
            _ => bad.cursor["changed_since_memory"] = json!(1),
        }
        assert!(validate(&bad, &fixture).is_err(), "{field}");
    }
    let (mut bad, fixture) = zsh();
    bad.carry["counts"][0][2] = json!(2);
    assert!(validate(&bad, &fixture).unwrap_err().contains("carry"));
}
#[devkit::test]
fn unrecorded_rewrite_and_unknown_cursor_fields_remain_semantic() {
    let (snapshot, fixture) = zsh();
    let mut bad = snapshot.clone();
    bad.cursor["unrecorded_rewrite"] = json!("rewritten");
    assert!(validate(&bad, &fixture).is_err());
    let mut bad = snapshot;
    bad.cursor["future_native_field"] = json!(1);
    assert!(validate(&bad, &fixture).is_err());
}

#[devkit::test]
fn recaptured_same_input_repeat_has_exact_prior_reading_authority() {
    let (original, fixture) = atuin();
    let mut repeated = original.clone();
    let root = Uuid::from_u128(1);
    let tail = Uuid::from_u128(2);
    let root_part = &original.parts[&root];
    let last = frames(&root_part.bytes, false)[1];
    let bytes = root_part.bytes[last.0..last.1].to_vec();
    let mut manifest = root_part.manifest.clone();
    manifest["previous_through"] = json!(root_part.bytes.len());
    repeated.parts.insert(
        tail,
        Part {
            id: tail,
            input: original.input,
            index: 1,
            previous: Some(root),
            manifest,
            bytes: bytes.clone(),
        },
    );
    repeated.cursor["material"] = json!(tail);
    repeated.cursor["part_index"] = json!(1);
    repeated.cursor["through"] = json!(bytes.len());
    repeated.cursor["memory_part"] = json!(1);
    repeated.memory.as_mut().unwrap().0 = tail;
    // No new event exists at (tail, 0): the prior exact same-input reading
    // proves this legitimately suppressed repeat, including typed identity.
    assert_eq!(
        validate(&original, &fixture).unwrap(),
        validate(&repeated, &fixture).unwrap()
    );
    let mut bad = repeated.clone();
    bad.facts[1].payload["native_id"] = json!("different-native-row");
    assert!(validate(&bad, &fixture).is_err());
    let mut bad = repeated;
    bad.parts.get_mut(&tail).unwrap().input = Uuid::from_u128(99);
    assert!(
        validate(&bad, &fixture)
            .unwrap_err()
            .contains("another input")
    );
}

#[devkit::test]
fn foreign_database_generation_and_unproved_record_selection_are_refused() {
    let (snapshot, fixture) = atuin();
    let mut bad = snapshot.clone();
    bad.parts.get_mut(&Uuid::from_u128(1)).unwrap().manifest["database_generation"] =
        json!("other:38:7");
    assert!(validate(&bad, &fixture).unwrap_err().contains("generation"));
    let mut bad = snapshot;
    bad.parts.get_mut(&Uuid::from_u128(1)).unwrap().manifest["records"] = json!([[0, 1]]);
    assert!(validate(&bad, &fixture).unwrap_err().contains("selective"));
}
