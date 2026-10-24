use super::*;
use sinex_store::sealed::{self, metadata};

#[test]
fn whole_erased_part_advances_replay_without_a_data_key_and_keeps_later_duplicate_identity() {
    let dir = tempfile::tempdir().unwrap();
    let blobs = Blobs::open(dir.path()).unwrap();
    let identity = sealed::Identity::generate();
    let recipient = identity.to_public();
    let authority =
        crate::sealed_access::Authority::new(identity, std::time::Duration::from_secs(60)).unwrap();
    let source = zsh::source("h".into());
    let first = MaterialId::new();
    let second = MaterialId::new();
    let erased_raw = b": 100:0;synthetic-repeat\n";
    let retained_raw = b": 100:9;synthetic-repeat\n";
    let old = sealed::new_key().unwrap();
    let fresh = sealed::new_key().unwrap();
    let names = sealed::MaterialSecrets::from_data_key(&old).naming_only();
    let token = source
        .erase_record(
            erased_raw,
            &Value::Null,
            &sinex_sources::RecordContext::none(),
        )
        .unwrap()
        .replay;
    let scope = metadata::Scope::Replay {
        source: source.spec().name.into(),
        material: first,
        parser: source.spec().parser.into(),
        parser_version: source.spec().parser_version as i32,
        length: erased_raw.len() as u64,
        whole: true,
    };
    let mut ciphertext = Vec::new();
    metadata::seal_replay(
        &mut ciphertext,
        &recipient,
        &scope,
        [metadata::ErasedRecord {
            start: 0,
            end: erased_raw.len() as u64,
            raw_digest: names.name_raw_record(erased_raw),
            transition: token,
        }],
        || Ok(()),
    )
    .unwrap();
    let replay_blob = blobs.put(&ciphertext).unwrap();
    let a = Stored {
        id: first.0,
        input: None,
        previous: None,
        part_index: 0,
        blob: blobs.put_sealed(erased_raw, &old).unwrap(),
        length: erased_raw.len() as i64,
        erased_replay_blob: Some(replay_blob),
        parser: source.spec().parser.into(),
        parser_version: source.spec().parser_version as i32,
        manifest: Value::Null,
        binding: None,
        host: Some("h".into()),
    };
    let b = Stored {
        id: second.0,
        input: None,
        previous: Some(first.0),
        part_index: 1,
        blob: blobs.put_sealed(retained_raw, &fresh).unwrap(),
        length: retained_raw.len() as i64,
        erased_replay_blob: None,
        parser: source.spec().parser.into(),
        parser_version: source.spec().parser_version as i32,
        manifest: Value::Null,
        binding: None,
        host: Some("h".into()),
    };
    let chain = [a, b];
    let parsers: Vec<Box<dyn Source>> = vec![
        Box::new(zsh::source("h".into())),
        Box::new(zsh::source("h".into())),
    ];
    let keys = HashMap::from([
        (first.0, (None, erased_raw.len() as u64, names)),
        (
            second.0,
            (
                Some(fresh.clone()),
                retained_raw.len() as u64,
                sealed::MaterialSecrets::from_data_key(&fresh),
            ),
        ),
    ]);
    // The retired data key is not held by any replay input or compatibility cache.
    drop(old);
    let (out, mut receive) = tokio::sync::mpsc::channel(8);
    let mut report = Read::default();
    read_chain_into(
        &blobs,
        &chain,
        &parsers,
        &BTreeSet::from([first.0, second.0]),
        &Scope::default(),
        OperationId::new(),
        &out,
        &mut report,
        &keys,
        Some(&authority),
    )
    .unwrap();
    drop(out);
    let mut events = Vec::new();
    while let Some(batch) = receive.blocking_recv() {
        for record in batch.unwrap().records {
            if let RecordOutcome::Events { events: found, .. } = record {
                events.extend(found);
            }
        }
    }
    assert_eq!(
        (report.materials, report.events_read, report.failures),
        (2, 1, 0)
    );
    assert_eq!(events.len(), 1);
    assert_eq!(events[0].location().unwrap().0.material, second);
    let payload = sealed::unseal_payload(&events[0].payload, &fresh.key).unwrap();
    assert_eq!(payload["command"], "synthetic-repeat");
    assert_eq!(payload["n"], 1);
    let mut wrong = chain.clone();
    wrong[0].parser_version += 1;
    let (out, _) = tokio::sync::mpsc::channel(8);
    assert!(
        read_chain_into(
            &blobs,
            &wrong,
            &parsers,
            &BTreeSet::new(),
            &Scope::default(),
            OperationId::new(),
            &out,
            &mut Read::default(),
            &keys,
            Some(&authority)
        )
        .is_err()
    );
}
