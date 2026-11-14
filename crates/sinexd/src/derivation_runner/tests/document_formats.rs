use super::*;
use sinex_derivations::{Input, PerEvent, Reads};
use sinex_model::EventPayload;
use sinex_model::event_types::library::{DocumentText, FileCopy};

fn copy(path: &str, content: &str, size: u64, anchor: u64) -> Event {
    let item_key = sinex_model::event_types::library::item_key("box", "/library", path);
    let copy = FileCopy {
        scan: "synthetic-scan".into(),
        part_index: 0,
        item: sinex_model::event_types::library::item_id(&item_key),
        item_key,
        scanned_at: at(0),
        host: "box".into(),
        root: "/library".into(),
        path: format!("/library/{path}"),
        content: content.into(),
        size,
        sha256: String::new(),
        md5: String::new(),
        media_type: "text/plain".into(),
        width: None,
        height: None,
        duration_ms: None,
        stored: true,
        device: None,
        inode: None,
        modified_at: Some(at(0)),
        moved_from: None,
        move_candidates: vec![],
        previous_content: None,
        vanished_at: None,
    };
    Event::build(
        &copy,
        "library.scan",
        location(anchor),
        ReadingOrder(0),
        *blake3::hash(path.as_bytes()).as_bytes(),
        Class::Open,
    )
    .unwrap()
}

/// The predecessor's generic projection, with a small synthetic text instead
/// of constructing an oversized public fixture.
struct PreviousDocumentText;
impl PerEvent for PreviousDocumentText {
    const NAME: &'static str = "document.text";
    const VERSION: u32 = 1;
    const INPUTS: &'static [&'static str] = &[FileCopy::EVENT_TYPE];
    const OUTPUTS: &'static [&'static str] = &[DocumentText::EVENT_TYPE];
    const READS: Reads = Reads::Nothing;
    fn derive(&self, input: Input<'_>, _: &Env) -> Result<Vec<Event>, String> {
        let copy: FileCopy = input.event.payload_as().unwrap();
        let payload = DocumentText {
            record: serde_json::to_string(&(
                &input.event.source,
                &input.event.event_type,
                sinex_model::record::identity(input.event),
            ))
            .unwrap(),
            content: copy.content,
            path: copy.path,
            format: "text".into(),
            part: 0,
            title: None,
            text: "previous generic projection".into(),
            reader: "sinexd".into(),
            modified_at: copy.modified_at,
        };
        Ok(vec![
            Event::build(
                &payload,
                Self::NAME,
                Origin::Derived {
                    parents: vec![input.event.id],
                },
                ReadingOrder(0),
                input.event.record_hash,
                input.event.class,
            )
            .unwrap(),
        ])
    }
}

#[devkit::test]
async fn structured_document_inputs_skip_blob_reads_and_version_replay_withdraws_projections(
    db: devkit::pg::TestDatabase,
) {
    let w = World::new(db).await;
    let directory = tempfile::tempdir().unwrap();
    let blobs = Blobs::open(directory.path()).unwrap();
    let original = b"{\"text\":\"original structured content\"}\n";
    let structured_blob = blobs.put(original).unwrap();
    let plain = b"complete ordinary document";
    let plain_blob = blobs.put(plain).unwrap();
    // There is deliberately no readable blob for this metadata-large input.
    // Fetching it would fail; no large allocation is needed to test selection.
    let absent = format!(
        "blake3:{}",
        blake3::hash(b"unreadable fixture blob").to_hex()
    );
    let events = vec![
        copy("dataset.jsonl", &absent, 348_589_691, 0),
        copy("records.NDJSON", &structured_blob, original.len() as u64, 1),
        copy("notes.txt", &plain_blob, plain.len() as u64, 2),
    ];
    w.admit(events.clone()).await;
    let mut old = w
        .driver_with(vec![], vec![Box::new(PreviousDocumentText)])
        .await;
    old.catch_up().await.unwrap();
    assert_eq!(w.live::<DocumentText>().await.len(), 3);
    old.end_runs().await.unwrap();
    let mut current = w
        .driver_with(
            vec![],
            vec![Box::new(crate::document_text::DocumentText {
                pdf_reader: None,
            })],
        )
        .await
        .with_blobs(blobs.clone());
    let comparison = current.compare("document.text", None, 10).await.unwrap();
    assert_eq!(comparison.removed, 2);
    current
        .switch("document.text", comparison.id.unwrap(), "test")
        .await
        .unwrap();
    current.catch_up().await.unwrap();
    let outputs = w.live::<DocumentText>().await;
    assert_eq!(outputs.len(), 1);
    assert_eq!(outputs[0].text.as_bytes(), plain);
    assert_eq!(current.counts["document.text"].failed, 0);
    assert_eq!(w.live::<FileCopy>().await.len(), 3);
    assert_eq!(blobs.read(&structured_blob).unwrap(), original);
    assert_eq!(
        w.count("SELECT commit_seq FROM derivation.positions WHERE derivation='document.text'")
            .await,
        w.count("SELECT max(commit_seq) FROM core.events WHERE event_type='library.copy'")
            .await
    );
    current.catch_up().await.unwrap();
    assert_eq!(w.live::<DocumentText>().await, outputs);
    assert_eq!(blobs.read(&structured_blob).unwrap(), original);
}
