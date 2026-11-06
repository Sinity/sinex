use super::*;

fn boundary() -> Hold {
    Hold {
        id: sinex_model::OperationId(uuid::Uuid::now_v7()),
        actor: "synthetic-backup".into(),
        created_at: sinex_model::Timestamp::now(),
        released_at: None,
        commit_position: 7,
        blob_changes: 3,
        schema_version: 1,
    }
}

#[devkit::test]
async fn frozen_producer_cut_keeps_checkpoint_before_later_acknowledgment() {
    let broker = devkit::nats::test_nats().unwrap();
    let client = async_nats::connect(broker.url()).await.unwrap();
    let source = Arc::new(tempfile::tempdir().unwrap());
    std::fs::write(source.path().join("checkpoint"), b"before-core-cut").unwrap();
    std::fs::write(source.path().join("outbox"), b"stable-frame-id:raw-bytes").unwrap();
    let publication = Publication::default();
    let captured_source = source.clone();
    let participant = Participant::start(
        client.clone(),
        Subjects::sinex(),
        "capture".into(),
        publication.clone(),
        Arc::new(move |cancel| {
            let directory = tempfile::tempdir().map_err(|e| e.to_string())?;
            let files = capture_tree(captured_source.path(), directory.path(), &cancel)?;
            Ok(Captured { directory, files })
        }),
    )
    .await
    .unwrap();
    let cut = Frozen::freeze(client.clone(), Subjects::sinex(), "capture".into())
        .await
        .unwrap();
    // An ACK arriving after P may not retire raw bytes or advance this checkpoint.
    let later_source = source.clone();
    let (attempting, attempted) = tokio::sync::oneshot::channel();
    let advance = tokio::spawn(async move {
        attempting.send(()).unwrap();
        let _publication = publication.enter().await;
        std::fs::write(later_source.path().join("checkpoint"), b"after-core-cut").unwrap();
        std::fs::remove_file(later_source.path().join("outbox")).unwrap();
    });
    attempted.await.unwrap();
    let output = tempfile::tempdir().unwrap();
    let files = cut.acquire(&boundary(), output.path()).await.unwrap();
    assert_eq!(files.len(), 2);
    cut.release().await.unwrap();
    advance.await.unwrap();
    assert_eq!(
        std::fs::read(output.path().join("checkpoint")).unwrap(),
        b"before-core-cut"
    );
    assert_eq!(
        std::fs::read(output.path().join("outbox")).unwrap(),
        b"stable-frame-id:raw-bytes"
    );
    // Only captured producer bytes are restored alongside old core P. The later
    // native checkpoint cannot cause the retained frame to be skipped on restart.
    std::fs::write(
        source.path().join("checkpoint"),
        std::fs::read(output.path().join("checkpoint")).unwrap(),
    )
    .unwrap();
    assert_eq!(
        std::fs::read(source.path().join("checkpoint")).unwrap(),
        b"before-core-cut"
    );
    cut.release().await.unwrap();
    drop(participant);
}

#[devkit::test]
async fn producer_restart_rejects_old_freeze_instead_of_certifying_new_checkpoint() {
    let broker = devkit::nats::test_nats().unwrap();
    let client = async_nats::connect(broker.url()).await.unwrap();
    let callback: Arc<dyn Fn(Arc<AtomicBool>) -> Result<Captured, String> + Send + Sync> =
        Arc::new(|_cancel| {
            Ok(Captured {
                directory: tempfile::tempdir().map_err(|e| e.to_string())?,
                files: Vec::new(),
            })
        });
    let participant = Participant::start(
        client.clone(),
        Subjects::sinex(),
        "capture".into(),
        Publication::default(),
        callback.clone(),
    )
    .await
    .unwrap();
    let cut = Frozen::freeze(client.clone(), Subjects::sinex(), "capture".into())
        .await
        .unwrap();
    drop(participant);
    // Flush a new subscription only after the old task's abort has run.
    tokio::task::yield_now().await;
    let _restarted = Participant::start(
        client,
        Subjects::sinex(),
        "capture".into(),
        Publication::default(),
        callback,
    )
    .await
    .unwrap();
    let output = tempfile::tempdir().unwrap();
    assert!(
        cut.acquire(&boundary(), output.path())
            .await
            .unwrap_err()
            .contains("did not capture")
    );
}

#[devkit::test]
fn captured_native_bytes_survive_source_deletion_and_reject_symlinks() {
    let source = tempfile::tempdir().unwrap();
    let output = tempfile::tempdir().unwrap();
    std::fs::write(source.path().join("raw"), b"retained-source-bytes").unwrap();
    let files = capture_tree(source.path(), output.path(), &AtomicBool::new(false)).unwrap();
    std::fs::remove_file(source.path().join("raw")).unwrap();
    assert_eq!(
        std::fs::read(output.path().join("raw")).unwrap(),
        b"retained-source-bytes"
    );
    let captured = Captured {
        directory: output,
        files,
    };
    validate(&captured, &AtomicBool::new(false)).unwrap();
    std::os::unix::fs::symlink("missing", source.path().join("link")).unwrap();
    let refused = tempfile::tempdir().unwrap();
    assert!(capture_tree(source.path(), refused.path(), &AtomicBool::new(false)).is_err());
}

#[devkit::test]
fn cancellation_stops_a_native_copy_at_a_transfer_unit_without_a_finished_identity() {
    struct CancellingReader<'a> {
        bytes: std::io::Cursor<Vec<u8>>,
        flag: &'a AtomicBool,
    }
    impl Read for CancellingReader<'_> {
        fn read(&mut self, buffer: &mut [u8]) -> std::io::Result<usize> {
            let bytes = self.bytes.read(buffer)?;
            self.flag.store(true, Ordering::Release);
            Ok(bytes)
        }
    }
    let flag = AtomicBool::new(false);
    let mut input = CancellingReader {
        bytes: std::io::Cursor::new(vec![7; 128 * 1024]),
        flag: &flag,
    };
    let mut output = Vec::new();
    let error = copy_prefix(&mut input, &mut output, 128 * 1024, &flag).unwrap_err();
    assert!(error.contains("cancelled"));
    assert_eq!(output, vec![7; 64 * 1024]);
    let source = tempfile::tempdir().unwrap();
    let destination = tempfile::tempdir().unwrap();
    std::fs::write(source.path().join("raw"), b"owned bytes").unwrap();
    assert!(
        capture_tree(source.path(), destination.path(), &flag)
            .unwrap_err()
            .contains("cancelled")
    );
    assert!(!destination.path().join("raw").exists());
}

#[devkit::test]
async fn acknowledged_wait_markers_keep_native_sealing_outside_the_response_hash() {
    use sinex_protocol::replies::ReplyFrame;
    let broker = devkit::nats::test_nats().unwrap();
    let client = async_nats::connect(broker.url()).await.unwrap();
    let mut incoming = client.subscribe("backup.wait").await.unwrap();
    let mut observed = client.subscribe("_SINEX_BACKUP.capture.>").await.unwrap();
    client.flush().await.unwrap();
    let (finish, finished) = tokio::sync::oneshot::channel();
    let owner = client.clone();
    let server = tokio::spawn(async move {
        let message = incoming.next().await.unwrap();
        crate::replies::respond_json_wait(
            &owner,
            message.reply.unwrap().to_string(),
            async move {
                finished.await.unwrap();
                Some(serde_json::json!({"native_generation":"sealed"}))
            },
            Duration::from_secs(2),
        )
        .await
        .unwrap();
    });
    let observer = tokio::spawn(async move {
        while let Some(message) = observed.next().await {
            if matches!(
                serde_json::from_slice::<ReplyFrame>(&message.payload),
                Ok(ReplyFrame::Waiting { .. })
            ) {
                finish.send(()).unwrap();
                return;
            }
        }
        panic!("native sealing never advertised its wait");
    });
    let value: serde_json::Value = crate::replies::request_inbox(
        &client,
        "backup.wait",
        "_SINEX_BACKUP.capture".into(),
        b"{}".to_vec(),
        Duration::from_secs(10),
    )
    .await
    .unwrap();
    assert_eq!(value, serde_json::json!({"native_generation":"sealed"}));
    observer.await.unwrap();
    server.await.unwrap();
}
