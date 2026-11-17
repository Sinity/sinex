use super::*;
use crate::material_access::{Access, MaterialAccess};
use sinex_model::Class;
use sinex_store::Blobs;
use std::io::Cursor;
use std::sync::{
    Arc,
    atomic::{AtomicBool, AtomicUsize, Ordering},
};

#[test]
fn checked_readers_honor_cancellation_for_reads_and_seeks() {
    let stop = Arc::new(AtomicBool::new(false));
    let stopped = stop.clone();
    let check: AdmissionCheck = Arc::new(move || {
        if stopped.load(Ordering::Acquire) {
            Err(crate::Error::new(
                command_surface::FailureKind::Cancelled,
                "capture cancelled",
            ))
        } else {
            Ok(())
        }
    });
    let mut first = CheckedReader {
        input: Cursor::new(b"first"),
        check: check.clone(),
    };
    let mut second = CheckedReader {
        input: Cursor::new(b"second"),
        check,
    };
    let mut byte = [0];
    first.read_exact(&mut byte).unwrap();
    assert_eq!(byte, [b'f']);
    second.seek(SeekFrom::Start(1)).unwrap();
    second.read_exact(&mut byte).unwrap();
    assert_eq!(byte, [b'e']);
    stop.store(true, Ordering::Release);
    assert!(first.read(&mut byte).is_err());
    assert!(second.read(&mut byte).is_err());
    assert!(first.seek(SeekFrom::Start(0)).is_err());
    assert!(second.seek(SeekFrom::Start(0)).is_err());
}

#[devkit::test]
async fn cancelled_owned_file_storage_preserves_original_and_retries_plain_and_sealed(
    db: devkit::pg::TestDatabase,
) {
    let mut conn = <sqlx::PgConnection as sqlx::Connection>::connect(&db.url(None))
        .await
        .unwrap();
    for class in [Class::Open, Class::Sealed] {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("original");
        let bytes: Vec<u8> = (0..(2 * 1024 * 1024)).map(|n| (n % 251) as u8).collect();
        std::fs::write(&path, &bytes).unwrap();
        let blobs = Blobs::open(&dir.path().join("store")).unwrap();
        let identity = sinex_sealing::Identity::generate();
        let recipient = identity.to_public();
        let material = MaterialAccess::new(
            blobs.clone(),
            class,
            Access {
                recipient: Some(&recipient),
                unlock: None,
            },
        )
        .unwrap();
        let calls = Arc::new(AtomicUsize::new(0));
        let checked = calls.clone();
        let error = material
            .sink(Arc::new(move || {
                if checked.fetch_add(1, Ordering::AcqRel) >= 3 {
                    Err(crate::Error::new(
                        command_surface::FailureKind::Cancelled,
                        "cancelled during storage",
                    ))
                } else {
                    Ok(())
                }
            }))
            .put_file(&path)
            .unwrap_err();
        assert!(error.contains("cancelled during storage"), "{error}");
        assert_eq!(calls.load(Ordering::Acquire), 4);
        assert_eq!(std::fs::read(&path).unwrap(), bytes);
        let blob = material.sink(Arc::new(|| Ok(()))).put_file(&path).unwrap();
        let mut reader = match &material.sealing {
            Some(sealed) => blobs
                .read_sealed(
                    &blobs.locate(&mut conn, &blob).await.unwrap(),
                    Some(bytes.len() as u64),
                    &sealed.key(&blob).unwrap(),
                    crate::material_access::sealing::read_limits(bytes.len() as u64).unwrap(),
                    || Ok(()),
                )
                .unwrap(),
            None => blobs
                .read_plain(
                    &blobs.locate(&mut conn, &blob).await.unwrap(),
                    Some(bytes.len() as u64),
                    || Ok(()),
                )
                .unwrap(),
        };
        let mut restored = Vec::new();
        reader.read_to_end(&mut restored).unwrap();
        assert_eq!(restored, bytes);
        assert_eq!(std::fs::read(path).unwrap(), bytes);
    }
}
