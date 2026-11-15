//! A built-in source's binding path chooses the Live lane; explicit EOF reads
//! stay in Bulk. Queue receives establish ordering without timing sleeps.
use super::*;
use crate::binding_reader::{self, BindingReader, Rounds};
use crate::material_access::{Access, Sink};
use crate::stage::ReadMode;
use sinex_source_core::drivers::live_stream::{Rotation, SpoolWriter};
use sinex_source_core::{Binding, Settings};
use sinex_sources::sources::journald;
use sqlx::Connection;

#[devkit::test]
async fn live_binding_precedes_derived_manual_stage_is_bulk(db: devkit::pg::TestDatabase) {
    for mode in [ReadMode::Once, ReadMode::ToEnd] {
        let dir = tempfile::tempdir().unwrap();
        let spool = dir.path().join("spool");
        let mut capture = SpoolWriter::open(
            &spool,
            Rotation {
                max_bytes: 1,
                max_age: std::time::Duration::from_secs(3600),
            },
        )
        .unwrap();
        for i in 0..2 {
            capture.append(Timestamp::now(),format!("{{\"__CURSOR\":\"synthetic-{mode:?}-{i}\",\"MESSAGE\":\"message{i}\",\"__REALTIME_TIMESTAMP\":\"1700000000000000\"}}").as_bytes()).unwrap();
        }
        capture.sync().unwrap();
        let url = db.url(None);
        let read = sqlx::PgConnection::connect(&url).await.unwrap();
        let mut writer_connection = claim(sqlx::PgConnection::connect(&url).await.unwrap())
            .await
            .unwrap();
        crate::catalog::install_built_in_sources(&mut writer_connection)
            .await
            .unwrap();
        let blobs = sinex_store::Blobs::open(&dir.path().join("store")).unwrap();
        let b = Binding::of(&journald::SPEC, &spool, Settings::new("synthetic"));
        let binding = b.name.clone();
        let (handle, mut queues) = channels();
        let h = handle.clone();
        let derived = tokio::spawn(async move {
            h.submit(
                Priority::Derived,
                batch(&format!("queued-derived-{mode:?}")),
            )
            .await
            .unwrap()
        });
        let queued = queues.derived.recv().await.unwrap();
        handle.derived.send(queued).await.unwrap();
        let h = handle.clone();
        let stored = blobs.clone();
        let reading = tokio::spawn(async move {
            let mut read = read;
            let mut readers = vec![BindingReader::new(&journald::FORMAT, b).unwrap()];
            let mut rounds = Rounds::default();
            let (_stop, stop) = tokio::sync::watch::channel(false);
            let result = binding_reader::read(
                &mut read,
                &stored,
                &h,
                &Sink(stored.clone()),
                &mut readers,
                &mut rounds,
                stop,
                Access::default(),
                mode,
                &mut |_, _| {},
            )
            .await;
            for (_, r) in result {
                r.unwrap();
            }
            rounds.end(&mut read).await.unwrap();
        });
        let queued = tokio::time::timeout(std::time::Duration::from_secs(5), async {
            match mode {
                ReadMode::Once => queues.live.recv().await,
                ReadMode::ToEnd => queues.bulk.recv().await,
            }
        })
        .await
        .expect("source must reach its declared lane")
        .unwrap();
        let mut observer = sqlx::PgConnection::connect(&url).await.unwrap();
        assert!(
            sinex_store::positions::load(&mut observer, &binding)
                .await
                .unwrap()
                .is_none(),
            "queued source has no ACK or committed cursor"
        );
        match mode {
            ReadMode::Once => handle.live.send(queued).await.unwrap(),
            ReadMode::ToEnd => handle.bulk.send(queued).await.unwrap(),
        };
        let writer = tokio::spawn(run(
            url,
            writer_connection,
            queues,
            Arc::new(sinex_store::NoDerivations),
            Some(blobs.clone()),
        ));
        reading.await.unwrap();
        let derived = derived.await.unwrap().commit_seq.unwrap().0;
        let (first,materials):(i64,i64)=sqlx::query_as("SELECT (SELECT min(e.commit_seq) FROM core.events e JOIN raw.materials m ON m.id=e.material_id JOIN raw.inputs i ON i.id=m.input_id WHERE i.binding=$1),(SELECT count(*) FROM raw.materials m JOIN raw.inputs i ON i.id=m.input_id WHERE i.binding=$1)")
            .bind(&binding).fetch_one(&mut observer).await.unwrap();
        match mode {
            ReadMode::Once => {
                assert!(
                    first < derived,
                    "a binding's Live job must precede queued Derived"
                );
                assert_eq!(materials, 1);
            }
            ReadMode::ToEnd => {
                assert!(derived < first, "explicit stage keeps Bulk ordering");
                assert_eq!(materials, 2, "manual read must drain every segment to EOF");
            }
        }
        drop(handle);
        writer.await.unwrap();
    }
}
