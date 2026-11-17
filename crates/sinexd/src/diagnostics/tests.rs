use super::*;
use crate::binding_reader::{BindingReader, once};
use crate::reader_context::ReaderContext;
use sinex_source_core::Settings;
use sinex_sources::sources::documents;

async fn capture(ctx: &mut ReaderContext, root: &Path) -> sinex_source_core::Binding {
    let mut settings = Settings::new("synthetic-host");
    settings.options = json!({"locate_media":true});
    let binding = (documents::FORMAT.plan.unwrap())(root, &settings)
        .unwrap()
        .remove(0);
    once(
        &mut ctx.read_conn,
        &ctx.blobs,
        &ctx.writer,
        vec![BindingReader::new(&documents::FORMAT, binding.clone()).unwrap()],
    )
    .await
    .unwrap();
    binding
}

async fn check_missing(database: &str, store: &Path, missing: usize) {
    // Exercise the actual doctor consumer, not just its selection helper.
    let output = doctor(database, store).await;
    let check = output["checks"]
        .as_array()
        .unwrap()
        .iter()
        .find(|check| check["check"] == "located_files")
        .expect("doctor must report located files");
    assert_eq!(check["ok"], true, "{output}");
    assert!(
        check["detail"]
            .as_str()
            .unwrap()
            .starts_with(&format!("{missing} located files ")),
        "{check}"
    );
}

#[devkit::test]
async fn doctor_counts_located_files_using_current_paths_after_replay(
    db: devkit::pg::TestDatabase,
) {
    use std::os::unix::ffi::OsStringExt;
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().join("downloads");
    let store = dir.path().join("store");
    std::fs::create_dir(&root).unwrap();
    let media = |tag| {
        let mut bytes = b"\0\0\0\x20ftypisom".to_vec();
        bytes.push(tag);
        bytes
    };
    std::fs::write(root.join("a.mp4"), media(b'a')).unwrap();
    std::fs::write(root.join("b.mp4"), media(b'a')).unwrap();
    let raw = std::ffi::OsString::from_vec(b"raw-\xff.mp4".to_vec());
    // These two names have equal display spelling but distinct native paths.
    std::fs::write(root.join(&raw), media(b'c')).unwrap();
    std::fs::write(root.join("raw-%FF.mp4"), media(b'd')).unwrap();
    let url = db.url(None);
    let mut ctx = ReaderContext::open(
        &url,
        &store,
        sinex_model::time_settings::TimeSettings::new("Europe/Warsaw", 4, 0).unwrap(),
    )
    .await
    .unwrap();
    let binding = capture(&mut ctx, &root).await;
    let original: uuid::Uuid = sqlx::query_scalar!("SELECT m.id FROM raw.materials m JOIN raw.inputs i ON i.id=m.input_id WHERE i.binding=$1 ORDER BY m.id LIMIT 1", binding.name)
        .fetch_one(&mut ctx.read_conn).await.unwrap();
    check_missing(&url, &store, 0).await;
    std::fs::remove_file(root.join("a.mp4")).unwrap();
    check_missing(&url, &store, 0).await; // one remaining copy satisfies the same content
    std::fs::write(root.join("a.mp4"), media(b'b')).unwrap();
    capture(&mut ctx, &root).await;
    std::fs::remove_file(root.join("b.mp4")).unwrap();
    check_missing(&url, &store, 1).await; // old a cannot satisfy its former content
    let replayed = crate::replay::run(
        &mut ctx.read_conn,
        &ctx.blobs,
        &ctx.writer,
        crate::replay::Options {
            scope: crate::replay::Scope {
                materials: vec![original],
                ..Default::default()
            },
            compare: false,
            resume: None,
            actor: "synthetic".into(),
            unlock: None,
        },
    )
    .await
    .unwrap();
    assert_eq!(replayed.failures, 0);
    assert!(replayed.admitted > 0);
    check_missing(&url, &store, 1).await; // replay processing time cannot outrank acquisition
    std::fs::write(root.join("a.mp4"), b"ordinary stored text").unwrap();
    capture(&mut ctx, &root).await;
    check_missing(&url, &store, 2).await; // newest non-located reading also shadows old media
    std::fs::remove_file(root.join("raw-%FF.mp4")).unwrap();
    check_missing(&url, &store, 3).await; // raw-byte path still exists, unlike its display twin
    ctx.close().await.unwrap();
}
