use super::*;
use sinex_sources::sources::atuin;

#[tokio::test]
async fn whole_database_image_class_checks_other_bindings_before_opening_provider_data() {
    let (_db, url, _) = crate::testdb::fresh().await;
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("source.db");
    let provider = rusqlite::Connection::open(&path).unwrap();
    provider.execute_batch("CREATE TABLE history(id TEXT,timestamp INTEGER,duration INTEGER,exit INTEGER,command TEXT,cwd TEXT,session TEXT,hostname TEXT);").unwrap();
    let mut ctx = OneShot::open(&url, &dir.path().join("store"))
        .await
        .unwrap();
    sinex_store::privacy::set_source_class(
        &mut ctx.read_conn,
        sinex_sources::sources::activitywatch::SPEC.name,
        Class::Sealed,
        "synthetic",
    )
    .await
    .unwrap();
    let mut settings = Settings::new("synthetic");
    settings.options =
        json!({"sqlite_images":{"scope":"whole_database","class":"open","interval_secs":3600}});
    let binding = Binding::of(&atuin::SPEC, &path, settings);
    let mut b = Bound::new(&atuin::KIND, binding).unwrap();
    let alias=dir.path().join("alias.db");std::fs::hard_link(&path,&alias).unwrap();
    let known=[sinex_store::bindings::Stored {
        binding:"other-selected-table".into(),source:sinex_sources::sources::activitywatch::SPEC.name.into(),input:alias.display().to_string(),settings:serde_json::to_value(&b.binding.settings).unwrap(),
    }];
    let err = check_image_class(&mut ctx.read_conn,&mut b,&known)
    .await
    .unwrap_err();
    assert!(err.contains("source/field class"));
    // The check itself did not create a pending read view or material.
    b.binding.settings.options["sqlite_images"]["class"] = json!("sealed");
    b.reader.configure(&b.binding.settings).unwrap();
    check_image_class(&mut ctx.read_conn,&mut b,&known)
    .await
    .unwrap();
    ctx.close().await.unwrap();
}
