use super::*;
use serde_json::{Value, json};

fn config(json: Value) -> Config {
    serde_json::from_value(json).unwrap()
}

#[devkit::test]
fn plans_present_inputs_without_source_enables() {
    let dir = tempfile::tempdir().unwrap();
    for name in ["b.zip", "a.zip"] {
        std::fs::write(dir.path().join(name), b"x").unwrap();
    }
    let planned = plan(
        &config(json!({"sources": {"media.spotify-history": {"paths": [dir.path()]}}})),
        "h",
    )
    .unwrap();
    assert_eq!(planned.len(), 2);
    assert!(
        planned
            .iter()
            .all(|(format, binding)| format.name == "spotify"
                && binding.source == "media.spotify-history"
                && binding.settings.host == "h")
    );
}

#[devkit::test]
fn exclusions_cover_every_binding_without_requiring_a_run() {
    let dir = tempfile::tempdir().unwrap();
    std::fs::write(dir.path().join("a.zip"), b"x").unwrap();
    let config = config(
        json!({"sources":{"media.spotify-history":{"paths":[dir.path()]}},"exclude":{"media.spotify-history":{"reason":"fixture exclusion"}}}),
    );
    assert!(plan(&config, "h").unwrap().is_empty());
    let found = discovery::discover(&config.context("h")).unwrap();
    assert!(
        found
            .iter()
            .any(|input| input.source == "media.spotify-history"
                && input.availability == Availability::Excluded
                && input.reason.as_deref() == Some("fixture exclusion"))
    );
}

#[devkit::test]
fn rejects_unknown_names_invalid_formats_and_removed_source_enables() {
    assert!(
        plan(&config(json!({"sources":{"no.such":{}}})), "h")
            .unwrap_err()
            .contains("no.such")
    );
    assert!(
        plan(
            &config(json!({"sources":{"desktop.clipboard":{"format":"spotify"}}})),
            "h"
        )
        .unwrap_err()
        .contains("spotify")
    );
    assert!(
        serde_json::from_value::<Config>(json!({"sources":{"desktop.clipboard":{"enable":true}}}))
            .is_err()
    );
}

#[devkit::test]
fn replanning_adds_late_files_once_and_keeps_other_inputs_running() {
    let dir = tempfile::tempdir().unwrap();
    let config = config(
        json!({"sources":{"media.spotify-history":{"paths":[dir.path()]},"terminal.atuin-history":{"paths":["/missing-sinex-fixture"]}}}),
    );
    let mut bindings = Bindings::default();
    bindings.extend(&config, "h", &[]);
    assert!(bindings.readers.is_empty());
    std::fs::write(dir.path().join("a.zip"), b"x").unwrap();
    bindings.extend(&config, "h", &[]);
    bindings.extend(&config, "h", &[]);
    assert_eq!(bindings.readers.len(), 1);
    std::fs::write(dir.path().join("b.zip"), b"x").unwrap();
    bindings.extend(&config, "h", &[]);
    assert_eq!(bindings.readers.len(), 2);
    assert!(
        bindings
            .said
            .values()
            .any(|message| message.contains("missing-sinex-fixture"))
    );
}

#[devkit::test]
fn every_included_source_declares_emitted_types_before_input_arrives() {
    let types = emitted(&Config::default()).unwrap();
    assert!(types.contains(&"command.executed"));
    assert!(types.contains(&"lake.observed"));
    let configured = config(json!({"derivations":{"metric.hourly":false}}));
    assert!(!configured.runs("metric.hourly"));
    assert!(configured.runs("command.fusion"));
}

#[devkit::test]
async fn rediscovery_reuses_saved_alias_and_cursor_after_restart(db: devkit::pg::TestDatabase) {
    use sqlx::Connection;
    use std::os::unix::fs::symlink;
    let mut conn = PgConnection::connect(&db.url(None)).await.unwrap();
    let root = tempfile::tempdir().unwrap();
    let real = root.path().join("history");
    let alias = root.path().join("alias");
    std::fs::write(&real, b"").unwrap();
    symlink(&real, &alias).unwrap();
    let format = sources::format("zsh").unwrap();
    let original = (format.plan.unwrap())(&alias, &Settings::new("h"))
        .unwrap()
        .remove(0);
    crate::backfill::record(&mut conn, &original).await.unwrap();
    let cursor = json!({"synthetic_offset":17});
    sqlx::query("INSERT INTO raw.positions(binding,cursor,carry) VALUES($1,$2,'null')")
        .bind(&original.name)
        .bind(&cursor)
        .execute(&mut conn)
        .await
        .unwrap();
    let config = config(
        json!({"sources":{"terminal.zsh-history":{"paths":[real],"zone":"UTC","class":"guarded"}}}),
    );
    for _ in 0..2 {
        let saved = saved(&mut conn, "h").await.unwrap();
        let proposed = plan(&config, "h").unwrap();
        let planned = discovery::reconcile(proposed, &saved).unwrap();
        assert_eq!(planned.len(), 1);
        assert_eq!(planned[0].1.name, original.name);
        assert_eq!(planned[0].1.input, alias);
        assert_eq!(
            planned[0].1.settings.class,
            Some(sinex_model::Class::Guarded)
        );
        crate::backfill::record(&mut conn, &planned[0].1)
            .await
            .unwrap();
        assert_eq!(
            sinex_store::positions::load(&mut conn, &original.name)
                .await
                .unwrap()
                .unwrap()
                .0,
            cursor
        );
    }
    let count: i64 = sqlx::query_scalar("SELECT count(*) FROM raw.bindings")
        .fetch_one(&mut conn)
        .await
        .unwrap();
    assert_eq!(count, 1);
}

#[devkit::test]
async fn rediscovery_keeps_location_admissions_and_progress(db: devkit::pg::TestDatabase) {
    use crate::reader_context::ReaderContext;
    use std::os::unix::fs::symlink;
    let root = tempfile::tempdir().unwrap();
    let history = root.path().join("history");
    let alias = root.path().join("alias");
    std::fs::write(&history, b": 1700000000:0;echo synthetic\n").unwrap();
    symlink(&history, &alias).unwrap();
    let format = sources::format("zsh").unwrap();
    let original = (format.plan.unwrap())(&alias, &Settings::new("h"))
        .unwrap()
        .remove(0);
    let store = root.path().join("store");
    let url = db.url(None);
    let mut context = ReaderContext::open(&url, &store).await.unwrap();
    binding_reader::once(
        &mut context.read_conn,
        &context.blobs,
        &context.writer,
        "h",
        vec![BindingReader::new(format, original.clone()).unwrap()],
    )
    .await
    .unwrap();
    let position = sinex_store::positions::load(&mut context.read_conn, &original.name)
        .await
        .unwrap()
        .unwrap();
    context.close().await.unwrap();
    let configuration = config(json!({"sources":{"terminal.zsh-history":{"paths":[history]}}}));
    let mut context = ReaderContext::open(&url, &store).await.unwrap();
    let saved = saved(&mut context.read_conn, "h").await.unwrap();
    let planned = discovery::reconcile(plan(&configuration, "h").unwrap(), &saved).unwrap();
    assert_eq!(planned[0].1.name, original.name);
    let readers = planned
        .into_iter()
        .map(|(format, binding)| BindingReader::new(format, binding).unwrap())
        .collect();
    binding_reader::once(
        &mut context.read_conn,
        &context.blobs,
        &context.writer,
        "h",
        readers,
    )
    .await
    .unwrap();
    let count: i64 = sqlx::query_scalar("SELECT count(*) FROM core.events")
        .fetch_one(&mut context.read_conn)
        .await
        .unwrap();
    assert_eq!(count, 1, "rediscovery must not admit the location again");
    assert_eq!(
        sinex_store::positions::load(&mut context.read_conn, &original.name)
            .await
            .unwrap()
            .unwrap(),
        position
    );
    context.close().await.unwrap();
}
