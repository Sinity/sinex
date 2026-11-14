use super::*;
use serde_json::{Value, json};

fn config(json: Value) -> Config {
    serde_json::from_value(json).unwrap()
}

async fn backup_binding_anchor(
    conn: &mut PgConnection,
    binding: &Binding,
    parser: &str,
) -> sinex_model::MaterialId {
    sinex_store::bindings::save(
        conn,
        &sinex_store::bindings::Stored {
            binding: binding.name.clone(),
            source: binding.source.clone(),
            input: binding.input.to_str().unwrap().into(),
            settings: serde_json::to_value(&binding.settings).unwrap(),
        },
    )
    .await
    .unwrap();
    let input = uuid::Uuid::now_v7();
    let material = sinex_model::MaterialId(uuid::Uuid::now_v7());
    let hash = format!("blake3:{}", blake3::hash(material.0.as_bytes()).to_hex());
    sqlx::query("INSERT INTO core.blobs(hash,size,status) VALUES($1,0,'present')")
        .bind(&hash)
        .execute(&mut *conn)
        .await
        .unwrap();
    sqlx::query("INSERT INTO raw.inputs(id,binding,kind,locator,host) VALUES($1,$2,'file',$3,$4)")
        .bind(input)
        .bind(&binding.name)
        .bind(binding.input.to_str().unwrap())
        .bind(&binding.settings.host)
        .execute(&mut *conn)
        .await
        .unwrap();
    sqlx::query("INSERT INTO raw.materials(id,input_id,blob,length,manifest,parser,parser_version) VALUES($1,$2,$3,0,'{}',$4,1)")
        .bind(material.0).bind(input).bind(hash).bind(parser).execute(&mut *conn).await.unwrap();
    sqlx::query(
        "INSERT INTO raw.positions(binding,cursor,carry,committed_seq) VALUES($1,$2,'{}',0)",
    )
    .bind(&binding.name)
    .bind(json!({"material":material,"through":0}))
    .execute(&mut *conn)
    .await
    .unwrap();
    material
}

#[devkit::test]
async fn backup_keeps_same_named_native_spool_separate_from_historical_lake_file(
    db: devkit::pg::TestDatabase,
) {
    let mut conn = PgConnection::connect(&db.url(None)).await.unwrap();
    let root = tempfile::tempdir().unwrap();
    let historical = root.path().join("retired.jsonl");
    std::fs::write(&historical, b"original retired capture\n").unwrap();
    let specs = sources::specs();
    let lake = specs
        .iter()
        .find(|spec| spec.name == "desktop.clipboard" && spec.parser.starts_with("sinnix-lake:"))
        .unwrap();
    let live = &sources::clipboard::SPEC;
    let old = Binding::of(lake, &historical, Settings::new("synthetic-host"));
    backup_binding_anchor(&mut conn, &old, lake.parser).await;
    let spool = root.path().join("native-spool");
    std::fs::create_dir(&spool).unwrap();
    let native = Binding::of(live, &spool, Settings::new("synthetic-host"));
    let material = backup_binding_anchor(&mut conn, &native, live.parser).await;
    sqlx::query("UPDATE raw.positions SET cursor=$2 WHERE binding=$1")
        .bind(&native.name)
        .bind(json!({"segment":0,"inner":{},"part_base":0,"last_material":material}))
        .execute(&mut conn)
        .await
        .unwrap();
    let selected = backup_bindings(
        &mut conn,
        vec![native.clone(), old.clone()],
        "synthetic-host",
    )
    .await
    .unwrap();
    assert_eq!(selected, vec![native.clone()]);
    assert_eq!(
        std::fs::read(&historical).unwrap(),
        b"original retired capture\n"
    );
    // Retained native state remains an obligation even when no longer configured.
    assert_eq!(
        backup_bindings(&mut conn, vec![], "synthetic-host")
            .await
            .unwrap(),
        vec![native]
    );
}

#[devkit::test]
async fn backup_refuses_unresolved_or_contradictory_retained_binding_identity(
    db: devkit::pg::TestDatabase,
) {
    let mut conn = PgConnection::connect(&db.url(None)).await.unwrap();
    let root = tempfile::tempdir().unwrap();
    let binding = Binding::of(
        &sources::clipboard::SPEC,
        root.path(),
        Settings::new("synthetic-host"),
    );
    let material =
        backup_binding_anchor(&mut conn, &binding, "sinnix-lake:desktop.clipboard").await;
    assert!(
        sinex_store::bindings::material_parser(&mut conn, "another-binding", material)
            .await
            .unwrap()
            .is_none()
    );
    assert!(
        backup_bindings(&mut conn, vec![binding.clone()], "synthetic-host")
            .await
            .unwrap_err()
            .to_string()
            .contains("disagrees")
    );
    sqlx::query("UPDATE raw.positions SET cursor='{}' WHERE binding=$1")
        .bind(&binding.name)
        .execute(&mut conn)
        .await
        .unwrap();
    assert!(
        backup_bindings(&mut conn, vec![], "synthetic-host")
            .await
            .unwrap_err()
            .to_string()
            .contains("no material evidence")
    );
    assert!(
        backup_bindings(&mut conn, vec![binding.clone()], "synthetic-host")
            .await
            .unwrap_err()
            .to_string()
            .contains("no material evidence")
    );
    sqlx::query("UPDATE raw.positions SET cursor=$2 WHERE binding=$1")
        .bind(&binding.name)
        .bind(json!({"material":uuid::Uuid::now_v7()}))
        .execute(&mut conn)
        .await
        .unwrap();
    assert!(
        backup_bindings(&mut conn, vec![], "synthetic-host")
            .await
            .unwrap_err()
            .to_string()
            .contains("no parser evidence")
    );
}

#[devkit::test]
fn plans_present_inputs_without_source_enables() {
    let dir = tempfile::tempdir().unwrap();
    for name in ["b.zip", "a.zip"] {
        std::fs::write(dir.path().join(name), b"x").unwrap();
    }
    let planned = plan(
        &config(json!({"discovery": {"inputs": {"spotify": [dir.path()]}}})),
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
        json!({"discovery":{"inputs":{"spotify":[dir.path()]}},"exclude":{"media.spotify-history":{"reason":"fixture exclusion"}}}),
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
fn rejects_unknown_names_input_formats_and_removed_source_settings() {
    assert!(
        plan(&config(json!({"sources":{"no.such":{}}})), "h")
            .unwrap_err()
            .to_string()
            .contains("no.such")
    );
    assert!(
        plan(
            &config(json!({"discovery":{"inputs":{"no-such-format":["/x"]}}})),
            "h"
        )
        .unwrap_err()
        .to_string()
        .contains("no-such-format")
    );
    // Where a source reads is found by discovery, never configured per source.
    for removed in [
        json!({"enable":true}),
        json!({"paths":["/x"]}),
        json!({"format":"spotify"}),
    ] {
        assert!(
            serde_json::from_value::<Config>(json!({"sources":{"desktop.clipboard":removed}}))
                .is_err()
        );
    }
}

#[devkit::test]
fn replanning_adds_late_files_once_and_keeps_other_inputs_running() {
    let dir = tempfile::tempdir().unwrap();
    let config = config(
        json!({"discovery":{"inputs":{"spotify":[dir.path()],"atuin":["/missing-sinex-fixture"]}}}),
    );
    let mut bindings = Bindings::default();
    bindings.extend(&config, "h", &[], &mut Default::default());
    assert!(bindings.readers.is_empty());
    std::fs::write(dir.path().join("a.zip"), b"x").unwrap();
    bindings.extend(&config, "h", &[], &mut Default::default());
    bindings.extend(&config, "h", &[], &mut Default::default());
    assert_eq!(bindings.readers.len(), 1);
    std::fs::write(dir.path().join("b.zip"), b"x").unwrap();
    bindings.extend(&config, "h", &[], &mut Default::default());
    assert_eq!(bindings.readers.len(), 2);
    assert!(
        bindings
            .said
            .values()
            .any(|message| message.contains("missing-sinex-fixture"))
    );
}

#[devkit::test]
fn every_included_source_declares_its_native_version_before_input_arrives() {
    let declarations = crate::catalog::source_declarations();
    for spec in sources::specs() {
        let declaration = declarations
            .iter()
            .find(|declaration| {
                declaration.source.name == spec.name
                    && declaration.source.version == spec.parser_version
            })
            .expect("every native specification has its exact declaration version");
        for event_type in spec.event_types {
            assert!(
                declaration
                    .kinds
                    .iter()
                    .any(|kind| kind.kind.name == *event_type),
                "{} v{} omits {event_type}",
                spec.name,
                spec.parser_version
            );
        }
    }
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
    crate::stage::record(&mut conn, &original).await.unwrap();
    let cursor = json!({"synthetic_offset":17});
    sqlx::query(
        "INSERT INTO raw.positions(binding,cursor,carry,committed_seq) VALUES($1,$2,'null',0)",
    )
    .bind(&original.name)
    .bind(&cursor)
    .execute(&mut conn)
    .await
    .unwrap();
    let config = config(
        json!({"discovery":{"inputs":{"zsh":[real]}},"sources":{"terminal.zsh-history":{"zone":"UTC","class":"guarded"}}}),
    );
    for _ in 0..2 {
        let saved = saved(&mut conn, &db.url(None), "h").await.unwrap();
        let proposed = plan(&config, "h").unwrap();
        let planned = discovery::reconcile(proposed, &saved).bindings;
        assert_eq!(planned.len(), 1);
        assert_eq!(planned[0].1.name, original.name);
        assert_eq!(planned[0].1.input, alias);
        assert_eq!(
            planned[0].1.settings.class,
            Some(sinex_model::Class::Guarded)
        );
        crate::stage::record(&mut conn, &planned[0].1)
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
    let mut context = ReaderContext::open(
        &url,
        &store,
        sinex_model::time_settings::TimeSettings::new("Europe/Warsaw", 4, 0)
            .expect("fixture day setting"),
    )
    .await
    .unwrap();
    binding_reader::once(
        &mut context.read_conn,
        &context.blobs,
        &context.writer,
        vec![BindingReader::new(format, original.clone()).unwrap()],
    )
    .await
    .unwrap();
    let position = sinex_store::positions::load(&mut context.read_conn, &original.name)
        .await
        .unwrap()
        .unwrap();
    context.close().await.unwrap();
    let configuration = config(json!({"discovery":{"inputs":{"zsh":[history]}}}));
    let mut context = ReaderContext::open(
        &url,
        &store,
        sinex_model::time_settings::TimeSettings::new("Europe/Warsaw", 4, 0)
            .expect("fixture day setting"),
    )
    .await
    .unwrap();
    let saved = saved(&mut context.read_conn, &url, "h").await.unwrap();
    let planned = discovery::reconcile(plan(&configuration, "h").unwrap(), &saved).bindings;
    assert_eq!(planned[0].1.name, original.name);
    let readers = planned
        .into_iter()
        .map(|(format, binding)| BindingReader::new(format, binding).unwrap())
        .collect();
    binding_reader::once(
        &mut context.read_conn,
        &context.blobs,
        &context.writer,
        readers,
    )
    .await
    .unwrap();
    let count: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM core.events WHERE event_type = 'command.imported'",
    )
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

#[devkit::test]
fn conflicting_saved_aliases_leave_other_inputs_reading_and_retry_when_resolved() {
    use std::os::unix::fs::symlink;
    let root = tempfile::tempdir().unwrap();
    let input = root.path().join("history");
    let alias = root.path().join("alias");
    let independent = root.path().join("other-history");
    for path in [&input, &independent] {
        std::fs::write(path, b"").unwrap();
    }
    symlink(&input, &alias).unwrap();
    let format = sources::format("zsh").unwrap();
    let saved: Vec<_> = [&input, &alias]
        .into_iter()
        .map(|path| {
            (format.plan.unwrap())(path, &Settings::new("h"))
                .unwrap()
                .remove(0)
        })
        .collect();
    let config = config(json!({"discovery":{"inputs":{"zsh":[input, independent]}}}));
    let mut bindings = Bindings::default();
    bindings.extend(&config, "h", &saved, &mut Default::default());
    assert_eq!(bindings.readers.len(), 1);
    assert_eq!(bindings.readers[0].binding.input, independent);
    assert!(
        bindings
            .said
            .values()
            .any(|message| message.contains("unavailable: discovery conflict"))
    );
    bindings.extend(&config, "h", &saved[..1], &mut Default::default());
    assert_eq!(bindings.readers.len(), 2);
    bindings.extend(&config, "h", &saved, &mut Default::default());
    assert_eq!(
        bindings.readers.len(),
        1,
        "a newly conflicting input stops reading"
    );
}

#[devkit::test]
async fn saved_names_need_no_material_scan_and_a_bad_row_does_not_hide_other_bindings(
    db: devkit::pg::TestDatabase,
) {
    use sqlx::Connection;
    let mut conn = PgConnection::connect(&db.url(None)).await.unwrap();
    for (name, settings) in [
        ("valid", json!({"host":"h"})),
        ("invalid", json!({"host":7})),
    ] {
        sinex_store::bindings::save(
            &mut conn,
            &sinex_store::bindings::Stored {
                binding: name.into(),
                source: "terminal.zsh-history".into(),
                input: format!("/synthetic/{name}"),
                settings,
            },
        )
        .await
        .unwrap();
    }
    // A binding can be recorded before it produces any materials.
    let bindings = saved(&mut conn, &db.url(None), "h").await.unwrap();
    assert_eq!(bindings.len(), 1);
    assert_eq!(bindings[0].name, "valid");
}

#[devkit::test]
async fn saved_bindings_reconnect_after_the_database_connection_is_lost(
    db: devkit::pg::TestDatabase,
) {
    use sqlx::Connection;
    let url = db.url(None);
    let mut conn = PgConnection::connect(&url).await.unwrap();
    sinex_store::bindings::save(
        &mut conn,
        &sinex_store::bindings::Stored {
            binding: "saved-name".into(),
            source: "terminal.zsh-history".into(),
            input: "/synthetic/history".into(),
            settings: json!({"host":"h"}),
        },
    )
    .await
    .unwrap();
    let pid: i32 = sqlx::query_scalar("SELECT pg_backend_pid()")
        .fetch_one(&mut conn)
        .await
        .unwrap();
    let mut second_connection = PgConnection::connect(&url).await.unwrap();
    let terminated: bool = sqlx::query_scalar("SELECT pg_terminate_backend($1)")
        .bind(pid)
        .fetch_one(&mut second_connection)
        .await
        .unwrap();
    assert!(terminated);
    let bindings = saved(&mut conn, &url, "h").await.unwrap();
    assert_eq!(bindings.len(), 1);
    assert_eq!(bindings[0].name, "saved-name");
    let alive: i32 = sqlx::query_scalar("SELECT 1")
        .fetch_one(&mut conn)
        .await
        .unwrap();
    assert_eq!(alive, 1);
}

#[devkit::test]
fn tags_require_an_explicit_choice() {
    assert!(!Config::default().runs("tag.applier"));
    assert!(config(serde_json::json!({"derivations": {"tag.applier": true}})).runs("tag.applier"));
}

#[devkit::test]
fn disabling_the_tag_applier_supplies_empty_rules_for_cleanup() {
    let c = config(serde_json::json!({"derivations":{"tag.applier":false},
        "tags":[{"tag":"x","event_type":"command.imported"}]}));
    assert!(c.derivation_configuration().tags.is_empty());
    let c = config(serde_json::json!({"derivations":{"tag.applier":true},
        "tags":[{"tag":"x","event_type":"command.imported"}]}));
    assert_eq!(c.derivation_configuration().tags.len(), 1);
}

#[devkit::test]
fn activity_break_threshold_is_configured_and_zero_is_refused() {
    let c = config(serde_json::json!({"activity":{"break_seconds":600}}));
    assert_eq!(c.derivation_configuration().activity.break_seconds, 600);
    assert!(
        serde_json::from_value::<Config>(serde_json::json!({"activity":{"break_seconds":0}}))
            .is_err()
    );
}

#[devkit::test]
fn project_registry_reaches_the_versioned_derivation() {
    let c = config(serde_json::json!({"project_attribution":{"projects":[{
        "project":"example","common_dirs":["/captured/example/.git"]}]}}));
    let configuration = c.derivation_configuration();
    assert_eq!(
        configuration.project_attribution.projects[0]["project"],
        "example"
    );
}

#[devkit::test]
fn read_failures_keep_their_start_until_a_read_succeeds() {
    let failures = ReadFailures::default();
    note(&failures, "b", Some("database is locked"));
    let since = failures.lock().unwrap()["b"].since.clone();
    note(&failures, "b", Some("attempt to write a readonly database"));
    let failure = failures.lock().unwrap()["b"].clone();
    assert_eq!(failure.error, "attempt to write a readonly database");
    assert_eq!(failure.since, since);
    note(&failures, "b", None);
    assert!(failures.lock().unwrap().is_empty());
}

/// Plan `configuration` once, admit the coverage it observed, and return the
/// live spotify coverage readings as binding and state.
async fn admit(
    context: &mut crate::reader_context::ReaderContext,
    bindings: &mut Bindings,
    coverage: &mut binding_reader::coverage::Coverage,
    configuration: &Config,
) -> Vec<(String, String)> {
    bindings.extend(configuration, "h", &[], coverage);
    let mut errors = Vec::new();
    coverage
        .flush(
            &mut context.read_conn,
            &context.blobs,
            &context.writer,
            crate::material_access::Access::default(),
            &mut errors,
        )
        .await;
    let errors: Vec<_> = errors
        .into_iter()
        .filter_map(|(source, result)| result.err().map(|error| format!("{source}: {error}")))
        .collect();
    assert!(errors.is_empty(), "{errors:?}");
    sqlx::query_as(
        "SELECT e.payload->>'binding', e.payload->>'state' FROM core.events e
         WHERE e.event_type='source.coverage' AND e.payload->>'observed_source'='media.spotify-history'
           AND NOT EXISTS (SELECT 1 FROM audit.archived a WHERE a.event_id=e.id)
         ORDER BY 1",
    )
    .fetch_all(&mut context.read_conn)
    .await
    .unwrap()
}

#[devkit::test]
async fn an_input_not_present_yet_is_not_missing_coverage(db: devkit::pg::TestDatabase) {
    let root = tempfile::tempdir().unwrap();
    let absent = root.path().join("never-staged.zip");
    // A path under a regular file cannot be read: the input is unavailable.
    let blocker = root.path().join("blocker");
    std::fs::write(&blocker, b"").unwrap();
    let blocked = blocker.join("export.zip");
    let configuration =
        config(json!({"discovery":{"inputs":{"spotify":[absent.clone(), blocked.clone()]}}}));
    let mut context = crate::reader_context::ReaderContext::open(
        &db.url(None),
        &root.path().join("store"),
        sinex_model::time_settings::TimeSettings::new("Europe/Warsaw", 4, 0)
            .expect("fixture day setting"),
    )
    .await
    .unwrap();
    let mut coverage = binding_reader::coverage::Coverage::default();
    let mut bindings = Bindings::default();
    let key = |path: &std::path::Path| {
        format!(
            "input media.spotify-history spotify read {}",
            path.display()
        )
    };
    let live = admit(&mut context, &mut bindings, &mut coverage, &configuration).await;
    assert_eq!(live, [(key(&blocked), "missing".to_owned())]);
    // Planned again a minute later, nothing changed and nothing is written.
    admit(&mut context, &mut bindings, &mut coverage, &configuration).await;
    let readings: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM core.events WHERE event_type='source.coverage' AND payload->>'observed_source'='media.spotify-history'",
    )
    .fetch_one(&mut context.read_conn)
    .await
    .unwrap();
    assert_eq!(readings, 1);
    // Once the path can be looked at, the input is waiting, no longer missing.
    std::fs::remove_file(&blocker).unwrap();
    std::fs::create_dir(&blocker).unwrap();
    let live = admit(&mut context, &mut bindings, &mut coverage, &configuration).await;
    assert_eq!(
        live,
        [(key(&blocked), "recorded_unknown_coverage".to_owned())]
    );
    coverage.end(&mut context.read_conn).await.unwrap();
    context.close().await.unwrap();
}
