//! The real range reader must supply the same evidence as the complete fold.
use super::*;
use sinex_derivations::state_interval::StateIntervals;

fn key(state: &str, env: &Env) -> Value {
    json!({"state":state,"day":env.time_settings.day(at(0)).unwrap().to_string()})
}
fn environment() -> Env {
    Env::at(at(1), TimeSettings::new("Europe/Warsaw", 4, 0).unwrap())
}
fn observation(scope: &str, window: &str, seconds: i64, order: i64, source: &str) -> Event {
    let mut event = focus(window, seconds.max(0));
    event.ts_orig = sinex_model::TsOrig::known(at(seconds), sinex_model::TimeQuality::Captured);
    event.payload["received_at"] = json!(at(seconds));
    event.source = source.into();
    event.reading_order = ReadingOrder(order);
    event.payload["scope"] = json!(scope);
    event.record_hash = *blake3::hash(event.payload.to_string().as_bytes()).as_bytes();
    event
}
type IntervalContents = (String, Value, Vec<sinex_model::EventId>, [u8; 32]);
fn contents(computed: &Computed) -> Vec<IntervalContents> {
    let mut result = computed
        .outputs
        .iter()
        .map(|event| {
            let Origin::Derived { parents } = &event.origin else {
                panic!("derived interval")
            };
            (
                event.payload["interval_id"].as_str().unwrap().to_owned(),
                event.payload.clone(),
                parents.clone(),
                event.record_hash,
            )
        })
        .collect::<Vec<_>>();
    result.sort_by(|a, b| a.0.cmp(&b.0));
    result
}
async fn compare_full(conn: &mut PgConnection, state: &str) -> Computed {
    let env = environment();
    let key = key(state, &env);
    let query = StateIntervals.inputs(&key, &env).unwrap();
    let bounded = sinex_store::derivations::read_window(conn, &to_window(&query, env.now))
        .await
        .unwrap();
    let mut full = to_window(&query, env.now);
    full.range = None;
    full.start = None;
    full.end = None;
    full.unknown_times = true;
    full.before.clear();
    full.after.clear();
    let full = sinex_store::derivations::read_window(conn, &full)
        .await
        .unwrap();
    let expected = StateIntervals.compute(
        &key,
        &full.into_iter().map(|r| r.event).collect::<Vec<_>>(),
        &env,
    );
    let actual = StateIntervals.compute(
        &key,
        &bounded.into_iter().map(|r| r.event).collect::<Vec<_>>(),
        &env,
    );
    assert_eq!(
        contents(&actual),
        contents(&expected),
        "payloads, start identities and exact parents"
    );
    assert_eq!(actual.waiting, expected.waiting);
    actual
}
fn interval<'a>(computed: &'a Computed, subject: &str) -> &'a Event {
    computed
        .outputs
        .iter()
        .find(|event| event.payload["subject_id"] == subject)
        .unwrap()
}
fn has_parent(event: &Event, id: sinex_model::EventId) -> bool {
    let Origin::Derived { parents } = &event.origin else {
        return false;
    };
    parents.contains(&id)
}

#[devkit::test]
async fn focus_bounds_keep_recorded_ties_cross_source_scope_and_literal_fallback(
    db: devkit::pg::TestDatabase,
) {
    let w = World::with_keys(db, state_keys()).await;
    let a = observation("order", "order-a", 17 * 3600, 20, "fixture.order");
    let b = observation("order", "order-b", 17 * 3600, 10, "fixture.order");
    w.admit(vec![a.clone()]).await;
    w.admit(vec![b]).await;
    let mut refresh = observation("order", "order-a", 19 * 3600, 30, "fixture.order");
    refresh.payload["title"] = json!("later label");
    refresh.record_hash = *blake3::hash(refresh.payload.to_string().as_bytes()).as_bytes();
    let closer = observation("order", "order-b", 3 * 86400, 40, "fixture.order");
    let stable_a = observation("stable", "stable-a", 100, 1, "z-source");
    let stable_b = observation("stable", "stable-b", 100, 1, "a-source");
    w.admit(vec![stable_a.clone()]).await;
    w.admit(vec![stable_b]).await;
    let stable_refresh = observation("stable", "stable-a", 19 * 3600, 2, "a-source");
    let stable_close = observation("stable", "stable-b", 3 * 86400, 3, "z-source");
    let prefix = observation("prefix", "class:app", 200, 1, "fixture.prefix");
    let prefix_id = prefix.id;
    let mut prefix_refresh = observation("prefix", "", 19 * 3600, 2, "fixture.prefix");
    prefix_refresh.payload["window"] = Value::Null;
    prefix_refresh.payload["class"] = json!("app");
    prefix_refresh.record_hash =
        *blake3::hash(prefix_refresh.payload.to_string().as_bytes()).as_bytes();
    let prefix_close = observation("prefix", "other", 3 * 86400, 3, "fixture.prefix");
    w.admit(vec![
        refresh.clone(),
        closer.clone(),
        stable_refresh,
        stable_close.clone(),
        prefix,
        prefix_refresh.clone(),
        prefix_close.clone(),
    ])
    .await;
    let mut conn = PgConnection::connect(&w.url).await.unwrap();
    let actual = compare_full(&mut conn, "desktop.focus").await;
    for (subject, start, closing, refreshed) in [
        ("order-a", a.id, closer.id, refresh.id),
        ("stable-a", stable_a.id, stable_close.id, stable_a.id),
        ("class:app", prefix_id, prefix_close.id, prefix_refresh.id),
    ] {
        let output = interval(&actual, subject);
        assert_eq!(output.payload["ended_at"], json!(at(3 * 86400)));
        assert!(has_parent(output, start));
        assert!(has_parent(output, closing));
        assert!(has_parent(output, refreshed));
    }
    assert!(
        !actual
            .outputs
            .iter()
            .any(|event| event.payload["subject_id"] == "order-b")
    );
}

/// Neutral legacy rows exercise preserved shapes that current typed sources no
/// longer emit. They are inserted, never patched through the immutable guard.
async fn legacy(
    conn: &mut PgConnection,
    template: &Event,
    seconds: Option<i64>,
    order: i64,
    payload: Value,
    source: &str,
    key: Option<&str>,
) -> sinex_model::EventId {
    let id = sinex_model::EventId::new();
    let hash = blake3::hash(payload.to_string().as_bytes());
    sqlx::query("INSERT INTO core.events(id,event_type,source,run_id,material_id,anchor,output_index,ts_orig,ts_quality,commit_seq,payload,equivalence_key,reading_order,record_hash,kind_version,validation,class,record_state)
        SELECT $1,e.event_type,$2,e.run_id,$3,0,e.output_index,$4,CASE WHEN $4::timestamptz IS NULL THEN 'unknown' ELSE 'captured' END,e.commit_seq+$5,$6,$7,$5,$8,e.kind_version,e.validation,e.class,e.record_state FROM core.events e WHERE e.id=$9")
        .bind(id.0).bind(source).bind(MaterialId::new().0).bind(seconds.map(|s| at(s).0)).bind(order).bind(&payload).bind(key)
        .bind(hash.as_bytes().as_slice()).bind(template.id.0).execute(conn).await.unwrap();
    id
}

#[devkit::test]
async fn scope_and_workspace_scalar_bounds_keep_legacy_keys_unknown_times_and_afk(
    db: devkit::pg::TestDatabase,
) {
    let w = World::with_keys(db, state_keys()).await;
    let template = focus("template", 0);
    w.admit(vec![template.clone()]).await;
    let workspace = Event::build(
        &sinex_model::event_types::desktop::WorkspaceSwitched {
            scope: Some("template-workspace".into()),
            host: None,
            received_at: at(0),
            workspace_id: Some(0),
            workspace_name: None,
        },
        "fixture.workspace",
        location(0),
        ReadingOrder(0),
        [1; 32],
        Class::Open,
    )
    .unwrap();
    w.admit(vec![workspace.clone()]).await;
    let stop_template = Event::build(
        &sinex_model::event_types::PresenceSpan {
            scope: None,
            host: None,
            bucket: "template".into(),
            row: 1,
            started_at: at(0),
            ended_at: at(1),
            status: "afk".into(),
        },
        "fixture.afk",
        location(0),
        ReadingOrder(0),
        [2; 32],
        Class::Open,
    )
    .unwrap();
    w.admit(vec![stop_template.clone()]).await;
    let mut conn = PgConnection::connect(&w.url).await.unwrap();
    let base = json!({"window":"bucket-a","class":"app","title":"first","scope":false,"bucket":"legacy-bucket","received_at":at(0)});
    let opened = legacy(
        &mut conn,
        &template,
        Some(0),
        10,
        base.clone(),
        "fixture.bucket",
        None,
    )
    .await;
    let mut stopped = json!({"bucket":"legacy-bucket","scope":42,"status":"afk","started_at":at(2*86400),"ended_at":at(2*86400+1),"row":2});
    let afk = legacy(
        &mut conn,
        &stop_template,
        Some(2 * 86400),
        20,
        stopped.clone(),
        "other-source",
        None,
    )
    .await;
    // Arrays do not satisfy the exact interruption predicate.
    stopped["status"] = json!(["afk"]);
    legacy(
        &mut conn,
        &stop_template,
        Some(19 * 3600),
        15,
        stopped,
        "other-source",
        None,
    )
    .await;
    let mut unknown = base.clone();
    unknown["window"] = json!("unknown-clock");
    legacy(
        &mut conn,
        &template,
        None,
        12,
        unknown,
        "fixture.bucket",
        None,
    )
    .await;
    let native = json!({"native_id":"captured","window":"native-a","class":"app","title":"first","received_at":at(0)});
    legacy(
        &mut conn,
        &template,
        Some(0),
        30,
        native.clone(),
        "fixture.native",
        None,
    )
    .await;
    let mut next = native;
    next["window"] = json!("native-b");
    let native_close = legacy(
        &mut conn,
        &template,
        Some(3 * 86400),
        40,
        next,
        "fixture.native",
        None,
    )
    .await;
    let empty = json!({"scope":"","bucket":"first","window":"empty-a","class":"app","title":"first","received_at":at(0)});
    legacy(
        &mut conn,
        &template,
        Some(0),
        41,
        empty.clone(),
        "fixture.empty",
        None,
    )
    .await;
    let mut empty_close = empty;
    empty_close["bucket"] = json!("second");
    empty_close["window"] = json!("empty-b");
    let empty_close = legacy(
        &mut conn,
        &template,
        Some(2 * 86400),
        42,
        empty_close,
        "other-source",
        None,
    )
    .await;
    let unknown = json!({"scope":"unknown-subject","window":null,"class":null,"title":"unknown","received_at":at(0)});
    legacy(
        &mut conn,
        &template,
        Some(0),
        43,
        unknown.clone(),
        "fixture.unknown",
        None,
    )
    .await;
    let unknown_close = legacy(
        &mut conn,
        &template,
        Some(19 * 3600),
        44,
        unknown,
        "fixture.unknown",
        None,
    )
    .await;
    let one =
        json!({"scope":"scalar","workspace_id":1,"workspace_name":"first","received_at":at(0)});
    legacy(
        &mut conn,
        &workspace,
        Some(0),
        50,
        one.clone(),
        "fixture.workspace",
        None,
    )
    .await;
    let mut same = one.clone();
    same["workspace_id"] = json!("1");
    same["workspace_name"] = json!("refreshed");
    let scalar_refresh = legacy(
        &mut conn,
        &workspace,
        Some(19 * 3600),
        60,
        same,
        "fixture.workspace",
        None,
    )
    .await;
    let mut two = one;
    two["workspace_id"] = json!(2);
    let scalar_close = legacy(
        &mut conn,
        &workspace,
        Some(3 * 86400),
        70,
        two,
        "fixture.workspace",
        None,
    )
    .await;
    let complex = json!({"scope":"complex","workspace_id":[1,{"aa":true}],"workspace_name":"first","received_at":at(0)});
    legacy(
        &mut conn,
        &workspace,
        Some(0),
        80,
        complex.clone(),
        "fixture.workspace",
        None,
    )
    .await;
    let mut literal = complex.clone();
    literal["workspace_id"] = Value::Null;
    literal["workspace_name"] = json!("[1,{\"aa\":true}]");
    let complex_refresh = legacy(
        &mut conn,
        &workspace,
        Some(19 * 3600),
        90,
        literal,
        "fixture.workspace",
        None,
    )
    .await;
    let mut changed = complex;
    changed["workspace_id"] = json!(3);
    let complex_close = legacy(
        &mut conn,
        &workspace,
        Some(3 * 86400),
        100,
        changed,
        "fixture.workspace",
        None,
    )
    .await;
    let focus = compare_full(&mut conn, "desktop.focus").await;
    let bucket = interval(&focus, "bucket-a");
    assert_eq!(bucket.payload["ended_at"], json!(at(2 * 86400)));
    assert!(has_parent(bucket, opened) && has_parent(bucket, afk));
    assert!(has_parent(interval(&focus, "native-a"), native_close));
    assert!(has_parent(interval(&focus, "empty-a"), empty_close));
    let unknown = focus
        .outputs
        .iter()
        .find(|event| event.payload["attributes"]["scope"] == "unknown-subject")
        .unwrap();
    assert_eq!(unknown.payload["ended_at"], json!(at(19 * 3600)));
    assert!(has_parent(unknown, unknown_close));
    let spaces = compare_full(&mut conn, "desktop.workspace").await;
    let scalar = interval(&spaces, "workspace:1");
    assert_eq!(scalar.payload["ended_at"], json!(at(3 * 86400)));
    assert!(has_parent(scalar, scalar_refresh) && has_parent(scalar, scalar_close));
    let complex = interval(&spaces, "workspace:[1,{\"aa\":true}]");
    assert_eq!(complex.payload["ended_at"], json!(at(3 * 86400)));
    assert!(has_parent(complex, complex_refresh) && has_parent(complex, complex_close));
}

/// Version 3's fold, with the old defect of omitting a key's prior observation.
/// It leaves a closed historical output that waiting-key processing cannot fix.
struct MissingPrior;
impl Keyed for MissingPrior {
    const NAME: &'static str = "state.intervals";
    const VERSION: u32 = 3;
    const USES_TIME_SETTINGS: bool = true;
    const INPUTS: &'static [&'static str] = <StateIntervals as Keyed>::INPUTS;
    const OUTPUTS: &'static [&'static str] = <StateIntervals as Keyed>::OUTPUTS;
    fn keys_for(&self, input: &KeyView<'_>, env: &Env) -> Vec<Value> {
        StateIntervals.keys_for(input, env)
    }
    fn inputs(&self, selected: &Value, env: &Env) -> Option<InputQuery> {
        let mut query = StateIntervals.inputs(selected, env)?;
        if *selected == key("desktop.focus", env) {
            query.range = None;
            query.window = Some((
                env.time_settings
                    .day_start(env.time_settings.day(at(0)).ok()?)
                    .ok()?,
                Timestamp::from_unix_ns(at(19 * 3600).unix_ns() + 1_000).ok()?,
            ));
        }
        Some(query)
    }
    fn compute(&self, selected: &Value, inputs: &[Event], env: &Env) -> Computed {
        StateIntervals.compute(selected, inputs, env)
    }
    fn key_state(&self, selected: &Value, env: &Env) -> KeyState {
        StateIntervals.key_state(selected, env)
    }
}

#[devkit::test]
async fn explicit_key_recompute_preserves_version_three_history_and_other_keys(
    db: devkit::pg::TestDatabase,
) {
    use sinex_store::admission::DerivedScope;
    let w = World::with_keys(db, vec![Box::new(MissingPrior)]).await;
    let prior = observation("history", "same", -7 * 3600, 1, "fixture.history");
    let current = observation("history", "same", 0, 2, "fixture.history");
    let closing = observation("history", "other", 19 * 3600, 3, "fixture.history");
    w.admit(vec![prior.clone(), current.clone(), closing.clone()])
        .await;
    let mut old = w.driver_with(vec![Box::new(MissingPrior)], vec![]).await;
    old.clock = Some(at(4 * 86400));
    old.catch_up().await.unwrap();
    old.end_runs().await.unwrap();
    let mut conn = PgConnection::connect(&w.url).await.unwrap();
    let target = key("desktop.focus", &environment());
    let wrong = sinex_store::derivations::outputs(&mut conn, "state.intervals", &target)
        .await
        .unwrap();
    assert_eq!(wrong.len(), 1);
    assert_eq!(wrong[0].payload["ended_at"], json!(at(19 * 3600)));
    let untouched: Vec<uuid::Uuid> = sqlx::query_scalar("SELECT event_id FROM derivation.outputs WHERE derivation='state.intervals' AND key<>$1 ORDER BY event_id")
        .bind(&target).fetch_all(&mut conn).await.unwrap();
    let mut corrected = w.driver_with(state_keys(), vec![]).await;
    corrected.clock = Some(at(4 * 86400));
    corrected.catch_up().await.unwrap();
    assert_eq!(
        sinex_store::derivations::outputs(&mut conn, "state.intervals", &target)
            .await
            .unwrap()[0]
            .id,
        wrong[0].id,
        "startup does not silently retire or repair closed historical keys"
    );
    w.handle
        .submit(
            Priority::Derived,
            Batch {
                run: corrected.run_of("state.intervals").unwrap(),
                kind: BatchKind::Derived {
                    derivation: "state.intervals".into(),
                    scope: DerivedScope::Recompute {
                        keys: vec![target.clone()],
                    }
                    .to_value(),
                },
                records: vec![],
                archive: vec![],
            },
        )
        .await
        .unwrap();
    corrected.catch_up().await.unwrap();
    assert!(
        sinex_store::derivations::outputs(&mut conn, "state.intervals", &target)
            .await
            .unwrap()
            .is_empty()
    );
    let after: Vec<uuid::Uuid> = sqlx::query_scalar("SELECT event_id FROM derivation.outputs WHERE derivation='state.intervals' AND key<>$1 ORDER BY event_id")
        .bind(&target).fetch_all(&mut conn).await.unwrap();
    assert_eq!(after, untouched);
    let archived: bool =
        sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM audit.archived WHERE event_id=$1)")
            .bind(wrong[0].id.0)
            .fetch_one(&mut conn)
            .await
            .unwrap();
    assert!(archived);
    let retained: Value = sqlx::query_scalar("SELECT payload FROM core.events WHERE id=$1")
        .bind(wrong[0].id.0)
        .fetch_one(&mut conn)
        .await
        .unwrap();
    assert_eq!(retained, wrong[0].payload);
    for input in [prior, current, closing] {
        let live: bool =
            sqlx::query_scalar("SELECT NOT EXISTS(SELECT 1 FROM audit.archived WHERE event_id=$1)")
                .bind(input.id.0)
                .fetch_one(&mut conn)
                .await
                .unwrap();
        assert!(live);
    }
    assert_eq!(<StateIntervals as Keyed>::VERSION, 3);
    corrected.end_runs().await.unwrap();
}
