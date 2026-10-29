use super::*;
use sinex_model::{ArchiveRequest, Batch, BatchKind, EventId, MaterialId, OperationId, RunId};

#[devkit::test]
async fn crawl_never_records_history_urls_in_binding_settings(db: devkit::pg::TestDatabase) {
    let dir = tempfile::tempdir().unwrap();
    let mut ctx = crate::reader_context::ReaderContext::open(
        &db.url(None),
        dir.path(),
        sinex_model::time_settings::TimeSettings::new("Europe/Warsaw", 4, 0)
            .expect("fixture day setting"),
    )
    .await
    .unwrap();
    let run = uuid::Uuid::now_v7();
    sqlx::query("INSERT INTO core.runs(id,name,host) VALUES ($1,'test','synthetic')")
        .bind(run)
        .execute(&mut ctx.read_conn)
        .await
        .unwrap();
    let mut visits = Vec::new();
    for (sequence, name, domain) in [
        (1i64, "hidden", "allowed.test"),
        (2, "excised", "allowed.test"),
        (3, "blocked", "other.test"),
        (4, "shown", "allowed.test"),
    ] {
        let id = uuid::Uuid::now_v7();
        let material = MaterialId::new();
        let input = uuid::Uuid::now_v7();
        let url = format!("https://{domain}/{name}");
        let blob = ctx.blobs.put(url.as_bytes()).unwrap();
        sqlx::query("INSERT INTO core.blobs(hash,size,status) VALUES ($1,$2,'present')")
            .bind(&blob)
            .bind(url.len() as i64)
            .execute(&mut ctx.read_conn)
            .await
            .unwrap();
        sqlx::query("INSERT INTO raw.bindings(binding,source,input,settings) VALUES ('visits','synthetic.visits','synthetic','{}') ON CONFLICT DO NOTHING")
            .execute(&mut ctx.read_conn).await.unwrap();
        sqlx::query("INSERT INTO raw.inputs(id,binding,kind,locator,host) VALUES ($1,'visits','file',$2,'synthetic')")
            .bind(input).bind(name).execute(&mut ctx.read_conn).await.unwrap();
        sqlx::query("INSERT INTO raw.materials(id,input_id,blob,length,manifest,parser,parser_version) VALUES ($1,$2,$3,$4,'{}','synthetic',1)")
            .bind(material.0).bind(input).bind(blob).bind(url.len() as i64).execute(&mut ctx.read_conn).await.unwrap();
        sqlx::query("INSERT INTO core.events(id,event_type,source,run_id,material_id,anchor,ts_quality,commit_seq,payload,equivalence_key,reading_order,record_hash,class) VALUES ($1,'visit.fused','synthetic.visits',$2,$3,0,'unknown',$4,$5,$6,0,decode(repeat('00',32),'hex'),'guarded')")
            .bind(id).bind(run).bind(material.0).bind(sequence).bind(serde_json::json!({"url":url})).bind(name)
            .execute(&mut ctx.read_conn).await.unwrap();
        sqlx::query("INSERT INTO core.live_equivalence(source,event_type,equivalence_key,event_id,reading_order,record_hash) VALUES ('synthetic.visits','visit.fused',core.encode_equivalence_key($1),$2,0,decode(repeat('00',32),'hex'))")
            .bind(name).bind(id).execute(&mut ctx.read_conn).await.unwrap();
        visits.push((EventId(id), material, url));
    }
    sqlx::query("UPDATE core.runs SET events=4 WHERE id=$1")
        .bind(run)
        .execute(&mut ctx.read_conn)
        .await
        .unwrap();
    sqlx::query("INSERT INTO core.source_counts(source,events,first_event,last_event,last_seen) SELECT source,count(*),min(ts_orig),max(ts_orig),max(ts_coided) FROM core.events GROUP BY source")
        .execute(&mut ctx.read_conn).await.unwrap();
    sqlx::query("INSERT INTO core.commit_log(commit_seq,event_id,event_type) SELECT commit_seq,id,event_type FROM core.events")
        .execute(&mut ctx.read_conn).await.unwrap();
    sqlx::query("UPDATE core.counters SET value=4 WHERE name='commit_seq'")
        .execute(&mut ctx.read_conn)
        .await
        .unwrap();
    ctx.writer
        .submit(
            crate::writer::Priority::Bulk,
            Batch {
                run: RunId::new(),
                kind: BatchKind::Api {
                    actor: "test".into(),
                },
                records: vec![],
                archive: vec![ArchiveRequest::Hide {
                    target: visits[0].0,
                    operation: OperationId::new(),
                }],
            },
        )
        .await
        .unwrap();
    let target = sinex_store::excision::Target::Material(visits[1].1);
    let plan = sinex_store::excision::plan(&mut ctx.read_conn, target)
        .await
        .unwrap();
    ctx.writer
        .excise(crate::writer::Excision {
            target,
            plan,
            operation: OperationId::new(),
            actor: "test".into(),
            blobs: ctx.blobs.clone(),
        })
        .await
        .unwrap();
    let selected = history_urls(&mut ctx.read_conn).await.unwrap();
    assert_eq!(selected, [visits[2].2.clone(), visits[3].2.clone()]);
    // A zero limit exercises the public command path without making network requests.
    for _ in 0..2 {
        run_crawl(&mut ctx).await;
    }
    let rows: Vec<(String, serde_json::Value)> = sqlx::query_as(
        "SELECT binding,settings FROM raw.bindings WHERE source='browser.page-crawl'",
    )
    .fetch_all(&mut ctx.read_conn)
    .await
    .unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].0, "browser.page-crawl:page-crawl@synthetic");
    assert_eq!(
        rows[0].1,
        serde_json::json!({"host":"synthetic", "options":{"allow":["allowed.test"],"limit":0,"pause":0}})
    );
    let bindings: String = sqlx::query_scalar(
        "SELECT coalesce(string_agg(row_to_json(b)::text, ''), '') FROM raw.bindings b",
    )
    .fetch_one(&mut ctx.read_conn)
    .await
    .unwrap();
    for (_, _, url) in visits {
        assert!(!bindings.contains(&url), "{bindings}");
    }
    assert!(!bindings.contains("other.test"));
    ctx.close().await.unwrap();
}

async fn run_crawl(ctx: &mut crate::reader_context::ReaderContext) {
    run(
        &mut ctx.read_conn,
        &ctx.blobs,
        &ctx.writer,
        "synthetic",
        vec!["allowed.test".into()],
        vec![],
        Some(0),
        Duration::ZERO,
    )
    .await
    .unwrap();
}

#[devkit::test]
fn crawl_filters_domains_and_fetched_urls_before_applying_the_limit() {
    let urls = [
        "https://other.test/blocked",
        "https://allowed.test/fetched",
        "https://sub.allowed.test/first",
        "https://allowed.test/second",
        "file:///local",
    ];
    let selected = select_urls(
        urls.into_iter().map(str::to_owned).collect(),
        &BTreeSet::from(["https://allowed.test/fetched".into()]),
        &["allowed.test".into()],
        Some(1),
    );
    assert_eq!(selected, ["https://sub.allowed.test/first"]);
}

#[devkit::test]
async fn a_second_crawl_refuses_without_waiting(db: devkit::pg::TestDatabase) {
    let dir = tempfile::tempdir().unwrap();
    let mut ctx = crate::reader_context::ReaderContext::open(
        &db.url(None),
        dir.path(),
        sinex_model::time_settings::TimeSettings::new("Europe/Warsaw", 4, 0)
            .expect("fixture day setting"),
    )
    .await
    .unwrap();
    let active = CRAWL_LOCK.lock().await;
    let result = tokio::time::timeout(
        Duration::from_secs(1),
        run(
            &mut ctx.read_conn,
            &ctx.blobs,
            &ctx.writer,
            "synthetic",
            vec!["allowed.test".into()],
            vec![],
            Some(0),
            Duration::ZERO,
        ),
    )
    .await
    .expect("a second crawl must answer while the first still holds the binding");
    assert!(matches!(result, Err(error) if error == "a crawl is running"));
    drop(active);
    run_crawl(&mut ctx).await;
    ctx.close().await.unwrap();
}
