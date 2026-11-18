//! An explicit native workload experiment, excluded from the ordinary suite.
use super::*;
use serde_json::Value;
use sinex_model::{
    Batch, BatchKind, Event, EventId, KindDeclaration, KindRef, MaterialId, Origin, ReadingOrder,
    RecordLocation, RecordOutcome, RunId, kinds::Search,
};
use std::time::Instant;

const SEED_ROWS: usize = 2_048;
const SEED_TEXT_BYTES: usize = 4_096;
const PAGE_ROWS: usize = 512;
const PAGE_TEXT_BYTES: usize = 16_384;

fn text(family: &str, row: usize, bytes: usize) -> String {
    use std::fmt::Write;
    let mut out = format!("{family} coastline bright meadow row{row:04} ");
    let mut token = 0u64;
    while out.len() < bytes {
        let value = ((token + row as u64 * 17) % 8_192).wrapping_mul(0x9e3779b97f4a7c15);
        write!(out, "{value:016x} ").unwrap();
        token += 1;
    }
    out.truncate(bytes);
    out
}

async fn admit_rows(
    conn: &mut crate::AdmissionContext,
    kind: &KindRef,
    start: usize,
    rows: usize,
    bytes: usize,
    family: &str,
    make_text: fn(&str, usize, usize) -> String,
) {
    let events = (start..start + rows)
        .map(|n| {
            let text = make_text(family, n, bytes);
            let hash = *blake3::hash(text.as_bytes()).as_bytes();
            let mut event = Event::submitted(
                kind.clone(),
                serde_json::json!({"text": text}),
                "synthetic.search.work".into(),
                Origin::Record {
                    location: RecordLocation {
                        material: MaterialId(uuid::Uuid::from_u128(n as u128 + 1)),
                        record_offset: 0,
                    },
                    output_index: 0,
                },
                ReadingOrder(1),
                hash,
                Class::Open,
            );
            event.id = EventId(
                uuid::Builder::from_unix_timestamp_millis(1_704_067_200_000 + n as u64, &[0; 10])
                    .into_uuid(),
            );
            event
        })
        .collect();
    let mut tx = conn.begin().await.unwrap();
    tx.admit(
        &Batch {
            run: RunId::new(),
            kind: BatchKind::Api {
                actor: "synthetic".into(),
            },
            records: vec![RecordOutcome::Events {
                location: None,
                events,
            }],
            archive: vec![],
        },
        &crate::NoDerivations,
    )
    .await
    .unwrap();
    tx.commit().await.unwrap();
}

async fn index_pages_observing(
    conn: &mut PgConnection,
    budget: Option<&WriteBudget>,
) -> Vec<Value> {
    let mut out = Vec::new();
    let mut position = crate::read_models::search_position(conn).await.unwrap();
    loop {
        let started = Instant::now();
        let page =
            crate::commit_log::after_in_one_snapshot(conn, position, 10_000, crate::PAGE_BYTES)
                .await
                .unwrap();
        let read_seconds = started.elapsed().as_secs_f64();
        let Some(up_to) = page.up_to else { break };
        let mut payload_hash = blake3::Hasher::new();
        for event in &page.events {
            payload_hash.update(event.row.payload.to_string().as_bytes());
        }
        let payload_bytes: usize = page
            .events
            .iter()
            .map(|e| e.row.payload.to_string().len())
            .sum();
        let started = Instant::now();
        let mut tx = conn.begin().await.unwrap();
        crate::read_models::apply_search_text(&mut tx, &page.events)
            .await
            .unwrap();
        crate::read_models::set_search_position(&mut tx, up_to)
            .await
            .unwrap();
        tx.commit().await.unwrap();
        let apply_and_commit_seconds = started.elapsed().as_secs_f64();
        let pending = if let Some(budget) = budget {
            budget.check();
            words_pending(conn).await
        } else {
            Value::Null
        };
        out.push(
            serde_json::json!({"rows":page.events.len(), "payload_bytes":payload_bytes,
            "from":position, "up_to":up_to, "read_seconds":read_seconds,
            "payload_hash":payload_hash.finalize().to_hex().to_string(), "pending_words":pending,
            "apply_and_commit_seconds":apply_and_commit_seconds}),
        );
        position = up_to;
    }
    out
}

async fn index_pages(conn: &mut PgConnection) -> Vec<Value> {
    index_pages_observing(conn, None).await
}

async fn drain(conn: &mut PgConnection) -> Value {
    let started = Instant::now();
    let mut indexes = Vec::new();
    for index in ["core.search_text_tsv", "core.search_trigram_text"] {
        let pages: i64 = sqlx::query_scalar("SELECT gin_clean_pending_list($1::regclass)")
            .bind(index)
            .fetch_one(&mut *conn)
            .await
            .unwrap();
        indexes.push(serde_json::json!({"index":index,"cleaned_pages":pages}));
    }
    serde_json::json!({"seconds":started.elapsed().as_secs_f64(),"indexes":indexes})
}

async fn io(conn: &mut PgConnection) -> Vec<(String, i64, i64, i64)> {
    // Flush this backend's database statistics at this completed-statement boundary.
    sqlx::query("SELECT pg_stat_force_next_flush()")
        .execute(&mut *conn)
        .await
        .unwrap();
    sqlx::query("SELECT pg_stat_clear_snapshot()")
        .execute(&mut *conn)
        .await
        .unwrap();
    sqlx::query_as(
        "SELECT indexrelname, idx_blks_read, idx_blks_hit, pg_relation_size(indexrelid)
        FROM pg_statio_user_indexes WHERE schemaname='core'
        AND indexrelname IN ('search_text_tsv','search_trigram_text') ORDER BY indexrelname",
    )
    .fetch_all(conn)
    .await
    .unwrap()
}

async fn workload(db: devkit::pg::TestDatabase, mode: Search, clear_seed: bool) {
    let mut conn = crate::AdmissionContext::connect(&db.url(None))
        .await
        .unwrap();
    let mut declaration = KindDeclaration::schemaless(
        KindRef {
            name: "synthetic.search.work".into(),
            version: 1,
        },
        "synthetic".into(),
    );
    declaration.search = mode;
    sinex_schema::kinds::register(&mut conn, "synthetic", std::slice::from_ref(&declaration))
        .await
        .unwrap();
    // Match the retained deployment's pending-list setting in this isolated database only.
    sqlx::raw_sql(
        "ALTER INDEX core.search_text_tsv SET (gin_pending_list_limit=65536);
        ALTER INDEX core.search_trigram_text SET (gin_pending_list_limit=65536)",
    )
    .execute(&mut conn)
    .await
    .unwrap();
    let total = Instant::now();
    admit_rows(
        &mut conn,
        &declaration.kind,
        0,
        SEED_ROWS,
        SEED_TEXT_BYTES,
        "seed",
        text,
    )
    .await;
    let seed = index_pages(&mut conn).await;
    let seed_done_seconds = total.elapsed().as_secs_f64();
    let pre_drain = if clear_seed {
        Some(drain(&mut conn).await)
    } else {
        None
    };
    let before = io(&mut conn).await;
    admit_rows(
        &mut conn,
        &declaration.kind,
        SEED_ROWS,
        PAGE_ROWS,
        PAGE_TEXT_BYTES,
        "probe",
        text,
    )
    .await;
    let pages = index_pages(&mut conn).await;
    let after_apply = io(&mut conn).await;
    // Both arms pay the deferred work before comparing total cost.
    let post_drain = drain(&mut conn).await;
    let after_drain = io(&mut conn).await;
    let total_seconds = total.elapsed().as_secs_f64();
    let expected = (SEED_ROWS + PAGE_ROWS) as i64;
    for query in [
        "events where coastline",
        "events where \"bright meadow\"",
        "events where coast*",
    ] {
        assert_eq!(
            ask(&mut conn, query).await.total as i64,
            expected,
            "{query}"
        );
    }
    assert_eq!(
        ask(&mut conn, "events where probe").await.total as usize,
        PAGE_ROWS
    );
    assert_eq!(ask(&mut conn, "events where absentmarker*").await.total, 0);
    let indexed: i64 = sqlx::query_scalar("SELECT count(*) FROM core.search_text")
        .fetch_one(&mut conn)
        .await
        .unwrap();
    let trigram: i64 = sqlx::query_scalar("SELECT count(*) FROM core.search_trigram")
        .fetch_one(&mut conn)
        .await
        .unwrap();
    assert_eq!(indexed, expected);
    assert_eq!(
        trigram,
        if mode == Search::WordsAndTrigram {
            expected
        } else {
            0
        }
    );
    assert_eq!(
        crate::read_models::search_position(&mut conn)
            .await
            .unwrap(),
        crate::query::current_seq(&mut conn).await.unwrap()
    );
    eprintln!(
        "{}",
        serde_json::json!({"search_workload":{
            "mode":format!("{mode:?}"), "drain_seed":clear_seed,
            "seed_rows":SEED_ROWS,"seed_text_bytes_each":SEED_TEXT_BYTES,
            "page_rows":PAGE_ROWS,"page_text_bytes_each":PAGE_TEXT_BYTES,
            "byte_budget":crate::PAGE_BYTES,"pending_limit_kib":65536,
            "seed_pages":seed,"seed_done_seconds":seed_done_seconds,
            "pre_drain":pre_drain,"pages":pages,"post_drain":post_drain,
            "io_before_page":before,"io_after_apply":after_apply,"io_after_final_drain":after_drain,
            "total_seed_admission_index_and_drains_seconds":total_seconds,
            "query_answers_checked":true,
            "excludes":"large live GIN size and cold-cache population; actual competing host work is recorded separately"
        }})
    );
    conn.close().await.unwrap();
}

#[devkit::test]
#[ignore = "explicit native workload experiment"]
async fn words_with_pending_seed(db: devkit::pg::TestDatabase) {
    workload(db, Search::Words, false).await;
}
#[devkit::test]
#[ignore = "explicit native workload experiment"]
async fn words_with_drained_seed(db: devkit::pg::TestDatabase) {
    workload(db, Search::Words, true).await;
}
#[devkit::test]
#[ignore = "explicit native workload experiment"]
async fn trigram_with_pending_seed(db: devkit::pg::TestDatabase) {
    workload(db, Search::WordsAndTrigram, false).await;
}
#[devkit::test]
#[ignore = "explicit native workload experiment"]
async fn trigram_with_drained_seed(db: devkit::pg::TestDatabase) {
    workload(db, Search::WordsAndTrigram, true).await;
}

fn distinct_text(family: &str, row: usize, bytes: usize) -> String {
    use std::fmt::Write;
    let mut out = format!("{family} coastline bright meadow row{row:04} ");
    let mut token = 0u64;
    while out.len() < bytes {
        let value = ((row as u64) << 20 | token).wrapping_mul(0x9e3779b97f4a7c15);
        write!(out, "k{value:016x} ").unwrap();
        token += 1;
    }
    out.truncate(bytes);
    out
}

async fn words_pending(conn: &mut PgConnection) -> Value {
    sqlx::query_scalar(
        "SELECT to_jsonb(m) FROM public.gin_metapage_info(public.get_raw_page('core.search_text_tsv',0)) m",
    )
    .fetch_one(conn)
    .await
    .unwrap()
}

async fn foreground_budget(
    db: devkit::pg::TestDatabase,
    work_mb: usize,
    matched_rows: Option<usize>,
) -> Value {
    const PENDING_KIB: u64 = 65_536;
    const SEED_BATCH: usize = 512;
    const MAX_SEED_ROWS: usize = 16_384;
    let mut conn = crate::AdmissionContext::connect(&db.url(None))
        .await
        .unwrap();
    // Contrib pageinspect is provided by the existing PostgreSQL package.
    // It is enabled only in this disposable database.
    sqlx::raw_sql(
        "CREATE EXTENSION IF NOT EXISTS pageinspect WITH SCHEMA public;
        ALTER TABLE core.search_text SET (autovacuum_enabled=false);
        ALTER TABLE core.search_trigram SET (autovacuum_enabled=false);
        SET work_mem='64MB'; SET maintenance_work_mem='512MB';
        ALTER INDEX core.search_text_tsv SET (fastupdate=true,gin_pending_list_limit=1048576)",
    )
    .execute(&mut conn)
    .await
    .unwrap();
    let block_bytes: i64 = sqlx::query_scalar("SELECT current_setting('block_size')::bigint")
        .fetch_one(&mut conn)
        .await
        .unwrap();
    let mut declaration = KindDeclaration::schemaless(
        KindRef {
            name: "synthetic.search.work".into(),
            version: 1,
        },
        "synthetic".into(),
    );
    declaration.search = Search::Words;
    sinex_schema::kinds::register(&mut conn, "synthetic", std::slice::from_ref(&declaration))
        .await
        .unwrap();
    let wal_start: String = sqlx::query_scalar("SELECT pg_current_wal_insert_lsn()::text")
        .fetch_one(&mut conn)
        .await
        .unwrap();
    let total = Instant::now();
    let mut seed_rows = 0;
    let mut seed_pages = Vec::new();
    let before;
    loop {
        assert!(
            seed_rows < MAX_SEED_ROWS,
            "bounded seed did not reach the intended pending threshold"
        );
        admit_rows(
            &mut conn,
            &declaration.kind,
            seed_rows,
            SEED_BATCH,
            SEED_TEXT_BYTES,
            "seed",
            distinct_text,
        )
        .await;
        seed_pages.extend(index_pages(&mut conn).await);
        seed_rows += SEED_BATCH;
        let meta = words_pending(&mut conn).await;
        let pending_pages = meta["n_pending_pages"].as_u64().unwrap();
        // PostgreSQL18 uses block size minus32 bytes for this threshold.
        let over_threshold = pending_pages * (block_bytes as u64 - 32) > PENDING_KIB * 1024;
        if matched_rows.is_some_and(|rows| seed_rows >= rows)
            || matched_rows.is_none() && over_threshold
        {
            assert!(
                over_threshold,
                "matched population must still cross the foreground threshold"
            );
            before = meta;
            break;
        }
    }
    assert_eq!(before["n_pending_tuples"].as_u64(), Some(seed_rows as u64));
    sqlx::query("ALTER INDEX core.search_text_tsv SET (gin_pending_list_limit=65536)")
        .execute(&mut conn)
        .await
        .unwrap();
    sqlx::query("SELECT set_config('work_mem',$1,false)")
        .bind(format!("{work_mb}MB"))
        .execute(&mut conn)
        .await
        .unwrap();
    let settings: (String, String) = sqlx::query_as(
        "SELECT current_setting('work_mem'),current_setting('maintenance_work_mem')",
    )
    .fetch_one(&mut conn)
    .await
    .unwrap();
    let io_before = io(&mut conn).await;
    admit_rows(
        &mut conn,
        &declaration.kind,
        seed_rows,
        1,
        PAGE_TEXT_BYTES,
        "probe",
        distinct_text,
    )
    .await;
    let apply_started = Instant::now();
    let pages = index_pages(&mut conn).await;
    let apply_seconds = apply_started.elapsed().as_secs_f64();
    let after = words_pending(&mut conn).await;
    let io_after = io(&mut conn).await;
    assert_eq!(
        after["n_pending_pages"].as_u64(),
        Some(0),
        "the single probe must trigger and finish foreground cleanup"
    );
    let final_drain = drain(&mut conn).await;
    let final_io = io(&mut conn).await;
    let total_seconds = total.elapsed().as_secs_f64();
    for query in [
        "events where coastline",
        "events where coast*",
        "events where \"bright meadow\"",
    ] {
        assert_eq!(
            ask(&mut conn, query).await.total as usize,
            seed_rows + 1,
            "{query}"
        );
    }
    assert_eq!(ask(&mut conn, "events where probe").await.total, 1);
    assert_eq!(ask(&mut conn, "events where absentmarker*").await.total, 0);
    let counts: (i64, i64) = sqlx::query_as(
        "SELECT (SELECT count(*) FROM core.search_text),(SELECT count(*) FROM core.search_trigram)",
    )
    .fetch_one(&mut conn)
    .await
    .unwrap();
    assert_eq!(counts, (seed_rows as i64 + 1, 0));
    assert_eq!(
        crate::read_models::search_position(&mut conn)
            .await
            .unwrap(),
        crate::query::current_seq(&mut conn).await.unwrap()
    );
    let database_bytes: i64 = sqlx::query_scalar("SELECT pg_database_size(current_database())")
        .fetch_one(&mut conn)
        .await
        .unwrap();
    let cluster_wal_bytes: i64 = sqlx::query_scalar(
        "SELECT pg_wal_lsn_diff(pg_current_wal_insert_lsn(),$1::pg_lsn)::bigint",
    )
    .bind(&wal_start)
    .fetch_one(&mut conn)
    .await
    .unwrap();
    let seed_hashes: Vec<_> = seed_pages
        .iter()
        .map(|page| page["payload_hash"].clone())
        .collect();
    let result = serde_json::json!({"work_mem_mb":work_mb,"settings":settings,
        "seed_rows":seed_rows,"seed_text_bytes_each":SEED_TEXT_BYTES,"seed_payload_hashes":seed_hashes,
        "pending_limit_kib":PENDING_KIB,"block_bytes":block_bytes,"pending_before":before,"pending_after":after,
        "threshold_crossed":true,"probe_rows":1,"probe_text_bytes":PAGE_TEXT_BYTES,"byte_budget":crate::PAGE_BYTES,
        "pages":pages,"apply_and_read_seconds":apply_seconds,
        "gin_io_before":io_before,"gin_io_after":io_after,"gin_io_after_final_drain":final_io,
        "final_drain":final_drain,"total_seed_admission_index_and_drains_seconds":total_seconds,
        "query_answers_checked":true,"internal_accumulator_flush_count":null,
        "database_bytes_after":database_bytes,"cluster_wal_bytes":cluster_wal_bytes,
        "interpretation_limit":"pending removal proves foreground cleanup; internal flush count is unobserved. Compare actual IO/wall and report lack of discrimination if budgets do not separate them. Large live index and cold-cache scale are not reproduced; host competition is recorded separately."});
    conn.close().await.unwrap();
    result
}

#[devkit::test]
#[ignore = "explicit paired foreground GIN cleanup experiment"]
async fn words_foreground_64_and_512_mib(db: devkit::pg::TestDatabase) {
    let small = foreground_budget(db, 64, None).await;
    let rows = small["seed_rows"].as_u64().unwrap() as usize;
    // The first helper has dropped its database/slot before another is taken.
    let second = devkit::pg::test_database().unwrap();
    let large = foreground_budget(second, 512, Some(rows)).await;
    assert_eq!(small["seed_rows"], large["seed_rows"]);
    assert_eq!(small["seed_payload_hashes"], large["seed_payload_hashes"]);
    for field in ["n_pending_pages", "n_pending_tuples"] {
        assert_eq!(
            small["pending_before"][field], large["pending_before"][field],
            "{field}"
        );
    }
    eprintln!(
        "{}",
        serde_json::json!({"foreground_words_budget_pair":[small,large]})
    );
}

/// Counts physical writes charged to this job, including its owned PostgreSQL.
/// Compilation happens before this fixture starts and is reported separately.
struct WriteBudget {
    io: std::path::PathBuf,
    before: u64,
    bytes: u64,
}
impl WriteBudget {
    fn start() -> Self {
        let cgroup = std::fs::read_to_string("/proc/self/cgroup").unwrap();
        let path = cgroup
            .lines()
            .find_map(|line| line.strip_prefix("0::"))
            .unwrap();
        assert!(
            path.contains("agentctl-") && path.ends_with(".service"),
            "run this disk experiment through its declared job"
        );
        let io = std::path::Path::new("/sys/fs/cgroup")
            .join(path.trim_start_matches('/'))
            .join("io.stat");
        let mib: u64 = std::env::var("SINEX_SEARCH_WRITE_BUDGET_MIB")
            .expect("set the explicit physical write budget")
            .parse()
            .unwrap();
        assert!(mib > 0);
        let before = Self::read(&io);
        Self {
            io,
            before,
            bytes: mib.checked_mul(1 << 20).unwrap(),
        }
    }
    fn read(path: &std::path::Path) -> u64 {
        std::fs::read_to_string(path)
            .unwrap()
            .split_whitespace()
            .filter_map(|part| part.strip_prefix("wbytes="))
            .map(|number| number.parse::<u64>().unwrap())
            .sum()
    }
    fn written(&self) -> u64 {
        Self::read(&self.io).saturating_sub(self.before)
    }
    fn check(&self) {
        assert!(
            self.written() <= self.bytes,
            "disposable search experiment exceeded its physical write budget: {} > {}",
            self.written(),
            self.bytes
        );
    }
}

async fn query_latencies(conn: &mut PgConnection, total: usize, incoming: usize) -> Vec<Value> {
    let mut out = Vec::new();
    for (query, expected) in [
        ("events where coastline", total),
        ("events where \"bright meadow\"", total),
        ("events where coast*", total),
        ("events where probe", incoming),
        ("events where absentmarker*", 0),
    ] {
        let started = Instant::now();
        let answer = ask(conn, query).await;
        let elapsed = started.elapsed().as_secs_f64();
        assert_eq!(answer.total as usize, expected, "{query}");
        out.push(serde_json::json!({"query":query,"seconds":elapsed,"total":answer.total}));
    }
    out
}

async fn pending_arm(
    server: &devkit::pg::Server,
    pending_kib: usize,
    kind: &KindRef,
    budget: &WriteBudget,
) -> Value {
    const MATURE_ROWS: usize = 8_192;
    const INCOMING_ROWS: usize = 18_432;
    let database = format!("gin_arm_{pending_kib}");
    server.clone_database("gin_seed", &database).unwrap();
    let mut conn = crate::AdmissionContext::connect(&server.url(&database, None))
        .await
        .unwrap();
    let initial = serde_json::json!({"pending":words_pending(&mut conn).await,
        "indexes":io(&mut conn).await,
        "position":crate::read_models::search_position(&mut conn).await.unwrap(),
        "rows":sqlx::query_scalar::<_,i64>("SELECT count(*) FROM core.search_text").fetch_one(&mut conn).await.unwrap()});
    assert_eq!(initial["pending"]["n_pending_tuples"], 0);
    assert_eq!(initial["rows"], MATURE_ROWS);
    assert!(
        initial["pending"]["n_entries"].as_u64().unwrap() > 0,
        "the start must be a mature main index"
    );
    assert!(
        initial["pending"]["n_data_pages"].as_u64().unwrap() > 0,
        "the start must include existing posting trees"
    );
    sqlx::query(
        "ALTER INDEX core.search_text_tsv SET (fastupdate=true,gin_pending_list_limit=65536)",
    )
    .execute(&mut conn)
    .await
    .unwrap();
    if pending_kib == 262_144 {
        sqlx::query("ALTER INDEX core.search_text_tsv SET (gin_pending_list_limit=262144)")
            .execute(&mut conn)
            .await
            .unwrap();
    } else {
        assert_eq!(pending_kib, 65_536);
    }
    sqlx::raw_sql("SET work_mem='512MB'; SET maintenance_work_mem='512MB'")
        .execute(&mut conn)
        .await
        .unwrap();
    let settings: (String, String) = sqlx::query_as(
        "SELECT current_setting('work_mem'),current_setting('maintenance_work_mem')",
    )
    .fetch_one(&mut conn)
    .await
    .unwrap();
    let io_before = io(&mut conn).await;
    let wal_before: String = sqlx::query_scalar("SELECT pg_current_wal_insert_lsn()::text")
        .fetch_one(&mut conn)
        .await
        .unwrap();
    let written_before = budget.written();
    let total_started = Instant::now();
    let mut samples = vec![
        serde_json::json!({"incoming":0,"queries":query_latencies(&mut conn,MATURE_ROWS,0).await}),
    ];
    let mut pages = Vec::new();
    for n in (0..INCOMING_ROWS).step_by(2_048) {
        budget.check();
        admit_rows(
            &mut conn,
            kind,
            MATURE_ROWS + n,
            2_048,
            SEED_TEXT_BYTES,
            "probe",
            text,
        )
        .await;
        pages.extend(index_pages_observing(&mut conn, Some(budget)).await);
        if (n + 2_048) % 4_096 == 0 || n + 2_048 == INCOMING_ROWS {
            samples.push(serde_json::json!({"incoming":n+2_048,
                "queries":query_latencies(&mut conn,MATURE_ROWS+n+2_048,n+2_048).await}));
        }
        budget.check();
    }
    let before_final_drain = words_pending(&mut conn).await;
    let final_drain = drain(&mut conn).await;
    let drained = words_pending(&mut conn).await;
    assert_eq!(drained["n_pending_pages"], 0);
    let final_queries =
        query_latencies(&mut conn, MATURE_ROWS + INCOMING_ROWS, INCOMING_ROWS).await;
    sqlx::query("CHECKPOINT").execute(&mut conn).await.unwrap();
    let total_seconds = total_started.elapsed().as_secs_f64();
    budget.check();
    let io_after = io(&mut conn).await;
    let buffer_accesses: i64 = io_after
        .iter()
        .zip(&io_before)
        .map(|(after, before)| {
            assert_eq!(after.0, before.0);
            assert!(after.1 >= before.1 && after.2 >= before.2);
            (after.1 - before.1) + (after.2 - before.2)
        })
        .sum();
    let wal: i64 = sqlx::query_scalar(
        "SELECT pg_wal_lsn_diff(pg_current_wal_insert_lsn(),$1::pg_lsn)::bigint",
    )
    .bind(&wal_before)
    .fetch_one(&mut conn)
    .await
    .unwrap();
    let mut prior_pending = 0;
    let mut observed_cleanups = 0;
    for page in &pages {
        let pending = page["pending_words"]["n_pending_tuples"].as_u64().unwrap();
        let offered = page["rows"].as_u64().unwrap();
        if pending < prior_pending + offered {
            observed_cleanups += 1;
        }
        prior_pending = pending;
    }
    let position = crate::read_models::search_position(&mut conn)
        .await
        .unwrap();
    assert_eq!(
        position,
        crate::query::current_seq(&mut conn).await.unwrap()
    );
    let database_bytes: i64 = sqlx::query_scalar("SELECT pg_database_size(current_database())")
        .fetch_one(&mut conn)
        .await
        .unwrap();
    let result = serde_json::json!({"pending_limit_kib":pending_kib,"settings":settings,
        "initial":initial,"incoming_rows":INCOMING_ROWS,"pages":pages,
        "observed_foreground_cleanup_pages":observed_cleanups,"before_final_drain":before_final_drain,
        "final_drain":final_drain,"drained":drained,"query_samples":samples,"final_queries":final_queries,
        "io_before":io_before,"io_after":io_after,"gin_buffer_accesses":buffer_accesses,"wal_bytes":wal,"position":position,
        "physical_writes":budget.written()-written_before,"database_bytes":database_bytes,
        "total_seconds_including_queries_and_drain":total_seconds});
    conn.close().await.unwrap();
    server.drop_database(&database).unwrap();
    result
}

#[devkit::test]
#[ignore = "explicit on-disk mature words GIN consolidation experiment"]
async fn words_pending_64_and_256_mib(db: devkit::pg::TestDatabase) {
    // SQLx uses the ordinary schema fixture. Runtime work has its own owned
    // server and storage, never the shared service's size-capped tmpfs.
    drop(db);
    let storage = std::path::PathBuf::from(
        std::env::var_os("SINEX_SEARCH_WORK_ROOT")
            .expect("set an explicit disposable PostgreSQL storage root"),
    );
    assert!(storage.is_dir(), "the selected storage root must exist");
    let mount = std::process::Command::new("findmnt")
        .args(["--json", "--bytes", "--target"])
        .arg(&storage)
        .args(["--output", "TARGET,SOURCE,FSTYPE,SIZE,AVAIL"])
        .output()
        .unwrap();
    assert!(
        mount.status.success(),
        "storage location must be inspectable"
    );
    let filesystem: Value = serde_json::from_slice(&mount.stdout).unwrap();
    assert_ne!(
        filesystem["filesystems"][0]["fstype"], "tmpfs",
        "this experiment requires an explicitly selected disk-backed root"
    );
    let budget = WriteBudget::start();
    let total = Instant::now();
    let options = devkit::pg::ClusterOptions {
        required_extensions: vec!["timescaledb".into(), "vector".into(), "pageinspect".into()],
        preload: vec!["timescaledb".into()],
        settings: vec![
            ("fsync".into(), "on".into()),
            ("synchronous_commit".into(), "on".into()),
            ("full_page_writes".into(), "on".into()),
            ("shared_buffers".into(), "64MB".into()),
            ("work_mem".into(), "512MB".into()),
            ("maintenance_work_mem".into(), "512MB".into()),
            ("autovacuum".into(), "off".into()),
            ("timescaledb.max_background_workers".into(), "0".into()),
            ("timescaledb.telemetry_level".into(), "off".into()),
            ("timescaledb.enable_chunk_skipping".into(), "on".into()),
            ("max_wal_size".into(), "1GB".into()),
        ],
    };
    let server = devkit::pg::Server::start_in(&options, &storage).unwrap();
    server.sql("postgres", "CREATE DATABASE gin_seed").unwrap();
    let mut seed = PgConnection::connect(&server.url("gin_seed", None))
        .await
        .unwrap();
    sinex_schema::apply(&mut seed).await.unwrap();
    sqlx::raw_sql(
        "CREATE EXTENSION pageinspect WITH SCHEMA public;
        ALTER TABLE core.search_text SET (autovacuum_enabled=false);
        ALTER TABLE core.search_trigram SET (autovacuum_enabled=false);
        ALTER INDEX core.search_text_tsv SET (fastupdate=true,gin_pending_list_limit=65536);
        ALTER INDEX core.search_trigram_text SET (fastupdate=true,gin_pending_list_limit=65536)",
    )
    .execute(&mut seed)
    .await
    .unwrap();
    seed.close().await.unwrap();
    let mut conn = crate::AdmissionContext::connect(&server.url("gin_seed", None))
        .await
        .unwrap();
    let mut declaration = KindDeclaration::schemaless(
        KindRef {
            name: "synthetic.search.work".into(),
            version: 1,
        },
        "synthetic".into(),
    );
    declaration.search = Search::Words;
    sinex_schema::kinds::register(&mut conn, "synthetic", std::slice::from_ref(&declaration))
        .await
        .unwrap();
    let mut seed_pages = Vec::new();
    for (start, generator) in [
        (0, distinct_text as fn(&str, usize, usize) -> String),
        (4_096, text),
    ] {
        for offset in (0..4_096).step_by(512) {
            budget.check();
            admit_rows(
                &mut conn,
                &declaration.kind,
                start + offset,
                512,
                SEED_TEXT_BYTES,
                "mature",
                generator,
            )
            .await;
            seed_pages.extend(index_pages(&mut conn).await);
        }
    }
    let seed_drain = drain(&mut conn).await;
    sqlx::query("VACUUM (ANALYZE) core.search_text")
        .execute(&mut conn)
        .await
        .unwrap();
    sqlx::query("CHECKPOINT").execute(&mut conn).await.unwrap();
    budget.check();
    let environment:(String,String,String,String,String)=sqlx::query_as(
        "SELECT current_setting('data_directory'),current_setting('shared_buffers'),current_setting('fsync'),current_setting('synchronous_commit'),current_setting('full_page_writes')")
        .fetch_one(&mut conn).await.unwrap();
    conn.close().await.unwrap();
    let small = pending_arm(&server, 65_536, &declaration.kind, &budget).await;
    let large = pending_arm(&server, 262_144, &declaration.kind, &budget).await;
    for field in ["pending", "position", "rows"] {
        assert_eq!(
            small["initial"][field], large["initial"][field],
            "matched seed {field}"
        );
    }
    let sizes = |arm: &Value| {
        arm["initial"]["indexes"]
            .as_array()
            .unwrap()
            .iter()
            .map(|index| (index[0].clone(), index[3].clone()))
            .collect::<Vec<_>>()
    };
    assert_eq!(
        sizes(&small),
        sizes(&large),
        "same fully drained index sizes"
    );
    let small_pages: Vec<_> = small["pages"]
        .as_array()
        .unwrap()
        .iter()
        .map(|page| (&page["payload_hash"], &page["rows"], &page["up_to"]))
        .collect();
    let large_pages: Vec<_> = large["pages"]
        .as_array()
        .unwrap()
        .iter()
        .map(|page| (&page["payload_hash"], &page["rows"], &page["up_to"]))
        .collect();
    assert_eq!(small_pages, large_pages, "same committed input pages");
    let deciding = small["observed_foreground_cleanup_pages"].as_u64().unwrap() >= 4
        && large["observed_foreground_cleanup_pages"].as_u64().unwrap() >= 1;
    eprintln!(
        "{}",
        serde_json::json!({"pending_coalescing_words_pair":{
            "environment":environment,"filesystem":filesystem,"seed_pages":seed_pages,"seed_drain":seed_drain,"arms":[small,large],
            "thresholds_discriminate":deciding,"physical_write_budget":budget.bytes,
            "physical_writes_total":budget.written(),"total_seconds":total.elapsed().as_secs_f64(),
            "excludes":"16GB production index, cold OS cache and other host consumers. Logical buffer work and WAL test consolidation; elapsed time is contextual. Budget checked between each page and checkpoint; a single SQL cleanup can finish before an overrun is observed."
        }})
    );
    assert!(
        deciding,
        "the selected corpus did not exercise both foreground thresholds"
    );
    budget.check();
    drop(server);
}
