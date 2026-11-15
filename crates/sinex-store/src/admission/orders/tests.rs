use super::*;
use sqlx::{Connection, Executor};

type Bases = BTreeMap<Key, Option<i64>>;
const SOURCE: &str = "synthetic.seed";
const KIND: &str = "synthetic.seed_record";

#[derive(Default)]
struct Offers {
    sources: Vec<String>,
    kinds: Vec<String>,
    hashes: Vec<Vec<u8>>,
    equivalence: Vec<Option<String>>,
    materials: Vec<Option<Uuid>>,
    anchors: Vec<Option<i64>>,
    outputs: Vec<Option<i64>>,
    expected: Bases,
}
fn material(n: usize) -> Uuid {
    Uuid::parse_str(&format!("01900000-0000-7000-8000-{n:012x}")).unwrap()
}
impl Offers {
    fn push(&mut self, n: usize, location: bool, expected: Option<i64>) {
        let eq = (!location).then(|| format!("synthetic-key-{n}"));
        let mat = location.then(|| material(n));
        let anchor = location.then_some(n as i64);
        let output = location.then_some((n % 2) as i64);
        let record = match &eq {
            Some(key) => format!("key:{key}"),
            None => format!(
                "record:{}:{}:{}",
                mat.unwrap(),
                anchor.unwrap(),
                output.unwrap()
            ),
        };
        let hash = *blake3::hash(record.as_bytes()).as_bytes();
        self.sources.push(SOURCE.into());
        self.kinds.push(KIND.into());
        self.hashes.push(hash.to_vec());
        self.equivalence.push(eq);
        self.materials.push(mat);
        self.anchors.push(anchor);
        self.outputs.push(output);
        self.expected
            .insert((SOURCE.into(), KIND.into(), hash), expected);
    }
}

async fn seed(conn: &mut PgConnection, offers: &Offers, candidate: bool) -> Bases {
    macro_rules! read {
        ($path:literal) => {{
            sqlx::query_file!(
                $path,
                &offers.sources,
                &offers.kinds,
                &offers.hashes,
                &offers.equivalence as &[Option<String>],
                &offers.materials as &[Option<Uuid>],
                &offers.anchors as &[Option<i64>],
                &offers.outputs as &[Option<i64>]
            )
            .fetch_all(conn)
            .await
            .unwrap()
            .into_iter()
            .map(|r| {
                let hash: [u8; 32] = r.key_hash.try_into().unwrap();
                ((r.source, r.event_type, hash), r.base)
            })
            .collect()
        }};
    }
    if candidate {
        read!("src/admission/orders/seed.sql")
    } else {
        read!("src/admission/orders/old_seed.sql")
    }
}

async fn work(conn: &mut PgConnection) -> i64 {
    sqlx::query_scalar("SELECT coalesce(sum(seq_tup_read+idx_tup_fetch),0)::bigint FROM pg_stat_xact_user_tables WHERE relid='core.events'::regclass OR relid IN(SELECT show_chunks('core.events')) OR relid IN(SELECT compress_relid FROM _timescaledb_catalog.compression_settings WHERE relid IN(SELECT show_chunks('core.events')))")
        .fetch_one(conn).await.unwrap()
}

async fn executor_work(conn: &mut PgConnection, offers: &Offers, candidate: bool) -> f64 {
    let query = if candidate {
        include_str!("seed.sql")
    } else {
        include_str!("old_seed.sql")
    };
    let sql = format!("EXPLAIN (ANALYZE,FORMAT JSON) {query}");
    let plan: serde_json::Value = sqlx::query_scalar(&sql)
        .bind(&offers.sources)
        .bind(&offers.kinds)
        .bind(&offers.hashes)
        .bind(&offers.equivalence)
        .bind(&offers.materials)
        .bind(&offers.anchors)
        .bind(&offers.outputs)
        .persistent(false)
        .fetch_one(conn)
        .await
        .unwrap();
    fn visits(plan: &serde_json::Value) -> f64 {
        let loops = plan
            .get("Actual Loops")
            .and_then(|v| v.as_f64())
            .unwrap_or(0.0);
        let own = [
            "Actual Rows",
            "Rows Removed by Filter",
            "Rows Removed by Join Filter",
            "Rows Removed by Index Recheck",
        ]
        .iter()
        .filter_map(|key| plan.get(key).and_then(|v| v.as_f64()))
        .sum::<f64>()
            * loops;
        own + plan
            .get("Plans")
            .and_then(|v| v.as_array())
            .map_or(0.0, |nodes| nodes.iter().map(visits).sum())
    }
    visits(&plan[0]["Plan"])
}

async fn measure(conn: &mut PgConnection, offers: &Offers, phase: &str) -> (i64, i64, f64, f64) {
    let mut tx = conn.begin().await.unwrap();
    tx.execute("SET LOCAL max_parallel_workers_per_gather=0; SET LOCAL jit=off")
        .await
        .unwrap();
    let before = work(&mut tx).await;
    for _ in 0..8 {
        assert_eq!(seed(&mut tx, offers, false).await, offers.expected);
    }
    let old = work(&mut tx).await - before;
    let before = work(&mut tx).await;
    for _ in 0..8 {
        assert_eq!(seed(&mut tx, offers, true).await, offers.expected);
    }
    let new = work(&mut tx).await - before;
    eprintln!(
        "{phase},{} offered keys over8: original={old},selected-history={new}",
        offers.sources.len()
    );
    let old_executor = executor_work(&mut tx, offers, false).await;
    let new_executor = executor_work(&mut tx, offers, true).await;
    eprintln!(
        "{phase} whole executor tuple visits including CTE/join loops: original={old_executor},selected-history={new_executor}"
    );
    tx.rollback().await.unwrap();
    (old, new, old_executor, new_executor)
}

#[devkit::test]
async fn missing_order_seed_selected_history_keeps_maxima_and_reduces_work(
    db: devkit::pg::TestDatabase,
) {
    let mut conn = PgConnection::connect(&db.url(None)).await.unwrap();
    // Half the selected history uses equivalence keys, half record locations.
    // The oldest rows own maxima, including an archived and a removed reading.
    conn.execute("INSERT INTO core.events(id,event_type,source,run_id,material_id,anchor,output_index,parents,ts_quality,commit_seq,payload,equivalence_key,reading_order,record_hash,class,record_state)
      SELECT ('01700000-0000-7000-8000-'||lpad(to_hex(g+1),12,'0'))::uuid,'synthetic.seed_record','synthetic.seed',uuidv7(),
        CASE WHEN g>=4096 THEN ('01900000-0000-7000-8000-'||lpad(to_hex(g%64),12,'0'))::uuid END,
        CASE WHEN g>=4096 THEN g%64 END,g%2,
        CASE WHEN g<4096 THEN ARRAY[uuidv7()] END,'unknown',g+1,jsonb_build_object('synthetic',repeat(md5(g::text),128)),
        CASE WHEN g<4096 THEN 'synthetic-key-'||(g%64)::text END,
        CASE WHEN g=0 THEN 4611686018427387911 WHEN g=4096 THEN 4611686018427387921 ELSE g+1 END,
        decode(repeat('01',32),'hex'),'open',CASE WHEN g=0 THEN 'removed' ELSE 'present' END FROM generate_series(0,8191)g;
      INSERT INTO core.events(id,event_type,source,run_id,parents,ts_quality,commit_seq,payload,equivalence_key,reading_order,record_hash,class)
      SELECT ('01700000-0000-7000-8000-'||lpad(to_hex(g+10000),12,'0'))::uuid,
        CASE WHEN g<8192 THEN 'synthetic.seed_record' ELSE 'synthetic.other_record' END,
        CASE WHEN g<8192 THEN 'synthetic.other' ELSE 'synthetic.seed' END,uuidv7(),ARRAY[uuidv7()],'unknown',g+10000,'{}','synthetic-key-'||(g%64)::text,9223372036854775800,decode(repeat('02',32),'hex'),'open'
        FROM generate_series(0,16383)g;
      INSERT INTO audit.archived(event_id,archived_seq,reason) VALUES('01700000-0000-7000-8000-000000001001',1,'archive');
      ANALYZE core.events").await.unwrap();
    let mut small = Offers::default();
    small.push(0, false, Some((1_i64 << 62) + 7));
    small.push(0, true, Some((1_i64 << 62) + 17));
    let mut many = Offers::default();
    for n in 0..64 {
        many.push(
            n,
            false,
            Some(if n == 0 {
                (1_i64 << 62) + 7
            } else {
                4033 + n as i64
            }),
        );
        many.push(
            n,
            true,
            Some(if n == 0 {
                (1_i64 << 62) + 17
            } else {
                8129 + n as i64
            }),
        );
    }
    for n in 128..192 {
        many.push(n, false, None);
        many.push(n, true, None);
    }
    // A retained durable counter still wins even when no history survives.
    many.push(256, false, Some(i64::MAX - 8));
    sqlx::query("INSERT INTO core.producer_orders(source,event_type,key_hash,reading_order) VALUES($1,$2,$3,$4)")
        .bind(SOURCE).bind(KIND).bind(many.hashes.last().unwrap()).bind(i64::MAX-8)
        .execute(&mut conn).await.unwrap();
    let mut cached = Offers::default();
    cached.push(256, false, Some(i64::MAX - 8));
    let (_, cached_work, _, _) = measure(&mut conn, &cached, "rowstore-cached").await;
    assert_eq!(cached_work, 0, "cached authority needs no event history");
    measure(&mut conn, &small, "rowstore-small").await;
    let (old_hot, new_hot, old_hot_executor, new_hot_executor) =
        measure(&mut conn, &many, "rowstore-many").await;
    let chunk:String=sqlx::query_scalar("SELECT tableoid::regclass::text FROM core.events WHERE id='01700000-0000-7000-8000-000000000001'")
        .fetch_one(&mut conn).await.unwrap();
    sqlx::query("SELECT compress_chunk($1::regclass)")
        .bind(chunk)
        .execute(&mut conn)
        .await
        .unwrap();
    let (_, cached_work, _, _) = measure(&mut conn, &cached, "compressed-cached").await;
    assert_eq!(
        cached_work, 0,
        "cached authority needs no compressed history"
    );
    measure(&mut conn, &small, "compressed-small").await;
    let (old_cold, new_cold, old_cold_executor, new_cold_executor) =
        measure(&mut conn, &many, "compressed-many").await;
    // Re-reading a materialized CTE is still history work. Both authority
    // reads and executor visits must improve.
    assert!(
        new_hot < old_hot
            && new_cold < old_cold
            && new_hot_executor < old_hot_executor
            && new_cold_executor < old_cold_executor,
        "selected history was not a work improvement: hot{old_hot}→{new_hot}/executor{old_hot_executor}→{new_hot_executor},compressed{old_cold}→{new_cold}/executor{old_cold_executor}→{new_cold_executor}"
    );
}
