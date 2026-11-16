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
    record_offsets: Vec<Option<i64>>,
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
        let record_offset = location.then_some(n as i64);
        let output = location.then_some((n % 2) as i64);
        let record = match &eq {
            Some(key) => format!("key:{key}"),
            None => format!(
                "record:{}:{}:{}",
                mat.unwrap(),
                record_offset.unwrap(),
                output.unwrap()
            ),
        };
        let hash = *blake3::hash(record.as_bytes()).as_bytes();
        self.sources.push(SOURCE.into());
        self.kinds.push(KIND.into());
        self.hashes.push(hash.to_vec());
        self.equivalence.push(eq);
        self.materials.push(mat);
        self.record_offsets.push(record_offset);
        self.outputs.push(output);
        self.expected
            .insert((SOURCE.into(), KIND.into(), hash), expected);
    }
}

async fn seed(conn: &mut PgConnection, offers: &Offers) -> Bases {
    sqlx::query_file!(
        "src/admission/orders/seed.sql",
        &offers.sources,
        &offers.kinds,
        &offers.hashes,
        &offers.equivalence as &[Option<String>],
        &offers.materials as &[Option<Uuid>],
        &offers.record_offsets as &[Option<i64>],
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
}

#[devkit::test]
async fn missing_order_seed_takes_maxima_from_history_and_stored_counters(
    db: devkit::pg::TestDatabase,
) {
    let mut conn = PgConnection::connect(&db.url(None)).await.unwrap();
    // Half the selected history uses equivalence keys, half record locations.
    // The oldest rows own maxima, including an archived and a removed reading.
    conn.execute("INSERT INTO core.events(id,event_type,source,run_id,material_id,record_offset,output_index,parents,ts_quality,commit_seq,payload,equivalence_key,reading_order,record_hash,class,record_state)
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
    for offers in [&small, &many] {
        assert_eq!(seed(&mut conn, offers).await, offers.expected);
    }
    let chunk:String=sqlx::query_scalar("SELECT tableoid::regclass::text FROM core.events WHERE id='01700000-0000-7000-8000-000000000001'")
        .fetch_one(&mut conn).await.unwrap();
    sqlx::query("SELECT compress_chunk($1::regclass)")
        .bind(chunk)
        .execute(&mut conn)
        .await
        .unwrap();
    // Compressed history gives the same bases.
    for offers in [&small, &many] {
        assert_eq!(seed(&mut conn, offers).await, offers.expected);
    }
    // A stored counter wins even when no history survives.
    let mut cached = Offers::default();
    cached.push(256, false, Some(i64::MAX - 8));
    sqlx::query("INSERT INTO core.producer_orders(source,event_type,key_hash,reading_order) VALUES($1,$2,$3,$4)")
        .bind(SOURCE).bind(KIND).bind(cached.hashes.last().unwrap()).bind(i64::MAX-8)
        .execute(&mut conn).await.unwrap();
    assert_eq!(seed(&mut conn, &cached).await, cached.expected);
    // Once the source and kind has a counter, a key without one is new: its
    // stored history is not read again.
    let mut counted = Offers::default();
    counted.push(1, false, None);
    counted.push(1, true, None);
    assert_eq!(seed(&mut conn, &counted).await, counted.expected);
}
