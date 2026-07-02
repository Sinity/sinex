use futures::StreamExt;
use xtask::sandbox::prelude::sinex_test;

use super::*;

#[derive(Debug)]
struct MockError(String);

impl fmt::Display for MockError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "mock error: {}", self.0)
    }
}

impl Error for MockError {}

/// A loader backed by a fixed record list (optionally failing).
struct MockLoader {
    records: Vec<JsonValue>,
    fail: bool,
}

impl MockLoader {
    fn new(records: Vec<JsonValue>) -> Self {
        Self {
            records,
            fail: false,
        }
    }

    fn failing() -> Self {
        Self {
            records: Vec::new(),
            fail: true,
        }
    }
}

impl DumpLoader for MockLoader {
    type Error = MockError;

    async fn load(&self) -> Result<Vec<JsonValue>, Self::Error> {
        if self.fail {
            return Err(MockError("load boom".to_owned()));
        }
        Ok(self.records.clone())
    }
}

fn dummy_material_id() -> Id<SourceMaterial> {
    Id::<SourceMaterial>::from_uuid(sinex_primitives::Uuid::nil())
}

fn config() -> IncrementalDumpConfig {
    IncrementalDumpConfig {
        order_key_field: "ts".to_owned(),
    }
}

async fn collect(
    adapter: &IncrementalDumpAdapter<MockLoader>,
    cursor: Option<IncrementalDumpCursor>,
) -> Vec<JsonValue> {
    let stream = adapter
        .open(dummy_material_id(), &config(), cursor)
        .await
        .unwrap();
    stream
        .collect::<Vec<_>>()
        .await
        .into_iter()
        .map(|r| serde_json::from_slice(&r.unwrap().bytes).unwrap())
        .collect()
}

/// Open and return the raw [`SourceRecord`]s (needed to derive cursors via
/// `cursor_after`, which a plain payload collect discards).
async fn open_records(
    adapter: &IncrementalDumpAdapter<MockLoader>,
    cursor: Option<IncrementalDumpCursor>,
) -> Vec<SourceRecord> {
    let stream = adapter
        .open(dummy_material_id(), &config(), cursor)
        .await
        .unwrap();
    stream
        .collect::<Vec<_>>()
        .await
        .into_iter()
        .map(Result::unwrap)
        .collect()
}

fn v_of(record: &SourceRecord) -> i64 {
    let json: JsonValue = serde_json::from_slice(&record.bytes).unwrap();
    json["v"].as_i64().unwrap()
}

#[sinex_test]
async fn first_import_emits_all_in_order() -> xtask::sandbox::TestResult<()> {
    let loader = MockLoader::new(vec![
        serde_json::json!({"ts": "2026-01-03", "v": 3}),
        serde_json::json!({"ts": "2026-01-01", "v": 1}),
        serde_json::json!({"ts": "2026-01-02", "v": 2}),
    ]);
    let adapter = IncrementalDumpAdapter::new(loader);
    let out = collect(&adapter, None).await;

    let vs: Vec<i64> = out.iter().map(|r| r["v"].as_i64().unwrap()).collect();
    assert_eq!(vs, vec![1, 2, 3], "emitted in ascending order-key order");
    Ok(())
}

#[sinex_test]
async fn superset_reimport_emits_only_new() -> xtask::sandbox::TestResult<()> {
    // Second export supersets the first; only records past the high-water
    // mark are emitted. The cursor is derived from a prior record (the real
    // checkpoint path), not hand-built.
    let records = vec![
        serde_json::json!({"ts": "2026-01-01", "v": 1}),
        serde_json::json!({"ts": "2026-01-02", "v": 2}),
        serde_json::json!({"ts": "2026-01-03", "v": 3}),
    ];
    let adapter = IncrementalDumpAdapter::new(MockLoader::new(records));
    let first = open_records(&adapter, None).await;
    let at_02 = first.iter().find(|r| v_of(r) == 2).unwrap();
    let cursor = adapter.cursor_after(at_02).unwrap();
    let out = open_records(&adapter, Some(cursor)).await;

    let vs: Vec<i64> = out.iter().map(v_of).collect();
    assert_eq!(vs, vec![3], "only the record past the high-water mark");
    Ok(())
}

#[sinex_test]
async fn non_unique_order_keys_all_emit() -> xtask::sandbox::TestResult<()> {
    // GDPR/Takeout timestamps are not unique. Distinct records that share an
    // order key must ALL be emitted — the content-hash tie-breaker keeps them
    // ordered instead of dropping siblings (the original Codex P1, PR #1776).
    let adapter = IncrementalDumpAdapter::new(MockLoader::new(vec![
        serde_json::json!({"ts": "2026-01-01", "v": 1}),
        serde_json::json!({"ts": "2026-01-01", "v": 2}),
        serde_json::json!({"ts": "2026-01-02", "v": 3}),
    ]));
    let out = open_records(&adapter, None).await;
    let mut vs: Vec<i64> = out.iter().map(v_of).collect();
    vs.sort();
    assert_eq!(
        vs,
        vec![1, 2, 3],
        "no record sharing a timestamp is dropped"
    );
    Ok(())
}

#[sinex_test]
async fn resume_across_shared_order_key_keeps_siblings() -> xtask::sandbox::TestResult<()> {
    // The exact data-loss scenario from the Codex P1 review (PR #1776): a run
    // is interrupted after consuming one of two records that share an order
    // key. On resume the sibling at the same timestamp must NOT be dropped.
    let adapter = IncrementalDumpAdapter::new(MockLoader::new(vec![
        serde_json::json!({"ts": "2026-01-01", "v": 1}),
        serde_json::json!({"ts": "2026-01-01", "v": 2}),
        serde_json::json!({"ts": "2026-01-02", "v": 3}),
    ]));
    let first = open_records(&adapter, None).await;
    // Checkpoint right after the first emitted record (lowest position).
    let consumed = v_of(&first[0]);
    let cursor = adapter.cursor_after(&first[0]).unwrap();
    let out = open_records(&adapter, Some(cursor)).await;

    let mut vs: Vec<i64> = out.iter().map(v_of).collect();
    vs.sort();
    let mut expected: Vec<i64> = vec![1, 2, 3]
        .into_iter()
        .filter(|v| *v != consumed)
        .collect();
    expected.sort();
    // Everything except the already-consumed record survives — crucially the
    // other record sharing ts=2026-01-01 is still present.
    assert_eq!(vs, expected, "sibling sharing the order key is not dropped");
    Ok(())
}

#[sinex_test]
async fn cursor_after_reports_composite_position() -> xtask::sandbox::TestResult<()> {
    let adapter = IncrementalDumpAdapter::new(MockLoader::new(vec![
        serde_json::json!({"ts": "2026-05-05", "v": 9}),
    ]));
    let records = open_records(&adapter, None).await;
    let position = adapter
        .cursor_after(&records[0])
        .unwrap()
        .high_water
        .expect("a consumed record yields a position");
    assert_eq!(position.order_key, "2026-05-05");
    assert_eq!(
        position.content_hash.len(),
        64,
        "BLAKE3 hex digest is 64 chars"
    );
    Ok(())
}

#[sinex_test]
async fn missing_order_key_field_fails_closed() -> xtask::sandbox::TestResult<()> {
    let loader = MockLoader::new(vec![serde_json::json!({"no_ts": "x"})]);
    let adapter = IncrementalDumpAdapter::new(loader);
    let result = adapter.open(dummy_material_id(), &config(), None).await;
    assert!(
        result.is_err(),
        "a record missing the order-key field must fail, not silently drop"
    );
    Ok(())
}

#[sinex_test]
async fn empty_dump_yields_no_records() -> xtask::sandbox::TestResult<()> {
    let adapter = IncrementalDumpAdapter::new(MockLoader::new(vec![]));
    let out = collect(&adapter, None).await;
    assert!(out.is_empty());
    Ok(())
}

#[sinex_test]
async fn load_failure_surfaces_typed_error() -> xtask::sandbox::TestResult<()> {
    let adapter = IncrementalDumpAdapter::new(MockLoader::failing());
    let result = adapter.open(dummy_material_id(), &config(), None).await;
    assert!(
        result.is_err(),
        "loader failure must surface, not yield empty"
    );
    Ok(())
}

#[sinex_test]
async fn input_shape_kind_is_incremental_dump() -> xtask::sandbox::TestResult<()> {
    assert_eq!(
        <IncrementalDumpAdapter<MockLoader> as InputShapeAdapter>::KIND,
        InputShapeKind::IncrementalDump
    );
    Ok(())
}
