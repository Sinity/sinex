use super::*;
use futures::stream;
use sinex_primitives::parser::{MaterialAnchor, SourceRecord};

use xtask::sandbox::prelude::sinex_test;

// -------------------------------------------------------------------------
// Fixture adapter — yields a fixed list of records.
// -------------------------------------------------------------------------

#[derive(Clone, Default)]
struct FixtureAdapter {
    records: Vec<SourceRecord>,
    fingerprint: Option<SourceRecordFingerprint>,
}

impl FixtureAdapter {
    fn with_records(records: Vec<SourceRecord>) -> Self {
        Self {
            records,
            fingerprint: None,
        }
    }

    fn with_fingerprint(fingerprint: SourceRecordFingerprint) -> Self {
        Self {
            records: Vec::new(),
            fingerprint: Some(fingerprint),
        }
    }
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
struct FixtureConfig;

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
struct FixtureCursor {
    next_frame: u64,
}

fn make_record(material_id: Id<SourceMaterial>, frame_index: u64, label: &str) -> SourceRecord {
    SourceRecord {
        material_id,
        anchor: MaterialAnchor::StreamFrame {
            material_offset: 0,
            frame_index,
        },
        bytes: label.as_bytes().to_vec(),
        logical_path: None,
        source_ts_hint: None,
        metadata: serde_json::Value::Null,
    }
}

fn dummy_material_id() -> Id<SourceMaterial> {
    Id::from_uuid(uuid::Uuid::new_v4())
}

#[async_trait]
impl InputShapeAdapter for FixtureAdapter {
    type Config = FixtureConfig;
    type Cursor = FixtureCursor;
    const KIND: InputShapeKind = InputShapeKind::StaticFile;

    async fn open(
        &self,
        _material_id: Id<SourceMaterial>,
        _config: &Self::Config,
        cursor: Option<Self::Cursor>,
    ) -> ParserResult<BoxStream<'static, ParserResult<SourceRecord>>> {
        let start = cursor.map_or(0, |c| c.next_frame as usize);
        let records: Vec<_> = self.records[start..].to_vec();
        let s = stream::iter(records.into_iter().map(Ok));
        Ok(Box::pin(s))
    }

    fn cursor_after(&self, record: &SourceRecord) -> ParserResult<Self::Cursor> {
        match &record.anchor {
            MaterialAnchor::StreamFrame { frame_index, .. } => Ok(FixtureCursor {
                next_frame: frame_index + 1,
            }),
            _ => Err(ParserError::Cursor("unexpected anchor".into())),
        }
    }

    fn input_fingerprint(
        &self,
        _config: &Self::Config,
    ) -> ParserResult<Option<SourceRecordFingerprint>> {
        Ok(self.fingerprint.clone())
    }
}

// -------------------------------------------------------------------------
// Test: sequential merge drains primary then secondary
// -------------------------------------------------------------------------

#[sinex_test]
async fn test_sequential_merge_drains_primary_first() -> xtask::sandbox::TestResult<()> {
    let mid = dummy_material_id();
    let primary = FixtureAdapter::with_records(vec![
        make_record(mid, 0, "p0"),
        make_record(mid, 1, "p1"),
    ]);
    let secondary = FixtureAdapter::with_records(vec![make_record(mid, 0, "s0")]);

    let adapter = ChainedAdapter(primary, secondary);
    let config = ChainedConfig {
        primary: FixtureConfig,
        secondary: FixtureConfig,
        interleaved: false,
    };

    let stream = adapter.open(mid, &config, None).await.unwrap();
    let records: Vec<_> = stream
        .collect::<Vec<_>>()
        .await
        .into_iter()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();

    assert_eq!(records.len(), 3);

    // First two come from primary.
    let lp0 = records[0].logical_path.as_ref().unwrap().as_str();
    let lp1 = records[1].logical_path.as_ref().unwrap().as_str();
    let lp2 = records[2].logical_path.as_ref().unwrap().as_str();

    assert!(
        lp0.starts_with(PRIMARY_PREFIX),
        "first record must be primary: {lp0}"
    );
    assert!(
        lp1.starts_with(PRIMARY_PREFIX),
        "second record must be primary: {lp1}"
    );
    assert!(
        lp2.starts_with(SECONDARY_PREFIX),
        "third record must be secondary: {lp2}"
    );

    Ok(())
}

#[sinex_test]
async fn input_fingerprint_prefers_primary_leg() -> xtask::sandbox::TestResult<()> {
    let primary = SourceRecordFingerprint::from_json(&serde_json::json!({
        "primary": 1
    }));
    let secondary = SourceRecordFingerprint::from_json(&serde_json::json!({
        "secondary": true
    }));
    let adapter = ChainedAdapter(
        FixtureAdapter::with_fingerprint(primary.clone()),
        FixtureAdapter::with_fingerprint(secondary),
    );
    let config = ChainedConfig {
        primary: FixtureConfig,
        secondary: FixtureConfig,
        interleaved: false,
    };

    let fingerprint = adapter
        .input_fingerprint(&config)?
        .ok_or_else(|| ParserError::Adapter("missing chained fingerprint".into()))?;

    assert_eq!(fingerprint.hash(), primary.hash());
    assert!(fingerprint.keys.contains(&"/primary".to_string()));
    Ok(())
}

#[sinex_test]
async fn input_fingerprint_falls_back_to_secondary_leg() -> xtask::sandbox::TestResult<()> {
    let secondary = SourceRecordFingerprint::from_json(&serde_json::json!({
        "secondary": true
    }));
    let adapter = ChainedAdapter(
        FixtureAdapter::default(),
        FixtureAdapter::with_fingerprint(secondary.clone()),
    );
    let config = ChainedConfig {
        primary: FixtureConfig,
        secondary: FixtureConfig,
        interleaved: false,
    };

    let fingerprint = adapter
        .input_fingerprint(&config)?
        .ok_or_else(|| ParserError::Adapter("missing chained fingerprint".into()))?;

    assert_eq!(fingerprint.hash(), secondary.hash());
    assert!(fingerprint.keys.contains(&"/secondary".to_string()));
    Ok(())
}

// -------------------------------------------------------------------------
// Test: classify_record distinguishes legs
// -------------------------------------------------------------------------

#[sinex_test]
async fn test_classify_record_primary() -> xtask::sandbox::TestResult<()> {
    let mid = dummy_material_id();
    let mut rec = make_record(mid, 0, "x");
    rec.logical_path = Some("primary/subpath".into());
    assert_eq!(classify_record(&rec).unwrap(), ChainedLeg::Primary);
    Ok(())
}

#[sinex_test]
async fn test_classify_record_secondary() -> xtask::sandbox::TestResult<()> {
    let mid = dummy_material_id();
    let mut rec = make_record(mid, 0, "x");
    rec.logical_path = Some("secondary/subpath".into());
    assert_eq!(classify_record(&rec).unwrap(), ChainedLeg::Secondary);
    Ok(())
}

#[sinex_test]
async fn test_classify_record_missing_prefix_errors() -> xtask::sandbox::TestResult<()> {
    let mid = dummy_material_id();
    let mut rec = make_record(mid, 0, "x");
    rec.logical_path = Some("unknown/subpath".into());
    assert!(classify_record(&rec).is_err());
    Ok(())
}

#[sinex_test]
async fn test_classify_record_no_path_errors() -> xtask::sandbox::TestResult<()> {
    let mid = dummy_material_id();
    let rec = make_record(mid, 0, "x");
    assert!(classify_record(&rec).is_err());
    Ok(())
}

// -------------------------------------------------------------------------
// Test: cursor_after updates only the producing leg
// -------------------------------------------------------------------------

#[sinex_test]
async fn test_cursor_after_primary_leg() -> xtask::sandbox::TestResult<()> {
    let mid = dummy_material_id();
    let adapter = ChainedAdapter(FixtureAdapter::default(), FixtureAdapter::default());

    let mut rec = make_record(mid, 5, "x");
    rec.logical_path = Some("primary/".into());

    let cursor = adapter.cursor_after(&rec).unwrap();
    assert!(cursor.primary.is_some());
    assert!(cursor.secondary.is_none());
    assert_eq!(cursor.primary.unwrap().next_frame, 6);
    Ok(())
}

#[sinex_test]
async fn test_cursor_after_secondary_leg() -> xtask::sandbox::TestResult<()> {
    let mid = dummy_material_id();
    let adapter = ChainedAdapter(FixtureAdapter::default(), FixtureAdapter::default());

    let mut rec = make_record(mid, 3, "x");
    rec.logical_path = Some("secondary/".into());

    let cursor = adapter.cursor_after(&rec).unwrap();
    assert!(cursor.primary.is_none());
    assert!(cursor.secondary.is_some());
    assert_eq!(cursor.secondary.unwrap().next_frame, 4);
    Ok(())
}

// -------------------------------------------------------------------------
// Test: empty adapter on one leg is harmless
// -------------------------------------------------------------------------

#[sinex_test]
async fn test_empty_primary_leg_yields_only_secondary() -> xtask::sandbox::TestResult<()> {
    let mid = dummy_material_id();
    let primary = FixtureAdapter::with_records(vec![]);
    let secondary = FixtureAdapter::with_records(vec![make_record(mid, 0, "s0")]);

    let adapter = ChainedAdapter(primary, secondary);
    let config = ChainedConfig {
        primary: FixtureConfig,
        secondary: FixtureConfig,
        interleaved: false,
    };

    let stream = adapter.open(mid, &config, None).await.unwrap();
    let records: Vec<_> = stream
        .collect::<Vec<_>>()
        .await
        .into_iter()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();

    assert_eq!(records.len(), 1);
    let lp = records[0].logical_path.as_ref().unwrap().as_str();
    assert!(lp.starts_with(SECONDARY_PREFIX));
    Ok(())
}

// -------------------------------------------------------------------------
// Test: strip_prefix restores original logical_path
// -------------------------------------------------------------------------

#[sinex_test]
async fn test_strip_prefix_restores_path() -> xtask::sandbox::TestResult<()> {
    let mid = dummy_material_id();
    let mut rec = make_record(mid, 0, "x");
    rec.logical_path = Some("primary/foo/bar.csv".into());

    let stripped = strip_prefix(&rec);
    assert_eq!(
        stripped
            .logical_path
            .as_deref()
            .map(camino::Utf8Path::as_str),
        Some("foo/bar.csv")
    );
    Ok(())
}

#[sinex_test]
async fn test_strip_prefix_bare_primary_gives_none() -> xtask::sandbox::TestResult<()> {
    let mid = dummy_material_id();
    let mut rec = make_record(mid, 0, "x");
    rec.logical_path = Some("primary/".into());

    let stripped = strip_prefix(&rec);
    assert!(stripped.logical_path.is_none());
    Ok(())
}
