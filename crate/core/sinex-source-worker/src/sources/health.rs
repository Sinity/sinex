//! Sleep merged-summary CSV parser (#1052).
//!
//! Reads `sleep_merged_summary.csv` from
//! `/realm/data/exports/health/processed/` — a join of Samsung Health
//! and Sleep As Android session rows — and emits one
//! `samsung-health`/`sleep.session` event per row.
//!
//! Same shape as the Raindrop CSV parser (#1091/PR #1263): one
//! [`StaticFileAdapter`] file → one row per intent → line anchor.

use async_trait::async_trait;
use serde::{Deserialize, Serialize};

use sinex_node_sdk::parser::{MaterialParser, ParserError, ParserResult, StaticFileAdapter};
use sinex_primitives::domain::{EventSource, EventType};
use sinex_primitives::parser::{
    InputShapeKind, MaterialAnchor, OccurrenceKey, ParsedEventIntent, ParserContext, ParserId,
    ParserManifest, SourceRecord, SourceUnitId, TimingConfidence, TimingEvidence,
};
use sinex_primitives::privacy::ProcessingContext;
use sinex_primitives::proof::{
    CheckpointFamily, Horizon, OccurrenceIdentity, PrivacyTier, RetentionPolicy, RuntimeShape,
    SourceUnitBinding, SourceUnitBuildImpact, SourceUnitDescriptor, SubjectRef,
};
use sinex_primitives::temporal::Timestamp;
use sinex_primitives::{register_source_unit, register_source_unit_binding};

// ---------------------------------------------------------------------------
// Raw CSV row
// ---------------------------------------------------------------------------

#[derive(Debug, Deserialize)]
struct SleepCsvRow {
    sh_datauuid: String,
    start_local: String,
    end_local: String,
    #[serde(default)]
    sh_duration_minutes: Option<f64>,
    #[serde(default)]
    sa_vs_sh_duration_minutes: Option<f64>,
    #[serde(default)]
    trimmed_event_count: u32,
    #[serde(default)]
    hr_avg: Option<f64>,
    #[serde(default)]
    hr_min: Option<f64>,
    #[serde(default)]
    hr_max: Option<f64>,
    #[serde(default)]
    events_hr: u32,
    #[serde(default)]
    events_light: u32,
    #[serde(default)]
    events_deep: u32,
    #[serde(default)]
    events_rem: u32,
    #[serde(default)]
    sa_comment: String,
}

// ---------------------------------------------------------------------------
// Parser
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct SleepParserConfig;

#[derive(Debug, Clone, Default)]
pub struct SleepMergedSummaryParser;

#[async_trait]
impl MaterialParser for SleepMergedSummaryParser {
    type Config = SleepParserConfig;

    fn manifest(&self) -> ParserManifest {
        ParserManifest {
            parser_id: ParserId::from_static("sleep-merged-summary"),
            parser_version: "1.0.0".into(),
            accepted_input_shapes: vec![InputShapeKind::StaticFile],
            source_unit_id: SourceUnitId::from_static("sleep-merged-summary"),
            declared_event_types: vec![(
                EventSource::from_static("samsung-health"),
                EventType::from_static("sleep.session"),
            )],
            privacy_contexts: vec![ProcessingContext::Metadata],
            proof_obligations: vec![
                "timestamp_intrinsic".into(),
                "anchor_csv_row".into(),
                "occurrence_key_sh_datauuid".into(),
                "deltas_dropped".into(),
            ],
            description: "Parses the merged Samsung Health + Sleep As \
                Android sleep summary CSV into typed sleep.session events. \
                Surfaces Samsung HR aggregates + per-stage event counts \
                and the SA-side user comment."
                .into(),
        }
    }

    async fn parse_record(
        &mut self,
        record: SourceRecord,
        ctx: &ParserContext,
    ) -> ParserResult<Vec<ParsedEventIntent>> {
        let mut reader = csv::ReaderBuilder::new()
            .has_headers(true)
            .from_reader(record.bytes.as_slice());

        let mut intents = Vec::new();
        for (row_index, row_result) in reader.deserialize::<SleepCsvRow>().enumerate() {
            let row = row_result.map_err(|e| {
                ParserError::Parse(format!(
                    "sleep summary CSV row {} parse error: {e}",
                    row_index + 1
                ))
            })?;
            intents.push(parse_row(row, (row_index + 1) as u64, ctx)?);
        }
        Ok(intents)
    }
}

fn parse_row(row: SleepCsvRow, line: u64, ctx: &ParserContext) -> ParserResult<ParsedEventIntent> {
    let start_at = parse_iso8601(&row.start_local)?;
    let end_at = parse_iso8601(&row.end_local)?;

    let occurrence_key = OccurrenceKey {
        source_unit_id: SourceUnitId::from_static("sleep-merged-summary"),
        fields: vec![("sh_datauuid".into(), row.sh_datauuid.clone())],
    };

    let payload = serde_json::json!({
        "sh_data_uuid": row.sh_datauuid,
        "start_at": start_at,
        "end_at": end_at,
        "duration_minutes": row.sh_duration_minutes.unwrap_or(0.0),
        "events_hr": row.events_hr,
        "events_light": row.events_light,
        "events_deep": row.events_deep,
        "events_rem": row.events_rem,
        "trimmed_event_count": row.trimmed_event_count,
        "hr_avg": row.hr_avg,
        "hr_min": row.hr_min,
        "hr_max": row.hr_max,
        "sa_comment": non_empty(&row.sa_comment),
        "sa_vs_sh_duration_minutes": row.sa_vs_sh_duration_minutes,
    });

    Ok(ParsedEventIntent::builder()
        .source_unit_id(ctx.source_unit_id.clone())
        .parser_id(ParserId::from_static("sleep-merged-summary"))
        .parser_version("1.0.0")
        .event_type(EventType::from_static("sleep.session"))
        .event_source(EventSource::from_static("samsung-health"))
        .payload(payload)
        .ts_orig(start_at)
        .timing(TimingEvidence::Intrinsic {
            field: "start_local".into(),
            confidence: TimingConfidence::Intrinsic,
        })
        .anchor(MaterialAnchor::Line {
            byte_start: 0,
            line,
        })
        .occurrence_key(occurrence_key)
        .privacy_context(ProcessingContext::Metadata)
        .build())
}

fn parse_iso8601(raw: &str) -> ParserResult<Timestamp> {
    use time::OffsetDateTime;
    use time::format_description::well_known::Rfc3339;
    let dt = OffsetDateTime::parse(raw, &Rfc3339)
        .map_err(|e| ParserError::Parse(format!("invalid sleep timestamp '{raw}': {e}")))?;
    Ok(Timestamp::new(dt))
}

fn non_empty(s: &str) -> Option<&str> {
    let trimmed = s.trim();
    (!trimmed.is_empty()).then_some(trimmed)
}

// ---------------------------------------------------------------------------
// Source unit descriptor + binding + registration
// ---------------------------------------------------------------------------

register_source_unit! {
    SourceUnitDescriptor {
        id: "sleep-merged-summary",
        namespace: "health",
        event_types: &[("samsung-health", "sleep.session")],
        privacy_tier: PrivacyTier::Sensitive,
        horizons: &[Horizon::Historical],
        retention: RetentionPolicy::Forever,
        proof_obligations: &[
            "timestamp_intrinsic",
            "anchor_csv_row",
            "occurrence_key_sh_datauuid",
            "deltas_dropped",
        ],
        occurrence_identity: OccurrenceIdentity::Uuid5From("(sh_datauuid)"),
        access_policy: "personal_health_data",
    }
}

register_source_unit_binding! {
    SourceUnitBinding::builder(
        SubjectRef::from_static("source_unit:sleep-merged-summary"),
        "sleep-merged-summary",
        "health",
    )
    .implementation("sinex-source-worker")
    .adapter("StaticFileAdapter")
    .output_event_type("sleep.session")
    .privacy_context("Metadata")
    .material_policy("static_export_file")
    .checkpoint_policy("static_file_cursor")
    .resource_shape("file_reader")
    .source_unit_id("sleep-merged-summary")
    .runner_pack("source-worker")
    .checkpoint_family(CheckpointFamily::AppendStream)
    .runtime_shape(RuntimeShape::OnDemand)
    .package_impact("sleep_merged_summary_source_unit")
    .implementation_mode("rust_in_pack:source-worker")
    .build_impact(SourceUnitBuildImpact::ZERO)
    .build()
}

crate::register_adapter_ingestor!(
    source_unit_id: "sleep-merged-summary",
    adapter: StaticFileAdapter,
    parser: SleepMergedSummaryParser,
);

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use sinex_primitives::Uuid;
    use sinex_primitives::ids::Id;

    use xtask::sandbox::prelude::sinex_test;

    fn test_ctx() -> ParserContext {
        ParserContext {
            source_unit_id: SourceUnitId::from_static("sleep-merged-summary"),
            source_material_id: Id::new(),
            record_anchor: MaterialAnchor::ByteRange { start: 0, len: 0 },
            operation_id: Uuid::new_v4(),
            job_id: Uuid::new_v4(),
            host: "test-host".into(),
            acquisition_time: Timestamp::now(),
        }
    }

    fn record_for(bytes: &[u8]) -> SourceRecord {
        SourceRecord {
            material_id: Id::new(),
            anchor: MaterialAnchor::ByteRange {
                start: 0,
                len: bytes.len() as u64,
            },
            bytes: bytes.to_vec(),
            logical_path: None,
            source_ts_hint: None,
            metadata: serde_json::Value::Null,
        }
    }

    // Real fixture row from /realm/data/exports/health/processed.
    const SAMPLE_CSV: &str = "sh_datauuid,start_local,end_local,sh_duration_minutes,start_delta_minutes,end_delta_minutes,sa_vs_sh_duration_minutes,trimmed_event_count,hr_avg,hr_min,hr_max,events_hr,events_light,events_deep,events_rem,sa_comment\n\
        e86b7115-e01d-45ce-98ed-b8c7248b93a3,2024-03-21T10:50:00+01:00,2024-03-21T12:40:00+01:00,110.0,0.0,-49.0,-49.4,6,,,,0,1,0,0,\n\
        4416b87f-9aae-45b9-8795-fb9e76c86345,2024-10-26T03:40:00+02:00,2024-10-26T06:07:00+02:00,147.0,1.0,248.0,246.6,45,70.02,57.00,87.00,29,2,2,2,#watch\n";

    #[sinex_test]
    async fn parses_two_sessions() -> TestResult<()> {
        let mut parser = SleepMergedSummaryParser;
        let intents = parser
            .parse_record(record_for(SAMPLE_CSV.as_bytes()), &test_ctx())
            .await
            .unwrap();
        assert_eq!(intents.len(), 2);
        for intent in &intents {
            assert_eq!(intent.event_source.as_str(), "samsung-health");
            assert_eq!(intent.event_type.as_str(), "sleep.session");
        }
        Ok(())
    }

    #[sinex_test]
    async fn timestamp_uses_start_local() -> TestResult<()> {
        let mut parser = SleepMergedSummaryParser;
        let intents = parser
            .parse_record(record_for(SAMPLE_CSV.as_bytes()), &test_ctx())
            .await
            .unwrap();
        let ts = intents[0].ts_orig.inner();
        assert_eq!(ts.year(), 2024);
        assert_eq!(ts.month() as u8, 3);
        assert_eq!(ts.day(), 21);
        Ok(())
    }

    #[sinex_test]
    async fn hr_fields_are_optional() -> TestResult<()> {
        let mut parser = SleepMergedSummaryParser;
        let intents = parser
            .parse_record(record_for(SAMPLE_CSV.as_bytes()), &test_ctx())
            .await
            .unwrap();
        // Row 0 has empty hr_avg/min/max → None in payload.
        assert!(intents[0].payload["hr_avg"].is_null());
        // Row 1 has populated hr_avg = 70.02.
        let avg = intents[1].payload["hr_avg"].as_f64().unwrap();
        assert!((avg - 70.02).abs() < 0.001);
        Ok(())
    }

    #[sinex_test]
    async fn sa_comment_blank_to_none_populated_to_some() -> TestResult<()> {
        let mut parser = SleepMergedSummaryParser;
        let intents = parser
            .parse_record(record_for(SAMPLE_CSV.as_bytes()), &test_ctx())
            .await
            .unwrap();
        assert!(intents[0].payload["sa_comment"].is_null());
        assert_eq!(intents[1].payload["sa_comment"], "#watch");
        Ok(())
    }

    #[sinex_test]
    async fn per_stage_event_counts_preserved() -> TestResult<()> {
        let mut parser = SleepMergedSummaryParser;
        let intents = parser
            .parse_record(record_for(SAMPLE_CSV.as_bytes()), &test_ctx())
            .await
            .unwrap();
        let p = &intents[1].payload;
        assert_eq!(p["events_hr"], 29);
        assert_eq!(p["events_light"], 2);
        assert_eq!(p["events_deep"], 2);
        assert_eq!(p["events_rem"], 2);
        assert_eq!(p["trimmed_event_count"], 45);
        Ok(())
    }

    #[sinex_test]
    async fn occurrence_key_is_sh_datauuid() -> TestResult<()> {
        let mut parser = SleepMergedSummaryParser;
        let intents = parser
            .parse_record(record_for(SAMPLE_CSV.as_bytes()), &test_ctx())
            .await
            .unwrap();
        let key = intents[0].occurrence_key.as_ref().unwrap();
        assert_eq!(
            key.fields,
            vec![(
                "sh_datauuid".into(),
                "e86b7115-e01d-45ce-98ed-b8c7248b93a3".into(),
            )]
        );
        Ok(())
    }

    #[sinex_test]
    async fn anchor_uses_csv_row() -> TestResult<()> {
        let mut parser = SleepMergedSummaryParser;
        let intents = parser
            .parse_record(record_for(SAMPLE_CSV.as_bytes()), &test_ctx())
            .await
            .unwrap();
        assert!(matches!(
            intents[0].anchor,
            MaterialAnchor::Line { line: 1, .. }
        ));
        assert!(matches!(
            intents[1].anchor,
            MaterialAnchor::Line { line: 2, .. }
        ));
        Ok(())
    }

    #[sinex_test]
    async fn invalid_timestamp_errors() -> TestResult<()> {
        let bad = "sh_datauuid,start_local,end_local,sh_duration_minutes,start_delta_minutes,end_delta_minutes,sa_vs_sh_duration_minutes,trimmed_event_count,hr_avg,hr_min,hr_max,events_hr,events_light,events_deep,events_rem,sa_comment\n\
            abc,not-a-time,2024-03-21T12:40:00+01:00,110.0,0.0,0.0,0.0,0,,,,0,0,0,0,\n";
        let mut parser = SleepMergedSummaryParser;
        let err = parser
            .parse_record(record_for(bad.as_bytes()), &test_ctx())
            .await
            .unwrap_err()
            .to_string();
        assert!(err.contains("invalid sleep timestamp"), "got: {err}");
        Ok(())
    }
}
