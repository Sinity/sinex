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

use crate::runtime::parser::{MaterialParser, ParserError, ParserResult};
use sinex_macros::SourceMeta;
use sinex_primitives::domain::{EventSource, EventType};
use sinex_primitives::parser::{
    InputShapeKind, MaterialAnchor, OccurrenceKey, ParsedEventIntent, ParserContext, ParserId,
    ParserManifest, SourceId, SourceRecord, TimingConfidence, TimingEvidence,
};
use sinex_primitives::privacy::ProcessingContext;
use sinex_primitives::source_contracts::{
    AccessScope, CheckpointFamily, Horizon, OccurrenceIdentity, PrivacyTier, ResourceProfile,
    RetentionPolicy, RunnerPack, RuntimeShape,
};
use sinex_primitives::temporal::Timestamp;

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

#[derive(Debug, Clone, Default, SourceMeta)]
#[source_meta(
    id = "sleep-merged-summary",
    namespace = "health",
    event_source = "samsung-health",
    event_type = "sleep.session",
    adapter = "StaticFileAdapter",
    privacy_tier = PrivacyTier::Sensitive,
    horizons(Horizon::Historical),
    retention = RetentionPolicy::Forever,
    occurrence_identity = OccurrenceIdentity::Uuid5From("(sh_datauuid)"),
    access_scope = AccessScope::StagedExport,
    implementation = "sinexd",
    privacy_context = ProcessingContext::Metadata,
    resource_profile = ResourceProfile::BoundedFile,
    runner_pack = RunnerPack::SinexdSource,
    checkpoint_family = CheckpointFamily::AppendStream,
    runtime_shape = RuntimeShape::OnDemand,
)]
pub struct SleepMergedSummaryParser;

#[async_trait]
impl MaterialParser for SleepMergedSummaryParser {
    type Config = SleepParserConfig;

    fn manifest(&self) -> ParserManifest {
        ParserManifest {
            parser_id: ParserId::from_static("sleep-merged-summary"),
            parser_version: "1.0.0".into(),
            accepted_input_shapes: vec![InputShapeKind::StaticFile],
            source_id: SourceId::from_static("sleep-merged-summary"),
            declared_event_types: vec![(
                EventSource::from_static("samsung-health"),
                EventType::from_static("sleep.session"),
            )],
            privacy_contexts: vec![ProcessingContext::Metadata],
            sensitivity_hints: Vec::new(),
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

    fn required_input_keys(&self) -> Vec<String> {
        ["sh_datauuid", "start_local", "end_local"]
            .into_iter()
            .map(str::to_owned)
            .collect()
    }
}

fn parse_row(row: SleepCsvRow, line: u64, ctx: &ParserContext) -> ParserResult<ParsedEventIntent> {
    let start_at = parse_iso8601(&row.start_local)?;
    let end_at = parse_iso8601(&row.end_local)?;

    let occurrence_key = OccurrenceKey {
        source_id: SourceId::from_static("sleep-merged-summary"),
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
        .source_id(ctx.source_id.clone())
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
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
#[path = "health_test.rs"]
mod tests;
