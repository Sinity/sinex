use super::*;
use sinex_primitives::Uuid;
use sinex_primitives::ids::Id;

use xtask::sandbox::prelude::sinex_test;

fn test_ctx() -> ParserContext {
    ParserContext {
        source_id: SourceId::from_static("sleep-merged-summary"),
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
