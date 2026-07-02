use super::*;
use xtask::sandbox::sinex_test;

#[sinex_test]
async fn stream_pressure_uses_byte_limit_when_bytes_are_tighter() -> TestResult<()> {
    let pressure = StreamPressureSnapshot::from_limits(10, 1_000, 950, 1_000);

    assert_eq!(pressure.message_fill_pct, 1.0);
    assert_eq!(pressure.byte_fill_pct, 95.0);
    assert_eq!(pressure.fill_pct, 95.0);
    assert_eq!(pressure.pressure_level, StreamPressureLevel::Critical);
    assert_eq!(
        pressure.limiting_dimension,
        Some(StreamPressureDimension::Bytes)
    );

    Ok(())
}

#[sinex_test]
async fn stream_pressure_classifies_warning_before_critical() -> TestResult<()> {
    let pressure = StreamPressureSnapshot::from_limits(80, 100, 40, 100);

    assert_eq!(pressure.fill_pct, 80.0);
    assert_eq!(pressure.pressure_level, StreamPressureLevel::Warning);
    assert_eq!(
        pressure.limiting_dimension,
        Some(StreamPressureDimension::Messages)
    );

    Ok(())
}

#[sinex_test]
async fn stream_pressure_is_nominal_without_limits() -> TestResult<()> {
    let pressure = StreamPressureSnapshot::from_limits(10, 0, 950, 0);

    assert_eq!(pressure.fill_pct, 0.0);
    assert_eq!(pressure.pressure_level, StreamPressureLevel::Nominal);
    assert_eq!(pressure.limiting_dimension, None);

    Ok(())
}

#[sinex_test]
async fn stream_pressure_warning_samples_are_sparse_per_stream() -> TestResult<()> {
    let mut state = HashMap::new();
    let pressure = StreamPressureSnapshot {
        message_fill_pct: 99.0,
        byte_fill_pct: 20.0,
        fill_pct: 99.0,
        pressure_level: StreamPressureLevel::Critical,
        limiting_dimension: Some(StreamPressureDimension::Messages),
    };

    let emitted = (1..=20)
        .filter_map(|_| {
            record_stream_pressure_warning_sample(&mut state, "PROD_SINEX_RAW_EVENTS", pressure)
        })
        .collect::<Vec<_>>();

    assert_eq!(emitted, vec![1, 2, 4, 8, 16]);

    Ok(())
}

#[sinex_test]
async fn stream_pressure_raw_and_dlq_byte_warnings_remain_bounded_independently()
-> TestResult<()> {
    let mut state = HashMap::new();
    let pressure = StreamPressureSnapshot {
        message_fill_pct: 45.0,
        byte_fill_pct: 100.0,
        fill_pct: 100.0,
        pressure_level: StreamPressureLevel::Critical,
        limiting_dimension: Some(StreamPressureDimension::Bytes),
    };

    let raw_emitted = (1..=64)
        .filter_map(|_| {
            record_stream_pressure_warning_sample(&mut state, "PROD_SINEX_RAW_EVENTS", pressure)
        })
        .collect::<Vec<_>>();
    let dlq_emitted = (1..=64)
        .filter_map(|_| {
            record_stream_pressure_warning_sample(
                &mut state,
                "PROD_SINEX_RAW_EVENTS_DLQ",
                pressure,
            )
        })
        .collect::<Vec<_>>();

    assert_eq!(raw_emitted, vec![1, 2, 4, 8, 16, 32, 64]);
    assert_eq!(dlq_emitted, vec![1, 2, 4, 8, 16, 32, 64]);
    assert!(
        raw_emitted.len() + dlq_emitted.len() < 16,
        "128 saturated RAW/DLQ samples should not produce per-sample warnings"
    );

    Ok(())
}

#[sinex_test]
async fn stream_pressure_warning_schedule_resets_when_classification_changes() -> TestResult<()>
{
    let mut state = HashMap::new();
    let warning = StreamPressureSnapshot {
        message_fill_pct: 81.0,
        byte_fill_pct: 20.0,
        fill_pct: 81.0,
        pressure_level: StreamPressureLevel::Warning,
        limiting_dimension: Some(StreamPressureDimension::Messages),
    };
    let critical = StreamPressureSnapshot {
        message_fill_pct: 20.0,
        byte_fill_pct: 97.0,
        fill_pct: 97.0,
        pressure_level: StreamPressureLevel::Critical,
        limiting_dimension: Some(StreamPressureDimension::Bytes),
    };

    assert_eq!(
        record_stream_pressure_warning_sample(&mut state, "PROD_SINEX_RAW_EVENTS_DLQ", warning),
        Some(1)
    );
    assert_eq!(
        record_stream_pressure_warning_sample(&mut state, "PROD_SINEX_RAW_EVENTS_DLQ", warning),
        Some(2)
    );
    assert_eq!(
        record_stream_pressure_warning_sample(&mut state, "PROD_SINEX_RAW_EVENTS_DLQ", warning),
        None
    );
    assert_eq!(
        record_stream_pressure_warning_sample(
            &mut state,
            "PROD_SINEX_RAW_EVENTS_DLQ",
            critical
        ),
        Some(1)
    );
    assert_eq!(
        record_stream_pressure_warning_sample(
            &mut state,
            "PROD_SINEX_RAW_EVENTS_DLQ",
            StreamPressureSnapshot {
                pressure_level: StreamPressureLevel::Nominal,
                limiting_dimension: None,
                ..critical
            },
        ),
        None
    );
    assert_eq!(
        record_stream_pressure_warning_sample(
            &mut state,
            "PROD_SINEX_RAW_EVENTS_DLQ",
            critical
        ),
        Some(1)
    );

    Ok(())
}
