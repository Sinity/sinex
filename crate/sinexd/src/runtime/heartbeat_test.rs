use super::*;
use xtask::sandbox::sinex_test;

#[derive(Debug, Default)]
struct RecordingSink(parking_lot::Mutex<Vec<serde_json::Value>>);

impl HeartbeatLogSink for RecordingSink {
    fn emit(&self, entry: &serde_json::Value) {
        self.0.lock().push(entry.clone());
    }
}

impl RecordingSink {
    fn heartbeat_records(&self) -> Vec<serde_json::Value> {
        self.0
            .lock()
            .iter()
            .filter(|entry| entry["message"] == "heartbeat")
            .cloned()
            .collect()
    }

    fn summary_records(&self) -> Vec<serde_json::Value> {
        self.0
            .lock()
            .iter()
            .filter(|entry| entry["message"] == "heartbeat.summary")
            .cloned()
            .collect()
    }
}

fn emitter_with_sink() -> (HeartbeatEmitter, Arc<RecordingSink>) {
    let sink = Arc::new(RecordingSink::default());
    // Disable periodic summaries so existing signal-bearing and suppression tests
    // are not affected by the summary cadence.
    let emitter = HeartbeatEmitter::new(
        ServiceName::new("heartbeat-test"),
        sinex_primitives::Seconds::from_secs(60),
    )
    .with_log_sink(sink.clone())
    .with_summary_every(0);
    (emitter, sink)
}

fn emitter_with_sink_and_summary_every(n: u64) -> (HeartbeatEmitter, Arc<RecordingSink>) {
    let sink = Arc::new(RecordingSink::default());
    let emitter = HeartbeatEmitter::new(
        ServiceName::new("heartbeat-test"),
        sinex_primitives::Seconds::from_secs(60),
    )
    .with_log_sink(sink.clone())
    .with_summary_every(n);
    (emitter, sink)
}

#[sinex_test]
async fn first_beat_emits_baseline_then_routine_beats_are_suppressed() -> TestResult<()> {
    let (emitter, sink) = emitter_with_sink();

    emitter.emit_heartbeat(None).await;
    emitter.emit_heartbeat(None).await;
    emitter.emit_heartbeat(None).await;

    let records = sink.heartbeat_records();
    assert_eq!(
        records.len(),
        1,
        "only the baseline record should be emitted for healthy steady state"
    );
    assert_eq!(records[0]["fields"]["event_type"], "runtime.heartbeat");
    assert_eq!(records[0]["fields"]["status"], "healthy");
    Ok(())
}

#[sinex_test]
async fn error_carrying_beat_is_emitted() -> TestResult<()> {
    let (emitter, sink) = emitter_with_sink();
    let handle = emitter.get_counter_handle();

    emitter.emit_heartbeat(None).await; // baseline
    emitter.emit_heartbeat(None).await; // suppressed
    handle.record_error("boom");
    emitter.emit_heartbeat(None).await; // signal-bearing

    let records = sink.heartbeat_records();
    assert_eq!(records.len(), 2, "baseline plus the error-carrying beat");
    let error_beat = &records[1];
    assert_eq!(error_beat["fields"]["errors_count"], 1);
    assert_eq!(error_beat["fields"]["last_error_message"], "boom");
    Ok(())
}

#[sinex_test]
async fn recovery_returns_to_suppressed_steady_state() -> TestResult<()> {
    let (emitter, sink) = emitter_with_sink();
    let handle = emitter.get_counter_handle();

    emitter.emit_heartbeat(None).await; // baseline
    handle.record_error("transient");
    emitter.emit_heartbeat(None).await; // signal-bearing
    emitter.emit_heartbeat(None).await; // healthy and error-free again

    // A single error stays far below the degraded threshold, so the
    // post-recovery beat is healthy and must be suppressed.
    let records = sink.heartbeat_records();
    assert_eq!(
        records.len(),
        2,
        "healthy error-free beats after recovery must be suppressed"
    );
    assert_eq!(records.last().unwrap()["fields"]["errors_count"], 1);
    Ok(())
}

/// Verify the periodic liveness summary cadence by construction (#1726).
///
/// With `summary_every = 3`, beats 0, 1, 2 produce no summary; beat 3 fires the first
/// compact summary; beat 6 fires the second; and so on.  The full baseline record is
/// emitted only on beat 0 (first beat).  This pins the AC: "steady-state sinexd journal
/// volume drops by an order of magnitude with no loss of health-transition observability."
#[sinex_test]
async fn periodic_summary_emits_compact_record_at_configured_cadence() -> TestResult<()> {
    // summary_every = 3: first summary fires at beat counter = 3, second at 6.
    let (emitter, sink) = emitter_with_sink_and_summary_every(3);

    for _ in 0..7 {
        emitter.emit_heartbeat(None).await;
    }

    // Exactly one full baseline record (beat 0); no further full records since healthy.
    let full_records = sink.heartbeat_records();
    assert_eq!(
        full_records.len(),
        1,
        "only the first beat produces a full baseline JSON record"
    );

    // Exactly two compact summaries: at beats 3 and 6.
    let summaries = sink.summary_records();
    assert_eq!(
        summaries.len(),
        2,
        "summaries must fire at beat multiples of summary_every (3 and 6 out of 0..6)"
    );

    // Summary fields are compact: only service_name, status, uptime_seconds, events_processed.
    let summary = &summaries[0];
    assert_eq!(summary["message"], "heartbeat.summary");
    assert_eq!(summary["level"], "INFO");
    let fields = &summary["fields"];
    assert!(fields["service_name"].is_string(), "service_name present");
    assert!(fields["status"].is_string(), "status present");
    assert!(
        fields["uptime_seconds"].is_number(),
        "uptime_seconds present"
    );
    assert!(
        fields["events_processed"].is_number(),
        "events_processed present"
    );
    // No full-metadata fields in summary.
    assert!(fields["version"].is_null(), "version absent from summary");
    assert!(fields["git_hash"].is_null(), "git_hash absent from summary");
    assert!(fields["metadata"].is_null(), "metadata absent from summary");

    Ok(())
}

/// Verify that summaries are disabled when `summary_every = 0`.
#[sinex_test]
async fn periodic_summary_disabled_when_summary_every_is_zero() -> TestResult<()> {
    let (emitter, sink) = emitter_with_sink_and_summary_every(0);

    for _ in 0..100 {
        emitter.emit_heartbeat(None).await;
    }

    assert_eq!(
        sink.summary_records().len(),
        0,
        "no summaries emitted when summary_every = 0"
    );
    Ok(())
}
