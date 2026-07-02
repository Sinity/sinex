use super::*;
use crate::runtime::parser::{MaterialParser, records_from_journal_lines};
use crate::sources::source_contracts::system::journald::JournaldParser;
use serde_json::Value;
use serde_json::json;
use sinex_primitives::events::SourceMaterial;
use sinex_primitives::ids::Id;
use sinex_primitives::parser::{MaterialAnchor, ParserContext, SourceId};
use sinex_primitives::primitives::Uuid;
use sinex_primitives::source_contracts::{BudgetPressureAction, WorkClass};
use sinex_primitives::temporal::Timestamp;
use sinex_primitives::{Event, JsonValue};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tracing_subscriber::fmt::MakeWriter;
use xtask::sandbox::{TestResult, sinex_test};

static TEST_PRESSURE_ACTIONS: &[BudgetPressureAction] = &[BudgetPressureAction::Inspect];

fn test_budget(
    max_pending_candidates: u32,
    max_pending_material_bytes: u64,
) -> ResourceBudgetSpec {
    ResourceBudgetSpec {
        work_class: WorkClass::ProjectionHot,
        steady_memory_mib: 1,
        burst_memory_mib: 1,
        cpu_weight: 100,
        max_input_bytes_per_sec: None,
        max_input_events_per_sec: None,
        max_pending_material_bytes,
        max_pending_candidates,
        max_unacked_transport_messages: None,
        batch_size: None,
        flush_interval_ms: None,
        checkpoint_interval_ms: None,
        expected_disk_write_bytes_per_min: None,
        expected_wal_write_bytes_per_min: None,
        pressure_actions: TEST_PRESSURE_ACTIONS,
    }
}

#[derive(Clone, Default)]
struct CapturedLogs {
    bytes: Arc<Mutex<Vec<u8>>>,
}

struct CapturedLogWriter {
    bytes: Arc<Mutex<Vec<u8>>>,
}

impl CapturedLogs {
    fn output(&self) -> String {
        let bytes = self.bytes.lock().expect("captured log mutex poisoned");
        String::from_utf8(bytes.clone()).expect("tracing output should be UTF-8")
    }
}

impl<'a> MakeWriter<'a> for CapturedLogs {
    type Writer = CapturedLogWriter;

    fn make_writer(&'a self) -> Self::Writer {
        CapturedLogWriter {
            bytes: Arc::clone(&self.bytes),
        }
    }
}

impl std::io::Write for CapturedLogWriter {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.bytes
            .lock()
            .expect("captured log mutex poisoned")
            .extend_from_slice(buf);
        Ok(buf.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

fn event_id() -> EventId {
    Id::<Event<JsonValue>>::new()
}

fn provisional(
    source: &str,
    event_type: &str,
    received_at: Timestamp,
    payload: serde_json::Value,
) -> ProvisionalEvent {
    ProvisionalEvent {
        event_id: event_id(),
        source: EventSource::new(source).expect("test source must be valid"),
        event_type: EventType::new(event_type).expect("test event type must be valid"),
        payload,
        ts_orig: received_at,
        received_at,
    }
}

fn journal_parser_ctx(mid: Id<SourceMaterial>) -> ParserContext {
    ParserContext {
        source_id: SourceId::from_static("system.journald"),
        source_material_id: mid,
        record_anchor: MaterialAnchor::Line {
            byte_start: 0,
            line: 1,
        },
        operation_id: Uuid::new_v4(),
        job_id: Uuid::new_v4(),
        host: "test-host".into(),
        acquisition_time: Timestamp::now(),
    }
}

fn payload_bytes(payload: &Value) -> TestResult<usize> {
    Ok(serde_json::to_vec(payload)?.len())
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ConfirmationBufferEvidence {
    pending_count: usize,
    timed_out_retained_count: usize,
    rejected_count: u64,
    late_confirmation_count: u64,
    retained_payload_bytes: usize,
    active_payload_bytes: usize,
    timed_out_retained_payload_bytes: usize,
    journald_payload_bytes: usize,
    runtime_action: RuntimePressureAction,
}

impl ConfirmationBufferEvidence {
    async fn capture(buffer: &ConfirmationBuffer) -> Self {
        Self::from_snapshot(buffer.snapshot().await)
    }

    fn from_snapshot(snapshot: ConfirmationBufferSnapshot) -> Self {
        Self {
            pending_count: snapshot.pending_count,
            timed_out_retained_count: snapshot.timed_out_retained_count,
            rejected_count: snapshot.rejected_count,
            late_confirmation_count: snapshot.late_confirmation_count,
            retained_payload_bytes: snapshot.retained_payload_bytes,
            active_payload_bytes: snapshot.active_payload_bytes,
            timed_out_retained_payload_bytes: snapshot.timed_out_retained_payload_bytes,
            journald_payload_bytes: snapshot
                .approximate_payload_bytes_by_kind
                .get("system.journald:journald.entry.written")
                .copied()
                .unwrap_or(0),
            runtime_action: snapshot.runtime_action,
        }
    }

    fn assert_drained_after(&self, before: &Self) {
        assert_eq!(self.pending_count, 0);
        assert_eq!(self.timed_out_retained_count, 0);
        assert_eq!(self.retained_payload_bytes, 0);
        assert_eq!(self.active_payload_bytes, 0);
        assert_eq!(self.timed_out_retained_payload_bytes, 0);
        assert_eq!(self.journald_payload_bytes, 0);
        assert_eq!(self.rejected_count, before.rejected_count);
        assert_eq!(
            self.late_confirmation_count, before.late_confirmation_count,
            "purge/drain paths that do not confirm late events must not mutate late-confirmation counters"
        );
    }
}

#[sinex_test]
async fn payload_budget_admits_at_limit_rejects_over_limit_and_recovers() -> TestResult<()> {
    let now = Timestamp::now();
    let at_limit_payload = json!({ "MESSAGE": "fits exactly at the byte budget" });
    let over_limit_payload = json!({ "MESSAGE": "this would exceed the retained byte budget" });
    let max_payload_bytes = payload_bytes(&at_limit_payload)?;
    let at_limit = provisional(
        "system.journald",
        "journald.entry.written",
        now,
        at_limit_payload,
    );
    let over_limit = provisional(
        "system.journald",
        "journald.entry.written",
        now,
        over_limit_payload,
    );
    let buffer = ConfirmationBuffer::with_capacity_grace_and_payload_budget(
        Duration::from_secs(60),
        16,
        Duration::from_secs(60),
        max_payload_bytes,
    );

    let admitted = buffer.add_provisional_with_pressure(at_limit.clone()).await;
    assert!(admitted.accepted);
    assert_eq!(admitted.rejection_reason, None);
    assert_eq!(
        admitted.runtime_action(),
        RuntimePressureAction::AdmitWithPressure
    );
    assert_eq!(admitted.rejected_redelivery_delay_ms(), None);
    assert_eq!(
        admitted.pressure_level,
        ConfirmationBufferPressureLevel::Critical
    );
    assert_eq!(admitted.projected_payload_bytes, max_payload_bytes);
    assert_eq!(buffer.retained_payload_bytes(), max_payload_bytes);

    let rejected = buffer
        .add_provisional_with_pressure(over_limit.clone())
        .await;
    assert!(!rejected.accepted);
    assert_eq!(
        rejected.rejection_reason,
        Some(ConfirmationBufferRejectionReason::PayloadBytes)
    );
    assert_eq!(
        rejected.rejected_redelivery_delay(),
        Some(Duration::from_secs(2))
    );
    assert_eq!(rejected.rejected_redelivery_delay_ms(), Some(2_000));
    assert_eq!(rejected.runtime_action(), RuntimePressureAction::Throttle);
    assert_eq!(
        rejected.pressure_level,
        ConfirmationBufferPressureLevel::Critical
    );
    assert_eq!(buffer.retained_payload_bytes(), max_payload_bytes);
    assert_eq!(buffer.rejected_count(), 1);
    let saturated_snapshot = buffer.snapshot().await;
    assert_eq!(
        saturated_snapshot.pressure_level,
        ConfirmationBufferPressureLevel::Critical
    );
    assert_eq!(
        saturated_snapshot.runtime_action,
        RuntimePressureAction::Throttle
    );

    let confirmed = buffer
        .confirm(at_limit.event_id)
        .await
        .expect("expected at-limit event to remain confirmable");
    assert_eq!(confirmed.event_id, at_limit.event_id);
    assert_eq!(buffer.retained_payload_bytes(), 0);

    let recovered = buffer.add_provisional_with_pressure(at_limit).await;
    assert!(recovered.accepted);
    assert_eq!(recovered.rejection_reason, None);
    assert_eq!(buffer.retained_payload_bytes(), max_payload_bytes);

    Ok(())
}

#[sinex_test]
async fn event_capacity_rejection_uses_short_resource_backoff() -> TestResult<()> {
    let now = Timestamp::now();
    let buffer = ConfirmationBuffer::with_capacity_grace_and_payload_budget(
        Duration::from_secs(60),
        1,
        Duration::from_secs(60),
        1024 * 1024,
    );
    let admitted = buffer
        .add_provisional_with_pressure(provisional(
            "system.journald",
            "journald.entry.written",
            now,
            json!({ "MESSAGE": "first" }),
        ))
        .await;
    let rejected = buffer
        .add_provisional_with_pressure(provisional(
            "system.journald",
            "journald.entry.written",
            now,
            json!({ "MESSAGE": "second" }),
        ))
        .await;

    assert_eq!(
        admitted.runtime_action(),
        RuntimePressureAction::AdmitWithPressure
    );
    assert_eq!(admitted.rejected_redelivery_delay(), None);
    assert_eq!(
        rejected.rejection_reason,
        Some(ConfirmationBufferRejectionReason::EventCapacity)
    );
    assert_eq!(
        rejected.rejected_redelivery_delay(),
        Some(Duration::from_secs(30))
    );
    assert_eq!(rejected.rejected_redelivery_delay_ms(), Some(30_000));
    assert_eq!(rejected.runtime_action(), RuntimePressureAction::Throttle);
    let saturated_snapshot = buffer.snapshot().await;
    assert_eq!(
        saturated_snapshot.pressure_level,
        ConfirmationBufferPressureLevel::Critical
    );
    assert_eq!(
        saturated_snapshot.runtime_action,
        RuntimePressureAction::Throttle
    );
    Ok(())
}

#[sinex_test]
async fn resource_budget_sets_candidate_and_payload_runtime_limits() -> TestResult<()> {
    let now = Timestamp::now();
    let first_payload = json!({ "MESSAGE": "accepted by exact budget" });
    let first_payload_bytes = payload_bytes(&first_payload)?;
    let buffer = ConfirmationBuffer::with_resource_budget(
        Duration::from_secs(60),
        test_budget(1, u64::try_from(first_payload_bytes)?),
    );

    assert_eq!(buffer.max_capacity(), 1);
    assert_eq!(buffer.max_payload_bytes(), first_payload_bytes);

    let admitted = buffer
        .add_provisional_with_pressure(provisional(
            "system.journald",
            "journald.entry.written",
            now,
            first_payload,
        ))
        .await;
    assert!(admitted.accepted);
    assert_eq!(admitted.rejection_reason, None);
    assert_eq!(buffer.retained_payload_bytes(), first_payload_bytes);

    let rejected = buffer
        .add_provisional_with_pressure(provisional(
            "system.journald",
            "journald.entry.written",
            now,
            json!({ "MESSAGE": "second event exceeds candidate budget" }),
        ))
        .await;
    assert!(!rejected.accepted);
    assert_eq!(
        rejected.rejection_reason,
        Some(ConfirmationBufferRejectionReason::EventCapacity)
    );

    Ok(())
}

#[sinex_test]
async fn resource_budget_rejects_payloads_above_material_byte_budget() -> TestResult<()> {
    let now = Timestamp::now();
    let buffer =
        ConfirmationBuffer::with_resource_budget(Duration::from_secs(60), test_budget(8, 1));

    let rejected = buffer
        .add_provisional_with_pressure(provisional(
            "system.journald",
            "journald.entry.written",
            now,
            json!({ "MESSAGE": "larger than one byte" }),
        ))
        .await;

    assert!(!rejected.accepted);
    assert_eq!(
        rejected.rejection_reason,
        Some(ConfirmationBufferRejectionReason::PayloadBytes)
    );
    assert_eq!(buffer.retained_payload_bytes(), 0);

    Ok(())
}

#[sinex_test]
async fn payload_budget_accounts_for_same_event_replacement() -> TestResult<()> {
    let now = Timestamp::now();
    let initial_payload = json!({ "MESSAGE": "small" });
    let replacement_payload = json!({ "MESSAGE": "larger replacement payload" });
    let oversized_payload = json!({ "MESSAGE": "oversized replacement payload".repeat(16) });
    let max_payload_bytes = payload_bytes(&replacement_payload)?;
    let initial = provisional(
        "system.journald",
        "journald.entry.written",
        now,
        initial_payload,
    );
    let replacement = ProvisionalEvent {
        payload: replacement_payload,
        ..initial.clone()
    };
    let oversized = ProvisionalEvent {
        payload: oversized_payload,
        ..initial.clone()
    };
    let buffer = ConfirmationBuffer::with_capacity_grace_and_payload_budget(
        Duration::from_secs(60),
        1,
        Duration::from_secs(60),
        max_payload_bytes,
    );

    assert!(
        buffer
            .add_provisional_with_pressure(initial.clone())
            .await
            .accepted
    );
    let replaced = buffer.add_provisional_with_pressure(replacement).await;
    assert!(replaced.accepted);
    assert_eq!(replaced.pending_count, 1);
    assert_eq!(replaced.projected_payload_bytes, max_payload_bytes);
    assert_eq!(buffer.len().await, 1);
    assert_eq!(buffer.retained_payload_bytes(), max_payload_bytes);

    let rejected = buffer.add_provisional_with_pressure(oversized).await;
    assert!(!rejected.accepted);
    assert_eq!(
        rejected.rejection_reason,
        Some(ConfirmationBufferRejectionReason::PayloadBytes)
    );
    assert_eq!(buffer.len().await, 1);
    assert_eq!(buffer.retained_payload_bytes(), max_payload_bytes);

    Ok(())
}

#[sinex_test]
async fn same_event_replacement_preserves_timeout_grace_state() -> TestResult<()> {
    let old = Timestamp::from_unix_timestamp(1).expect("timestamp in range");
    let initial = provisional(
        "system.journald",
        "journald.entry.written",
        old,
        json!({ "MESSAGE": "original" }),
    );
    let replacement = ProvisionalEvent {
        payload: json!({ "MESSAGE": "redelivered replacement" }),
        ..initial.clone()
    };
    let buffer = ConfirmationBuffer::with_capacity_and_grace(
        Duration::from_millis(0),
        1,
        Duration::from_millis(0),
    );

    assert!(buffer.add_provisional(initial).await);
    assert_eq!(buffer.check_timeouts().await, vec![replacement.event_id]);

    let replaced = buffer.add_provisional_with_pressure(replacement).await;
    assert!(replaced.accepted);
    let retained = buffer.snapshot().await;
    assert_eq!(retained.pending_count, 1);
    assert_eq!(retained.timed_out_retained_count, 1);

    let purged = buffer.purge_expired().await;
    assert_eq!(purged.len(), 1);
    assert_eq!(buffer.len().await, 0);

    Ok(())
}

#[sinex_test]
async fn snapshot_reports_pending_timeout_rejections_and_payload_bytes() -> TestResult<()> {
    let buffer = ConfirmationBuffer::with_capacity_and_grace(
        Duration::from_millis(0),
        2,
        Duration::from_secs(60),
    );
    let old = Timestamp::from_unix_timestamp(1).expect("timestamp in range");
    let first = provisional(
        "system.journald",
        "journald.entry.written",
        old,
        json!({ "MESSAGE": "Late confirmation arrived after provisional timeout" }),
    );
    let second = provisional(
        "sinexd.event_engine",
        "batch.stats",
        old,
        json!({ "events_processed": 42 }),
    );
    let rejected = provisional(
        "system.journald",
        "journald.entry.written",
        old,
        json!({ "MESSAGE": "should be rejected at capacity" }),
    );

    assert!(buffer.add_provisional(first).await);
    assert!(buffer.add_provisional(second).await);
    assert!(!buffer.add_provisional(rejected).await);
    let timed_out = buffer.check_timeouts().await;
    assert_eq!(timed_out.len(), 2);

    let snapshot = buffer.snapshot().await;
    assert_eq!(snapshot.pending_count, 2);
    assert_eq!(snapshot.timed_out_retained_count, 2);
    assert_eq!(snapshot.rejected_count, 1);
    assert_eq!(snapshot.late_confirmation_count, 0);
    assert!(snapshot.approximate_payload_bytes > 0);
    assert_eq!(snapshot.active_payload_bytes, 0);
    assert_eq!(
        snapshot.timed_out_retained_payload_bytes,
        snapshot.approximate_payload_bytes
    );
    assert_eq!(
        snapshot.retained_payload_bytes,
        snapshot.approximate_payload_bytes
    );
    assert_eq!(snapshot.max_payload_bytes, buffer.max_payload_bytes());
    assert!(
        snapshot
            .approximate_payload_bytes_by_kind
            .contains_key("system.journald:journald.entry.written")
    );
    assert!(
        snapshot
            .approximate_payload_bytes_by_kind
            .contains_key("sinexd.event_engine:batch.stats")
    );

    Ok(())
}

#[sinex_test]
async fn snapshot_splits_active_and_timed_out_retained_payload_bytes() -> TestResult<()> {
    let buffer = ConfirmationBuffer::with_capacity_and_grace(
        Duration::from_secs(60),
        2,
        Duration::from_secs(60),
    );
    let old = Timestamp::from_unix_timestamp(1).expect("timestamp in range");
    let current = Timestamp::now();
    let timed_out_payload = json!({ "MESSAGE": "delayed confirmation retained in grace" });
    let active_payload = json!({ "MESSAGE": "fresh provisional event" });
    let timed_out_bytes = payload_bytes(&timed_out_payload)?;
    let active_bytes = payload_bytes(&active_payload)?;

    assert!(
        buffer
            .add_provisional(provisional(
                "system.journald",
                "journald.entry.written",
                old,
                timed_out_payload,
            ))
            .await
    );
    assert!(
        buffer
            .add_provisional(provisional(
                "sinexd.event_engine",
                "batch.stats",
                current,
                active_payload,
            ))
            .await
    );

    let timed_out = buffer.check_timeouts().await;
    assert_eq!(timed_out.len(), 1);

    let snapshot = buffer.snapshot().await;
    assert_eq!(snapshot.pending_count, 2);
    assert_eq!(snapshot.timed_out_retained_count, 1);
    assert_eq!(snapshot.active_payload_bytes, active_bytes);
    assert_eq!(snapshot.timed_out_retained_payload_bytes, timed_out_bytes);
    assert_eq!(
        snapshot.approximate_payload_bytes,
        active_bytes + timed_out_bytes
    );
    assert_eq!(
        snapshot.retained_payload_bytes,
        snapshot.approximate_payload_bytes
    );

    Ok(())
}

#[sinex_test]
async fn watermark_late_confirmations_are_counted_without_retaining_backlog() -> TestResult<()>
{
    let buffer = ConfirmationBuffer::with_capacity_and_grace(
        Duration::from_millis(0),
        16,
        Duration::from_secs(60),
    );
    let old = Timestamp::from_unix_timestamp(1).expect("timestamp in range");
    let first = provisional(
        "system.journald",
        "journald.entry.written",
        old,
        json!({ "MESSAGE": "late confirmation 1" }),
    );
    let second = provisional(
        "system.journald",
        "journald.entry.written",
        old,
        json!({ "MESSAGE": "late confirmation 2" }),
    );
    let watermark = if first.event_id.as_uuid() > second.event_id.as_uuid() {
        first.event_id
    } else {
        second.event_id
    };

    assert!(buffer.add_provisional(first).await);
    assert!(buffer.add_provisional(second).await);
    assert_eq!(buffer.check_timeouts().await.len(), 2);

    let confirmed = buffer
        .confirm_kind_up_to("system.journald", "journald.entry.written", watermark)
        .await;

    assert_eq!(confirmed.len(), 2);
    let snapshot = buffer.snapshot().await;
    assert_eq!(snapshot.pending_count, 0);
    assert_eq!(snapshot.timed_out_retained_count, 0);
    assert_eq!(snapshot.late_confirmation_count, 2);

    Ok(())
}

#[sinex_test]
async fn timed_out_journald_payload_retention_is_bounded_by_capacity_and_grace()
-> TestResult<()> {
    const CAPACITY: usize = 16;
    const OVERFLOW_ATTEMPTS: usize = 32;

    let buffer = ConfirmationBuffer::with_capacity_and_grace(
        Duration::from_millis(0),
        CAPACITY,
        Duration::from_millis(0),
    );
    let old = Timestamp::from_unix_timestamp(1).expect("timestamp in range");
    let feedback_payload = "Late confirmation arrived after provisional timeout ".repeat(64);

    for index in 0..CAPACITY {
        assert!(
            buffer
                .add_provisional(provisional(
                    "system.journald",
                    "journald.entry.written",
                    old,
                    json!({
                        "MESSAGE": feedback_payload,
                        "SEQ": index,
                        "_SYSTEMD_UNIT": "sinexd.service"
                    }),
                ))
                .await
        );
    }
    for index in 0..OVERFLOW_ATTEMPTS {
        assert!(
            !buffer
                .add_provisional(provisional(
                    "system.journald",
                    "journald.entry.written",
                    old,
                    json!({
                        "MESSAGE": feedback_payload,
                        "SEQ": CAPACITY + index,
                        "_SYSTEMD_UNIT": "sinexd.service"
                    }),
                ))
                .await
        );
    }

    assert_eq!(buffer.check_timeouts().await.len(), CAPACITY);
    let retained = ConfirmationBufferEvidence::capture(&buffer).await;
    assert_eq!(retained.pending_count, CAPACITY);
    assert_eq!(retained.timed_out_retained_count, CAPACITY);
    assert_eq!(retained.rejected_count, OVERFLOW_ATTEMPTS as u64);
    assert!(retained.retained_payload_bytes > 0);
    assert_eq!(retained.active_payload_bytes, 0);
    assert_eq!(
        retained.timed_out_retained_payload_bytes,
        retained.retained_payload_bytes
    );
    assert_eq!(
        retained.journald_payload_bytes,
        retained.retained_payload_bytes
    );

    let purged = buffer.purge_expired().await;
    assert_eq!(purged.len(), CAPACITY);
    let drained = ConfirmationBufferEvidence::capture(&buffer).await;
    drained.assert_drained_after(&retained);

    Ok(())
}

#[sinex_test]
async fn delayed_confirmation_feedback_logs_are_sparse_and_journald_suppressed()
-> TestResult<()> {
    const LATE_EVENTS: usize = 20;
    const OVERFLOW_ATTEMPTS: usize = 8;
    let buffer = ConfirmationBuffer::with_capacity_and_grace(
        Duration::from_millis(0),
        LATE_EVENTS,
        Duration::from_secs(60),
    );
    let old = Timestamp::from_unix_timestamp(1).expect("timestamp in range");
    let mut watermark = None;

    for index in 0..LATE_EVENTS {
        let event = provisional(
            "system.journald",
            "journald.entry.written",
            old,
            json!({
                "MESSAGE": format!("feedback candidate {index}"),
                "_SYSTEMD_UNIT": "sinexd.service"
            }),
        );
        watermark = Some(watermark.map_or(event.event_id, |previous: EventId| {
            if event.event_id.as_uuid() > previous.as_uuid() {
                event.event_id
            } else {
                previous
            }
        }));
        assert!(buffer.add_provisional(event).await);
    }
    for index in 0..OVERFLOW_ATTEMPTS {
        let rejected = buffer
            .add_provisional_with_pressure(provisional(
                "system.journald",
                "journald.entry.written",
                old,
                json!({
                    "MESSAGE": format!("overflow feedback candidate {index}"),
                    "_SYSTEMD_UNIT": "sinexd.service"
                }),
            ))
            .await;
        assert!(!rejected.accepted);
        assert_eq!(
            rejected.rejection_reason,
            Some(ConfirmationBufferRejectionReason::EventCapacity)
        );
        assert_eq!(rejected.runtime_action(), RuntimePressureAction::Throttle);
        assert_eq!(rejected.rejected_redelivery_delay_ms(), Some(30_000));
    }

    assert_eq!(buffer.check_timeouts().await.len(), LATE_EVENTS);
    let before = ConfirmationBufferEvidence::capture(&buffer).await;
    assert_eq!(before.pending_count, LATE_EVENTS);
    assert_eq!(before.timed_out_retained_count, LATE_EVENTS);
    assert_eq!(before.rejected_count, OVERFLOW_ATTEMPTS as u64);
    assert_eq!(before.active_payload_bytes, 0);
    assert!(before.timed_out_retained_payload_bytes > 0);
    assert_eq!(
        before.retained_payload_bytes,
        before.timed_out_retained_payload_bytes
    );
    assert_eq!(before.journald_payload_bytes, before.retained_payload_bytes);
    assert_eq!(before.runtime_action, RuntimePressureAction::Throttle);

    let captured = CapturedLogs::default();
    let subscriber = tracing_subscriber::fmt()
        .with_ansi(false)
        .with_max_level(tracing::Level::WARN)
        .without_time()
        .with_writer(captured.clone())
        .finish();

    {
        let _guard = tracing::subscriber::set_default(subscriber);
        buffer
            .confirm_kind_up_to(
                "system.journald",
                "journald.entry.written",
                watermark.expect("watermark set"),
            )
            .await;
    }

    let after = ConfirmationBufferEvidence::capture(&buffer).await;
    assert_eq!(after.pending_count, 0);
    assert_eq!(after.timed_out_retained_count, 0);
    assert_eq!(after.late_confirmation_count, LATE_EVENTS as u64);
    assert_eq!(after.rejected_count, OVERFLOW_ATTEMPTS as u64);
    assert_eq!(after.retained_payload_bytes, 0);
    assert_eq!(after.active_payload_bytes, 0);
    assert_eq!(after.timed_out_retained_payload_bytes, 0);
    assert_eq!(
        after.journald_payload_bytes, 0,
        "confirmed backlog should not leave payload attribution behind"
    );

    let log_output = captured.output();
    let feedback_lines = log_output
        .lines()
        .filter(|line| line.contains("Late confirmations accepted after timeout"))
        .collect::<Vec<_>>();
    assert_eq!(
        feedback_lines.len(),
        5,
        "20 late confirmations should log only totals 1,2,4,8,16: {log_output}"
    );
    assert!(
        log_output.contains("runtime.confirmation_late_total"),
        "aggregate feedback log should carry the metric field: {log_output}"
    );

    let mid = Id::<SourceMaterial>::new();
    let journal_lines = feedback_lines
        .iter()
        .enumerate()
        .map(|(index, line)| {
            json!({
                "__CURSOR": format!("s=feedback;i={index}"),
                "__REALTIME_TIMESTAMP": format!("{}", 1_700_000_000_000_000_i64 + index as i64),
                "_SYSTEMD_UNIT": "sinexd.service",
                "SYSLOG_IDENTIFIER": "sinexd",
                "MESSAGE": line,
            })
            .to_string()
        })
        .collect::<Vec<_>>();
    let line_refs = journal_lines.iter().map(String::as_str).collect::<Vec<_>>();
    let records = records_from_journal_lines(mid, &line_refs);
    let mut parser = JournaldParser;
    let ctx = journal_parser_ctx(mid);

    for record in records {
        let intents = parser
            .parse_record(record.expect("journal record should parse"), &ctx)
            .await
            .expect("journald parser should parse feedback-shaped JSON");
        assert!(
            intents.is_empty(),
            "confirmation feedback journal entry should be suppressed"
        );
    }

    let ordinary = json!({
        "__CURSOR": "s=ordinary;i=1",
        "__REALTIME_TIMESTAMP": "1700000000001000",
        "_SYSTEMD_UNIT": "sinexd.service",
        "SYSLOG_IDENTIFIER": "sinexd",
        "MESSAGE": "source catalog exported",
    })
    .to_string();
    let ordinary_records = records_from_journal_lines(mid, &[ordinary.as_str()]);
    let ordinary_intents = parser
        .parse_record(
            ordinary_records[0]
                .as_ref()
                .expect("ordinary journal record should parse")
                .clone(),
            &ctx,
        )
        .await
        .expect("ordinary sinexd journal entry should parse");
    assert_eq!(ordinary_intents.len(), 1);
    assert_eq!(
        ordinary_intents[0].payload["message"],
        Value::from("source catalog exported")
    );

    Ok(())
}

#[sinex_test]
async fn late_confirmation_aggregate_log_schedule_is_sparse() -> TestResult<()> {
    assert!(should_log_late_confirmation_aggregate(1));
    assert!(should_log_late_confirmation_aggregate(2));
    assert!(should_log_late_confirmation_aggregate(1024));
    assert!(should_log_late_confirmation_aggregate(10_000));
    assert!(!should_log_late_confirmation_aggregate(3));
    assert!(!should_log_late_confirmation_aggregate(9_999));
    assert!(!should_log_late_confirmation_aggregate(10_001));

    Ok(())
}
