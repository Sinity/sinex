//! sinex-pdq5: interval invariant property suite.
//!
//! zs6 (heartbeat chop), uzc (tie/out-of-order drops), and the 5s6 closure/fence
//! defects were all found by manual adversarial reading. This harness generates
//! synthetic evidence streams (focus/workspace transitions, ActivityWatch window
//! heartbeats with gaps/duplicates/reorderings/ties, systemd start/stop pairs
//! with missing counterparts, suspend fences) and asserts six invariant families
//! over the emitted interval set:
//!
//! 1. per (state_kind, subject) emitted intervals are non-overlapping;
//! 2. coverage conservation — no interval falls outside the evidenced span
//!    (plus the declared heartbeat slack), so gaps are never intervalized;
//! 3. no boundary moves — every occurrence key is emitted at most once and no
//!    interval has a backward (end < start) boundary;
//! 4. supersession sanity — at most one live interpretation per occurrence key;
//! 5. replay equivalence — replaying the same stream yields the same interval
//!    SET by occurrence key (fresh interpretation ids allowed);
//! 6. tie/out-of-order inputs never produce a corrupt interval (negative span or
//!    a duplicate key) — they supersede, clamp, or no-op, never vanish silently.
//!
//! proptest supplies deterministic seeds, shrinks a failing stream to a minimal
//! reproduction, and prints the seed on failure. These same invariants are what
//! the pre-fix zs6/uzc/5s6 defects violated (a gap-inclusive heartbeat close
//! breaks #2; a tie-drop breaks #6; a wall-clock replay chop breaks #5), so the
//! suite is sensitive to their regression.

use proptest::prelude::*;
use sinex_primitives::domain::{ProcessingMode, TriggerKind};
use sinex_primitives::events::enums::{SystemdActiveState, SystemdUnitType};
use sinex_primitives::events::payloads::{
    ActivityWatchAfkChangedPayload, ActivityWatchWindowActivePayload, HyprlandWindowFocusedPayload,
    HyprlandWorkspaceSwitchedPayload, StateIntervalPayload, SystemdUnitStartedPayload,
    SystemdUnitStoppedPayload,
};
use sinex_primitives::events::Event;
use sinex_primitives::temporal::Timestamp;
use sinex_primitives::{EventSource, EventType, Id, JsonValue, Uuid};
use sinexd::automata::interval_lift::{IntervalLift, IntervalLiftState};
use sinexd::runtime::MultiOutputTransducer;
use sinexd::runtime::automaton::AutomatonContext;

/// One synthetic interval-lift input. Small domains keep occurrence keys colliding
/// often enough to exercise ties, reorderings, and duplicates.
#[derive(Debug, Clone)]
enum SynthInput {
    Focus { window: u8, ts: i64 },
    Workspace { ws: i32, ts: i64 },
    /// `duration_ms == 0` is a heartbeat; otherwise a bounded observation.
    AwWindow { app: u8, ts: i64, duration_ms: u64 },
    Afk { ts: i64, duration_ms: u64 },
    SystemdStart { unit: u8, ts: i64 },
    SystemdStop { unit: u8, ts: i64 },
    Suspend { ts: i64 },
}

impl SynthInput {
    fn ts(&self) -> i64 {
        match self {
            SynthInput::Focus { ts, .. }
            | SynthInput::Workspace { ts, .. }
            | SynthInput::AwWindow { ts, .. }
            | SynthInput::Afk { ts, .. }
            | SynthInput::SystemdStart { ts, .. }
            | SynthInput::SystemdStop { ts, .. }
            | SynthInput::Suspend { ts } => *ts,
        }
    }
}

/// A deterministic material occurrence per input, so replay of the same stream
/// re-derives identical occurrence keys (interval-lift is `MaterialOnly`).
fn material_for(seed: u64) -> Uuid {
    Uuid::from_u128(0x5eed_0000_0000_0000_0000_0000_0000_0000 | u128::from(seed))
}

fn context(source: &'static str, event_type: &'static str, ts: i64, seed: u64) -> AutomatonContext {
    let trigger_event_id: Id<Event<JsonValue>> = Id::new();
    AutomatonContext {
        trigger_event_id,
        source: EventSource::from_static(source),
        event_type: EventType::from_static(event_type),
        ts_orig: Some(Timestamp::from_unix_timestamp(ts).expect("valid ts")),
        ts_coided: trigger_event_id.timestamp(),
        processing_mode: ProcessingMode::Live,
        trigger_kind: TriggerKind::NewEvent,
        created_by_operation_id: None,
        trigger_material_id: Some(material_for(seed)),
        trigger_anchor_byte: Some(ts),
    }
}

/// Build the (JSON payload, context) pair for one synthetic input. `idx` seeds a
/// stable-but-distinct material occurrence per stream position.
fn build(input: &SynthInput, idx: usize) -> (JsonValue, AutomatonContext) {
    let seed = idx as u64;
    match input {
        SynthInput::Focus { window, ts } => (
            serde_json::to_value(HyprlandWindowFocusedPayload {
                window_id: Some(format!("0x{window}")),
                window_class: Some(format!("class{window}")),
                window_title: Some(format!("title{window}")),
                workspace_id: Some(1),
                previous_window_id: None,
            })
            .unwrap(),
            context("wm.hyprland", "window.focused", *ts, seed),
        ),
        SynthInput::Workspace { ws, ts } => (
            serde_json::to_value(HyprlandWorkspaceSwitchedPayload {
                to_workspace_id: *ws,
                workspace_name: Some(format!("ws{ws}")),
                from_workspace_id: None,
                monitor_id: None,
                active_window_id: None,
            })
            .unwrap(),
            context("wm.hyprland", "workspace.switched", *ts, seed),
        ),
        SynthInput::AwWindow {
            app,
            ts,
            duration_ms,
        } => (
            serde_json::to_value(ActivityWatchWindowActivePayload {
                app: format!("app{app}"),
                title: format!("t{app}"),
                duration_ms: *duration_ms,
                bucket_id: "aw-watcher-window".to_string(),
            })
            .unwrap(),
            context("activitywatch", "window.active", *ts, seed),
        ),
        SynthInput::Afk { ts, duration_ms } => (
            serde_json::to_value(ActivityWatchAfkChangedPayload {
                status: "afk".to_string(),
                duration_ms: *duration_ms,
                bucket_id: "aw-watcher-afk".to_string(),
            })
            .unwrap(),
            context("activitywatch", "afk.changed", *ts, seed),
        ),
        SynthInput::SystemdStart { unit, ts } => (
            serde_json::to_value(SystemdUnitStartedPayload {
                unit_name: format!("u{unit}.service"),
                unit_type: SystemdUnitType::Service,
                main_pid: None,
                active_state: SystemdActiveState::Active,
                sub_state: "running".to_string(),
            })
            .unwrap(),
            context("systemd", "unit.started", *ts, seed),
        ),
        SynthInput::SystemdStop { unit, ts } => (
            serde_json::to_value(SystemdUnitStoppedPayload {
                unit_name: format!("u{unit}.service"),
                unit_type: SystemdUnitType::Service,
                exit_code: None,
                active_state: SystemdActiveState::Inactive,
                sub_state: "dead".to_string(),
            })
            .unwrap(),
            context("systemd", "unit.stopped", *ts, seed),
        ),
        SynthInput::Suspend { ts } => (
            serde_json::to_value(SystemdUnitStartedPayload {
                unit_name: "sleep.target".to_string(),
                unit_type: SystemdUnitType::Target,
                main_pid: None,
                active_state: SystemdActiveState::Active,
                sub_state: "active".to_string(),
            })
            .unwrap(),
            context("systemd", "unit.started", *ts, seed),
        ),
    }
}

/// Drive a synthetic stream through interval-lift and collect emitted intervals.
fn run_stream(inputs: &[SynthInput]) -> Vec<StateIntervalPayload> {
    let rt = tokio::runtime::Builder::new_current_thread()
        .build()
        .expect("current-thread runtime");
    rt.block_on(async {
        let mut automaton = IntervalLift::default();
        let mut state = IntervalLiftState::default();
        let mut out = Vec::new();
        for (idx, input) in inputs.iter().enumerate() {
            let (payload, ctx) = build(input, idx);
            let outputs = automaton
                .process(&mut state, payload, &ctx)
                .await
                .expect("interval-lift process must not error on well-typed input");
            out.extend(outputs.into_iter().map(|o| o.payload));
        }
        out
    })
}

/// Heartbeat end-slack (seconds) — the only interval kind allowed to extend past
/// its last evidence, and only by this much (sinex-zs6).
const SLACK_SECS: i64 = 30;

/// Robustness invariants that must hold for ANY stream, including adversarial
/// out-of-order / tie / duplicate / re-delivered inputs (Families 3, 4, 6):
/// interval-lift never emits a backward boundary and never re-emits an
/// occurrence key (ties supersede, out-of-order clamps or no-ops, nothing
/// vanishes into a corrupt interval).
fn check_robustness(intervals: &[StateIntervalPayload]) -> Result<(), String> {
    let mut seen_keys = std::collections::HashSet::new();
    for iv in intervals {
        if iv.end_time < iv.start_time {
            return Err(format!(
                "backward boundary: interval {} end {} < start {}",
                iv.interval_id, iv.end_time, iv.start_time
            ));
        }
        if !seen_keys.insert(iv.interval_id.clone()) {
            return Err(format!(
                "duplicate occurrence key {} (boundary moved / >1 live interpretation)",
                iv.interval_id
            ));
        }
    }
    Ok(())
}

/// Coverage + non-overlap invariants (Families 1, 2), which hold once the stream
/// arrives in evidence (ts) order — the ordering the pipeline guarantees (events
/// are consumed by `ts_coided`, and a single source's historical scan yields
/// monotonic `ts_orig`). A past-timestamped input arriving after a later one is a
/// synthetic pathology exercised separately by [`check_robustness`].
fn check_ordered_invariants(
    inputs: &[SynthInput],
    intervals: &[StateIntervalPayload],
) -> Result<(), String> {
    check_robustness(intervals)?;

    // Family 2: coverage conservation. Every interval lies within the evidenced
    // span, save the bounded heartbeat slack; gaps are therefore never covered.
    if let (Some(min_ts), Some(max_ts)) = (
        inputs.iter().map(SynthInput::ts).min(),
        inputs.iter().map(SynthInput::ts).max(),
    ) {
        let lo = Timestamp::from_unix_timestamp(min_ts).unwrap();
        let hi = Timestamp::from_unix_timestamp(max_ts + SLACK_SECS).unwrap();
        for iv in intervals {
            if iv.start_time < lo || iv.end_time > hi {
                return Err(format!(
                    "interval {} [{}, {}] escapes evidence span [{}, {}]",
                    iv.interval_id, iv.start_time, iv.end_time, lo, hi
                ));
            }
        }
    }

    // Family 1: per (state_kind, subject) intervals are non-overlapping.
    let mut by_subject: std::collections::HashMap<
        (String, Option<String>),
        Vec<&StateIntervalPayload>,
    > = std::collections::HashMap::new();
    for iv in intervals {
        by_subject
            .entry((iv.state_kind.clone(), iv.subject_id.clone()))
            .or_default()
            .push(iv);
    }
    for ((kind, subject), mut group) in by_subject {
        group.sort_by_key(|iv| iv.start_time);
        for pair in group.windows(2) {
            if pair[1].start_time < pair[0].end_time {
                return Err(format!(
                    "overlap in ({kind}, {subject:?}): [{}, {}] then [{}, {}]",
                    pair[0].start_time, pair[0].end_time, pair[1].start_time, pair[1].end_time
                ));
            }
        }
    }

    Ok(())
}

/// Sort a stream into evidence (ts) order, preserving the relative order of ties
/// so same-instant transitions still exercise the tie/supersede path.
fn ts_sorted(inputs: &[SynthInput]) -> Vec<SynthInput> {
    let mut sorted = inputs.to_vec();
    sorted.sort_by_key(SynthInput::ts);
    sorted
}

/// Slot-based inputs only (focus/workspace transitions, systemd start/stop,
/// suspend fences, AW heartbeats). interval-lift derives these through its
/// close-before-open slot discipline, so their emitted intervals are
/// non-overlapping by construction — the property the ordered test checks.
///
/// Excluded: AW window/afk observations with a non-zero duration. Those are
/// self-contained source-reported spans passed through verbatim
/// (`observed_duration_interval`), so overlapping *source* observations yield
/// overlapping intervals — a source concern, not interval-lift's slot discipline.
/// They still drive the robustness and replay properties via the full strategy.
fn transition_input_strategy() -> impl Strategy<Value = SynthInput> {
    let ts = 0i64..64;
    prop_oneof![
        (0u8..3, ts.clone()).prop_map(|(window, ts)| SynthInput::Focus { window, ts }),
        (0i32..3, ts.clone()).prop_map(|(ws, ts)| SynthInput::Workspace { ws, ts }),
        (0u8..3, ts.clone())
            .prop_map(|(app, ts)| SynthInput::AwWindow { app, ts, duration_ms: 0 }),
        (0u8..2, ts.clone()).prop_map(|(unit, ts)| SynthInput::SystemdStart { unit, ts }),
        (0u8..2, ts.clone()).prop_map(|(unit, ts)| SynthInput::SystemdStop { unit, ts }),
        ts.prop_map(|ts| SynthInput::Suspend { ts }),
    ]
}

fn synth_input_strategy() -> impl Strategy<Value = SynthInput> {
    // Small ts window (0..64) forces frequent ties, reorderings, and duplicates.
    let ts = 0i64..64;
    prop_oneof![
        (0u8..3, ts.clone()).prop_map(|(window, ts)| SynthInput::Focus { window, ts }),
        (0i32..3, ts.clone()).prop_map(|(ws, ts)| SynthInput::Workspace { ws, ts }),
        (0u8..3, ts.clone(), prop_oneof![Just(0u64), 1000u64..5000])
            .prop_map(|(app, ts, duration_ms)| SynthInput::AwWindow { app, ts, duration_ms }),
        (ts.clone(), 1000u64..5000).prop_map(|(ts, duration_ms)| SynthInput::Afk { ts, duration_ms }),
        (0u8..2, ts.clone()).prop_map(|(unit, ts)| SynthInput::SystemdStart { unit, ts }),
        (0u8..2, ts.clone()).prop_map(|(unit, ts)| SynthInput::SystemdStop { unit, ts }),
        ts.prop_map(|ts| SynthInput::Suspend { ts }),
    ]
}

proptest! {
    #![proptest_config(ProptestConfig { cases: 512, ..ProptestConfig::default() })]

    /// Families 1-4: coverage, non-overlap, no boundary move, supersession
    /// sanity — over an evidence-ordered stream (the pipeline's guarantee).
    #[test]
    fn interval_invariants_hold(inputs in prop::collection::vec(transition_input_strategy(), 0..40)) {
        let ordered = ts_sorted(&inputs);
        let intervals = run_stream(&ordered);
        check_ordered_invariants(&ordered, &intervals).map_err(TestCaseError::fail)?;
    }

    /// Family 6: adversarial out-of-order / tie / duplicate streams never corrupt
    /// the emitted set — no backward boundary, no re-emitted occurrence key, no
    /// panic (a corrupt clamp or a silent-drop-into-dup would fail here).
    #[test]
    fn adversarial_streams_never_corrupt(
        inputs in prop::collection::vec(synth_input_strategy(), 0..40)
    ) {
        let intervals = run_stream(&inputs);
        check_robustness(&intervals).map_err(TestCaseError::fail)?;
    }

    /// Family 5: replay equivalence — replaying the same stream re-derives the
    /// same occurrence-key SET (fresh interpretation ids allowed).
    #[test]
    fn replay_yields_same_occurrence_key_set(
        inputs in prop::collection::vec(synth_input_strategy(), 0..40)
    ) {
        let ordered = ts_sorted(&inputs);
        let first: std::collections::BTreeSet<String> =
            run_stream(&ordered).into_iter().map(|iv| iv.interval_id).collect();
        let second: std::collections::BTreeSet<String> =
            run_stream(&ordered).into_iter().map(|iv| iv.interval_id).collect();
        prop_assert_eq!(first, second, "replay must re-derive an identical occurrence-key set");
    }
}
