//! Two-mode input-time watermark for windowed automaton flushes (sinex-5s6).
//!
//! Windowed automata (activity windows, sessions, hourly/daily buckets) close
//! trailing accumulators on a periodic timer. Consulting raw wall-clock `now`
//! for that decision is correct only when the automaton is *caught up* to live
//! input: during historical replay/backfill, events arrive with old `ts_orig`
//! at wall-clock speed, so a wall-clock flush would declare a multi-hour "gap"
//! after nearly every event and chop historical continuity into fragments.
//!
//! The watermark is the clock the flush actually consults:
//! - **Catchup** (a backlog of old-timestamped events is actively flowing):
//!   watermark = the highest input `ts_orig` seen. Wall time cannot advance it,
//!   so replay never invents gaps that did not occur in the world.
//! - **Live** (caught up — either quiet or receiving current-timestamped
//!   events): watermark = wall clock (minus an idle grace for late arrivals),
//!   so a genuine quiet period still closes the trailing window/session without
//!   waiting for a future event to arrive.
//!
//! The mode signal is self-contained in the adapter — no cross-consumer lag
//! plumbing — from two locally-tracked facts: the input-time high-water mark
//! (`max_input_ts_orig`) and the wall-clock instant of the last received event
//! (`last_input_wall`). "Old-timestamped events still arriving" is Catchup;
//! "quiet" or "current-timestamped events arriving" is Live. A Catchup→Live
//! transition (the backlog drains, events stop or catch up to the present)
//! re-evaluates open accumulators against the now-advancing wall clock on the
//! next flush, exactly as the settled design requires.

use sinex_primitives::temporal::Timestamp;
use time::Duration;

/// Input-time lag beyond which a *flowing* stream is treated as replay/backfill
/// rather than live. Normal live jitter (seconds) stays well under this; a
/// backlog of historical events sits far above it.
const CATCHUP_INPUT_LAG: Duration = Duration::minutes(5);

/// Wall-clock window within which a just-received event counts as "the stream
/// is currently flowing". Sized above the default 60 s flush interval so a
/// steadily-fed catch-up stream reads as flowing across flush ticks.
const RECEIVING_WINDOW: Duration = Duration::seconds(150);

/// Grace subtracted from wall clock in Live mode before closing a trailing
/// accumulator, absorbing late/out-of-order arrivals. Zero preserves the exact
/// pre-5s6 live flush timing (the per-automaton `gap_threshold` already
/// supplies the primary quiet margin); retained as a named tuning knob.
const LIVE_IDLE_GRACE: Duration = Duration::ZERO;

/// Which clock the flush consulted — recorded for observability and tests.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WatermarkMode {
    /// Caught up: watermark tracks wall clock (minus idle grace).
    Live,
    /// Backlog of old-timestamped events flowing: watermark = max input ts_orig.
    Catchup,
}

/// Decide the flush watermark and mode from the adapter's input-time high-water
/// mark and the wall-clock time of the last received event.
///
/// - `max_input_ts_orig`: highest `ts_orig` durably incorporated (None if none yet).
/// - `last_input_wall`: wall-clock instant the last event was received (None if none yet).
/// - `now`: current wall-clock time.
#[must_use]
pub fn flush_watermark(
    max_input_ts_orig: Option<Timestamp>,
    last_input_wall: Option<Timestamp>,
    now: Timestamp,
) -> (Timestamp, WatermarkMode) {
    let Some(max_input) = max_input_ts_orig else {
        // No input observed yet: there is nothing to close, so the choice is
        // immaterial; Live/now keeps the degenerate case simple.
        return (now, WatermarkMode::Live);
    };
    let flowing = last_input_wall.is_some_and(|w| now - w < RECEIVING_WINDOW);
    let behind = now - max_input > CATCHUP_INPUT_LAG;
    if flowing && behind {
        // Old-timestamped events are actively arriving: replay/backfill. Only
        // input time may advance the watermark, so continuity is never chopped
        // by processing delay.
        (max_input, WatermarkMode::Catchup)
    } else {
        // Caught up (quiet, or current-timestamped events arriving): advance
        // with the wall clock, but never behind the highest input already seen.
        let live = now - LIVE_IDLE_GRACE;
        let watermark = if live >= max_input { live } else { max_input };
        (watermark, WatermarkMode::Live)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ts(unix: i64) -> Timestamp {
        Timestamp::from_unix_timestamp(unix).expect("valid timestamp")
    }

    #[test]
    fn no_input_yet_is_live_now() {
        let now = ts(1_000_000);
        let (wm, mode) = flush_watermark(None, None, now);
        assert_eq!(mode, WatermarkMode::Live);
        assert_eq!(wm, now);
    }

    #[test]
    fn backlog_of_old_events_flowing_is_catchup_at_input_time() {
        // Latest input is 2h behind wall clock and an event arrived just now:
        // classic replay/backfill. Watermark must be the input high-water, not
        // wall time, so a downstream gap check sees only real input-time gaps.
        let now = ts(1_000_000);
        let max_input = ts(1_000_000 - 7200); // 2h old
        let last_wall = ts(1_000_000 - 1); // received 1s ago (flowing)
        let (wm, mode) = flush_watermark(Some(max_input), Some(last_wall), now);
        assert_eq!(mode, WatermarkMode::Catchup);
        assert_eq!(wm, max_input);
    }

    #[test]
    fn caught_up_and_quiet_is_live_now() {
        // Latest input is old (2h) but nothing has arrived recently: a genuine
        // quiet live period, not a backlog. Watermark advances to wall clock so
        // the trailing window/session can finally close.
        let now = ts(1_000_000);
        let max_input = ts(1_000_000 - 7200);
        let last_wall = ts(1_000_000 - 600); // last event 10 min ago (not flowing)
        let (wm, mode) = flush_watermark(Some(max_input), Some(last_wall), now);
        assert_eq!(mode, WatermarkMode::Live);
        assert_eq!(wm, now);
    }

    #[test]
    fn caught_up_and_active_is_live() {
        // Current-timestamped events flowing (input ≈ now): live, watermark = now.
        let now = ts(1_000_000);
        let max_input = ts(1_000_000 - 5); // 5s behind — live jitter
        let last_wall = ts(1_000_000 - 1);
        let (wm, mode) = flush_watermark(Some(max_input), Some(last_wall), now);
        assert_eq!(mode, WatermarkMode::Live);
        assert_eq!(wm, now);
    }
}
