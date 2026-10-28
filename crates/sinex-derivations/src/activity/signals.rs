use super::*;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum Signal {
    Operator,
    Physical,
    Claim,
    Absent,
    Focus,
    Playback,
    Delegated,
    Coverage,
    NoInput,
}

#[derive(Clone)]
pub(super) struct Piece<'a> {
    pub start: i128,
    pub end: i128,
    pub scope: String,
    pub signal: Signal,
    pub event: &'a Event,
    pub reason: Option<&'static str>,
}

fn physical(event: &Event) -> bool {
    event
        .payload
        .pointer("/extra/layer")
        .and_then(Value::as_str)
        == Some("evdev")
        && event
            .payload
            .pointer("/extra/virtual")
            .and_then(Value::as_bool)
            == Some(false)
}

pub(super) fn pieces(inputs: &[Event]) -> Vec<Piece<'_>> {
    let mut pieces = Vec::new();
    for event in inputs {
        let Some(start) = event.ts_orig.at else {
            continue;
        };
        let Some((start, end)) = geometry(event.event_type, &event.payload, start) else {
            continue;
        };
        let payload = &event.payload;
        let (signal, reason) = match event.event_type {
            "key.pressed" | "pointer.button" | "pointer.moved" | "pointer.scrolled"
                if physical(event) =>
            {
                (Signal::Physical, None)
            }
            "presence.span" | "presence.interval" => (Signal::Claim, None),
            "focus.fused" => (Signal::Focus, None),
            "play.fused" => (Signal::Playback, None),
            "rest.fused" => (Signal::Absent, Some("sleep")),
            "state.interval" => match (payload["state_kind"].as_str(), payload["label"].as_str()) {
                (Some("desktop.lock"), Some("locked")) => (Signal::Absent, Some("lock")),
                (Some("desktop.display"), Some("off")) => (Signal::Absent, Some("display_off")),
                (Some("machine.suspend"), Some("suspended")) => (Signal::Absent, Some("suspend")),
                _ => continue,
            },
            "journal.entry"
                if payload["identifier"] == "systemd-sleep"
                    && payload["message"].as_str().is_some_and(|message| {
                        message.starts_with("Performing sleep operation")
                    }) =>
            {
                (Signal::Absent, Some("suspend"))
            }
            "operator_log.entry" if event.source == "operator.raw-log" => (Signal::Operator, None),
            "input.coverage" => (Signal::Coverage, None),
            "work.act" if payload["agency"] == "operator" => {
                (Signal::Operator, Some("recorded_act"))
            }
            "work.act" if payload["agency"] == "delegated" => (Signal::Delegated, None),
            _ => continue,
        };
        let event_scope = scope(payload, event.event_type);
        pieces.push(Piece {
            start,
            end,
            scope: event_scope,
            signal,
            event,
            reason,
        });
    }
    pieces.sort_by_cached_key(|piece| (piece.scope.clone(), piece.start, stable(piece.event)));
    // Only observed physical events bound a bout. Virtual input and focus
    // never bridge its gaps or extend it beyond the last observed input.
    let physical: Vec<_> = pieces
        .iter()
        .filter(|piece| piece.signal == Signal::Physical)
        .cloned()
        .collect();
    for pair in physical.windows(2) {
        let (first, next) = (&pair[0], &pair[1]);
        let same_window = first
            .event
            .payload
            .get("window")
            .and_then(Value::as_str)
            .zip(next.event.payload.get("window").and_then(Value::as_str))
            .is_some_and(|(first, next)| !first.is_empty() && first == next);
        let interrupted = pieces.iter().any(|piece| {
            piece.scope == first.scope
                && piece.signal == Signal::Focus
                && piece.start > first.start
                && piece.start < next.start
        });
        let quiet = next.start - first.start > BOUT_NS
            && next.start - first.start <= GAP_NS
            && same_window
            && !interrupted;
        if first.scope == next.scope && (next.start - first.start <= BOUT_NS || quiet) {
            pieces.push(Piece {
                end: next.start,
                signal: Signal::Operator,
                reason: quiet.then_some("reading_gap"),
                ..first.clone()
            });
            pieces.push(Piece {
                start: first.start,
                end: next.start,
                signal: Signal::Operator,
                reason: quiet.then_some("reading_gap"),
                ..next.clone()
            });
        }
    }
    // Silence is absence only inside a recorded physical capture interval.
    // A missing capture interval leaves it unknown.
    let coverage: Vec<_> = pieces
        .iter()
        .filter(|piece| piece.signal == Signal::Coverage)
        .cloned()
        .collect();
    for interval in coverage {
        let mut last = interval.start;
        for input in physical.iter().filter(|piece| {
            piece.scope == interval.scope
                && piece.start >= interval.start
                && piece.start <= interval.end
        }) {
            let end = input.start;
            let start = last + 180 * SECOND;
            if start < end {
                pieces.push(Piece {
                    start,
                    end,
                    signal: Signal::NoInput,
                    reason: Some("no_input"),
                    ..interval.clone()
                });
            }
            last = end;
        }
        if last + 180 * SECOND < interval.end {
            pieces.push(Piece {
                start: last + 180 * SECOND,
                signal: Signal::NoInput,
                reason: Some("no_input"),
                ..interval
            });
        }
    }
    pieces
}

pub(super) fn barrier(piece: &Piece<'_>, from: i128, until: i128) -> bool {
    piece.signal == Signal::Absent && piece.start <= until && piece.end >= from
}

/// Sleep describes the person across devices. Suspend describes every seat
/// on its captured host; a lock with no captured seat does not claim another.
pub(super) fn absence_scope(piece: &Piece<'_>, other: &str) -> bool {
    if piece.scope == other || piece.reason == Some("sleep") {
        return true;
    }
    if piece.reason != Some("suspend") {
        return false;
    }
    fn host(scope: &str) -> Option<String> {
        let value: Value = serde_json::from_str(scope).ok()?;
        (value[0] == "host")
            .then(|| value[1].as_str().map(str::to_owned))
            .flatten()
    }
    host(&piece.scope)
        .zip(host(other))
        .is_some_and(|(one, other)| one == other)
}
