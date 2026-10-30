//! Sequence continuity is scoped to the provider's host, lane and generation.
use super::Envelope;
use serde::{Deserialize, Deserializer, Serialize};
use sinex_model::Timestamp;
use sinex_model::event_types::capture::{Gap, SequenceGap};

#[derive(Clone, Default, Serialize)]
#[serde(transparent)]
pub struct Carry(Vec<Head>);

// Earlier parser versions carried null. It means no continuity evidence.
impl<'de> Deserialize<'de> for Carry {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        Ok(match Option::<Vec<Head>>::deserialize(deserializer)? {
            Some(heads) => Self(heads),
            None => Self::default(),
        })
    }
}

#[derive(Clone, Serialize, Deserialize)]
struct Head {
    host: String,
    lane: String,
    boot: Option<String>,
    session: Option<String>,
    sequence: u64,
    at: Timestamp,
    uncertain: bool,
    #[serde(default)]
    after_failure: bool,
}

pub(super) fn failed(carry: &Carry) -> Carry {
    let mut next = carry.clone();
    for head in &mut next.0 {
        head.after_failure = true;
    }
    next
}

pub(super) fn observe(carry: &Carry, envelope: &Envelope) -> (Option<Gap>, Carry) {
    let mut next = carry.clone();
    let previous = next
        .0
        .iter_mut()
        .find(|head| head.host == envelope.host && head.lane == envelope.lane);
    let current = Head {
        host: envelope.host.clone(),
        lane: envelope.lane.clone(),
        boot: envelope.boot.clone(),
        session: envelope.session.clone(),
        sequence: envelope.seq,
        at: envelope.observed_at,
        uncertain: false,
        after_failure: false,
    };
    let Some(previous) = previous else {
        next.0.push(current);
        return (None, next);
    };
    if previous.boot != current.boot || previous.session != current.session {
        *previous = current;
        return (None, next);
    }
    if previous.after_failure {
        *previous = Head {
            uncertain: previous.uncertain,
            ..current
        };
        return (None, next);
    }
    if current.sequence == previous.sequence {
        return (None, next);
    }
    if current.sequence < previous.sequence {
        // Without a new generation this could be a reset or reordered input.
        // Neither establishes how many records are absent.
        previous.uncertain = true;
        return (None, next);
    }
    let gap = match previous.sequence.checked_add(1) {
        Some(first) if !previous.uncertain && current.sequence > first => {
            let ordered_time = current.at >= previous.at;
            Some(Gap {
                stream: serde_json::to_string(&(
                    &current.host,
                    &current.lane,
                    &current.boot,
                    &current.session,
                ))
                .expect("stream identity serializes"),
                start: ordered_time.then_some(previous.at),
                end: ordered_time.then_some(current.at),
                reason: "sequence".into(),
                sequence: Some(SequenceGap {
                    first_missing: first,
                    last_missing: current.sequence - 1,
                }),
            })
        }
        _ => None,
    };
    let uncertain = previous.uncertain;
    *previous = Head {
        uncertain,
        ..current
    };
    (gap, next)
}

#[cfg(test)]
mod tests;
