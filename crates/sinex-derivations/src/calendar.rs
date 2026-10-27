//! Calendar instances: each `calendar.event` series expanded into the dated
//! instances it plans, so a day's plans can be read beside what happened.
//!
//! The key is one series, `(calendar, uid)`, and reads every reading of it:
//! the event itself and its changed instances (those with a recurrence ID).
//! A changed series recomputes as a whole. The series' rule is expanded in
//! its own zone (a floating or whole-day series in the zone it was read in);
//! `EXDATE` starts are left out and `RDATE` starts added. A changed instance
//! replaces the instance whose start it names; one naming no start the rule
//! gives is kept as its own instance, and a cancelled one plans nothing. A
//! cancelled or removed series plans nothing.
//!
//! A rule with neither `COUNT` nor `UNTIL` needs an explicit expansion range.
//! Until one is supplied, its recorded occurrence is kept with the reason
//! the rule was not expanded.
use crate::derived_event::derived;
use crate::{Computed, Env, InputQuery, KeyState, KeyView, Keyed};
use jiff::civil::DateTime;
use jiff::tz::TimeZone;
use serde_json::{Value, json};
use sinex_model::event_types::account::{CalendarEvent, CalendarInstance};
use sinex_model::{Event, EventPayload, RecordState, Timestamp};

pub mod rule;

pub struct Instances;

fn series_of(key: &Value) -> Option<(&str, &str)> {
    Some((key.get("calendar")?.as_str()?, key.get("uid")?.as_str()?))
}

fn timestamp(at: jiff::Timestamp) -> Option<Timestamp> {
    Timestamp::from_unix_ns(at.as_nanosecond()).ok()
}

fn instant(at: Timestamp) -> Option<jiff::Timestamp> {
    jiff::Timestamp::from_nanosecond(at.unix_ns()).ok()
}

/// The zone a series' local times are in: the zone its start was written
/// in, or for a floating or whole-day start the zone it was read in.
fn zone_of(event: &CalendarEvent) -> TimeZone {
    let named = match event.zone.as_str() {
        "floating" | "date" | "unknown" => event.local_zone.as_deref(),
        zone => Some(zone),
    };
    named
        .and_then(|name| TimeZone::get(name).ok())
        .unwrap_or(TimeZone::UTC)
}

fn instance(
    series: &CalendarEvent,
    series_start: Timestamp,
    start: Timestamp,
    end: Option<Timestamp>,
    changed: Option<&CalendarEvent>,
    rule_not_expanded: Option<String>,
) -> CalendarInstance {
    let own = changed.unwrap_or(series);
    CalendarInstance {
        calendar: series.calendar.clone(),
        uid: series.uid.clone(),
        series_start,
        start,
        end,
        all_day: own.all_day,
        summary: own.summary.clone().or_else(|| series.summary.clone()),
        location: own.location.clone().or_else(|| series.location.clone()),
        status: own.status.clone().or_else(|| series.status.clone()),
        changed: changed.is_some(),
        rule_not_expanded,
    }
}

/// The local starts of `series`, and why its rule was not expanded when it
/// was not.
fn starts(series: &CalendarEvent, first: Timestamp) -> (Vec<Timestamp>, Option<String>) {
    let zone = zone_of(series);
    let Some(first_instant) = instant(first) else {
        return (vec![first], None);
    };
    let local = |at: jiff::Timestamp| at.to_zoned(zone.clone()).datetime();
    let global = |at: DateTime| {
        zone.to_ambiguous_zoned(at)
            .compatible()
            .ok()
            .and_then(|z| timestamp(z.timestamp()))
    };
    let (mut out, not_expanded) = match series.rrule.as_deref().map(rule::Rule::parse) {
        None => (vec![first], None),
        Some(Err(reason)) => (vec![first], Some(reason)),
        Some(Ok(rule)) if rule.until.is_none() && rule.count.is_none() => (
            vec![first],
            Some("an expansion range is required for a rule without COUNT or UNTIL".into()),
        ),
        Some(Ok(rule)) => {
            let until = rule.until.map(|until| match until {
                rule::Until::Date(date) => date
                    .tomorrow()
                    .map_or(DateTime::MAX, |d| {
                        d.to_datetime(jiff::civil::Time::midnight())
                    })
                    .checked_sub(jiff::Span::new().nanoseconds(1))
                    .unwrap_or(DateTime::MAX),
                rule::Until::Local(at) => at,
                rule::Until::Utc(at) => TimeZone::UTC
                    .to_ambiguous_zoned(at)
                    .compatible()
                    .map_or(DateTime::MAX, |z| local(z.timestamp())),
            });
            let last = until.unwrap_or(DateTime::MAX);
            let starts = rule.starts(local(first_instant), |at| at <= last);
            (starts.into_iter().filter_map(global).collect(), None)
        }
    };
    out.extend(series.added_starts.iter().copied());
    out.retain(|s| !series.excluded_starts.contains(s));
    out.sort_unstable();
    out.dedup();
    (out, not_expanded)
}

/// An instance's end: the series' length added to its start, in days for
/// a whole-day series so a day stays a day across a zone change.
fn end_of(series: &CalendarEvent, first: Timestamp, start: Timestamp) -> Option<Timestamp> {
    let end = series.end?;
    if series.all_day {
        let zone = zone_of(series);
        let days = (instant(first)?.to_zoned(zone.clone()).date())
            .until(instant(end)?.to_zoned(zone.clone()).date())
            .ok()?;
        let at = instant(start)?
            .to_zoned(zone)
            .checked_add(days)
            .ok()?
            .timestamp();
        return timestamp(at);
    }
    Timestamp::from_unix_ns(start.unix_ns() + (end.unix_ns() - first.unix_ns())).ok()
}

fn live(event: &Event) -> Option<CalendarEvent> {
    (event.record_state != RecordState::Removed)
        .then(|| event.payload_as::<CalendarEvent>())
        .flatten()
        .filter(|e| e.cancelled_at.is_none())
}

impl Keyed for Instances {
    const NAME: &'static str = "calendar.instances";
    const VERSION: u32 = 2;
    const INPUTS: &'static [&'static str] = &[CalendarEvent::EVENT_TYPE];
    const OUTPUTS: &'static [&'static str] = &[CalendarInstance::EVENT_TYPE];

    fn keys_for(&self, input: &KeyView<'_>, _: &Env) -> Vec<Value> {
        let p = input.payload;
        match (p.get("calendar"), p.get("uid")) {
            (Some(calendar), Some(uid)) => vec![json!({"calendar": calendar, "uid": uid})],
            _ => vec![],
        }
    }

    fn inputs(&self, key: &Value, _: &Env) -> Option<InputQuery> {
        let (calendar, uid) = series_of(key)?;
        // Every reading of the series, whenever it starts: years 1 to 9999.
        let earliest = Timestamp::from_unix_secs(-62_135_596_800).ok()?;
        let latest = Timestamp::from_unix_secs(253_402_300_799).ok()?;
        Some(
            InputQuery::window(&[CalendarEvent::EVENT_TYPE], earliest, latest)
                .only("calendar", json!(calendar))
                .only("uid", json!(uid)),
        )
    }

    fn compute(&self, _: &Value, inputs: &[Event], _: &Env) -> Computed {
        let series = inputs
            .iter()
            .find(|e| e.payload.get("recurrence_id").is_none_or(Value::is_null));
        let changed: Vec<(&Event, Option<CalendarEvent>)> = inputs
            .iter()
            .filter(|e| e.payload.get("recurrence_id").is_some_and(|r| !r.is_null()))
            .map(|e| (e, live(e)))
            .collect();
        let mut outputs = Vec::new();
        let mut used = vec![false; changed.len()];
        match series {
            // A removed or cancelled series plans nothing.
            Some(event) if live(event).is_none() => return Computed::default(),
            Some(event) => {
                let series_event = live(event).expect("checked");
                let Some(first) = series_event.start else {
                    return Computed::default();
                };
                let (starts, not_expanded) = starts(&series_event, first);
                for start in starts {
                    let found = changed.iter().position(|(e, _)| {
                        e.payload_as::<CalendarEvent>()
                            .and_then(|c| c.recurrence_start)
                            == Some(start)
                    });
                    match found {
                        Some(i) => {
                            used[i] = true;
                            let (changed_event, payload) = &changed[i];
                            let Some(own) = payload else { continue };
                            let Some(own_start) = own.start else { continue };
                            let payload = instance(
                                &series_event,
                                start,
                                own_start,
                                own.end,
                                Some(own),
                                not_expanded.clone(),
                            );
                            outputs.push(derived(&payload, Self::NAME, [event, *changed_event]));
                        }
                        None => {
                            let payload = instance(
                                &series_event,
                                start,
                                start,
                                end_of(&series_event, first, start),
                                None,
                                not_expanded.clone(),
                            );
                            outputs.push(derived(&payload, Self::NAME, [event]));
                        }
                    }
                }
            }
            None => {}
        }
        // Changed instances the series does not give (or with no series read).
        for (i, (event, payload)) in changed.iter().enumerate() {
            if used[i] {
                continue;
            }
            let Some(own) = payload else { continue };
            let (Some(series_start), Some(start)) = (own.recurrence_start.or(own.start), own.start)
            else {
                continue;
            };
            let payload = instance(own, series_start, start, own.end, Some(own), None);
            outputs.push(derived(&payload, Self::NAME, [*event]));
        }
        Computed {
            outputs,
            waiting: false,
        }
    }

    fn key_state(&self, _: &Value, _: &Env) -> KeyState {
        KeyState::Closed
    }
}

#[cfg(test)]
mod tests;
