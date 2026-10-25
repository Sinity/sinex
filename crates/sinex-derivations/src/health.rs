//! Hourly and daily health figures over Google Fit points. A point is a
//! `health.record` of provider `google-fit`, read as it was stored: its
//! series is the data source it came from, and each numeric entry of its
//! `fitValue` list is one value position.
//!
//! The hourly derivation is keyed by civil hour and reads that hour's points,
//! so a key holds at most one hour of points. It emits one figure per series
//! with points starting in the hour, naming those points as parents. The daily
//! derivation is keyed by logical day and reads that day's hourly figures,
//! never the points: the hours of a logical day run from its start to its end,
//! so each hourly figure belongs to exactly one day.
use crate::derived_event::derived;
use crate::{Computed, Env, InputQuery, KeyState, KeyView, Keyed};
use serde_json::{Value, json};
use sinex_model::event_types::health::HealthRecord;
use sinex_model::event_types::{FigureValue, HealthFigure};
use sinex_model::{Event, EventPayload, Timestamp};
use std::collections::BTreeMap;

/// The provider whose records are series points.
const PROVIDER: &str = "google-fit";

pub struct HourlyFigures;
pub struct DailyFigures;

/// One series being summed: its data type and each value position.
#[derive(Default)]
struct Series<'a> {
    category: String,
    points: u64,
    values: BTreeMap<u32, FigureValue>,
    parents: Vec<&'a Event>,
}

impl Series<'_> {
    fn add(&mut self, position: u32, count: u64, sum: f64, min: f64, max: f64) {
        let value = self.values.entry(position).or_insert(FigureValue {
            position,
            count: 0,
            sum: 0.0,
            min,
            max,
        });
        value.count += count;
        value.sum += sum;
        value.min = value.min.min(min);
        value.max = value.max.max(max);
    }
}

/// The data source a Fit point's native ID names: `<data source>:<start>:<end>`.
fn series_of(native_id: &str) -> &str {
    native_id.rsplitn(3, ':').nth(2).unwrap_or(native_id)
}

/// A Fit value entry's number: `intVal` or `fpVal`, directly or under `value`.
fn number(entry: &Value) -> Option<f64> {
    let entry = entry.get("value").unwrap_or(entry);
    entry
        .get("intVal")
        .or_else(|| entry.get("fpVal"))
        .and_then(Value::as_f64)
}

fn figure(
    grain: &str,
    period: (Timestamp, Timestamp),
    day: &str,
    series: String,
    summed: &Series<'_>,
) -> HealthFigure {
    HealthFigure {
        grain: grain.to_owned(),
        period_start: period.0,
        period_end: period.1,
        day: day.to_owned(),
        provider: PROVIDER.to_owned(),
        category: summed.category.clone(),
        series,
        points: summed.points,
        values: summed.values.values().cloned().collect(),
    }
}

fn hour_of(key: &Value) -> Option<Timestamp> {
    serde_json::from_value(key.get("hour")?.clone()).ok()
}

fn day_of(key: &Value) -> Option<jiff::civil::Date> {
    key.get("day")?.as_str()?.parse().ok()
}

fn closed_after(end: Option<Timestamp>, env: &Env) -> KeyState {
    match end {
        Some(end) if env.now < end => KeyState::Open,
        _ => KeyState::Closed,
    }
}

impl Keyed for HourlyFigures {
    const NAME: &'static str = "health.hourly";
    const VERSION: u32 = 1;
    const INPUTS: &'static [&'static str] = &[HealthRecord::EVENT_TYPE];
    const OUTPUTS: &'static [&'static str] = &[HealthFigure::EVENT_TYPE];

    fn keys_for(&self, input: &KeyView<'_>, env: &Env) -> Vec<Value> {
        if input.payload.get("provider").and_then(Value::as_str) != Some(PROVIDER) {
            return vec![];
        }
        input
            .ts_orig
            .and_then(|t| env.civil.hour_start(t).ok())
            .map(|hour| json!({"hour": hour}))
            .into_iter()
            .collect()
    }

    fn inputs(&self, key: &Value, env: &Env) -> Option<InputQuery> {
        let hour = hour_of(key)?;
        let mut q = InputQuery::window(
            &[HealthRecord::EVENT_TYPE],
            hour,
            env.civil.hour_end(hour).ok()?,
        );
        q.equal.push(("provider", json!(PROVIDER)));
        Some(q)
    }

    fn compute(&self, key: &Value, inputs: &[Event], env: &Env) -> Computed {
        let Some(hour) = hour_of(key) else {
            return Computed::default();
        };
        let (Ok(end), Ok(day)) = (env.civil.hour_end(hour), env.civil.day(hour)) else {
            return Computed::default();
        };
        let mut series: BTreeMap<String, Series<'_>> = BTreeMap::new();
        for point in inputs {
            let p = &point.payload;
            let Some(native_id) = p.get("native_id").and_then(Value::as_str) else {
                continue;
            };
            let summed = series.entry(series_of(native_id).to_owned()).or_default();
            if let Some(category) = p.get("category").and_then(Value::as_str) {
                category.clone_into(&mut summed.category);
            }
            summed.points += 1;
            summed.parents.push(point);
            let entries = p
                .pointer("/values/fitValue")
                .and_then(Value::as_array)
                .map_or(&[][..], Vec::as_slice);
            for (position, entry) in entries.iter().enumerate() {
                if let (Some(n), Ok(position)) = (number(entry), u32::try_from(position)) {
                    summed.add(position, 1, n, n, n);
                }
            }
        }
        let day = day.to_string();
        let outputs = series
            .into_iter()
            .map(|(name, summed)| {
                let payload = figure("hour", (hour, end), &day, name, &summed);
                derived(&payload, Self::NAME, summed.parents)
            })
            .collect();
        Computed {
            outputs,
            waiting: false,
        }
    }

    fn key_state(&self, key: &Value, env: &Env) -> KeyState {
        closed_after(hour_of(key).and_then(|h| env.civil.hour_end(h).ok()), env)
    }
}

impl Keyed for DailyFigures {
    const NAME: &'static str = "health.daily";
    const VERSION: u32 = 1;
    const INPUTS: &'static [&'static str] = &[HealthFigure::EVENT_TYPE];
    const OUTPUTS: &'static [&'static str] = &[HealthFigure::EVENT_TYPE];

    fn keys_for(&self, input: &KeyView<'_>, _: &Env) -> Vec<Value> {
        let p = input.payload;
        if p.get("grain").and_then(Value::as_str) != Some("hour") {
            return vec![];
        }
        p.get("day")
            .and_then(Value::as_str)
            .map(|day| json!({"day": day}))
            .into_iter()
            .collect()
    }

    fn inputs(&self, key: &Value, env: &Env) -> Option<InputQuery> {
        let day = day_of(key)?;
        let mut q = InputQuery::window(
            &[HealthFigure::EVENT_TYPE],
            env.civil.day_start(day).ok()?,
            env.civil.day_end(day).ok()?,
        );
        q.equal.push(("grain", json!("hour")));
        q.equal.push(("provider", json!(PROVIDER)));
        Some(q)
    }

    fn compute(&self, key: &Value, inputs: &[Event], env: &Env) -> Computed {
        let Some(day) = day_of(key) else {
            return Computed::default();
        };
        let (Ok(start), Ok(end)) = (env.civil.day_start(day), env.civil.day_end(day)) else {
            return Computed::default();
        };
        let mut series: BTreeMap<String, Series<'_>> = BTreeMap::new();
        for hourly in inputs {
            let Some(figure) = hourly.payload_as::<HealthFigure>() else {
                continue;
            };
            let summed = series.entry(figure.series).or_default();
            summed.category = figure.category;
            summed.points += figure.points;
            summed.parents.push(hourly);
            for v in figure.values {
                summed.add(v.position, v.count, v.sum, v.min, v.max);
            }
        }
        let day = day.to_string();
        let outputs = series
            .into_iter()
            .map(|(name, summed)| {
                let payload = figure("day", (start, end), &day, name, &summed);
                derived(&payload, Self::NAME, summed.parents)
            })
            .collect();
        Computed {
            outputs,
            waiting: false,
        }
    }

    fn key_state(&self, key: &Value, env: &Env) -> KeyState {
        closed_after(day_of(key).and_then(|d| env.civil.day_end(d).ok()), env)
    }
}

#[cfg(test)]
mod tests;
