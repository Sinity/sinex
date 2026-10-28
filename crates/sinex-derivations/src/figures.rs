//! Metric figures: each metric's number per civil hour and per logical day,
//! as `metric.figure` events. These are the daily and hourly figures every
//! reader uses: the panel, the brief and the statistical methods read them
//! through the query language, and `trace` leads from a day's number to the
//! hours and the events behind it.
//!
//! A metric is declared once in [`METRICS`]: the events it counts, how it
//! counts them, the fields it breaks down by, and the sources whose coverage
//! says whether a period was observed. Fused kinds are counted, never one
//! source's view of them, so a command two shells saw counts once.
//!
//! The hourly derivation is keyed by metric and logical day and emits that
//! day's civil hours (23 to 25 of them) from the day's input events. The
//! daily derivation is keyed the same way and reads the day's hourly figures,
//! never the inputs: a day's hours partition it, so counts, seconds and a
//! ratio's parts add up exactly.
//!
//! An interval counts the seconds it overlaps each hour. A focus with no end
//! yet counts up to now, but never past the end of the logical day it started
//! in: missing evidence is not continued activity. A play with no end is of
//! unknown length and counts no seconds.
use crate::derived_event::derived;
use crate::{Computed, Env, InputQuery, KeyState, KeyView, Keyed, Kind, Neighbour};
use serde_json::{Value, json};
use sinex_model::event_types::MetricFigure;
use sinex_model::{Event, EventPayload, Timestamp};
use std::collections::BTreeMap;

/// How a metric counts the events of a period.
#[derive(Clone, Copy)]
pub enum Law {
    /// How many events started in the period.
    Count,
    /// The seconds of the period covered by the events' intervals, each
    /// running from its time to the payload field `end`; overlaps count once.
    /// With `open`, an interval with no end yet is still running; without it,
    /// one with no end is of unknown length and counts nothing.
    Seconds { end: &'static str, open: bool },
    /// Of the events whose payload passes `denominator`, the share that also
    /// passes `numerator`.
    Ratio {
        numerator: fn(&Value) -> bool,
        denominator: fn(&Value) -> bool,
    },
}

/// One declared metric.
#[derive(Clone, Copy)]
pub struct Metric {
    pub name: &'static str,
    pub description: &'static str,
    /// The events it counts.
    pub inputs: fn() -> Vec<Kind>,
    pub law: Law,
    /// Payload fields it is also broken down by.
    pub dimensions: &'static [&'static str],
    /// The sources that observe its inputs; a period is observed while any
    /// of them is.
    pub sources: &'static [&'static str],
    /// Whether the brief compares each day with the days before it, and the
    /// correlation family pairs it with the others.
    pub tracked: bool,
}

impl Metric {
    /// The kinds it counts, built once for every metric: admission asks for
    /// them for each event it marks keys for.
    pub fn kinds(&'static self) -> &'static [Kind] {
        static KINDS: std::sync::OnceLock<Vec<Vec<Kind>>> = std::sync::OnceLock::new();
        let all = KINDS.get_or_init(|| METRICS.iter().map(|m| (m.inputs)()).collect());
        METRICS
            .iter()
            .position(|m| std::ptr::eq(m, self))
            .map_or(&[], |i| &all[i])
    }

    fn admits(&'static self, event_type: &str, payload: &Value) -> bool {
        self.kinds().iter().any(|k| k.admits(event_type, payload))
    }
}

const SHELLS: &[&str] = &[
    "terminal.atuin-history",
    "terminal.zsh-history",
    "terminal.bash-history",
    "terminal.fish-history",
];
const FOCUS: &[&str] = &["desktop.hyprland", "desktop.activitywatch"];
const MUSIC: &[&str] = &["media.spotify-history", "media.mpris", "desktop.media"];
const WEB: &[&str] = &[
    "browser.chromium-visits",
    "browser.takeout-chrome-history",
    "browser.navigation",
];

fn exit_known(p: &Value) -> bool {
    p.get("exit_code").is_some_and(Value::is_i64)
}

/// Every metric.
pub static METRICS: &[Metric] = &[
    Metric {
        name: "shell.commands",
        description: "Commands run",
        inputs: || vec![Kind::all("command.fused")],
        law: Law::Count,
        dimensions: &["host", "cwd"],
        sources: SHELLS,
        tracked: true,
    },
    Metric {
        name: "shell.failed_ratio",
        description: "Share of commands with a known exit code that failed",
        inputs: || vec![Kind::all("command.fused")],
        law: Law::Ratio {
            numerator: |p| exit_known(p) && p["exit_code"] != 0,
            denominator: exit_known,
        },
        dimensions: &[],
        sources: SHELLS,
        tracked: false,
    },
    Metric {
        name: "shell.sessions",
        description: "Shell sessions",
        inputs: || vec![Kind::all("shell.session")],
        law: Law::Count,
        dimensions: &["host"],
        sources: SHELLS,
        tracked: false,
    },
    Metric {
        name: "shell.session_seconds",
        description: "Time in shell sessions",
        inputs: || vec![Kind::all("shell.session")],
        law: Law::Seconds {
            end: "ended_at",
            open: false,
        },
        dimensions: &[],
        sources: SHELLS,
        tracked: false,
    },
    Metric {
        name: "activity.active_seconds",
        description: "Time at the machine",
        inputs: || vec![Kind::only("presence.span", &[("status", "not-afk")])],
        law: Law::Seconds {
            end: "ended_at",
            open: false,
        },
        dimensions: &[],
        sources: &["desktop.activitywatch"],
        tracked: true,
    },
    Metric {
        name: "focus.seconds",
        description: "Time some window had focus, time away from the machine included",
        inputs: || vec![Kind::all("focus.fused")],
        law: Law::Seconds {
            end: "ended_at",
            open: true,
        },
        dimensions: &["app"],
        sources: FOCUS,
        tracked: false,
    },
    Metric {
        name: "focus.switches",
        description: "Window focus changes",
        inputs: || vec![Kind::all("focus.fused")],
        law: Law::Count,
        dimensions: &[],
        sources: FOCUS,
        tracked: true,
    },
    Metric {
        name: "music.listen_seconds",
        description: "Time listening",
        inputs: || vec![Kind::all("play.fused")],
        law: Law::Seconds {
            end: "ended_at",
            open: false,
        },
        dimensions: &[],
        sources: MUSIC,
        tracked: false,
    },
    Metric {
        name: "music.plays",
        description: "Tracks played",
        inputs: || vec![Kind::all("play.fused")],
        law: Law::Count,
        dimensions: &["artist"],
        sources: MUSIC,
        tracked: false,
    },
    Metric {
        name: "git.commits",
        description: "Commits made",
        inputs: || vec![Kind::all("git.commit")],
        law: Law::Count,
        dimensions: &["repository"],
        sources: &["code.git"],
        tracked: false,
    },
    Metric {
        name: "chat.lines",
        description: "Chat lines said (messages and actions)",
        inputs: || {
            vec![
                Kind::only("chat.message", &[("kind", "message")]),
                Kind::only("chat.message", &[("kind", "action")]),
            ]
        },
        law: Law::Count,
        dimensions: &["buffer"],
        sources: &["communication.weechat-log"],
        tracked: false,
    },
    Metric {
        name: "mail.messages",
        description: "Mail messages",
        inputs: || vec![Kind::all("mail.fused")],
        law: Law::Count,
        dimensions: &[],
        sources: &[
            "communication.imap",
            "communication.maildir",
            "communication.mbox",
            "communication.outlook",
            "communication.outlook-csv",
        ],
        tracked: false,
    },
    Metric {
        name: "machine.journal_errors",
        description: "Journal entries at error priority or worse",
        // syslog priorities 0-3: emergency, alert, critical, error.
        inputs: || {
            (0..=3)
                .map(|p| Kind {
                    event_type: "journal.entry".into(),
                    equal: vec![("priority".into(), json!(p))],
                })
                .collect()
        },
        law: Law::Count,
        dimensions: &["unit"],
        sources: &["machine.journald"],
        tracked: false,
    },
    Metric {
        name: "input.key_presses",
        description: "Keys pressed, a held key's repeats included",
        inputs: || vec![Kind::all("key.pressed")],
        law: Law::Count,
        dimensions: &[],
        sources: &["desktop.input-log"],
        tracked: true,
    },
    Metric {
        name: "input.clicks",
        description: "Pointer buttons pressed",
        inputs: || {
            vec![Kind {
                event_type: "pointer.button".into(),
                equal: vec![("pressed".into(), json!(true))],
            }]
        },
        law: Law::Count,
        dimensions: &["button"],
        sources: &["desktop.input-log"],
        tracked: false,
    },
    Metric {
        name: "web.visits",
        description: "Pages visited",
        inputs: || vec![Kind::all("visit.fused")],
        law: Law::Count,
        dimensions: &[],
        sources: WEB,
        tracked: true,
    },
];

pub fn metric(name: &str) -> Option<&'static Metric> {
    METRICS.iter().find(|m| m.name == name)
}

fn key_parts(key: &Value) -> Option<(&'static Metric, jiff::civil::Date)> {
    let m = metric(key.get("metric")?.as_str()?)?;
    let day = key.get("day")?.as_str()?.parse().ok()?;
    Some((m, day))
}

fn day_state(key: &Value, env: &Env) -> KeyState {
    match key_parts(key).and_then(|(_, d)| env.time_settings.day_end(d).ok()) {
        Some(end) if env.now >= end => KeyState::Closed,
        Some(_) => KeyState::Open,
        None => KeyState::Closed,
    }
}

/// Where an interval with no end stops counting: now, or the end of the
/// logical day it started in, whichever is earlier.
fn open_end(start: Timestamp, env: &Env) -> Timestamp {
    let day_end = env
        .time_settings
        .day(start)
        .and_then(|d| env.time_settings.day_end(d))
        .unwrap_or(start);
    env.now.min(day_end).max(start)
}

/// The span an input covers in nanoseconds: an instant for counted events,
/// its interval for seconds.
fn span(law: Law, start: Timestamp, payload: &Value, env: &Env) -> (i128, i128) {
    let end = match law {
        Law::Seconds { end, open } => payload
            .get(end)
            .and_then(|v| serde_json::from_value::<Option<Timestamp>>(v.clone()).ok())
            .flatten()
            .unwrap_or_else(|| if open { open_end(start, env) } else { start })
            .max(start),
        Law::Count | Law::Ratio { .. } => start,
    };
    (start.unix_ns(), end.unix_ns())
}

/// The breakdown value of a payload field; `unknown` when it has none.
fn dimension_value(payload: &Value, field: &str) -> String {
    match payload.get(field) {
        Some(Value::String(s)) => s.clone(),
        Some(v) if !v.is_null() => v.to_string(),
        _ => "unknown".to_owned(),
    }
}

/// What a figure adds up while it is being computed.
#[derive(Default)]
struct Sum<'a> {
    rows: u64,
    /// Count and ratio parts, or seconds.
    value: f64,
    numerator: f64,
    denominator: f64,
    /// Seconds: the clipped spans, joined at the end.
    spans: Vec<(i128, i128)>,
    parents: Vec<&'a Event>,
}

impl Sum<'_> {
    fn figure(
        &self,
        m: &Metric,
        grain: &str,
        period: (Timestamp, Timestamp),
        day: &str,
        dimension: (&str, &str),
    ) -> MetricFigure {
        let (value, numerator, denominator) = match m.law {
            Law::Count => (Some(self.value), None, None),
            Law::Seconds { .. } => (Some(self.value + union_secs(&self.spans)), None, None),
            Law::Ratio { .. } => (
                (self.denominator > 0.0).then(|| self.numerator / self.denominator),
                Some(self.numerator),
                Some(self.denominator),
            ),
        };
        MetricFigure {
            metric: m.name.to_owned(),
            grain: grain.to_owned(),
            period_start: period.0,
            period_end: period.1,
            day: day.to_owned(),
            dimension: dimension.0.to_owned(),
            dimension_value: dimension.1.to_owned(),
            value,
            numerator,
            denominator,
            rows: self.rows,
        }
    }
}

/// The length of a union of spans, in seconds.
fn union_secs(spans: &[(i128, i128)]) -> f64 {
    let mut sorted = spans.to_vec();
    sorted.sort_unstable();
    let mut total = 0i128;
    let mut current: Option<(i128, i128)> = None;
    for (s, e) in sorted {
        match &mut current {
            Some((_, ce)) if s <= *ce => *ce = (*ce).max(e),
            _ => {
                if let Some((cs, ce)) = current.replace((s, e)) {
                    total += ce - cs;
                }
            }
        }
    }
    if let Some((cs, ce)) = current {
        total += ce - cs;
    }
    total as f64 / 1e9
}

/// One figure per breakdown: the total under `("", "")`, then one per value
/// of each dimension.
type Figures<'a> = BTreeMap<(String, String), Sum<'a>>;

fn add<'a>(m: &Metric, sums: &mut Figures<'a>, event: &'a Event, part: (i128, i128)) {
    let mut breakdowns = vec![(String::new(), String::new())];
    breakdowns.extend(
        m.dimensions
            .iter()
            .map(|d| ((*d).to_owned(), dimension_value(&event.payload, d))),
    );
    for b in breakdowns {
        let sum = sums.entry(b).or_default();
        sum.rows += 1;
        sum.parents.push(event);
        match m.law {
            Law::Count => sum.value += 1.0,
            Law::Seconds { .. } => sum.spans.push(part),
            Law::Ratio {
                numerator,
                denominator,
            } => {
                if denominator(&event.payload) {
                    sum.denominator += 1.0;
                    if numerator(&event.payload) {
                        sum.numerator += 1.0;
                    }
                }
            }
        }
    }
}

pub struct HourlyMetrics;
pub struct DailyMetrics;

impl Keyed for HourlyMetrics {
    const NAME: &'static str = "metric.hourly";
    const VERSION: u32 = 1;
    // Every type some metric counts (a test checks the list); `keys_for`
    // picks the metrics.
    const INPUTS: &'static [&'static str] = &[
        "command.fused",
        "shell.session",
        "presence.span",
        "focus.fused",
        "play.fused",
        "git.commit",
        "chat.message",
        "mail.fused",
        "journal.entry",
        "visit.fused",
        "key.pressed",
        "pointer.button",
    ];
    const OUTPUTS: &'static [&'static str] = &[MetricFigure::EVENT_TYPE];

    fn keys_for(&self, input: &KeyView<'_>, env: &Env) -> Vec<Value> {
        let Some(start) = input.ts_orig else {
            return vec![];
        };
        let mut keys = Vec::new();
        for m in METRICS
            .iter()
            .filter(|m| m.admits(input.event_type, input.payload))
        {
            let (_, end) = span(m.law, start, input.payload, env);
            let Ok(mut day) = env.time_settings.day(start) else {
                continue;
            };
            loop {
                keys.push(json!({"metric": m.name, "day": day.to_string()}));
                let Ok(next_start) = env.time_settings.day_end(day) else {
                    break;
                };
                if next_start.unix_ns() >= end {
                    break;
                }
                let Ok(next) = day.tomorrow() else { break };
                day = next;
            }
        }
        keys
    }

    fn inputs(&self, key: &Value, env: &Env) -> Option<InputQuery> {
        let (m, day) = key_parts(key)?;
        let kinds = m.kinds().to_vec();
        let mut q = InputQuery::of(
            kinds.clone(),
            env.time_settings.day_start(day).ok()?,
            env.time_settings.day_end(day).ok()?,
        );
        if let Law::Seconds { .. } = m.law {
            // The interval that may still run when the day starts.
            q.before.push(Neighbour {
                kinds,
                per: None,
                count: 1,
            });
        }
        Some(q)
    }

    fn compute(&self, key: &Value, inputs: &[Event], env: &Env) -> Computed {
        let Some((m, day)) = key_parts(key) else {
            return Computed::default();
        };
        let (Ok(start), Ok(end)) = (
            env.time_settings.day_start(day),
            env.time_settings.day_end(day),
        ) else {
            return Computed::default();
        };
        let label = day.to_string();
        let mut outputs = Vec::new();
        let mut hour = start;
        while hour < end {
            let Ok(next) = env.time_settings.hour_end(hour) else {
                break;
            };
            let period = (hour.unix_ns(), next.unix_ns());
            let mut sums: Figures<'_> = BTreeMap::new();
            for e in inputs {
                let Some(at) = e.ts_orig.at else { continue };
                if !m.admits(&e.event_type, &e.payload) {
                    continue;
                }
                let (s, f) = span(m.law, at, &e.payload, env);
                let part = (s.max(period.0), f.min(period.1));
                let counted = match m.law {
                    Law::Seconds { .. } => part.1 > part.0,
                    Law::Count | Law::Ratio { .. } => (period.0..period.1).contains(&s),
                };
                if counted {
                    add(m, &mut sums, e, part);
                }
            }
            for ((dimension, value), sum) in &sums {
                let payload = sum.figure(m, "hour", (hour, next), &label, (dimension, value));
                outputs.push(derived(&payload, Self::NAME, sum.parents.iter().copied()));
            }
            hour = next;
        }
        Computed {
            error: None,
            outputs,
            waiting: false,
        }
    }

    fn key_state(&self, key: &Value, env: &Env) -> KeyState {
        day_state(key, env)
    }
}

impl Keyed for DailyMetrics {
    const NAME: &'static str = "metric.daily";
    const VERSION: u32 = 1;
    const INPUTS: &'static [&'static str] = &[MetricFigure::EVENT_TYPE];
    const OUTPUTS: &'static [&'static str] = &[MetricFigure::EVENT_TYPE];

    fn keys_for(&self, input: &KeyView<'_>, _: &Env) -> Vec<Value> {
        let p = input.payload;
        if p.get("grain").and_then(Value::as_str) != Some("hour") {
            return vec![];
        }
        match (p.get("metric"), p.get("day")) {
            (Some(metric), Some(day)) => vec![json!({"metric": metric, "day": day})],
            _ => vec![],
        }
    }

    fn inputs(&self, key: &Value, env: &Env) -> Option<InputQuery> {
        let (m, day) = key_parts(key)?;
        Some(
            InputQuery::window(
                &[MetricFigure::EVENT_TYPE],
                env.time_settings.day_start(day).ok()?,
                env.time_settings.day_end(day).ok()?,
            )
            .only("grain", json!("hour"))
            .only("metric", json!(m.name)),
        )
    }

    fn compute(&self, key: &Value, inputs: &[Event], env: &Env) -> Computed {
        let Some((m, day)) = key_parts(key) else {
            return Computed::default();
        };
        let (Ok(start), Ok(end)) = (
            env.time_settings.day_start(day),
            env.time_settings.day_end(day),
        ) else {
            return Computed::default();
        };
        let mut sums: Figures<'_> = BTreeMap::new();
        for hourly in inputs {
            let Some(f) = hourly.payload_as::<MetricFigure>() else {
                continue;
            };
            let sum = sums.entry((f.dimension, f.dimension_value)).or_default();
            sum.rows += f.rows;
            sum.parents.push(hourly);
            match m.law {
                Law::Count | Law::Seconds { .. } => sum.value += f.value.unwrap_or(0.0),
                Law::Ratio { .. } => {
                    sum.numerator += f.numerator.unwrap_or(0.0);
                    sum.denominator += f.denominator.unwrap_or(0.0);
                }
            }
        }
        let label = day.to_string();
        let outputs = sums
            .iter()
            .map(|((dimension, value), sum)| {
                let payload = sum.figure(m, "day", (start, end), &label, (dimension, value));
                derived(&payload, Self::NAME, sum.parents.iter().copied())
            })
            .collect();
        Computed {
            error: None,
            outputs,
            waiting: false,
        }
    }

    fn key_state(&self, key: &Value, env: &Env) -> KeyState {
        day_state(key, env)
    }
}

#[cfg(test)]
mod tests;
