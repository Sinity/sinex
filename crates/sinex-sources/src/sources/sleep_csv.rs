//! Optional joined sleep CSV. Supplied civil times and unresolved DST cases
//! remain distinct from native health records and known instants.
use crate::binding::{self, InputFormat, Settings};
use crate::drivers::export_archive::{ExportArchive, Framing, PART_BYTES};
use crate::registry::{DeclaredSource, Driver, SourceSpec};
use crate::{Parser, RecordContext, RecordError};
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use sinex_model::{Class, Timestamp, event_types::sleep_import::SleepSessionImported};
use std::path::PathBuf;

pub const SPEC: SourceSpec = SourceSpec {
    name: "health.sleep-csv",
    driver: Driver::ExportArchive,
    excisable: false,
    purpose: "imported joined sleep-session rows",
    class: Class::Guarded,
    parser: "sleep-csv",
    parser_version: 1,
    event_types: &["sleep.session_imported"],
    framer: crate::Framer::export(frame, Framing::Csv),
};

pub struct SleepCsvParser {
    pub dataset: String,
    pub zone: jiff::tz::TimeZone,
    pub zone_name: String,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct Carry {
    member: Option<(String, u64)>,
    header: Vec<String>,
}

const KNOWN: &[&str] = &[
    "sh_datauuid",
    "start_local",
    "end_local",
    "sh_duration_minutes",
    "sa_vs_sh_duration_minutes",
    "hr_avg",
    "hr_min",
    "hr_max",
    "trimmed_event_count",
    "events_hr",
    "events_light",
    "events_deep",
    "events_rem",
    "sa_comment",
];

impl Parser for SleepCsvParser {
    type Record<'a> = &'a [u8];
    type Carry = Carry;
    type Payload = SleepSessionImported;

    fn parse(
        &self,
        record: &&[u8],
        carry: &Carry,
        context: &RecordContext<'_>,
    ) -> Result<(Vec<SleepSessionImported>, Carry), RecordError> {
        let mut cells = super::fields::csv_cells(record)?;
        if let Some(first) = cells.first_mut() {
            *first = first.trim_start_matches('\u{feff}').into();
        }
        let member = context
            .member
            .map(|m| {
                let index = m
                    .details
                    .get("member_index")
                    .and_then(serde_json::Value::as_u64)
                    .ok_or_else(|| RecordError::new("export member index is missing"))?;
                Ok::<_, RecordError>((m.path.clone(), index))
            })
            .transpose()?;
        let header = if carry.member == member {
            carry.header.as_slice()
        } else {
            &[]
        };
        if header.is_empty() {
            for required in ["sh_datauuid", "start_local", "end_local"] {
                if !cells.iter().any(|name| name == required) {
                    return Err(RecordError::new(format!(
                        "sleep CSV header lacks {required}"
                    )));
                }
            }
            let unique: std::collections::BTreeSet<_> = cells.iter().collect();
            if unique.len() != cells.len() || cells.iter().any(String::is_empty) {
                return Err(RecordError::new(
                    "sleep CSV header has empty or repeated names",
                ));
            }
            return Ok((
                vec![],
                Carry {
                    member,
                    header: cells,
                },
            ));
        }
        if cells.len() != header.len() {
            return Err(RecordError::new(
                "sleep CSV row has a different number of cells than its header",
            ));
        }
        let row: Map<String, Value> = header
            .iter()
            .cloned()
            .zip(cells.into_iter().map(Value::String))
            .collect();
        let text = |name: &str| -> Result<String, RecordError> {
            row.get(name)
                .and_then(Value::as_str)
                .filter(|s| !s.trim().is_empty())
                .map(str::to_owned)
                .ok_or_else(|| RecordError::new(format!("sleep CSV requires {name}")))
        };
        let optional = |name: &str| {
            row.get(name)
                .and_then(Value::as_str)
                .filter(|s| !s.trim().is_empty())
        };
        let number = |name: &str| -> Result<Option<f64>, RecordError> {
            optional(name)
                .map(|s| {
                    s.trim()
                        .parse::<f64>()
                        .ok()
                        .filter(|n| n.is_finite())
                        .ok_or_else(|| {
                            RecordError::new(format!("sleep CSV {name} is not a finite number"))
                        })
                })
                .transpose()
        };
        let count = |name: &str| -> Result<Option<u64>, RecordError> {
            optional(name)
                .map(|s| {
                    s.trim().parse::<u64>().map_err(|_| {
                        RecordError::new(format!("sleep CSV {name} is not an unsigned count"))
                    })
                })
                .transpose()
        };
        let start_local = text("start_local")?;
        let end_local = text("end_local")?;
        let (started_at, start_unresolved) = self.resolve(&start_local)?;
        let (ended_at, end_unresolved) = self.resolve(&end_local)?;
        if let (Some(start), Some(end)) = (started_at, ended_at)
            && end < start
        {
            return Err(RecordError::new("sleep CSV end precedes start"));
        }
        let payload = SleepSessionImported {
            dataset: self.dataset.clone(),
            provider_session_id: text("sh_datauuid")?,
            start_local,
            end_local,
            zone: self.zone_name.clone(),
            started_at,
            ended_at,
            start_unresolved,
            end_unresolved,
            sh_duration_minutes: number("sh_duration_minutes")?,
            sa_vs_sh_duration_minutes: number("sa_vs_sh_duration_minutes")?,
            hr_avg: number("hr_avg")?,
            hr_min: number("hr_min")?,
            hr_max: number("hr_max")?,
            trimmed_event_count: count("trimmed_event_count")?,
            events_hr: count("events_hr")?,
            events_light: count("events_light")?,
            events_deep: count("events_deep")?,
            events_rem: count("events_rem")?,
            sa_comment: optional("sa_comment").map(str::to_owned),
            extra: row
                .iter()
                .filter(|(name, _)| !KNOWN.contains(&name.as_str()))
                .map(|(k, v)| (k.clone(), v.clone()))
                .collect(),
        };
        Ok((
            vec![payload],
            Carry {
                member,
                header: header.to_vec(),
            },
        ))
    }
}

impl SleepCsvParser {
    fn resolve(&self, raw: &str) -> Result<(Option<Timestamp>, Option<String>), RecordError> {
        if let Ok(at) = super::fields::rfc3339(raw) {
            return Ok((Some(at), None));
        }
        let bad =
            || RecordError::new("invalid sleep CSV local timestamp; expected ISO date and time");
        if raw
            .get(10..)
            .is_none_or(|s| s.contains(['+', '-', 'Z', '[', ']']))
        {
            return Err(bad());
        }
        let civil = raw.parse::<jiff::civil::DateTime>().map_err(|_| bad())?;
        let ambiguous = self.zone.to_ambiguous_zoned(civil);
        let reason = match ambiguous.offset() {
            jiff::tz::AmbiguousOffset::Gap { .. } => Some("nonexistent local time".into()),
            jiff::tz::AmbiguousOffset::Fold { .. } => Some("ambiguous local time".into()),
            jiff::tz::AmbiguousOffset::Unambiguous { .. } => None,
        };
        if reason.is_some() {
            return Ok((None, reason));
        }
        let zoned = ambiguous.unambiguous().map_err(|_| bad())?;
        let at = Timestamp::from_unix_ns(zoned.timestamp().as_nanosecond()).map_err(|_| bad())?;
        Ok((Some(at), None))
    }
}

fn validate(settings: &Settings) -> Result<SleepCsvParser, String> {
    if settings
        .options
        .get("read_sleep_csv")
        .and_then(Value::as_bool)
        != Some(true)
    {
        return Err("sleep CSV is disabled; set read_sleep_csv=true explicitly".into());
    }
    let dataset = settings
        .option("dataset")
        .filter(|s| !s.trim().is_empty())
        .ok_or("sleep CSV requires dataset")?;
    let zone_name = settings
        .zone
        .as_deref()
        .ok_or("sleep CSV requires an explicit zone")?;
    let zone = binding::zone(Some(zone_name))?;
    Ok(SleepCsvParser {
        dataset: dataset.into(),
        zone,
        zone_name: zone_name.into(),
    })
}

pub fn source(settings: &Settings) -> Result<DeclaredSource<SleepCsvParser>, String> {
    Ok(DeclaredSource {
        spec: SPEC,
        parser: validate(settings)?,
    })
}

pub fn frame(bytes: &[u8]) -> (Vec<(usize, usize)>, usize) {
    Framing::Csv.frame(bytes, true)
}

pub fn driver(path: PathBuf, host: String) -> ExportArchive {
    ExportArchive {
        path,
        host,
        select: |m| m.ends_with(".csv"),
        framing: Framing::Csv,
        part_bytes: PART_BYTES,
    }
}

pub const FORMAT: InputFormat = InputFormat {
    name: "sleep-csv",
    specs: || vec![&SPEC],
    plan: Some(|input, settings| {
        validate(settings)?;
        Ok(binding::single(&SPEC, input, settings))
    }),
    source: |b| Ok(Box::new(source(&b.settings)?)),
    reader: Some(|b| {
        validate(&b.settings)?;
        Ok(binding::shared(driver(
            b.input.clone(),
            b.settings.host.clone(),
        )))
    }),
    capture: None,
};

#[cfg(test)]
mod tests;
