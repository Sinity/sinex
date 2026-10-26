//! The home router's lanes in the Sinnix capture lake
//! (`<machine root>/router/<lane>/<lane>-YYYYMMDD.jsonl`), on `AppendFile`.
//! Sinnix samples the router over SSH every half hour and writes one
//! `sinnix-capture-v1` record per sample and lane: the log lines new since the
//! last sample, the DHCP leases, nlbwmon's traffic table, and the clients of
//! each Wi-Fi access point. Every line, lease, table row and client is its own
//! event, with the sample's time.
//!
//! The router logs its local time without a zone, so the binding's zone
//! (recorded when it is planned) reads it. In the repeated hour when summer
//! time ends, a line takes the later of its two instants unless that is after
//! the sample that read it.
use super::fields::{FieldReader, from_unix_secs_f64};
use crate::drivers::append_file::{AppendFile, lines};
use crate::registry::{DeclaredSource, Driver, SourceSpec};
use crate::{Parser, RecordError};
use serde::Deserialize;
use serde_json::{Map, Value};
use sinex_model::event_types::router::{
    DeviceTrafficSampled, DhcpLeaseSampled, Directions, RouterLogLine, WifiClientSampled,
};
use sinex_model::{Class, Timestamp};
use std::path::{Path, PathBuf};

const fn spec(lane: &'static str, event_type: &'static [&'static str]) -> SourceSpec {
    SourceSpec {
        name: "network.router",
        driver: Driver::AppendFile,
        excisable: false,
        purpose: "which devices were on the home network, their traffic and the router's log",
        // Leases and clients name the household's devices, and the log
        // their addresses.
        class: Class::Guarded,
        parser: lane,
        parser_version: 1,
        event_types: event_type,
        framer: crate::Framer::incremental(lines),
    }
}

/// One declaration per lane, each its own parser name, so a replay knows
/// which lane a stored day file is.
const LANES: [(SourceSpec, &str); 4] = [
    (spec("router-syslog", &["router_log.line"]), "syslog"),
    (spec("router-leases", &["dhcp_lease.sampled"]), "leases"),
    (spec("router-nlbw", &["device_traffic.sampled"]), "nlbw"),
    (
        spec("router-associations", &["wifi_client.sampled"]),
        "associations",
    ),
];

sinex_model::payload_enum! {
    #[derive(Clone, Debug, PartialEq)]
    pub enum Router {
        Line(RouterLogLine),
        Lease(DhcpLeaseSampled),
        Traffic(DeviceTrafficSampled),
        Client(WifiClientSampled),
    }
}

/// One record of a lane's day file, around one sample's payload.
#[derive(Deserialize)]
struct CaptureRecord {
    schema: String,
    schema_version: i64,
    lane: String,
    ts: f64,
    host: String,
    seq: i64,
    payload: Value,
}

/// The sample a payload came from.
struct Sample {
    sampled_at: Timestamp,
    host: String,
    seq: i64,
}

pub struct RouterParser {
    /// The lane this binding's day files belong to.
    pub lane: &'static str,
    /// The zone the router's log times are written in.
    pub zone: jiff::tz::TimeZone,
}

impl Parser for RouterParser {
    type Record<'a> = &'a [u8];
    type Carry = ();
    type Payload = Router;

    fn parse(
        &self,
        record: &&[u8],
        _: &(),
        _context: &crate::RecordContext<'_>,
    ) -> Result<(Vec<Router>, ()), RecordError> {
        let captured: CaptureRecord = serde_json::from_slice(record)
            .map_err(|e| RecordError::new(format!("not a capture record: {e}")))?;
        if captured.schema != "sinnix-capture-v1" || captured.schema_version != 1 {
            return Err(RecordError::new(format!(
                "capture record schema {} version {}, expected sinnix-capture-v1 version 1",
                captured.schema, captured.schema_version
            )));
        }
        if captured.lane != self.lane {
            return Err(RecordError::new(format!(
                "lane {:?} in a {} day file",
                captured.lane, self.lane
            )));
        }
        let sample = Sample {
            sampled_at: from_unix_secs_f64(captured.ts, "ts")?,
            host: captured.host,
            seq: captured.seq,
        };
        let out = match self.lane {
            "syslog" => self.log_lines(captured.payload, &sample)?,
            "leases" => leases(captured.payload, &sample)?,
            "nlbw" => traffic(captured.payload, &sample)?,
            "associations" => clients(captured.payload, &sample)?,
            other => return Err(RecordError::new(format!("{other} is not a router lane"))),
        };
        Ok((out, ()))
    }
}

impl RouterParser {
    fn log_lines(&self, payload: Value, sample: &Sample) -> Result<Vec<Router>, RecordError> {
        let mut payload = object(payload, "payload")?;
        payload
            .texts("lines")?
            .into_iter()
            .map(|line| Ok(Router::Line(self.log_line(&line, sample)?)))
            .collect()
    }

    /// `Tue Oct 20 01:52:08 2026 daemon.err miniupnpd[4107]: text`; the
    /// program and its pid are optional, and a line without that header is
    /// kept whole as its message.
    fn log_line(&self, line: &str, sample: &Sample) -> Result<RouterLogLine, RecordError> {
        let mut out = RouterLogLine {
            logged_at: None,
            facility: None,
            severity: None,
            program: None,
            pid: None,
            message: line.to_owned(),
            sampled_at: sample.sampled_at,
            host: sample.host.clone(),
            seq: sample.seq,
        };
        let Some((civil, rest)) = log_time(line) else {
            return Ok(out);
        };
        out.logged_at = Some(self.instant(civil, sample.sampled_at)?);
        out.message = rest.to_owned();
        let Some((priority, rest)) = rest.split_once(' ') else {
            return Ok(out);
        };
        let Some((facility, severity)) = priority.rsplit_once('.') else {
            return Ok(out);
        };
        out.facility = Some(facility.to_owned());
        out.severity = Some(severity.to_owned());
        out.message = rest.to_owned();
        if let Some((tag, message)) = rest.split_once(": ")
            && let Some((program, pid)) = program_tag(tag)
        {
            out.program = Some(program.to_owned());
            out.pid = pid;
            out.message = message.to_owned();
        }
        Ok(out)
    }

    fn instant(
        &self,
        civil: jiff::civil::DateTime,
        sampled_at: Timestamp,
    ) -> Result<Timestamp, RecordError> {
        let ambiguous = self.zone.to_ambiguous_timestamp(civil);
        let chosen = match ambiguous.offset() {
            jiff::tz::AmbiguousOffset::Fold { .. } => {
                let later = ambiguous.later();
                match later {
                    Ok(t) if t.as_nanosecond() <= sampled_at.unix_ns() => later,
                    _ => ambiguous.earlier(),
                }
            }
            _ => ambiguous.compatible(),
        }
        .map_err(|e| RecordError::new(format!("log time {civil}: {e}")))?;
        Timestamp::from_unix_ns(chosen.as_nanosecond())
            .map_err(|_| RecordError::new(format!("log time {civil} is out of range")))
    }
}

const MONTHS: [&str; 12] = [
    "Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec",
];

/// The time at the start of a log line (`Tue Oct  6 01:52:08 2026`) and the
/// rest after it.
fn log_time(line: &str) -> Option<(jiff::civil::DateTime, &str)> {
    let mut rest = line;
    let mut words = [""; 5];
    for word in &mut words {
        rest = rest.trim_start_matches(' ');
        let end = rest.find(' ')?;
        *word = &rest[..end];
        rest = &rest[end..];
    }
    let [_weekday, month, day, clock, year] = words;
    let month = MONTHS.iter().position(|m| *m == month)? as i8 + 1;
    let mut clock = clock.split(':').map(str::parse::<i8>);
    let (Some(Ok(hour)), Some(Ok(minute)), Some(Ok(second)), None) =
        (clock.next(), clock.next(), clock.next(), clock.next())
    else {
        return None;
    };
    let civil = jiff::civil::DateTime::new(
        year.parse().ok()?,
        month,
        day.parse().ok()?,
        hour,
        minute,
        second,
        0,
    )
    .ok()?;
    Some((civil, rest.strip_prefix(' ')?))
}

/// `program`, `program[pid]` or `program [pid]`.
fn program_tag(tag: &str) -> Option<(&str, Option<u32>)> {
    let (program, pid) = match tag.strip_suffix(']') {
        Some(open) => {
            let (program, pid) = open.rsplit_once('[')?;
            (program.trim_end_matches(' '), Some(pid.parse().ok()?))
        }
        None => (tag, None),
    };
    let plain = !program.is_empty()
        && !program.contains([' ', '[', ']', ':'])
        && program.chars().all(|c| !c.is_control());
    plain.then_some((program, pid))
}

fn object(value: Value, what: &str) -> Result<FieldReader, RecordError> {
    match value {
        Value::Object(object) => Ok(FieldReader::new(object)),
        other => Err(RecordError::new(format!(
            "{what} is not an object: {other}"
        ))),
    }
}

fn required<T>(value: Option<T>, name: &str) -> Result<T, RecordError> {
    value.ok_or_else(|| RecordError::new(format!("{name} is missing")))
}

/// The leases, `[{mac, ip, hostname, client_id, expires}]`; dnsmasq writes
/// `*` for a name or client ID the device did not give.
fn leases(payload: Value, sample: &Sample) -> Result<Vec<Router>, RecordError> {
    let Value::Array(items) = payload else {
        return Err(RecordError::new("the leases payload is not a list"));
    };
    items
        .into_iter()
        .enumerate()
        .map(|(i, item)| {
            let mut f = object(item, &format!("lease {i}"))?;
            let given = |v: Option<String>| v.filter(|s| s != "*");
            Ok(Router::Lease(DhcpLeaseSampled {
                mac: f.required_text("mac")?,
                ip: f.required_text("ip")?,
                hostname: given(f.text("hostname")?),
                client_id: given(f.text("client_id")?),
                // dnsmasq writes 0 for a lease that does not expire.
                expires_at: match f.int("expires")? {
                    None | Some(0) => None,
                    Some(secs) => Some(Timestamp::from_unix_secs(secs).map_err(|_| {
                        RecordError::new(format!("lease {i}: expires {secs} is out of range"))
                    })?),
                },
                sampled_at: sample.sampled_at,
                host: sample.host.clone(),
                seq: sample.seq,
                extra: f.rest(),
            }))
        })
        .collect()
}

/// nlbwmon's table for one period, `{period, rows: [...]}`.
fn traffic(payload: Value, sample: &Sample) -> Result<Vec<Router>, RecordError> {
    let mut payload = object(payload, "payload")?;
    let period = payload.required_text("period")?;
    let Some(Value::Array(rows)) = payload.value("rows") else {
        return Err(RecordError::new("rows is not a list"));
    };
    rows.into_iter()
        .enumerate()
        .map(|(i, row)| {
            let mut f = object(row, &format!("rows/{i}"))?;
            let family = f
                .int("family")?
                .map(|n| {
                    u8::try_from(n).map_err(|_| {
                        RecordError::new(format!("rows/{i}: family {n} is out of range"))
                    })
                })
                .transpose()?;
            let port = f
                .int("port")?
                .map(|n| {
                    u16::try_from(n).map_err(|_| {
                        RecordError::new(format!("rows/{i}: port {n} is out of range"))
                    })
                })
                .transpose()?;
            Ok(Router::Traffic(DeviceTrafficSampled {
                period: period.clone(),
                mac: f.required_text("mac")?,
                ip: f.required_text("ip")?,
                family,
                proto: f.text("proto")?,
                port,
                layer7: f.text("layer7")?,
                conns: f.unsigned("conns")?,
                rx_bytes: f.unsigned("rx_bytes")?,
                rx_pkts: f.unsigned("rx_pkts")?,
                tx_bytes: f.unsigned("tx_bytes")?,
                tx_pkts: f.unsigned("tx_pkts")?,
                sampled_at: sample.sampled_at,
                host: sample.host.clone(),
                seq: sample.seq,
                extra: f.rest(),
            }))
        })
        .collect()
}

/// A `{rx, tx}` pair; other fields in it are kept in `extra` under its
/// name.
fn directions(
    f: &mut FieldReader,
    name: &str,
    extra: &mut Map<String, Value>,
) -> Result<Option<Directions>, RecordError> {
    let Some(mut pair) = f.object(name)? else {
        return Ok(None);
    };
    let out = Directions {
        rx: pair.unsigned("rx")?,
        tx: pair.unsigned("tx")?,
    };
    let rest = pair.rest();
    if !rest.is_empty() {
        extra.insert(name.to_owned(), Value::Object(rest));
    }
    Ok(Some(out))
}

/// Each access point's clients, `{aps: [{ap, data: {freq, clients: {mac:
/// {...}}}}]`; an access point that did not answer has empty data.
fn clients(payload: Value, sample: &Sample) -> Result<Vec<Router>, RecordError> {
    let mut payload = object(payload, "payload")?;
    let Some(Value::Array(aps)) = payload.value("aps") else {
        return Err(RecordError::new("aps is not a list"));
    };
    let mut out = Vec::new();
    for (i, ap) in aps.into_iter().enumerate() {
        let mut ap = object(ap, &format!("aps/{i}"))?;
        let name = required(ap.text("ap")?, "ap")?;
        let Some(mut data) = ap.object("data")? else {
            continue;
        };
        let freq = data
            .unsigned("freq")?
            .map(|f| u32::try_from(f).map_err(|_| RecordError::new(format!("freq {f}"))))
            .transpose()?;
        let Some(Value::Object(clients)) = data.value("clients") else {
            continue;
        };
        for (mac, client) in clients {
            let mut f = object(client, &format!("{name} client {mac}"))?;
            let signal = f.int("signal")?;
            let aid = f.unsigned("aid")?;
            let assoc = f.boolean("assoc")?;
            let auth = f.boolean("auth")?;
            let authorized = f.boolean("authorized")?;
            let mut more = Map::new();
            let bytes = directions(&mut f, "bytes", &mut more)?;
            let packets = directions(&mut f, "packets", &mut more)?;
            let rate = directions(&mut f, "rate", &mut more)?;
            let airtime = directions(&mut f, "airtime", &mut more)?;
            let mut extra = f.rest();
            extra.extend(more);
            out.push(Router::Client(WifiClientSampled {
                ap: name.clone(),
                freq,
                signal,
                aid,
                assoc,
                auth,
                authorized,
                bytes,
                packets,
                rate,
                airtime,
                mac,
                sampled_at: sample.sampled_at,
                host: sample.host.clone(),
                seq: sample.seq,
                extra,
            }));
        }
    }
    Ok(out)
}

pub fn source(
    lane: &'static str,
    zone: jiff::tz::TimeZone,
) -> Result<Box<dyn crate::Source>, String> {
    let (spec, _) = LANES
        .iter()
        .find(|(_, name)| *name == lane)
        .ok_or_else(|| format!("{lane} is not a router lane"))?;
    Ok(Box::new(DeclaredSource {
        spec: spec.clone(),
        parser: RouterParser { lane, zone },
    }))
}

/// The lane a day file (`<lane>-YYYYMMDD.jsonl`) belongs to.
fn lane_of(path: &Path) -> Option<&'static SourceSpec> {
    let stem = path.file_name()?.to_str()?.strip_suffix(".jsonl")?;
    let (lane, day) = stem.rsplit_once('-')?;
    if day.len() != 8 || !day.bytes().all(|b| b.is_ascii_digit()) {
        return None;
    }
    LANES
        .iter()
        .find(|(_, name)| *name == lane)
        .map(|(spec, _)| spec)
}

fn lane_name(parser: &str) -> Result<&'static str, String> {
    LANES
        .iter()
        .find(|(spec, _)| spec.parser == parser)
        .map(|(_, lane)| *lane)
        .ok_or_else(|| format!("{parser} is not a router lane"))
}

pub fn driver(path: PathBuf, host: String) -> AppendFile {
    AppendFile {
        path,
        host,
        framer: crate::Framer::incremental(lines),
    }
}

/// `--read router=PATH`: every lane day file under PATH (the router
/// directory, one lane's directory or one day file), read in the binding's
/// zone, recorded when it is planned.
pub const FORMAT: crate::binding::InputFormat = crate::binding::InputFormat {
    name: "router",
    specs: || LANES.iter().map(|(spec, _)| spec).collect(),
    plan: Some(|input, settings| {
        let settings = settings.clone().with_zone()?;
        Ok(crate::binding::files(input, true, |p| lane_of(p).is_some())
            .into_iter()
            .filter_map(|file| {
                lane_of(&file).map(|spec| crate::Binding::of(spec, &file, settings.clone()))
            })
            .collect())
    }),
    source: |b| source(lane_name(&b.parser)?, b.settings.zone()?),
    reader: Some(|b| {
        Ok(crate::binding::appended(driver(
            b.input.clone(),
            b.settings.host.clone(),
        )))
    }),
    capture: None,
};

#[cfg(test)]
mod tests;
