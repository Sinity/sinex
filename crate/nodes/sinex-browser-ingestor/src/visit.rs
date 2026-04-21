use chrono::{NaiveDateTime, TimeZone, Utc};
use chrono_tz::Europe::Warsaw;
use serde_json::{Map, Value};
use sinex_node_sdk::{NodeResult, SinexError};
use sinex_primitives::{Timestamp, utils::timestamp_helpers::parse_flexible_timestamp};
use url::Url;

const TIMESTAMP_FIELDS: &[&str] = &[
    "iso_time",
    "time",
    "visit_time",
    "visitTime",
    "lastVisitTime",
    "timestamp",
    "DateTime",
    "date",
];

const TRACKING_PREFIXES: &[&str] = &[
    "utm_",
    "fbclid",
    "gclid",
    "igshid",
    "yclid",
    "dclid",
    "ref_",
    "spm",
    "sc_",
    "mc_",
    "mkt_",
    "pk_campaign",
    "pk_kwd",
    "ga_",
    "gs_",
    "ved",
    "ei",
    "sa",
    "rlz",
    "dpr",
    "biw",
    "bih",
];

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BrowserVisitRecord {
    pub browser: String,
    pub title: String,
    pub url: String,
    pub normalized_url: Option<String>,
    pub visit_time: Timestamp,
    pub referrer: Option<String>,
    pub transition: Option<String>,
    pub visit_id: Option<String>,
    pub visit_duration_ms: Option<u64>,
    pub source_file: String,
    pub line_number: Option<u64>,
    pub db_row_id: Option<u64>,
    pub material_bytes: Vec<u8>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ParsedDumpStats {
    pub delta_line_count: u64,
}

#[must_use]
pub fn infer_browser_from_path(path: &camino::Utf8Path, browser_override: Option<&str>) -> String {
    if let Some(browser_override) = browser_override {
        return browser_override.to_string();
    }

    let filename = path.file_name().unwrap_or_default().to_ascii_lowercase();

    for browser in [
        "chrome",
        "edge",
        "firefox",
        "floorp",
        "qutebrowser",
        "zen",
        "merged",
        "browser",
    ] {
        if filename.starts_with(browser) {
            return browser.to_string();
        }
    }

    "browser".to_string()
}

#[must_use]
pub fn normalize_url(url: &str) -> Option<String> {
    let url = url.trim();
    if url.is_empty() {
        return None;
    }

    let Ok(mut parsed) = Url::parse(url) else {
        return None;
    };

    let scheme = parsed.scheme().to_ascii_lowercase();
    if !matches!(scheme.as_str(), "http" | "https") {
        return None;
    }

    let host = parsed.host_str()?;

    let host = host
        .trim()
        .to_ascii_lowercase()
        .strip_prefix("www.")
        .unwrap_or(host)
        .to_string();

    if parsed.scheme() != "https" {
        let _ = parsed.set_scheme("https");
    }
    let _ = parsed.set_host(Some(&host));

    if parsed.path().len() > 1 && parsed.path().ends_with('/') {
        let trimmed = parsed.path().trim_end_matches('/').to_string();
        parsed.set_path(&trimmed);
    }

    let keep = special_param_whitelist(&host);
    let filtered: Vec<(String, String)> = parsed
        .query_pairs()
        .filter(|(key, _)| {
            keep.iter().any(|allowed| *allowed == key.as_ref())
                || !TRACKING_PREFIXES
                    .iter()
                    .any(|prefix| key.as_ref().starts_with(prefix))
        })
        .map(|(key, value)| (key.into_owned(), value.into_owned()))
        .collect();

    if filtered.is_empty() {
        parsed.set_query(None);
    } else {
        let mut serializer = url::form_urlencoded::Serializer::new(String::new());
        for (key, value) in filtered {
            serializer.append_pair(&key, &value);
        }
        parsed.set_query(Some(&serializer.finish()));
    }

    Some(parsed.to_string())
}

fn special_param_whitelist(host: &str) -> &'static [&'static str] {
    match host {
        "youtube.com" => &["v", "list", "t"],
        "youtu.be" => &["v", "t"],
        "github.com" => &["ref", "sha"],
        "reddit.com" => &["sort", "type", "t"],
        "twitter.com" => &["s", "q"],
        _ => &[],
    }
}

#[must_use]
pub fn payload_timestamp(payload: &Map<String, Value>) -> Option<Timestamp> {
    for field in TIMESTAMP_FIELDS {
        let Some(value) = payload.get(*field) else {
            continue;
        };
        if let Some(timestamp) = parse_history_timestamp_value(value) {
            return Some(timestamp);
        }
    }
    None
}

#[must_use]
pub fn parse_history_timestamp_value(value: &Value) -> Option<Timestamp> {
    match value {
        Value::String(value) => parse_history_timestamp_str(value),
        Value::Number(value) => {
            if let Some(value) = value.as_i64() {
                return parse_numeric_timestamp_i64(value);
            }
            if let Some(value) = value.as_u64().and_then(|value| i64::try_from(value).ok()) {
                return parse_numeric_timestamp_i64(value);
            }
            value.as_f64().and_then(parse_numeric_timestamp_f64)
        }
        _ => None,
    }
}

#[must_use]
pub fn parse_history_timestamp_str(value: &str) -> Option<Timestamp> {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        return None;
    }

    if let Some(timestamp) = parse_numeric_timestamp_text(trimmed) {
        return Some(timestamp);
    }

    if let Some(timestamp) = parse_flexible_timestamp(trimmed) {
        return Some(timestamp);
    }

    parse_slash_timestamp(trimmed)
}

#[must_use]
pub fn parse_numeric_timestamp_i64(value: i64) -> Option<Timestamp> {
    let digits = decimal_digits_i128(i128::from(value));
    let unit_nanos = timestamp_unit_nanos(digits);
    Timestamp::from_unix_timestamp_nanos(i128::from(value) * unit_nanos)
}

#[must_use]
pub fn parse_numeric_timestamp_f64(value: f64) -> Option<Timestamp> {
    if let Some(timestamp) = parse_numeric_timestamp_text(&value.to_string()) {
        return Some(timestamp);
    }

    let magnitude = value.abs();
    let divisors = if magnitude >= 1e18 {
        [1_000_000_000.0, 1.0]
    } else if magnitude >= 1e15 {
        [1_000_000.0, 1.0]
    } else if magnitude >= 1e12 {
        [1_000.0, 1.0]
    } else {
        [1.0, 1.0]
    };

    for divisor in divisors {
        let nanos = ((value / divisor) * 1_000_000_000.0).round();
        if !nanos.is_finite() {
            continue;
        }
        if let Some(timestamp) = Timestamp::from_unix_timestamp_nanos(nanos as i128) {
            return Some(timestamp);
        }
    }

    None
}

#[must_use]
fn parse_numeric_timestamp_text(value: &str) -> Option<Timestamp> {
    let trimmed = value.trim();
    let (negative, digits) = match trimmed.as_bytes() {
        [b'-', rest @ ..] => (true, std::str::from_utf8(rest).ok()?),
        [b'+', rest @ ..] => (false, std::str::from_utf8(rest).ok()?),
        _ => (false, trimmed),
    };

    let (whole, fraction) = digits.split_once('.').unwrap_or((digits, ""));
    if whole.is_empty()
        || !whole.chars().all(|ch| ch.is_ascii_digit())
        || !fraction.chars().all(|ch| ch.is_ascii_digit())
    {
        return None;
    }

    let whole_value = whole.parse::<i128>().ok()?;
    let digits = decimal_digits_i128(whole_value);
    let unit_nanos = timestamp_unit_nanos(digits);
    let mut nanos = whole_value.checked_mul(unit_nanos)?;

    if !fraction.is_empty() {
        let fraction_value = fraction.parse::<i128>().ok()?;
        let scale = 10_i128.checked_pow(u32::try_from(fraction.len()).ok()?)?;
        nanos = nanos.checked_add(fraction_value.checked_mul(unit_nanos)? / scale)?;
    }

    if negative {
        nanos = -nanos;
    }

    Timestamp::from_unix_timestamp_nanos(nanos)
}

fn decimal_digits_i128(value: i128) -> u32 {
    let magnitude = value.unsigned_abs();
    magnitude.checked_ilog10().unwrap_or(0) + 1
}

fn timestamp_unit_nanos(digits: u32) -> i128 {
    if digits >= 18 {
        1
    } else if digits >= 15 {
        1_000
    } else if digits >= 12 {
        1_000_000
    } else {
        1_000_000_000
    }
}

#[must_use]
pub fn parse_slash_timestamp(value: &str) -> Option<Timestamp> {
    for format in [
        "%m/%d/%Y %H:%M:%S",
        "%m/%d/%Y %H:%M",
        "%m/%d/%y %H:%M:%S",
        "%m/%d/%y %H:%M",
    ] {
        let Ok(naive) = NaiveDateTime::parse_from_str(value, format) else {
            continue;
        };
        let localized = Warsaw
            .from_local_datetime(&naive)
            .single()
            .or_else(|| Warsaw.from_local_datetime(&naive).earliest())
            .or_else(|| Warsaw.from_local_datetime(&naive).latest())?;
        let utc = localized.with_timezone(&Utc);
        if let Some(timestamp) =
            Timestamp::from_unix_timestamp_nanos(i128::from(utc.timestamp_nanos_opt()?))
        {
            return Some(timestamp);
        }
    }
    None
}

#[must_use]
pub fn extract_optional_string(payload: &Map<String, Value>, fields: &[&str]) -> Option<String> {
    for field in fields {
        if let Some(Value::String(value)) = payload.get(*field)
            && !value.trim().is_empty()
        {
            return Some(value.clone());
        }
    }
    None
}

#[must_use]
pub fn extract_optional_u64(payload: &Map<String, Value>, fields: &[&str]) -> Option<u64> {
    for field in fields {
        let Some(value) = payload.get(*field) else {
            continue;
        };
        match value {
            Value::Number(value) => {
                if let Some(value) = value.as_u64() {
                    return Some(value);
                }
                if let Some(value) = value.as_i64()
                    && value >= 0
                {
                    return Some(value as u64);
                }
            }
            Value::String(value) => {
                if let Ok(value) = value.parse::<u64>() {
                    return Some(value);
                }
            }
            _ => {}
        }
    }
    None
}

pub fn build_material_bytes(payload: &Map<String, Value>) -> NodeResult<Vec<u8>> {
    serde_json::to_vec(payload).map_err(|error| {
        SinexError::serialization("failed to encode browser history material")
            .with_std_error(&error)
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn normalize_url_strips_tracking_params() {
        let normalized =
            normalize_url("https://www.youtube.com/watch?v=abc123&utm_source=test&list=playlist");
        assert_eq!(
            normalized.as_deref(),
            Some("https://youtube.com/watch?v=abc123&list=playlist")
        );
    }

    #[test]
    fn normalize_url_ignores_non_http_schemes() {
        assert_eq!(normalize_url("chrome-extension://abc/onetab.html"), None);
        assert_eq!(normalize_url("file:///tmp/test.html"), None);
    }

    #[test]
    fn parse_numeric_timestamp_supports_milliseconds_with_fraction() {
        let timestamp = parse_numeric_timestamp_f64(1_729_462_321_215.972).unwrap();
        assert_eq!(timestamp.format_rfc3339(), "2024-10-20T22:12:01.215972Z");
    }

    #[test]
    fn parse_slash_timestamp_uses_warsaw_timezone() {
        let timestamp = parse_slash_timestamp("12/19/2025 16:55:45").unwrap();
        assert_eq!(timestamp.format_rfc3339(), "2025-12-19T15:55:45Z");
    }

    #[test]
    fn payload_timestamp_uses_known_fields() {
        let payload = json!({
            "title": "Example",
            "url": "https://example.com",
            "visitTime": 1759527495463.789
        });
        let payload = payload.as_object().unwrap();
        let timestamp = payload_timestamp(payload).unwrap();
        assert_eq!(timestamp.format_rfc3339(), "2025-10-03T21:38:15.463789Z");
    }
}
