use crate::visit::{
    BrowserVisitRecord, ParsedDumpStats, build_material_bytes, extract_optional_string,
    extract_optional_u64, infer_browser_from_path, normalize_url, parse_history_timestamp_str,
    payload_timestamp,
};
use csv::ReaderBuilder;
use serde_json::{Map, Value};
use sinex_node_sdk::{
    DiscoveredFile, ImportFileChangeKind, NodeResult, SinexError, read_file_content,
};
use std::io::{Read, Seek, SeekFrom};

pub const SUPPORTED_HISTORY_EXTENSIONS: &[&str] = &[".csv", ".json", ".jsonl", ".ndjson"];

pub struct ParsedDumpFile {
    pub visits: Vec<BrowserVisitRecord>,
    pub stats: ParsedDumpStats,
}

pub fn parse_dump_file(
    file: &DiscoveredFile,
    browser_override: Option<&str>,
) -> NodeResult<ParsedDumpFile> {
    match extension(file.path.as_str()) {
        Some("jsonl" | "ndjson") => {
            if matches!(file.change_kind, ImportFileChangeKind::Appended) {
                parse_append_only_dump(file, browser_override)
            } else {
                let content = std::fs::read_to_string(file.path.as_std_path())
                    .map_err(|error| SinexError::io(error.to_string()))?;
                parse_json_lines(&content, file, browser_override, 0)
            }
        }
        Some("json") => parse_json_dump(file, browser_override),
        Some("csv") => parse_csv_dump(file, browser_override),
        _ => Err(SinexError::validation(format!(
            "unsupported browser history dump format: {}",
            file.path
        ))),
    }
}

fn parse_append_only_dump(
    file: &DiscoveredFile,
    browser_override: Option<&str>,
) -> NodeResult<ParsedDumpFile> {
    let mut handle = std::fs::File::open(file.path.as_std_path())
        .map_err(|error| SinexError::io(error.to_string()))?;
    handle
        .seek(SeekFrom::Start(file.start_offset_bytes))
        .map_err(|error| SinexError::io(error.to_string()))?;

    let mut buffer = String::new();
    handle
        .read_to_string(&mut buffer)
        .map_err(|error| SinexError::io(error.to_string()))?;

    parse_json_lines(&buffer, file, browser_override, file.start_line_number)
}

fn parse_json_dump(
    file: &DiscoveredFile,
    browser_override: Option<&str>,
) -> NodeResult<ParsedDumpFile> {
    let bytes = read_file_content(file).map_err(|error| SinexError::io(error.to_string()))?;
    let Ok(payload) = serde_json::from_slice::<Value>(&bytes) else {
        return parse_json_lines(&String::from_utf8_lossy(&bytes), file, browser_override, 0);
    };

    let mut visits = Vec::new();
    let mut line_count = 0u64;
    match payload {
        Value::Array(items) => {
            for (index, item) in items.into_iter().enumerate() {
                line_count += 1;
                let Some(payload) = item.as_object() else {
                    continue;
                };
                if let Some(visit) = build_visit_from_payload(
                    payload,
                    file,
                    browser_override,
                    Some((index + 1) as u64),
                )? {
                    visits.push(visit);
                }
            }
        }
        Value::Object(payload) => {
            line_count = 1;
            if let Some(visit) =
                build_visit_from_payload(&payload, file, browser_override, Some(1))?
            {
                visits.push(visit);
            }
        }
        _ => {}
    }

    Ok(ParsedDumpFile {
        visits,
        stats: ParsedDumpStats {
            delta_line_count: line_count,
        },
    })
}

fn parse_csv_dump(
    file: &DiscoveredFile,
    browser_override: Option<&str>,
) -> NodeResult<ParsedDumpFile> {
    let bytes = read_file_content(file).map_err(|error| SinexError::io(error.to_string()))?;
    let mut reader = ReaderBuilder::new()
        .flexible(true)
        .from_reader(bytes.as_slice());

    let headers = reader
        .headers()
        .map_err(|error| {
            SinexError::parse(format!(
                "failed to read CSV headers from {}: {error}",
                file.path
            ))
        })?
        .clone();

    let browser = infer_browser_from_path(&file.path, browser_override);
    let mut visits = Vec::new();
    let mut line_count = 0u64;

    for (index, row) in reader
        .deserialize::<std::collections::BTreeMap<String, String>>()
        .enumerate()
    {
        line_count += 1;
        let row = row.map_err(|error| {
            SinexError::parse(format!(
                "failed to parse CSV row {} from {}: {error}",
                index + 2,
                file.path
            ))
        })?;
        let mut payload = Map::new();
        for header in &headers {
            if let Some(value) = row.get(header) {
                payload.insert(header.to_string(), Value::String(value.clone()));
            }
        }

        let timestamp = parse_csv_timestamp(&payload).ok_or_else(|| {
            SinexError::validation(format!(
                "CSV row {} in {} is missing a usable timestamp",
                index + 2,
                file.path
            ))
        })?;
        let url = extract_optional_string(&payload, &["url", "NavigatedToUrl", "navigatedtourl"])
            .unwrap_or_default();
        let title = extract_optional_string(&payload, &["title", "PageTitle", "pagetitle"])
            .unwrap_or_default();
        let material_bytes = build_material_bytes(&payload)?;

        visits.push(BrowserVisitRecord {
            browser: browser.clone(),
            title,
            url: url.clone(),
            normalized_url: normalize_url(&url),
            visit_time: timestamp,
            referrer: extract_optional_string(
                &payload,
                &["referrer", "external_referrer_url", "referring_url"],
            ),
            transition: extract_optional_string(&payload, &["transition"]),
            visit_id: extract_optional_string(&payload, &["visitId", "visit_id", "id"]),
            visit_duration_ms: extract_optional_u64(
                &payload,
                &["visit_duration_ms", "visit_duration", "visit_duration_us"],
            )
            .map(normalize_duration_ms),
            source_file: file.path.to_string(),
            line_number: Some((index + 2) as u64),
            db_row_id: None,
            material_bytes,
        });
    }

    Ok(ParsedDumpFile {
        visits,
        stats: ParsedDumpStats {
            delta_line_count: line_count,
        },
    })
}

fn parse_json_lines(
    content: &str,
    file: &DiscoveredFile,
    browser_override: Option<&str>,
    starting_line_number: u64,
) -> NodeResult<ParsedDumpFile> {
    let mut visits = Vec::new();
    let mut line_count = 0u64;

    for line in content.lines() {
        let line = line.trim();
        if line.is_empty() {
            continue;
        }
        line_count += 1;
        let payload: Value = match serde_json::from_str(line) {
            Ok(payload) => payload,
            Err(_) => continue,
        };
        let Some(payload) = payload.as_object() else {
            continue;
        };
        if let Some(visit) = build_visit_from_payload(
            payload,
            file,
            browser_override,
            Some(starting_line_number + line_count),
        )? {
            visits.push(visit);
        }
    }

    Ok(ParsedDumpFile {
        visits,
        stats: ParsedDumpStats {
            delta_line_count: line_count,
        },
    })
}

fn build_visit_from_payload(
    payload: &Map<String, Value>,
    file: &DiscoveredFile,
    browser_override: Option<&str>,
    line_number: Option<u64>,
) -> NodeResult<Option<BrowserVisitRecord>> {
    let Some(timestamp) = payload_timestamp(payload) else {
        return Ok(None);
    };

    let browser = infer_browser_from_path(&file.path, browser_override);
    let url = extract_optional_string(payload, &["url"]).unwrap_or_default();
    let title = extract_optional_string(payload, &["title"]).unwrap_or_default();
    let material_bytes = build_material_bytes(payload)?;

    Ok(Some(BrowserVisitRecord {
        browser,
        title,
        url: url.clone(),
        normalized_url: normalize_url(&url),
        visit_time: timestamp,
        referrer: extract_optional_string(
            payload,
            &["referrer", "external_referrer_url", "referring_url"],
        ),
        transition: extract_optional_string(payload, &["transition"]),
        visit_id: extract_optional_string(payload, &["visitId", "visit_id", "id"]),
        visit_duration_ms: extract_optional_u64(
            payload,
            &["visit_duration_ms", "visit_duration", "visit_duration_us"],
        )
        .map(normalize_duration_ms),
        source_file: file.path.to_string(),
        line_number,
        db_row_id: None,
        material_bytes,
    }))
}

fn normalize_duration_ms(raw: u64) -> u64 {
    if raw >= 1_000_000 { raw / 1_000 } else { raw }
}

fn parse_csv_timestamp(payload: &Map<String, Value>) -> Option<sinex_primitives::Timestamp> {
    extract_optional_string(payload, &["DateTime", "datetime"])
        .and_then(|value| parse_history_timestamp_str(&value))
        .or_else(|| {
            let date = extract_optional_string(payload, &["date"])?;
            let time = extract_optional_string(payload, &["time"])?;
            parse_history_timestamp_str(&format!("{date} {time}"))
        })
}

fn extension(path: &str) -> Option<&str> {
    std::path::Path::new(path)
        .extension()
        .and_then(std::ffi::OsStr::to_str)
}

#[cfg(test)]
mod tests {
    use super::*;
    use camino::Utf8PathBuf;
    use sinex_node_sdk::ImportedFileFingerprint;

    fn test_file(path: &str, change_kind: ImportFileChangeKind) -> DiscoveredFile {
        DiscoveredFile {
            path: Utf8PathBuf::from(path),
            filename: std::path::Path::new(path)
                .file_name()
                .unwrap()
                .to_string_lossy()
                .to_string(),
            fingerprint: ImportedFileFingerprint {
                size_bytes: 0,
                modified_unix_ms: None,
            },
            start_offset_bytes: 0,
            start_line_number: 0,
            change_kind,
        }
    }

    #[test]
    fn parse_json_array_dump() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("chrome_history.json");
        std::fs::write(
            &path,
            r#"[{"visitId":"42","url":"https://example.com?a=1&utm_source=x","title":"Example","visitTime":1759527495463.789,"transition":"link"}]"#,
        )
        .unwrap();

        let file = test_file(path.to_str().unwrap(), ImportFileChangeKind::New);
        let parsed = parse_json_dump(&file, None).unwrap();
        assert_eq!(parsed.visits.len(), 1);
        assert_eq!(parsed.visits[0].browser, "chrome");
        assert_eq!(
            parsed.visits[0].normalized_url.as_deref(),
            Some("https://example.com/?a=1")
        );
    }

    #[test]
    fn parse_csv_dump_handles_edge_datetime() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("edge_history.csv");
        std::fs::write(
            &path,
            "DateTime,NavigatedToUrl,PageTitle\n2023-11-23T16:52:21.618Z,https://example.com,Example\n",
        )
        .unwrap();

        let file = test_file(path.to_str().unwrap(), ImportFileChangeKind::New);
        let parsed = parse_csv_dump(&file, None).unwrap();
        assert_eq!(parsed.visits.len(), 1);
        assert_eq!(parsed.visits[0].browser, "edge");
        assert_eq!(parsed.visits[0].line_number, Some(2));
    }

    #[test]
    fn parse_append_only_ndjson_starts_at_prior_line_count() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("full_history.ndjson");
        std::fs::write(
            &path,
            "{\"url\":\"https://example.com\",\"title\":\"Example\",\"iso_time\":\"2026-03-17T03:10:25.870548+00:00\"}\n",
        )
        .unwrap();

        let mut file = test_file(path.to_str().unwrap(), ImportFileChangeKind::Appended);
        file.start_line_number = 12;
        let parsed = parse_append_only_dump(&file, None).unwrap();
        assert_eq!(parsed.visits[0].line_number, Some(13));
    }
}
