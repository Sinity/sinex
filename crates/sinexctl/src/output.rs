//! Printing results for a terminal: answers as aligned tables with local
//! times and a footer, everything else as indented JSON. `--json` prints the
//! result as it came.
use serde_json::Value;
use std::fmt::Write;

/// Widest a cell is shown; longer values end in `…`. Presentation only: the
/// row is whole in `--json`.
const CELL: usize = 60;

pub struct Local {
    zone: jiff::tz::TimeZone,
}

impl Local {
    pub fn new(zone_name: Option<&str>) -> Self {
        let zone = zone_name
            .and_then(|z| jiff::tz::TimeZone::get(z).ok())
            .unwrap_or_else(jiff::tz::TimeZone::system);
        Self { zone }
    }

    /// A UTC time as local `YYYY-MM-DD HH:MM:SS`; other text unchanged.
    fn time(&self, s: &str) -> Option<String> {
        if s.len() < 20 || !s.as_bytes()[..4].iter().all(u8::is_ascii_digit) {
            return None;
        }
        let ts: jiff::Timestamp = s.parse().ok()?;
        Some(
            ts.to_zoned(self.zone.clone())
                .strftime("%Y-%m-%d %H:%M:%S")
                .to_string(),
        )
    }
}

fn cell(local: &Local, v: &Value) -> String {
    let text = match v {
        Value::Null => "—".to_string(),
        Value::String(s) => local.time(s).unwrap_or_else(|| s.clone()),
        Value::Number(n) => n.to_string(),
        Value::Bool(b) => b.to_string(),
        other => other.to_string(),
    };
    let flat: String = text
        .chars()
        .map(|c| if c.is_control() { ' ' } else { c })
        .collect();
    if flat.chars().count() > CELL {
        let mut cut: String = flat.chars().take(CELL - 1).collect();
        cut.push('…');
        cut
    } else {
        flat
    }
}

/// Rows as an aligned table.
pub fn table(columns: &[String], rows: &[Value], local: &Local) -> String {
    let cells: Vec<Vec<String>> = rows
        .iter()
        .map(|r| columns.iter().map(|c| cell(local, &r[c])).collect())
        .collect();
    let widths: Vec<usize> = columns
        .iter()
        .enumerate()
        .map(|(i, c)| {
            cells
                .iter()
                .map(|r| r[i].chars().count())
                .chain([c.chars().count()])
                .max()
                .unwrap_or(0)
        })
        .collect();
    let mut out = String::new();
    let line = |out: &mut String, items: &[String]| {
        let padded: Vec<String> = items
            .iter()
            .zip(&widths)
            .map(|(s, w)| format!("{s:<w$}"))
            .collect();
        let _ = writeln!(out, "{}", padded.join("  ").trim_end());
    };
    line(&mut out, columns);
    for r in &cells {
        line(&mut out, r);
    }
    out
}

/// An answer (spec `query.md` §10): its reading, the rows, and a footer with
/// counts and the next cursor.
pub fn answer(result: &Value, local: &Local, next_hint: Option<&str>) -> String {
    let mut out = String::new();
    if let Some(r) = result["read_as"].as_str() {
        let _ = writeln!(out, "# {r}");
    }
    let rows = result["rows"].as_array().cloned().unwrap_or_default();
    let columns: Vec<String> = match result["columns"].as_array() {
        Some(c) if !c.is_empty() => c
            .iter()
            .filter_map(|v| v.as_str().map(str::to_string))
            .collect(),
        _ => rows
            .first()
            .and_then(Value::as_object)
            .map(|o| o.keys().cloned().collect())
            .unwrap_or_default(),
    };
    if !rows.is_empty() {
        out.push_str(&table(&columns, &rows, local));
    }
    if let Some(recall) = result.get("recall").filter(|r| !r.is_null()) {
        for s in recall["by_source"].as_array().into_iter().flatten() {
            let _ = writeln!(
                out,
                "  {}: {} events",
                s["source"].as_str().unwrap_or("?"),
                s["events"]
            );
        }
        let none: Vec<&str> = recall["no_data"]
            .as_array()
            .into_iter()
            .flatten()
            .filter_map(Value::as_str)
            .collect();
        if !none.is_empty() {
            let _ = writeln!(out, "  no data in this window: {}", none.join(", "));
        }
    }
    let total = result["total"].as_i64().unwrap_or(rows.len() as i64);
    let zone = result["resolved"]["zone"].as_str().unwrap_or("UTC");
    let _ = write!(out, "{} of {total} rows; times in {zone}", rows.len());
    match (result["page"]["cursor"].as_str(), next_hint) {
        (Some(c), Some(hint)) => {
            let _ = writeln!(out, "; next page: {hint} --cursor {c}");
        }
        (Some(c), None) => {
            let _ = writeln!(out, "; next cursor {c}");
        }
        _ => out.push('\n'),
    }
    out
}

#[cfg(test)]
mod tests;
