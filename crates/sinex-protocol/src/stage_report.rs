//! A stage's report as text: the operation, each staged file with how it is
//! read, its products' accounting, and the members no parser reads. The
//! daemon's answer and `sinexd stage` print the same.
use serde_json::Value;

/// A stage report as text: the operation, each file with how it is read
/// and its products' accounting, and the members no parser reads.
pub fn render(report: &Value) -> String {
    use std::fmt::Write;
    let mut out = String::new();
    let _ = writeln!(
        out,
        "stage {}  {}",
        report["operation"].as_str().unwrap_or_default(),
        report["result"].as_str().unwrap_or("not ended")
    );
    for file in report["inventory"]["files"]
        .as_array()
        .into_iter()
        .flatten()
    {
        let members = file["members"].as_array().map_or(0, Vec::len);
        let reading = match file["reading"]["as"].as_str() {
            Some("parsed") => format!(
                "read by {}",
                file["reading"]["formats"]
                    .as_array()
                    .into_iter()
                    .flatten()
                    .filter_map(Value::as_str)
                    .collect::<Vec<_>>()
                    .join(", ")
            ),
            _ => {
                let candidates: Vec<&str> = file["reading"]["candidates"]
                    .as_array()
                    .into_iter()
                    .flatten()
                    .filter_map(Value::as_str)
                    .collect();
                if candidates.is_empty() {
                    "stored, not interpreted (no parser)".into()
                } else {
                    format!(
                        "stored, not interpreted (candidates: {})",
                        candidates.join(", ")
                    )
                }
            }
        };
        let _ = writeln!(
            out,
            "{}  {members} member(s)  {reading}",
            file["path"].as_str().unwrap_or_default()
        );
        for (source, product) in file["products"].as_object().into_iter().flatten() {
            let a = &product["accounting"];
            let n = |key: &str| a[key].as_u64().unwrap_or(0);
            let _ = write!(
                out,
                "  {source}  {}  records {} (read {}, headers {}, failures {})  readings {}: new {}, same {}, revised {}, older {}, superseded {}",
                product["state"].as_str().unwrap_or_default(),
                n("framed"),
                n("read"),
                n("headers"),
                n("failures"),
                n("readings"),
                n("new"),
                n("same"),
                n("revised"),
                n("older"),
                n("superseded"),
            );
            if n("removed") > 0 {
                let _ = write!(out, ", removed at source {}", n("removed"));
            }
            if let Some(first) = a["first_failure"].as_str() {
                let _ = write!(out, "; first unreadable: {first}");
            }
            if let Some(error) = product["error"].as_str() {
                let _ = write!(out, "; {error}");
            }
            out.push('\n');
        }
        let unread: Vec<&str> = file["members"]
            .as_array()
            .into_iter()
            .flatten()
            .filter(|m| m["disposition"] == "stored_not_interpreted")
            .filter_map(|m| m["name"].as_str())
            .collect();
        if !unread.is_empty() && file["reading"]["as"] == "parsed" {
            let _ = writeln!(
                out,
                "  stored, not interpreted: {} member(s): {}",
                unread.len(),
                unread.join(", ")
            );
        }
    }
    out
}

#[cfg(test)]
mod tests;
