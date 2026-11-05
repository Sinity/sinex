//! The typed stage report as text, shared by the CLI and daemon output.
pub use sinex_model::stage::{Report, Reports};
use sinex_model::{
    lifecycle::OperationResult,
    stage::{Disposition, FileReading, ProductState},
};

pub fn render(report: &Report) -> String {
    use std::fmt::Write;
    let mut out = String::new();
    let result = match report.result {
        Some(OperationResult::Ok) => "ok",
        Some(OperationResult::StoppedEarly) => "stopped_early",
        None => "not ended",
    };
    let _ = writeln!(out, "stage {}  {result}", report.operation);
    for file in &report.inventory.files {
        let members = file.members.len();
        let reading = match &file.reading {
            FileReading::Parsed { formats } => format!("read by {}", formats.join(", ")),
            FileReading::Unparsed { candidates } if candidates.is_empty() => {
                "stored, not interpreted (no parser)".into()
            }
            FileReading::Unparsed { candidates } => format!(
                "stored, not interpreted (candidates: {})",
                candidates.join(", ")
            ),
        };
        let _ = writeln!(
            out,
            "{}  {members} member(s)  {reading}",
            file.path.display()
        );
        for (source, product) in &file.products {
            let state = match product.state {
                ProductState::Queued => "queued",
                ProductState::Read => "read",
                ProductState::Failed => "failed",
            };
            let a = &product.accounting;
            let _ = write!(
                out,
                "  {source}  {state}  records {} (read {}, headers {}, failures {})  readings {}: new {}, same {}, revised {}, older {}, superseded {}",
                a.framed,
                a.read,
                a.headers,
                a.failures,
                a.readings,
                a.new,
                a.same,
                a.revised,
                a.older,
                a.superseded
            );
            if a.removed > 0 {
                let _ = write!(out, ", removed at source {}", a.removed);
            }
            if let Some(first) = &a.first_failure {
                let _ = write!(out, "; first unreadable: {first}");
            }
            if let Some(error) = &product.error {
                let _ = write!(out, "; {error}");
            }
            out.push('\n');
        }
        let unread: Vec<_> = file
            .members
            .iter()
            .filter(|m| matches!(m.disposition, Disposition::StoredNotInterpreted { .. }))
            .map(|m| m.name.as_str())
            .collect();
        if !unread.is_empty() && matches!(file.reading, FileReading::Parsed { .. }) {
            let _ = writeln!(
                out,
                "  stored, not interpreted: {} member(s): {}",
                unread.len(),
                unread.join(", ")
            );
        }
    }
    for failure in &report.failures {
        let _ = writeln!(out, "failed: {failure}");
    }
    out
}

#[cfg(test)]
mod tests;
