//! The one presentation compiler: a brief, a context pack and a "what did I do"
//! answer are sections of findings compiled against one evaluation context.
//! Within a byte budget whole findings are dropped, never parts of one, and
//! each section says how many it dropped; a section whose sources had no
//! coverage is shown absent with the reason.
use crate::finding::Finding;
use serde::{Deserialize, Serialize};
use std::fmt::Write as _;

/// One section to compile.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Section {
    pub title: String,
    pub findings: Vec<Finding>,
    /// Set when the section has nothing to say because its sources could not
    /// observe the window; shown instead of omitted.
    pub absent: Option<String>,
}

/// What every section of one presentation shares.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Context {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub citation: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub producer_versions: Option<serde_json::Value>,
    pub title: String,
    /// The evaluation instant, RFC 3339.
    pub evaluated_at: String,
    /// The commit position every section was read at.
    pub as_of: i64,
    pub day_setting: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct RenderedSection {
    pub title: String,
    pub lines: Vec<String>,
    pub shown: usize,
    pub dropped: usize,
    pub absent: Option<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Presentation {
    pub context: Context,
    pub sections: Vec<RenderedSection>,
    pub markdown: String,
}

/// Compile sections into a presentation of at most `budget` bytes of
/// findings. Findings rank observed before partly observed before unknown,
/// then by effect size; ties keep their given order.
pub fn compile(ctx: Context, sections: Vec<Section>, budget: usize) -> Presentation {
    let mut used = 0usize;
    let mut sections = sections;
    for section in &mut sections {
        section.findings.sort_by_key(Finding::rank);
    }
    let mut rendered: Vec<_> = sections
        .iter()
        .map(|section| RenderedSection {
            title: section.title.clone(),
            shown: 0,
            lines: Vec::new(),
            dropped: 0,
            absent: section.absent.clone(),
        })
        .collect();
    // Give each section its highest-ranked finding before spending the
    // remaining budget on another finding from the same section.
    let rounds = sections
        .iter()
        .map(|section| section.findings.len())
        .max()
        .unwrap_or(0);
    for round in 0..rounds {
        for (section, output) in sections.iter().zip(&mut rendered) {
            let Some(finding) = section.findings.get(round) else {
                continue;
            };
            let mut line = format!("- {}", finding.markdown());
            if let Some(citation) = &ctx.citation {
                let _ = write!(line, " ({citation})");
            }
            if used + line.len() + 1 > budget {
                output.dropped += 1;
                continue;
            }
            used += line.len() + 1;
            output.lines.push(line);
            output.shown += 1;
        }
    }
    let mut md = format!(
        "# {}\n\nAs of commit {} ({}), days {}.\n",
        ctx.title, ctx.as_of, ctx.evaluated_at, ctx.day_setting
    );
    if let Some(versions) = &ctx.producer_versions {
        let mut by_producer =
            std::collections::BTreeMap::<&str, std::collections::BTreeSet<String>>::new();
        let mut by_parser =
            std::collections::BTreeMap::<&str, std::collections::BTreeSet<i64>>::new();
        for row in versions.as_array().into_iter().flatten() {
            if let (Some(parser), Some(version)) =
                (row["parser"].as_str(), row["parser_version"].as_i64())
            {
                by_parser.entry(parser).or_default().insert(version);
            }
            if let Some(producer) = row["producer"].as_str() {
                by_producer.entry(producer).or_default().insert(format!(
                    "{} ({})",
                    row["version"].as_str().unwrap_or_default(),
                    row["build"].as_str().unwrap_or_default()
                ));
            }
        }
        let mixed: Vec<_> = by_producer
            .into_iter()
            .filter(|(_, versions)| versions.len() > 1)
            .map(|(producer, versions)| {
                format!(
                    "{producer}: {}",
                    versions.into_iter().collect::<Vec<_>>().join(", ")
                )
            })
            .collect();
        if !mixed.is_empty() {
            let _ = writeln!(
                md,
                "\nIncludes several producer versions: {}.",
                mixed.join("; ")
            );
        }
        let mixed_parsers: Vec<_> = by_parser
            .into_iter()
            .filter(|(_, versions)| versions.len() > 1)
            .map(|(parser, versions)| {
                format!(
                    "{parser}: {}",
                    versions
                        .into_iter()
                        .map(|version| version.to_string())
                        .collect::<Vec<_>>()
                        .join(", ")
                )
            })
            .collect();
        if !mixed_parsers.is_empty() {
            let _ = writeln!(
                md,
                "\nIncludes several parser versions: {}.",
                mixed_parsers.join("; ")
            );
        }
    }
    for s in &rendered {
        let _ = write!(md, "\n## {}\n\n", s.title);
        if let Some(reason) = &s.absent {
            let _ = writeln!(md, "Absent: {reason}.");
            continue;
        }
        if s.lines.is_empty() && s.dropped == 0 {
            md.push_str("Nothing to report.\n");
        }
        for l in &s.lines {
            md.push_str(l);
            md.push('\n');
        }
        if s.dropped > 0 {
            let _ = writeln!(
                md,
                "({} more finding(s) left out to fit the budget.)",
                s.dropped
            );
        }
    }
    Presentation {
        context: ctx,
        sections: rendered,
        markdown: md,
    }
}

#[cfg(test)]
mod tests;
