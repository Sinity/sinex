//! The one presentation compiler (spec `analysis.md` §12): a brief, a context
//! pack and a "what did I do" answer are sections of findings compiled
//! against one evaluation context. Within a byte budget whole findings are
//! dropped, never parts of one, and each section says how many it dropped;
//! a section whose sources had no opportunity is shown absent with the reason.
use crate::finding::Finding;
use serde::Serialize;
use std::fmt::Write as _;

/// One section to compile.
#[derive(Clone, Debug, Serialize)]
pub struct Section {
    pub title: String,
    pub findings: Vec<Finding>,
    /// Set when the section has nothing to say because its sources could not
    /// observe the window; shown instead of omitted.
    pub absent: Option<String>,
}

/// What every section of one presentation shares.
#[derive(Clone, Debug, Serialize)]
pub struct Context {
    pub title: String,
    /// The evaluation instant, RFC 3339.
    pub evaluated_at: String,
    /// The commit position every section was read at.
    pub as_of: i64,
    pub day_setting: String,
}

#[derive(Clone, Debug, Serialize)]
pub struct RenderedSection {
    pub title: String,
    pub lines: Vec<String>,
    pub shown: usize,
    pub dropped: usize,
    pub absent: Option<String>,
}

#[derive(Clone, Debug, Serialize)]
pub struct Presentation {
    pub context: Context,
    pub sections: Vec<RenderedSection>,
    pub markdown: String,
}

/// Compile sections into a presentation of at most `budget` bytes of
/// findings. Findings rank by support status, then effect size; ties keep
/// their given order.
pub fn compile(ctx: Context, sections: Vec<Section>, budget: usize) -> Presentation {
    let mut used = 0usize;
    let mut rendered = Vec::with_capacity(sections.len());
    for mut s in sections {
        s.findings.sort_by_key(Finding::rank);
        let mut lines = Vec::new();
        let mut dropped = 0;
        for f in &s.findings {
            let line = format!("- {}", f.markdown());
            if used + line.len() + 1 > budget {
                dropped += 1;
                continue;
            }
            used += line.len() + 1;
            lines.push(line);
        }
        rendered.push(RenderedSection {
            title: s.title,
            shown: lines.len(),
            lines,
            dropped,
            absent: s.absent,
        });
    }
    let mut md = format!(
        "# {}\n\nAs of commit {} ({}), days {}.\n",
        ctx.title, ctx.as_of, ctx.evaluated_at, ctx.day_setting
    );
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
mod tests {
    use super::*;
    use crate::finding::{Caveat, Check, Checks, Role, Support, Template};

    fn finding(value: f64, caveat: bool) -> Finding {
        Finding {
            template: Template::MetricValue {
                metric: "m".into(),
                unit: "d".into(),
                value,
                state: "observed".into(),
                description: "Thing".into(),
            },
            role: Role::MeasuredFact,
            status: if caveat {
                Support::PartiallySupported
            } else {
                Support::Supported
            },
            caveats: if caveat {
                vec![Caveat::Provisional]
            } else {
                vec![]
            },
            evidence: vec![],
            checks: Checks {
                references: Check::Passed,
                calculation: Check::Passed,
                coverage: Check::Passed,
                semantics: Check::ByConstruction,
            },
        }
    }

    fn ctx() -> Context {
        Context {
            title: "Brief".into(),
            evaluated_at: "now".into(),
            as_of: 1,
            day_setting: "04:00".into(),
        }
    }

    #[test]
    fn the_budget_drops_whole_findings_and_says_so() {
        let sections = vec![Section {
            title: "S".into(),
            findings: vec![finding(1.0, true), finding(2.0, false)],
            absent: None,
        }];
        let p = compile(ctx(), sections, 40);
        // The supported finding ranks first; the qualified one does not fit
        // and is dropped whole, caveat included.
        assert_eq!(p.sections[0].lines, ["- Thing (m) on d: 2"]);
        assert_eq!(p.sections[0].dropped, 1);
        assert!(
            p.markdown
                .contains("(1 more finding(s) left out to fit the budget.)")
        );
        assert!(!p.markdown.contains("Thing (m) on d: 1"));
    }

    #[test]
    fn absent_sections_are_shown_with_their_reason() {
        let p = compile(
            ctx(),
            vec![Section {
                title: "Rest".into(),
                findings: vec![],
                absent: Some("no rest source observed this window".into()),
            }],
            1000,
        );
        assert!(
            p.markdown
                .contains("## Rest\n\nAbsent: no rest source observed this window.")
        );
    }
}
