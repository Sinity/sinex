use super::*;
use crate::finding::{Caveat, Template};

fn finding(value: f64, caveat: bool) -> Finding {
    Finding {
        template: Template::MetricValue {
            metric: "m".into(),
            unit: "d".into(),
            value,
            state: "observed".into(),
            description: "Thing".into(),
        },
        caveats: if caveat {
            vec![Caveat::LowerBound]
        } else {
            vec![]
        },
        evidence: vec![],
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

#[devkit::test]
fn the_budget_drops_whole_findings_and_says_so() {
    let sections = vec![Section {
        title: "S".into(),
        findings: vec![finding(1.0, true), finding(2.0, false)],
        absent: None,
    }];
    let p = compile(ctx(), sections, 40);
    // The observed finding ranks first; the lower bound does not fit
    // and is dropped whole, caveat included.
    assert_eq!(p.sections[0].lines, ["- Thing (m) on d: 2"]);
    assert_eq!(p.sections[0].dropped, 1);
    assert!(
        p.markdown
            .contains("(1 more finding(s) left out to fit the budget.)")
    );
    assert!(!p.markdown.contains("Thing (m) on d: 1"));
}

#[devkit::test]
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
