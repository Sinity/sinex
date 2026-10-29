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
        citation: None,
        producer_versions: None,
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

#[devkit::test]
fn presentations_disclose_mixed_producer_versions_from_the_stored_context() {
    let mut context = ctx();
    context.producer_versions = Some(serde_json::json!([
        {"producer":"synthetic.source","version":"1","build":"a","events":2},
        {"producer":"synthetic.source","version":"2","build":"b","events":1}
    ]));
    let presentation = compile(context, vec![], 100);
    assert!(presentation.markdown.contains("Includes several producer versions: synthetic.source: 1 (a), 2 (b)."));
}

#[devkit::test]
fn a_large_quality_section_does_not_consume_the_whole_presentation() {
    let quality = Section { title: "Data quality".into(), findings: vec![finding(1.0, false); 20], absent: None };
    let activity = Section { title: "The day".into(), findings: vec![finding(2.0, false)], absent: None };
    let mut context = ctx();
    context.citation = Some("sinex:ev/synthetic-pack".into());
    let presentation = compile(context, vec![quality, activity], 100);
    assert_eq!(presentation.sections[1].shown, 1);
    assert!(presentation.sections[1].lines[0].contains("sinex:ev/synthetic-pack"));
    assert!(presentation.sections[0].dropped > 0);
}

#[devkit::test]
fn mixed_material_parser_versions_are_disclosed_when_run_metadata_is_missing() {
    let mut context=ctx();context.producer_versions=Some(serde_json::json!([
        {"producer":"synthetic","version":null,"build":null,"parser":"synthetic-parser","parser_version":1},
        {"producer":"synthetic","version":null,"build":null,"parser":"synthetic-parser","parser_version":2}
    ]));
    let presentation=compile(context,vec![],100);
    assert!(presentation.markdown.contains("Includes several parser versions: synthetic-parser: 1, 2."));
}
