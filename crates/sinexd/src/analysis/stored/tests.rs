use super::*;
use sinex_model::event_types::{
    MetricFigure,
    coverage::{CoverageCompleteness, CoverageState},
};
use sinex_model::{
    Class, EventPayload, MaterialId, Origin, ReadingOrder, RecordLocation, Timestamp,
};

fn captured<P: EventPayload>(payload: &P) -> Event {
    Event::build(
        payload,
        "synthetic",
        Origin::Record {
            location: RecordLocation {
                material: MaterialId::new(),
                anchor: 0,
            },
            output_index: 0,
        },
        ReadingOrder(1),
        [1; 32],
        Class::Open,
    )
    .unwrap()
}
fn question() -> AnalysisRequest {
    let settings = TimeSettings::new("Europe/Warsaw", 4, 0).expect("fixture day setting");
    let day = super::super::parse_day("2026-09-14").unwrap();
    let start = settings.day_start(day).unwrap();
    let end = settings.day_end(day).unwrap();
    AnalysisRequest {
        selection: None,
        request: "synthetic-question".into(),
        operation: "recipe".into(),
        recipe: Some("trends".into()),
        from: day.to_string(),
        to: day.to_string(),
        metrics: vec!["git.commits".into()],
        min_observed_fraction: None,
        zone: "Europe/Warsaw".into(),
        day_start: "04:00:00".into(),
        grants: vec![],
        started_at: start,
        ended_at: end,
        by: "test".into(),
        requested_at: end,
    }
}
fn figure(request: &AnalysisRequest) -> Event {
    captured(&MetricFigure {
        metric: "git.commits".into(),
        grain: "day".into(),
        period_start: request.started_at,
        period_end: request.ended_at,
        day: request.to.clone(),
        dimension: String::new(),
        dimension_value: String::new(),
        value: Some(2.0),
        numerator: None,
        denominator: None,
        rows: 2,
    })
}
fn observation(request: &AnalysisRequest, name: &str, state: CoverageState) -> Event {
    captured(&SourceCoverage {
        observed_run: None,
        observed_class: None,
        observation: name.into(),
        observed_source: "code.git".into(),
        binding: "synthetic".into(),
        host: "synthetic".into(),
        started_at: request.started_at,
        ended_at: request.ended_at,
        state,
        reason: None,
        completeness: CoverageCompleteness::Sample,
        bounded_by: "declared synthetic interval".into(),
        events: 2,
        failures: 0,
    })
}

#[devkit::test]
fn stored_analysis_is_order_independent_and_refusals_remain_parented() {
    let request = question();
    let input = vec![
        captured(&request),
        figure(&request),
        observation(&request, "a", CoverageState::Observed),
        observation(&request, "b", CoverageState::RecordedUnknownCoverage),
    ];
    let env = Env::at(request.ended_at, sinex_model::time_settings::TimeSettings::new("Europe/Warsaw", 4, 0).expect("fixture day setting"));
    let first = Keyed::compute(&StoredProducts, &json!(request), &input, &env);
    assert!(first.error.is_none());
    let reversed: Vec<_> = input.iter().rev().cloned().collect();
    let again = Keyed::compute(&StoredProducts, &json!(request), &reversed, &env);
    assert!(again.error.is_none());
    assert_eq!(first.outputs.len(), again.outputs.len());
    for (a, b) in first.outputs.iter().zip(&again.outputs) {
        assert_eq!(a.payload, b.payload);
        assert_eq!(a.record_hash, b.record_hash);
        let Origin::Derived { parents } = &a.origin else {
            panic!("stored analysis must be derived")
        };
        assert!(parents.contains(&input[0].id));
    }
    let answer = first
        .outputs
        .iter()
        .find(|e| e.event_type == "recipe.answer")
        .unwrap();
    assert_eq!(
        answer.payload["tables"]["metrics"][0]["trend"]["status"],
        "not_estimable"
    );
}

#[devkit::test]
fn an_empty_answer_cites_the_request_and_unknown_coverage_never_measures_zero() {
    let request = question();
    let inputs = vec![captured(&request)];
    let output = Keyed::compute(
        &StoredProducts,
        &json!(request),
        &inputs,
        &Env::at(request.ended_at, sinex_model::time_settings::TimeSettings::new("Europe/Warsaw", 4, 0).expect("fixture day setting")),
    );
    assert!(output.error.is_none());
    let panel = output
        .outputs
        .iter()
        .find(|e| e.event_type == "analysis.panel")
        .unwrap();
    assert_eq!(panel.payload["cells"][0]["cell"]["state"], "missing");
    let Origin::Derived { parents } = &panel.origin else {
        panic!("empty panel needs its request parent")
    };
    assert_eq!(parents, &[inputs[0].id]);
    let input = vec![
        inputs[0].clone(),
        figure(&request),
        observation(&request, "unknown", CoverageState::RecordedUnknownCoverage),
    ];
    let output = Keyed::compute(
        &StoredProducts,
        &json!(request),
        &input,
        &Env::at(request.ended_at, sinex_model::time_settings::TimeSettings::new("Europe/Warsaw", 4, 0).expect("fixture day setting")),
    );
    let panel = output
        .outputs
        .iter()
        .find(|e| e.event_type == "analysis.panel")
        .unwrap();
    assert_eq!(panel.payload["cells"][0]["cell"]["state"], "recorded");
    assert_eq!(panel.payload["cells"][0]["cell"]["value"], 2.0);
}

#[devkit::test]
fn figures_cut_under_another_logical_day_are_incomparable_instead_of_zero() {
    let request = question();
    let mut other_period = figure(&request);
    let shifted = Timestamp::from_unix_ns(request.started_at.unix_ns() + 1).unwrap();
    other_period.ts_orig.at = Some(shifted);
    other_period.payload["period_start"] = json!(shifted);
    let inputs = vec![captured(&request), other_period, observation(&request, "observed", CoverageState::Observed)];
    let output = Keyed::compute(&StoredProducts, &json!(request), &inputs, &Env::at(request.ended_at, sinex_model::time_settings::TimeSettings::new("Europe/Warsaw", 4, 0).expect("fixture day setting")));
    assert!(output.error.is_none());
    let panel = output.outputs.iter().find(|event| event.event_type == "analysis.panel").unwrap();
    assert_eq!(panel.payload["cells"][0]["cell"], json!({"state":"missing","reason":"incomparable_period"}));
}

#[devkit::test]
fn a_closed_day_replaces_the_provisional_claim_at_its_actual_evaluation_clock() {
    let mut request = question();
    request.operation="brief".into();request.recipe=None;
    let inputs=vec![captured(&request),figure(&request),observation(&request,"observed",CoverageState::Observed)];
    let opened=Timestamp::from_unix_ns(request.ended_at.unix_ns()-1_000_000_000).unwrap();
    let closed=Timestamp::from_unix_ns(request.ended_at.unix_ns()+1_000_000_000).unwrap();
    let settings=TimeSettings::new("Europe/Warsaw",4,0).unwrap();
    let before=Keyed::compute(&StoredProducts,&json!(request),&inputs,&Env::at(opened,settings.clone()));
    let after=Keyed::compute(&StoredProducts,&json!(request),&inputs,&Env::at(closed,settings));
    let before=before.outputs.iter().find(|event|event.event_type=="context.pack").unwrap();
    let after=after.outputs.iter().find(|event|event.event_type=="context.pack").unwrap();
    assert_eq!(before.payload["evaluated_at"],json!(opened));
    assert_eq!(after.payload["evaluated_at"],json!(closed));
    assert!(before.payload["analysis"]["sections"].to_string().contains("provisional"));
    assert!(!after.payload["analysis"]["sections"].to_string().contains("provisional"));
}

#[devkit::test]
fn a_daily_brief_keeps_the_failed_read_reason_and_its_exact_source_citation() {
    let mut request=question();request.operation="brief".into();request.recipe=None;
    let mut failed=observation(&request,"failed-read",CoverageState::Missing);
    failed.payload["reason"]=json!("synthetic input unavailable");
    failed.payload["failures"]=json!(1);failed.payload["events"]=json!(0);
    let citation=format!("sinex:ev/{}",failed.id);
    let inputs=vec![captured(&request),figure(&request),failed];
    let output=Keyed::compute(&StoredProducts,&json!(request),&inputs,
        &Env::at(request.ended_at,TimeSettings::new("Europe/Warsaw",4,0).unwrap()));
    assert!(output.error.is_none());
    let pack=output.outputs.iter().find(|event|event.event_type=="context.pack").unwrap();
    let quality=&pack.payload["analysis"]["sections"][0]["findings"];
    let failure=quality.as_array().unwrap().iter().find(|finding|finding["template"]["description"].as_str().is_some_and(|text|text.contains("synthetic input unavailable"))).unwrap();
    assert_eq!(failure["evidence"],json!([citation]));
    let panel=output.outputs.iter().find(|event|event.event_type=="analysis.panel").unwrap();
    assert_eq!(panel.payload["cells"][0]["cell"]["state"],"recorded");
}
