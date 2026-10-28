use super::*;
use sinex_model::{Class, MaterialId, Origin, ReadingOrder, RecordLocation};

fn at(seconds: i64) -> Timestamp {
    Timestamp::from_unix_secs(1_700_000_000 + seconds).unwrap()
}
fn record(kind: &str, payload: Value) -> Event {
    (sinex_model::event_types::find(kind).unwrap().build)(
        &payload,
        "synthetic.attribution",
        Origin::Record {
            location: RecordLocation {
                material: MaterialId::new(),
                anchor: 0,
            },
            output_index: 0,
        },
        ReadingOrder(0),
        [0; 32],
        Class::Open,
    )
    .unwrap()
}
fn act(cwd: &str, repo: Option<&str>) -> Event {
    let seed = Event::submitted(
        sinex_model::KindRef {
            name: "synthetic.act".into(),
            version: 1,
        },
        json!({}),
        "synthetic".into(),
        Origin::Record {
            location: RecordLocation {
                material: MaterialId::new(),
                anchor: 0,
            },
            output_index: 0,
        },
        ReadingOrder(0),
        [0; 32],
        Class::Open,
    );
    record(
        "work.act",
        json!({"act":"act","session":"session","episode":null,
        "objective":repo.map(|repo| json!({"kind":"task","reference":"task-1","repository":repo})),
        "label":null,"label_source":evidence(&seed),"machine":"box","at":at(0),"ended_at":null,
        "evidence":evidence(&seed),"agency":"operator","action":"prompt","path":null,"failed":false,"cwd":cwd}),
    )
}
fn span(id: &str, scope: &str, acts: &[Event], start: i64, end: i64) -> Event {
    record(
        "activity.span",
        json!({"span_id":id,"scope":scope,"day":"2023-11-14",
        "partition_start":at(0),"partition_end":at(3600),"started_at":at(start),"ended_at":at(end),
        "window":null,"agency":"operator","delegated":false,
        "agency_evidence":acts.iter().map(evidence).collect::<Vec<_>>(),"presence":"present",
        "presence_method":"physical","presence_claims":[],"focus":[],"apps":[],"titles":[],
        "display":"unknown","playback":false,"input_records":0,"operator_present_s":end-start,
        "profile":"test","continuation_before":false,"continuation_after":false,"provisional":false}),
    )
}
fn compute(events: &[Event], projects: Value) -> Vec<ProjectSpan> {
    let env = Env::at(at(3600));
    ProjectSpans(AttributionProfile {
        projects: projects.as_array().unwrap().clone(),
    })
    .compute(&Value::Null, events, &env)
    .outputs
    .iter()
    .map(|event| event.payload_as::<ProjectSpan>().unwrap())
    .collect()
}
#[devkit::test]
fn exact_roots_match_components_and_the_recorded_effective_period() {
    let projects =
        json!([{"project":"app","roots":[{"path":"/work/app","from":at(-10),"until":at(10)}]}]);
    for (path, expected) in [
        ("/work/app/src", true),
        ("/work/apple", false),
        ("/work/app/../other", false),
    ] {
        let act = act(path, None);
        let span = span("span", "box", std::slice::from_ref(&act), 0, 10);
        let output = compute(&[act, span], projects.clone());
        assert_eq!(!output[0].shares.is_empty(), expected, "{path}");
    }
    let act = act("/work/app", None);
    let span = span("span", "box", std::slice::from_ref(&act), 0, 10);
    let output = compute(
        &[act, span],
        json!([{"project":"app","roots":[{"path":"/work/app","from":at(1)}]}]),
    );
    assert_eq!(output[0].reason, "unknown");
}
#[devkit::test]
fn worktrees_join_by_captured_git_identity_without_a_live_lookup() {
    let mut act = act("/retired/worktree", None);
    act.payload["git_identity"] =
        json!({"common_dir":"/old/app/.git","root_commits":["root"],"head":"sha","branch":"work"});
    let span = span("span", "box", std::slice::from_ref(&act), 0, 600);
    let output = compute(
        &[act, span],
        json!([{"project":"app","common_dirs":["/old/app/.git"]}]),
    );
    assert_eq!(output[0].shares[0].project, "app");
    assert_eq!(output[0].ladder_rung, Some(3));
}
#[devkit::test]
fn competing_exact_owners_leave_time_unallocated_and_keep_evidence() {
    let first = act("/a", Some("org/a"));
    let second = act("/b", Some("org/b"));
    let span = span("span", "box", &[first.clone(), second.clone()], 0, 600);
    let output = compute(
        &[first, second, span],
        json!([
        {"project":"a","repositories":["org/a"]},{"project":"b","repositories":["org/b"]}]),
    );
    assert_eq!(output[0].reason, "conflicting");
    assert!(output[0].shares.is_empty());
    assert_eq!(output[0].unallocated_numerator, output[0].denominator);
    assert_eq!(output[0].conflicting_projects, ["a", "b"]);
}
#[devkit::test]
fn three_observers_of_ten_minutes_count_ten_operator_minutes() {
    let act = act("/a", Some("org/a"));
    let mut events: Vec<_> = ["box", "phone", "tablet"]
        .into_iter()
        .map(|scope| span(scope, scope, std::slice::from_ref(&act), 0, 600))
        .collect();
    events.push(act);
    let output = compute(&events, json!([{"project":"a","repositories":["org/a"]}]));
    assert_eq!(
        output
            .iter()
            .map(|span| span.supported_seconds)
            .sum::<f64>(),
        600.0
    );
    for span in output {
        assert_eq!(
            span.shares.iter().map(|share| share.numerator).sum::<u64>()
                + span.unallocated_numerator,
            span.denominator
        );
    }
}
#[devkit::test]
fn a_background_agent_at_the_same_time_never_takes_the_focused_span() {
    let act = act("/a", Some("org/a"));
    let span = span("span", "box", &[], 0, 600);
    let output = compute(
        &[act, span],
        json!([{"project":"a","repositories":["org/a"]}]),
    );
    assert_eq!(output[0].reason, "unknown");
}

#[devkit::test]
fn cited_delegated_context_does_not_establish_operator_ownership() {
    let mut act = act("/a",Some("org/a")); act.payload["agency"] = json!("delegated");
    let span = span("span","box",std::slice::from_ref(&act),0,600);
    let output = compute(&[act,span],json!([{"project":"a","repositories":["org/a"]}]));
    assert_eq!(output[0].reason,"unknown"); assert!(output[0].shares.is_empty());
}

#[devkit::test]
fn a_malformed_effective_until_does_not_turn_a_historical_root_into_a_current_one() {
    let act = act("/a",None); let span = span("span","box",std::slice::from_ref(&act),0,600);
    let output = compute(&[act,span],json!([{"project":"a","roots":[{"path":"/a","from":at(-1),"until":"invalid"}]}]));
    assert_eq!(output[0].reason,"unknown");
}
