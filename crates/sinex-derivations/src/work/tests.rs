use super::*;
use sinex_model::{Class, MaterialId, Origin, ReadingOrder, RecordLocation, Timestamp};

fn at(seconds: i64) -> Timestamp {
    Timestamp::from_unix_secs(1_700_000_000 + seconds).unwrap()
}
fn record(kind: &str, payload: Value, anchor: u64) -> Event {
    (sinex_model::event_types::find(kind).unwrap().build)(
        &payload,
        "synthetic.work",
        Origin::Record {
            location: RecordLocation {
                material: MaterialId::new(),
                anchor,
            },
            output_index: 0,
        },
        ReadingOrder(0),
        [0; 32],
        Class::Open,
    )
    .unwrap()
}
fn session(id: &str, branch: Option<&str>) -> Event {
    record(
        "polylogue.session",
        json!({"session":id,"origin":"test","kind":"main","cwd":["/synthetic/repo"],
        "repo":"synthetic/repo","branch":branch,"started":at(0),"last_activity":at(1_000_000),"links":[]}),
        0,
    )
}
fn prompt(id: &str, at_s: i64) -> Event {
    record(
        "polylogue.message",
        json!({"message":format!("{id}/{at_s}"),"session":id,"role":"user", "written_by":"human",
        "prompt_kind":"prose","at":at(at_s),"text":"Continue the recorded objective", "usage":{},"attachments":[],"copies":[]}),
        at_s as u64,
    )
}
fn compute(inputs: &[Event]) -> Vec<Event> {
    let env = Env::at(at(2_000_000));
    let mut acts = Vec::new();
    for session in inputs
        .iter()
        .filter(|event| event.event_type == "polylogue.session")
    {
        let selected: Vec<_> = inputs
            .iter()
            .filter(|event| event.payload["session"] == session.payload["session"])
            .cloned()
            .collect();
        acts.extend(
            Keyed::compute(
                &WorkActs,
                &json!({"session":session.payload["session"]}),
                &selected,
                &env,
            )
            .outputs,
        );
    }
    let keys: BTreeSet<_> = acts
        .iter()
        .filter_map(|event| event.payload["episode"].as_str().map(str::to_owned))
        .collect();
    let mut outputs = Vec::new();
    for episode in keys {
        let mut result =
            Keyed::compute(&WorkEpisodes, &json!({"episode":episode}), &acts, &env).outputs;
        let episode_event = result
            .iter()
            .find(|event| event.event_type == "work.episode")
            .unwrap()
            .clone();
        let mut selected: Vec<_> = acts
            .iter()
            .filter(|event| event.payload["episode"] == episode)
            .cloned()
            .collect();
        selected.push(episode_event);
        result.extend(
            Keyed::compute(&ContextPacks, &json!({"episode":episode}), &selected, &env).outputs,
        );
        outputs.extend(result);
    }
    outputs
}

#[devkit::test]
fn branch_objectives_survive_days_and_keep_agent_work_separate() {
    let inputs = vec![
        session("a", Some("first")),
        session("b", Some("second")),
        prompt("a", 0),
        prompt("b", 300),
        prompt("a", 5 * 86_400),
    ];
    let output = compute(&inputs);
    let episodes: Vec<_> = output
        .iter()
        .filter_map(|event| event.payload_as::<WorkEpisode>())
        .collect();
    assert_eq!(episodes.len(), 2);
    let episode = episodes
        .iter()
        .find(|episode| episode.label == "first")
        .unwrap();
    assert_eq!(episode.trail.len(), 2);
    assert_eq!(episode.state, "dormant");
    assert_eq!(episode.operator_s, None);
    assert_eq!(episode.suspensions.len(), 1);
    let resume = output
        .iter()
        .find_map(|event| event.payload_as::<WorkResumption>())
        .unwrap();
    assert_eq!(resume.resumed_at, at(5 * 86_400));
    assert_eq!(resume.lag_s, None);
    let suspension = output
        .iter()
        .find_map(|event| event.payload_as::<WorkSuspension>())
        .unwrap();
    assert_eq!(suspension.away_s, 5.0 * 86_400.0);
    assert_eq!(suspension.intervening_episodes.len(), 1);
    let mut reversed = inputs.clone();
    reversed.reverse();
    let payloads = |events: Vec<Event>| {
        events
            .into_iter()
            .map(|event| (event.event_type, event.payload))
            .collect::<Vec<_>>()
    };
    assert_eq!(payloads(output), payloads(compute(&reversed)));
}

#[devkit::test]
fn missing_branch_and_dispatch_prompts_do_not_create_operator_episodes() {
    let mut background = session("bg", Some("build"));
    background.payload["mode"] = json!("bg");
    assert!(
        compute(&[
            session("unknown", None),
            prompt("unknown", 10),
            background,
            prompt("bg", 20)
        ])
        .is_empty()
    );
}

#[devkit::test]
fn removing_the_last_objective_evidence_removes_all_outputs() {
    assert!(!compute(&[session("a", Some("task")), prompt("a", 20)]).is_empty());
    assert!(compute(&[session("a", Some("task"))]).is_empty());
}

#[devkit::test]
fn resume_trail_keeps_edit_locations_and_unresolved_failures() {
    let tool = record(
        "polylogue.tool_call",
        json!({"call":"tool:a#1","session":"a","message":"a/0","tool":"edit",
        "action":"file_edit","path":"/synthetic/repo/main.rs","outcome":"error","at":at(10)}),
        10,
    );
    let output = compute(&[session("a", Some("task")), prompt("a", 0), tool.clone()]);
    let card = output
        .iter()
        .find_map(|event| event.payload_as::<ContextPack>())
        .unwrap();
    assert_eq!(card.trail.len(), 2);
    assert_eq!(
        card.last_edit_path.as_deref(),
        Some("/synthetic/repo/main.rs")
    );
    assert_eq!(card.open_failures.len(), 1);
    assert_eq!(card.open_failures[0].event_type, "work.act");
    assert!(card.stated_goal.is_some());
}

#[devkit::test]
fn wake_days_follow_rest_while_calculation_days_stay_fixed() {
    let sleep = |id: &str, start, end| {
        record(
            "rest.fused",
            json!({"record":id,"started_at":at(start),"ended_at":at(end),"kind":"sleep","observed_by":[]}),
            start as u64,
        )
    };
    let inputs = vec![
        sleep("first", 0, 8 * 3600),
        sleep("second", 28 * 3600, 36 * 3600),
    ];
    let output = [8 * 3600, 36 * 3600]
        .into_iter()
        .flat_map(|start| {
            Keyed::compute(
                &ActivityDays,
                &json!({"start":at(start)}),
                &inputs,
                &Env::at(at(40 * 3600)),
            )
            .outputs
        })
        .collect::<Vec<_>>();
    let days: Vec<_> = output
        .iter()
        .map(|event| event.payload_as::<ActivityDay>().unwrap())
        .collect();
    assert_eq!(days.len(), 2);
    assert_eq!(
        (days[0].started_at, days[0].ended_at),
        (at(8 * 3600), at(36 * 3600))
    );
    assert!(!days[0].provisional);
    assert!(days[1].provisional);
    assert!(
        Keyed::compute(&ActivityDays, &json!({}), &[], &Env::at(at(40 * 3600)))
            .outputs
            .is_empty()
    );
}

#[devkit::test]
fn brief_excursions_are_counted_without_splitting_the_episode() {
    let output = compute(&[
        session("a", Some("first")),
        session("b", Some("second")),
        prompt("a", 0),
        prompt("b", 60),
        prompt("a", 80),
    ]);
    let episode = output
        .iter()
        .filter_map(|event| event.payload_as::<WorkEpisode>())
        .find(|episode| episode.label == "first")
        .unwrap();
    assert_eq!(episode.absorbed_excursions, 1);
    assert!(episode.suspensions.is_empty());
}

#[devkit::test]
fn work_queries_select_a_session_then_the_recorded_objective_range() {
    let env = Env::at(at(1000));
    let input = session("a", Some("task"));
    let query = Keyed::inputs(&WorkActs, &json!({"session":"a"}), &env).unwrap();
    assert!(query.admits(&input));
    assert!(!query.admits(&session("b", Some("task"))));
    let output = Keyed::compute(
        &WorkActs,
        &json!({"session":"a"}),
        &[input, prompt("a", 0)],
        &env,
    );
    let key = Keyed::keys_for(
        &WorkEpisodes,
        &KeyView {
            event_type: "work.act",
            ts_orig: Some(at(0)),
            payload: &output.outputs[0].payload,
        },
        &env,
    )
    .remove(0);
    let query = Keyed::inputs(&WorkEpisodes, &key, &env).unwrap();
    let Some(InputRange::Extent { kinds }) = query.range else {
        panic!("objective range")
    };
    assert_eq!(kinds[0].equal, [("episode", key["episode"].clone())]);
    assert_eq!(
        Keyed::compute(&WorkEpisodes, &key, &output.outputs, &env).outputs[0].payload["agent_s"],
        Value::Null
    );
}

#[devkit::test]
fn conflicting_recorded_objectives_do_not_choose_by_arrival_order() {
    let inputs = [
        session("a", Some("first")),
        session("a", Some("second")),
        prompt("a", 0),
    ];
    assert!(
        Keyed::compute(
            &WorkActs,
            &json!({"session":"a"}),
            &inputs,
            &Env::at(at(100))
        )
        .outputs
        .is_empty()
    );
}

#[devkit::test]
fn act_instants_without_duration_never_extend_into_later_hour_queries() {
    let env = Env::at(at(10_000));
    let acts = Keyed::compute(
        &WorkActs,
        &json!({"session":"a"}),
        &[session("a", None), prompt("a", 0)],
        &env,
    )
    .outputs;
    assert_eq!(
        acts.len(),
        1,
        "an objective is not needed to establish an act"
    );
    assert!(acts[0].payload["episode"].is_null());
    let hour = env.time_settings.hour_start(at(7200)).unwrap();
    let query =
        Keyed::inputs(&crate::activity::ActivitySpans, &json!({"hour":hour}), &env).unwrap();
    assert!(!query.admits(&acts[0]));
}
