use super::*;
use sinex_model::{Class, MaterialId, Origin, ReadingOrder, RecordLocation, Timestamp};

fn at(seconds: i64) -> Timestamp {
    Timestamp::from_unix_secs(1_700_000_000 + seconds).unwrap()
}
fn record(kind: &str, payload: Value, anchor: u64) -> Event {
    let origin = Origin::Record {
        location: RecordLocation {
            material: MaterialId::new(),
            anchor,
        },
        output_index: 0,
    };
    if let Some(kind) = sinex_model::event_types::find(kind) {
        return (kind.build)(
            &payload,
            "synthetic.work",
            origin,
            ReadingOrder(0),
            [0; 32],
            Class::Open,
        )
        .unwrap();
    }
    let at = ["at", "start", "started"].into_iter().find_map(|key| {
        payload
            .get(key)
            .and_then(|value| serde_json::from_value::<Timestamp>(value.clone()).ok())
    });
    let mut event = Event::submitted(
        sinex_model::KindRef {
            name: kind.into(),
            version: 1,
        },
        payload,
        "synthetic.work".into(),
        origin,
        ReadingOrder(0),
        [0; 32],
        Class::Open,
    );
    event.ts_orig = sinex_model::TsOrig::maybe(at, sinex_model::TimeQuality::Intrinsic);
    event
}
fn session(id: &str, branch: Option<&str>) -> Event {
    let mut event=record("polylogue.work_snapshot",json!({
        "session":id,"reducer_version":2,"source_position":1,"complete":true,
        "repo":"synthetic/repo","branch":branch,"machine":"box","kind":"main","mode":null,"cwd":["/synthetic/repo"],
        "objectives":[],"record":{"product":"work_snapshot","snapshot":{
            "reducer_version":2,"position":1,"session_id":id,"session":{"id":id},"retired":false,"unavailable":[],"messages":[],"calls":[],"turns":[],"sources":[],"native":[]}}}),0);
    event.kind_version=4;
    event
}
fn fixture_evidence(at:&Value)->Value {
    if at.is_null(){json!({"quality":"unknown","precision":null,"until":null})}
    else{json!({"quality":"intrinsic","precision":"instant","until":null})}
}
fn qualify_fixture_times(snapshot:&mut Event){
    for (kind,fields)in [("messages",vec![("at","at_evidence")]),("calls",vec![("at","at_evidence"),("result_at","result_at_evidence")]),("turns",vec![("start","start_evidence"),("end","end_evidence")])]{
        for entry in snapshot.payload["record"]["snapshot"][kind].as_array_mut().unwrap(){
            if kind=="calls" {
                if entry.get("entry_id").is_none(){entry["entry_id"]=json!(format!("fixture-entry:{}:{}",entry["session"],entry["call"]));}
                if entry.get("physical_session").is_none(){entry["physical_session"]=entry["session"].clone();}
            }
            for (field,evidence)in &fields{if entry.get(*evidence).is_none(){entry[*evidence]=fixture_evidence(&entry[*field]);}}
        }
    }
}
fn prompt(id: &str, seconds: i64) -> Event {
    let mut snapshot=session(id,None);
    snapshot.payload["record"]["snapshot"]["messages"]=json!([{"message":format!("{id}/{seconds}"),"role":"user","written_by":"human","prompt_kind":"prose","at":at(seconds),"source_event":"synthetic-source"}]);
    qualify_fixture_times(&mut snapshot);snapshot
}
fn tool(payload:Value,anchor:u64)->Event {
    let id=payload["session"].as_str().unwrap().to_owned();let mut snapshot=session(&id,None);
    let mut call=payload;
    if call.get("call").is_none(){call["call"]=json!(format!("{id}/{anchor}"));}
    if call.get("entry_id").is_none(){call["entry_id"]=json!(format!("fixture-entry:{id}:{anchor}"));}
    if call.get("physical_session").is_none(){call["physical_session"]=json!(id);}
    if call.get("outcome").is_none(){call["outcome"]=json!({"kind":"no_result"});}
    else if call["outcome"].is_string(){call["outcome"]=json!({"kind":call["outcome"]});}
    snapshot.payload["record"]["snapshot"]["calls"]=json!([call]);qualify_fixture_times(&mut snapshot);snapshot
}
/// Assemble fixture parts before invoking the consumer. Every production
/// computation below receives only complete atomic snapshots, never per-entry events.
fn complete_snapshots(parts:&[Event])->Vec<Event>{
    let mut by_session=BTreeMap::<String,Vec<&Event>>::new();
    for part in parts.iter().filter(|event|event.event_type=="polylogue.work_snapshot"){
        by_session.entry(part.payload["session"].as_str().unwrap().into()).or_default().push(part);
    }
    let mut result=Vec::new();
    for parts in by_session.values(){
        let metadata:Vec<_>=parts.iter().copied().filter(|part|["messages","calls","turns"].iter().all(|kind|part.payload["record"]["snapshot"][*kind].as_array().unwrap().is_empty())).collect();
        if metadata.len()>1 {result.extend(metadata.into_iter().map(|event|event.clone()));continue}
        let mut snapshot:Event=(*metadata.first().copied().unwrap_or(parts[0])).clone();
        for kind in ["messages","calls","turns"] {
            snapshot.payload["record"]["snapshot"][kind]=Value::Array(parts.iter().flat_map(|part|part.payload["record"]["snapshot"][kind].as_array().unwrap().clone()).collect());
        }
        result.push(snapshot);
    }
    result
}
fn work_acts(inputs:&[Event],env:&Env)->Vec<Event>{
    complete_snapshots(inputs).into_iter().flat_map(|snapshot|{
        Keyed::compute(&WorkActs,&json!({"session":snapshot.payload["session"]}),&[snapshot],env).outputs
    }).filter(|event|event.event_type=="work.act").collect()
}
fn compute(inputs: &[Event]) -> Vec<Event> {
    let env = Env::at(at(2_000_000), sinex_model::time_settings::TimeSettings::new("Europe/Warsaw", 4, 0).expect("fixture day setting"));
    let mut acts = work_acts(inputs, &env);
    acts.extend(
        inputs
            .iter()
            .filter(|event| matches!(event.event_type.as_str(), "rest.fused" | "state.interval"))
            .cloned(),
    );
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
    assert_eq!(suspension.suspended_at, at(300));
    assert_eq!(suspension.away_s, Some(5.0 * 86_400.0 - 300.0));
    assert_eq!(suspension.intervening_episodes.len(), 1);
    let mut reversed = inputs.clone();
    reversed.reverse();
    // Recomputing creates new readings; compare the stable record references.
    fn stable_references(value: &mut Value) {
        match value {
            Value::Object(fields) => {
                if fields.contains_key("record") && fields.contains_key("event_type") {
                    fields.remove("event");
                }
                fields.values_mut().for_each(stable_references);
            }
            Value::Array(values) => values.iter_mut().for_each(stable_references),
            _ => {}
        }
    }
    let payloads = |events: Vec<Event>| {
        events
            .into_iter()
            .map(|mut event| {
                stable_references(&mut event.payload);
                (event.event_type, event.payload)
            })
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
    let tool = tool(
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
                &Env::at(at(40 * 3600), sinex_model::time_settings::TimeSettings::new("Europe/Warsaw", 4, 0).expect("fixture day setting")),
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
        Keyed::compute(&ActivityDays, &json!({}), &[], &Env::at(at(40 * 3600), sinex_model::time_settings::TimeSettings::new("Europe/Warsaw", 4, 0).expect("fixture day setting")))
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
    let env = Env::at(at(1000), sinex_model::time_settings::TimeSettings::new("Europe/Warsaw", 4, 0).expect("fixture day setting"));
    let input = session("a", Some("task"));
    let query = Keyed::inputs(&WorkActs, &json!({"session":"a"}), &env).unwrap();
    assert!(query.admits(&input));
    assert!(!query.admits(&session("b", Some("task"))));
    let output = Keyed::compute(
        &WorkActs,
        &json!({"session":"a"}),
        &complete_snapshots(&[input, prompt("a", 0)]),
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
    assert_eq!(kinds[0].equal, [("episode".into(), key["episode"].clone())]);
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
            &Env::at(at(100), sinex_model::time_settings::TimeSettings::new("Europe/Warsaw", 4, 0).expect("fixture day setting"))
        )
        .outputs.iter().all(|event|event.event_type=="work.coverage"&&event.payload["state"]=="unavailable")
    );
}

#[devkit::test]
fn act_instants_without_duration_never_extend_into_later_hour_queries() {
    let env = Env::at(at(10_000), sinex_model::time_settings::TimeSettings::new("Europe/Warsaw", 4, 0).expect("fixture day setting"));
    let acts = Keyed::compute(
        &WorkActs,
        &json!({"session":"a"}),
        &complete_snapshots(&[session("a", None), prompt("a", 0)]),
        &env,
    )
    .outputs.into_iter().filter(|event|event.event_type=="work.act").collect::<Vec<_>>();
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

#[test]
fn a_recorded_objective_switch_stores_an_open_suspension_until_return() {
    let inputs = vec![
        session("a", Some("first")),
        session("b", Some("second")),
        prompt("a", 0),
        prompt("b", 100),
    ];
    let env = Env::at(at(200), sinex_model::time_settings::TimeSettings::new("Europe/Warsaw", 4, 0).expect("fixture day setting"));
    let acts = work_acts(&inputs, &env);
    let target = acts
        .iter()
        .find(|event| event.payload["session"] == "a")
        .unwrap()
        .payload["episode"]
        .clone();
    let key = json!({"episode":target});
    let initial = Keyed::compute(&WorkEpisodes, &key, &acts, &env);
    let suspension = initial
        .outputs
        .iter()
        .find_map(|event| event.payload_as::<WorkSuspension>())
        .unwrap();
    assert_eq!(suspension.suspended_at, at(100));
    assert_eq!((suspension.returned_at, suspension.away_s), (None, None));
    assert_eq!(
        initial
            .outputs
            .iter()
            .find_map(|event| event.payload_as::<WorkEpisode>())
            .unwrap()
            .state,
        "suspended"
    );
    assert!(
        !initial
            .outputs
            .iter()
            .any(|event| event.event_type == "work.resumption")
    );
    let mut returned = inputs;
    returned.push(prompt("a", 500));
    let result = Keyed::compute(
        &WorkEpisodes,
        &key,
        &work_acts(&returned, &Env::at(at(600), sinex_model::time_settings::TimeSettings::new("Europe/Warsaw", 4, 0).expect("fixture day setting"))),
        &Env::at(at(600), sinex_model::time_settings::TimeSettings::new("Europe/Warsaw", 4, 0).expect("fixture day setting")),
    );
    let closed = result
        .outputs
        .iter()
        .find_map(|event| event.payload_as::<WorkSuspension>())
        .unwrap();
    assert_eq!(
        closed.suspension, suspension.suspension,
        "a return completes the same interruption record"
    );
    assert_eq!(
        (closed.returned_at, closed.away_s),
        (Some(at(500)), Some(400.0))
    );
    assert_eq!(
        result
            .outputs
            .iter()
            .filter(|event| event.event_type == "work.resumption")
            .count(),
        1
    );
}

#[test]
fn an_act_with_no_objective_does_not_establish_an_objective_switch() {
    let output = compute(&[
        session("a", Some("first")),
        session("unknown", None),
        prompt("a", 0),
        prompt("unknown", 100),
        prompt("a", 500),
    ]);
    assert!(
        !output
            .iter()
            .any(|event| event.event_type == "work.suspension")
    );
}

#[test]
fn a_recorded_absence_interrupts_work_before_any_return() {
    let mut metadata = session("a", Some("first"));
    metadata.payload["machine"] = json!("box");
    let inputs = vec![metadata, prompt("a", 0)];
    let env = Env::at(at(200), sinex_model::time_settings::TimeSettings::new("Europe/Warsaw", 4, 0).expect("fixture day setting"));
    let mut acts = work_acts(&inputs, &env);
    let episode = acts[0].payload["episode"].clone();
    let absence = record(
        "state.interval",
        json!({"interval_id":"sleep","state_kind":"machine.suspend","subject_id":"box","label":"suspended","started_at":at(100),"ended_at":null,"start_event_type":"suspend.started","end_event_type":null,"attributes":{"host":"box"}}),
        100,
    );
    acts.push(absence.clone());
    let result = Keyed::compute(&WorkEpisodes, &json!({"episode":episode}), &acts, &env);
    let interruption = result
        .outputs
        .iter()
        .find(|event| event.event_type == "work.suspension")
        .unwrap();
    let payload = interruption.payload_as::<WorkSuspension>().unwrap();
    assert_eq!(payload.interruption_source, "absence");
    assert_eq!(payload.suspended_at, at(100));
    assert_eq!(payload.returned_at, None);
    assert!(
        matches!(&interruption.origin,Origin::Derived {parents} if parents.contains(&absence.id))
    );
}

#[devkit::test]
fn explicit_task_then_pull_request_anchors_outrank_a_branch_without_reading_prose() {
    let mut metadata = session("a", Some("branch"));
    metadata.payload["objectives"] = json!([
        {"kind":"pull_request","repository":"synthetic/repo","reference":"https://forge.example/synthetic/repo/pull/7"},
        {"kind":"task","repository":"synthetic/repo","reference":"task-7"}]);
    let inputs = [metadata.clone(), prompt("a", 0)];
    let acts = work_acts(&inputs, &Env::at(at(10), sinex_model::time_settings::TimeSettings::new("Europe/Warsaw", 4, 0).expect("fixture day setting")));
    assert_eq!(acts[0].payload["objective"]["kind"], "task");
    assert_eq!(acts[0].payload["objective"]["reference"], "task-7");
    metadata.payload["objectives"]
        .as_array_mut()
        .unwrap()
        .remove(1);
    let acts = work_acts(&[metadata, prompt("a", 0)], &Env::at(at(10), sinex_model::time_settings::TimeSettings::new("Europe/Warsaw", 4, 0).expect("fixture day setting")));
    assert_eq!(acts[0].payload["objective"]["kind"], "pull_request");
    let mut prose = prompt("a", 0);
    prose.payload["record"]["snapshot"]["messages"][0]["text"] = json!("Task task-99. gh pr checkout 99");
    let acts = work_acts(&[session("a", None), prose], &Env::at(at(10), sinex_model::time_settings::TimeSettings::new("Europe/Warsaw", 4, 0).expect("fixture day setting")));
    assert!(acts[0].payload["objective"].is_null());
}
#[devkit::test]
fn conflicting_task_links_do_not_fall_back_to_a_lower_priority_branch() {
    let mut metadata = session("a", Some("branch"));
    metadata.payload["objectives"] = json!([
        {"kind":"task","repository":"synthetic/repo","reference":"task-1"},
        {"kind":"task","repository":"synthetic/repo","reference":"task-2"}]);
    let acts = work_acts(&[metadata, prompt("a", 0)], &Env::at(at(10), sinex_model::time_settings::TimeSettings::new("Europe/Warsaw", 4, 0).expect("fixture day setting")));
    assert!(acts[0].payload["objective"].is_null());
}
#[devkit::test]
fn work_acts_retain_exact_terminal_repository_and_revision_check_evidence() {
    let mut metadata = session("a", Some("branch"));
    metadata.payload["git_identity"] = json!({"common_dir":"/captured/repo/.git","root_commits":["root"],"head":"rev-a","branch":"branch"});
    metadata.payload["terminal"] = json!({"host":"box","adapter":"test-terminal","process_id":11,"window_id":"native-7","parent_process_id":9});
    let tool = tool(
        json!({"session":"a","at":at(1),"action":"shell",
        "outcome":"error","revision":"rev-a","check_name":"compile","check_passed":false}),
        1,
    );
    let acts = work_acts(&[metadata, tool], &Env::at(at(10), sinex_model::time_settings::TimeSettings::new("Europe/Warsaw", 4, 0).expect("fixture day setting")));
    let act = acts[0].payload_as::<WorkAct>().unwrap();
    assert_eq!(act.git_identity.unwrap().common_dir, "/captured/repo/.git");
    assert_eq!(act.terminal.unwrap().window_id, "native-7");
    assert_eq!(act.revision.as_deref(), Some("rev-a"));
    assert_eq!(act.check_name.as_deref(), Some("compile"));
    assert_eq!(act.check_passed, Some(false));
}

#[devkit::test]
fn what_was_i_doing_keeps_last_edit_and_only_resolves_checks_on_the_same_revision() {
    let mut metadata = session("a", Some("repair"));
    metadata.payload["objectives"] =
        json!([{"kind":"task","repository":"synthetic/repo","reference":"task-7"}]);
    let check = |seconds, revision, passed| {
        tool(
            json!({"session":"a","at":at(seconds),
        "action":"shell","revision":revision,"check_name":"compile","check_passed":passed}),
            seconds as u64,
        )
    };
    let edit = tool(
        json!({"session":"a","at":at(1),"action":"file_edit","path":"/synthetic/repo/lib.rs"}),
        1,
    );
    let mut inputs = vec![
        metadata,
        prompt("a", 0),
        edit,
        check(2, "rev-a", false),
        check(3, "rev-b", true),
    ];
    let card = |inputs: &[Event]| {
        compute(inputs)
            .iter()
            .find_map(|event| event.payload_as::<ContextPack>())
            .unwrap()
    };
    let before = card(&inputs);
    assert_eq!(
        before.last_edit_path.as_deref(),
        Some("/synthetic/repo/lib.rs")
    );
    assert_eq!(
        before.open_failures.len(),
        1,
        "a pass on another revision proves nothing about rev-a"
    );
    inputs.push(check(4, "rev-a", true));
    let after = card(&inputs);
    assert!(after.open_failures.is_empty());
    assert_eq!(after.trail.len(), 5);
    assert!(after.stated_goal.is_some());
    assert_eq!(
        before.pack, after.pack,
        "the prepared resume card keeps its identity"
    );
}

#[devkit::test]
fn atomic_snapshot_entries_cite_only_the_snapshot_and_unknown_times_are_visible(){
    let env=Env::at(at(100),sinex_model::time_settings::TimeSettings::new("Europe/Warsaw",4,0).unwrap());
    let mut snapshot=session("atomic",Some("task"));
    snapshot.payload["record"]["snapshot"]["messages"]=json!([
        {"message":"timed","role":"user","written_by":"human","prompt_kind":"prose","at":at(0)},
        {"message":"untimed","role":"user","written_by":"human","prompt_kind":"prose","at":null}]);
    snapshot.payload["record"]["snapshot"]["calls"]=json!([{"call":"call","session":"atomic","action":"file_edit","path":"/synthetic/repo/file.rs","at":at(1),"result_at":at(2),"outcome":{"kind":"error"},"check_passed":false}]);
    snapshot.payload["record"]["snapshot"]["turns"]=json!([{"turn":"turn","session":"atomic","start":at(0),"end":at(3),"errors":1}]);
    qualify_fixture_times(&mut snapshot);
    let computed=Keyed::compute(&WorkActs,&json!({"session":"atomic"}),&[snapshot.clone()],&env);
    let acts:Vec<_>=computed.outputs.iter().filter(|event|event.event_type=="work.act").collect();
    assert_eq!(acts.len(),3);
    for event in &computed.outputs {assert_eq!(event.origin,Origin::Derived{parents:vec![snapshot.id]});}
    assert!(acts.iter().all(|event|event.payload["evidence"]["event_type"]=="polylogue.work_snapshot"));
    let coverage=computed.outputs.iter().find_map(|event|event.payload_as::<WorkCoverage>()).unwrap();
    assert_eq!(coverage.state,WorkCoverageState::Partial);
    assert_eq!((coverage.eligible_entries,coverage.timed_entries,coverage.untimed_entries),(Some(4),Some(3),Some(1)));
    assert_eq!(coverage.reasons["entry_time_unknown"],1);
    assert!(computed.outputs.last().unwrap().ts_orig.at.is_none());
    let failed=acts.iter().find(|event|event.payload["action"]=="file_edit").unwrap();
    assert_eq!(failed.payload["check_passed"],false);
}

#[devkit::test]
fn empty_snapshot_and_newer_unsupported_reducer_do_not_fall_back_to_old_acts(){
    let env=Env::at(at(100),sinex_model::time_settings::TimeSettings::new("Europe/Warsaw",4,0).unwrap());
    let empty=session("atomic",Some("task"));
    let outputs=Keyed::compute(&WorkActs,&json!({"session":"atomic"}),&[empty.clone()],&env).outputs;
    assert_eq!(outputs.len(),1);
    let coverage=outputs[0].payload_as::<WorkCoverage>().unwrap();
    assert_eq!(coverage.state,WorkCoverageState::Complete);
    assert_eq!((coverage.eligible_entries,coverage.timed_entries,coverage.untimed_entries),(Some(0),Some(0),Some(0)));
    let mut invalid=empty.clone();invalid.payload["source_position"]=json!(-1);invalid.payload["record"]["snapshot"]["position"]=json!(-1);
    let invalid=Keyed::compute(&WorkActs,&json!({"session":"atomic"}),&[invalid],&env).outputs;
    assert_eq!(invalid[0].payload["state"],"unavailable");
    let old=complete_snapshots(&[empty,prompt("atomic",0)]).remove(0);
    let mut unsupported=old.clone();unsupported.id=sinex_model::EventId::new();
    unsupported.payload["reducer_version"]=json!(3);
    unsupported.payload["record"]["snapshot"]["reducer_version"]=json!(3);
    let outputs=Keyed::compute(&WorkActs,&json!({"session":"atomic"}),&[old,unsupported],&env).outputs;
    assert_eq!(outputs.len(),1);
    let coverage=outputs[0].payload_as::<WorkCoverage>().unwrap();
    assert_eq!(coverage.state,WorkCoverageState::Unavailable);
    assert_eq!(coverage.timed_entries,None);
    assert_eq!(coverage.reasons["unsupported_reducer_or_kind_version"],1);
}

#[devkit::test]
fn source_time_quality_periods_sparse_coverage_and_retirement_remain_explicit(){
    let env=Env::at(at(100),sinex_model::time_settings::TimeSettings::new("UTC",0,0).unwrap());
    let mut snapshot=complete_snapshots(&[session("a",Some("task")),prompt("a",0),prompt("a",10)]).remove(0);
    snapshot.payload["record"]["snapshot"]["messages"][0]["at_evidence"]=json!({"quality":"captured","precision":"instant"});
    snapshot.payload["record"]["snapshot"]["messages"][1]["at_evidence"]=json!({"quality":"intrinsic","precision":"day"});
    snapshot.payload["record"]["snapshot"]["sources"]=json!([{"selected":true,"projection":{"block_count":3,"block_ordinals":[0]}}]);
    let computed=Keyed::compute(&WorkActs,&json!({"session":"a"}),&[snapshot.clone()],&env);
    let acts:Vec<_>=computed.outputs.iter().filter(|event|event.event_type=="work.act").collect();
    assert_eq!(acts.len(),1);assert_eq!(acts[0].ts_orig.quality,sinex_model::TimeQuality::Captured);
    let declared=<WorkAct as sinex_model::EventPayload>::declaration().evaluate(&acts[0].payload,sinex_model::Class::Open).unwrap();
    assert_eq!(declared.ts_orig,acts[0].ts_orig);
    let coverage=computed.outputs.iter().find_map(|event|event.payload_as::<WorkCoverage>()).unwrap();
    assert_eq!(coverage.state,WorkCoverageState::Partial);
    assert_eq!(coverage.reasons["entry_time_not_instant"],1);assert_eq!(coverage.reasons["sparse_selected_source"],1);
    snapshot.payload["record"]["snapshot"]=json!({"reducer_version":2,"position":1,"session_id":"a","session":null,"retired":true,"unavailable":[],"messages":[],"calls":[],"turns":[],"sources":[],"native":[]});
    for field in ["repo","branch","kind","mode","machine","git_identity","terminal","boot_id"]{snapshot.payload[field]=Value::Null;}
    snapshot.payload["objectives"]=json!([]);
    let retired=Keyed::compute(&WorkActs,&json!({"session":"a"}),&[snapshot],&env).outputs;
    assert_eq!(retired.len(),1);let coverage=retired[0].payload_as::<WorkCoverage>().unwrap();
    assert_eq!(coverage.state,WorkCoverageState::Complete);assert_eq!(coverage.eligible_entries,Some(0));assert_eq!(coverage.reasons["retired"],1);
}


#[devkit::test]
fn native_redefinition_entries_stay_distinct_and_ambiguous_failure_stays_unknown(){
    let env=Env::at(at(100),sinex_model::time_settings::TimeSettings::new("UTC",0,0).unwrap());
    let mut snapshot=session("a",Some("task"));
    snapshot.payload["record"]["snapshot"]["calls"]=json!([
      {"entry_id":"native-definition:a:0","call":"display:same","session":"a","physical_session":"physical:one","native_call_id":"same","original_block":0,"unavailable":["ambiguous_native_call_id"],"action":"shell","at":at(1),"result_at":at(2),"outcome":null,"outcome_reason":"ambiguous_result_evidence"},
      {"entry_id":"native-definition:a:1","call":"display:same","session":"a","physical_session":"physical:one","native_call_id":"same","original_block":1,"unavailable":["ambiguous_native_call_id"],"action":"shell","at":at(3),"result_at":at(4),"outcome":null,"outcome_reason":"ambiguous_result_evidence"}]);
    qualify_fixture_times(&mut snapshot);
    let computed=Keyed::compute(&WorkActs,&json!({"session":"a"}),&[snapshot.clone()],&env);
    let acts:Vec<_>=computed.outputs.iter().filter(|event|event.event_type=="work.act").collect();assert_eq!(acts.len(),2);assert_ne!(acts[0].payload["act"],acts[1].payload["act"]);
    for act in acts{assert!(act.payload["failed"].is_null());let evidence=act.payload["evidence"]["record"].as_str().unwrap();assert!(evidence.contains("display:same"));assert!(evidence.contains("physical:one"));assert!(evidence.contains("native-definition:"));}
    let coverage=computed.outputs.iter().find_map(|event|event.payload_as::<WorkCoverage>()).unwrap();assert_eq!(coverage.state,WorkCoverageState::Partial);assert_eq!(coverage.reasons["tool_outcome_unknown"],2);assert_eq!(coverage.reasons["tool_entry_unavailable"],2);
    snapshot.payload["record"]["snapshot"]["calls"][1]["entry_id"]=snapshot.payload["record"]["snapshot"]["calls"][0]["entry_id"].clone();
    let invalid=Keyed::compute(&WorkActs,&json!({"session":"a"}),&[snapshot],&env);assert_eq!(invalid.outputs.len(),1);assert_eq!(invalid.outputs[0].payload["state"],"unavailable");
}

#[devkit::test]
fn highest_reducer_controls_retirement_while_older_raw_snapshot_remains_citable(){
    let env=Env::at(at(100),sinex_model::time_settings::TimeSettings::new("UTC",0,0).unwrap());let current=prompt("a",1);
    let mut old=current.clone();old.id=sinex_model::EventId::new();old.kind_version=3;old.payload["reducer_version"]=json!(1);old.payload["record"]["snapshot"]["reducer_version"]=json!(1);
    let only_old=Keyed::compute(&WorkActs,&json!({"session":"a"}),&[old.clone()],&env);assert_eq!(only_old.outputs[0].payload["state"],"unavailable");
    let selected=Keyed::compute(&WorkActs,&json!({"session":"a"}),&[old.clone(),current.clone()],&env);let acts:Vec<_>=selected.outputs.iter().filter(|event|event.event_type=="work.act").collect();assert_eq!(acts.len(),1);assert_eq!(acts[0].origin,Origin::Derived{parents:vec![current.id]});
    let mut retired=current;retired.payload["record"]["snapshot"]=json!({"reducer_version":2,"position":1,"session_id":"a","session":null,"retired":true,"unavailable":[],"messages":[],"calls":[],"turns":[],"sources":[],"native":[]});
    for field in ["repo","branch","kind","mode","machine","git_identity","terminal","boot_id"]{retired.payload[field]=Value::Null;}retired.payload["objectives"]=json!([]);
    let cleared=Keyed::compute(&WorkActs,&json!({"session":"a"}),&[old,retired],&env);assert_eq!(cleared.outputs.len(),1);assert_eq!(cleared.outputs[0].payload["eligible_entries"],0);assert_eq!(cleared.outputs[0].payload["state"],"complete");
}
