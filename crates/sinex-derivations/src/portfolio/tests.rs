use super::*;
use crate::{Env,Keyed};
use crate::activity::SECOND;
use serde_json::{Value,json};
use sinex_model::{Event,Origin,Class,MaterialId,ReadingOrder,RecordLocation,Timestamp};
use sinex_model::event_types::portfolio::{InputWorkload,RhythmDeviation};
fn record(kind:&str,payload:Value,anchor:u64)->Event{
    (sinex_model::event_types::find(kind).unwrap().build)(&payload,"synthetic.portfolio",Origin::Record{
        location:RecordLocation{material:MaterialId::new(),anchor},output_index:0},ReadingOrder(0),[0;32],Class::Open).unwrap()
}
fn at(seconds:i64)->Timestamp{Timestamp::from_unix_secs(1_700_000_000+seconds).unwrap()}
fn key_point(seconds:i64,physical:bool)->Event{record("key.pressed",json!({"session":"keyboard","logged_at":at(seconds+50),
    "kernel_at":at(seconds),"key":"KEY_A","value":1,"n":seconds as u64,
    "extra":{"scope":"seat","layer":if physical{"evdev"}else{"uinput"},"virtual":!physical}}),seconds as u64)}
fn key(env:&Env)->Value{json!({"scope":"seat","day":env.time_settings.day(at(0)).unwrap().to_string()})}
fn coverage(env:&Env)->Event{let day=env.time_settings.day(at(0)).unwrap();record("input.coverage",json!({"coverage_id":"physical",
    "scope":"seat","started_at":env.time_settings.day_start(day).unwrap(),"ended_at":env.time_settings.day_end(day).unwrap()}),0)}
fn workload(events:&[Event])->InputWorkload{let env=Env::at(at(100_000), sinex_model::time_settings::TimeSettings::new("Europe/Warsaw", 4, 0).expect("fixture day setting"));input::InputWorkloads.compute(&key(&env),events,&env).outputs.iter()
    .find_map(|event|event.payload_as::<InputWorkload>()).unwrap()}
#[devkit::test]
fn input_uses_original_physical_instants_and_preserves_small_sample_unknowns(){
    let result=workload(&[key_point(0,true),key_point(1,true),key_point(2,false)]);
    assert_eq!(result.key_presses,2);assert_eq!(result.supported_seconds,1.0);
    assert_eq!(result.typing_seconds,None);assert_eq!(result.estimation,"fewer_than_200_key_presses");
    assert_eq!(result.coverage,"unknown");assert!(result.missing_seconds>0.0);
}
#[devkit::test]
fn complete_capture_distinguishes_zero_input_from_missing_capture(){
    let env=Env::at(at(100_000), sinex_model::time_settings::TimeSettings::new("Europe/Warsaw", 4, 0).expect("fixture day setting"));let observed=workload(&[coverage(&env)]);
    assert_eq!(observed.key_presses,0);assert_eq!(observed.coverage,"observed");assert_eq!(observed.missing_seconds,0.0);
    assert!(input::InputWorkloads.compute(&key(&env),&[],&env).outputs.is_empty());
}
#[devkit::test]
fn input_full_incremental_reversed_and_changed_readings_agree(){
    let env=Env::at(at(100_000), sinex_model::time_settings::TimeSettings::new("Europe/Warsaw", 4, 0).expect("fixture day setting"));let mut events:Vec<_>=(0..200).map(|n|key_point(n,true)).collect();events.push(coverage(&env));
    let all=workload(&events);assert_eq!(all.key_presses,200);assert_eq!(all.typing_seconds,Some(199.0));
    let earlier=workload(&events[..100]);assert_eq!(earlier.typing_seconds,None);
    let mut reversed=events.clone();reversed.reverse();assert_eq!(workload(&reversed),all);
    events.remove(1);assert_eq!(workload(&events).typing_seconds,None);
}
fn daily(day_offset:i64,keys:u64,complete:bool)->Event{
    let env=Env::at(at(day_offset*86400), sinex_model::time_settings::TimeSettings::new("Europe/Warsaw", 4, 0).expect("fixture day setting"));let day=env.time_settings.day(env.now).unwrap();
    record("input.workload",json!({"scope":"seat","day":day.to_string(),"period_start":env.time_settings.day_start(day).unwrap(),
        "period_end":env.time_settings.day_end(day).unwrap(),"key_presses":keys,"pointer_events":0,"bouts":1,
        "supported_seconds":10.0,"coverage_seconds":if complete{86400}else{0},"missing_seconds":if complete{0}else{86400},
        "typing_intervals":[],"typing_seconds":if keys>=200{Some(10.0)}else{None},"estimation":"test",
        "coverage":if complete{"observed"}else{"unknown"},"profile":"test"}),day_offset.max(0)as u64)
}
fn deviations(events:&[Event],target:&Event)->Vec<RhythmDeviation>{
    let env=Env::at(at(30*86400), sinex_model::time_settings::TimeSettings::new("Europe/Warsaw", 4, 0).expect("fixture day setting"));rhythm::RhythmDeviations(rhythm::RhythmProfile::default())
        .compute(&json!({"scope":"seat","day":target.payload["day"]}),events,&env).outputs.iter()
        .map(|event|event.payload_as::<RhythmDeviation>().unwrap()).collect()
}
#[devkit::test]
fn rhythm_is_prior_only_and_insufficient_evidence_is_not_zero_deviation(){
    let target=daily(20,500,true);let mut rows:Vec<_>=(0..14).map(|day|daily(day,300,true)).collect();
    rows.push(target.clone());rows.push(daily(21,1_000_000,true));
    let output=deviations(&rows,&target);let keys=&output[0];
    assert_eq!(keys.prior_observed_days,14);assert_eq!(keys.baseline,Some(300.0));assert_eq!(keys.deviation,Some(200.0));
    assert!(keys.baseline_until<=target.ts_orig.at.unwrap());
    rows.remove(0);let output=deviations(&rows,&target);assert_eq!(output[0].deviation,None);
    assert_eq!(output[0].estimation,"insufficient_prior_observed_days");
}
#[devkit::test]
fn rhythm_recomputes_after_a_late_prior_correction_and_never_counts_unknown_days(){
    let target=daily(20,500,true);let mut rows:Vec<_>=(0..14).map(|day|daily(day,300,true)).collect();rows.push(target.clone());
    rows.push(daily(15,10_000,false));let before=deviations(&rows,&target);assert_eq!(before[0].prior_observed_days,14);
    for row in rows.iter_mut().take(8){row.payload["key_presses"]=json!(400)}
    let after=deviations(&rows,&target);assert_eq!(after[0].baseline,Some(400.0));
    rows.reverse();assert_eq!(deviations(&rows,&target),after);
}
#[devkit::test]
fn late_rhythm_input_dirties_its_future_baseline_days(){
    let event=daily(0,300,true);let env=Env::at(at(40*86400), sinex_model::time_settings::TimeSettings::new("Europe/Warsaw", 4, 0).expect("fixture day setting"));
    let view=crate::KeyView{event_type:&event.event_type,ts_orig:event.ts_orig.at,payload:&event.payload};
    let keys=rhythm::RhythmDeviations(rhythm::RhythmProfile::default()).keys_for(&view,&env);
    assert_eq!(keys.len(),29);
}
#[devkit::test]
fn mark_links_wait_for_native_keys_or_host_scoped_containing_sessions(){
    let command=record("command.fused",json!({"record":"native-command-7","command":"example","started_at":at(0),"duration_ns":10*SECOND,
        "exit_code":0,"host":"box","cwd":"/example","observed_by":[]}),0);
    let session=record("shell.session",json!({"host":"box","started_at":at(0),"ended_at":at(10),"day":"2023-11-14","commands":1,"failed":0,"cwds":["/example"]}),0);
    let mark=record("operator.mark",json!({"host":"box","at":at(5),"text":"Example","cwd":"/example","command":"example",
        "app":null,"title":null,"by":"synthetic","written_at":at(5),"command_record":"native-command-7"}),0);
    let env=Env::at(at(100), sinex_model::time_settings::TimeSettings::new("Europe/Warsaw", 4, 0).expect("fixture day setting"));let key=json!({"day":env.time_settings.day(at(5)).unwrap().to_string()});
    let before=marks::MarkLinks.compute(&key,std::slice::from_ref(&mark),&env);assert!(before.outputs.is_empty());
    let after=marks::MarkLinks.compute(&key,&[mark.clone(),command.clone(),session],&env);assert_eq!(after.outputs.len(),2);
    for link in &after.outputs{assert!(matches!(&link.origin,Origin::Derived{parents}if parents.contains(&mark.id)));}
    let mut no_key=mark;no_key.payload.as_object_mut().unwrap().remove("command_record");
    assert!(marks::MarkLinks.compute(&key,&[no_key,command],&env).outputs.is_empty(),"command text is not a native key");
}

#[devkit::test]
fn default_portfolio_registers_the_named_stored_products(){
    let kinds:std::collections::BTreeSet<_>=crate::keyed().iter().flat_map(|derivation|derivation.output_types().iter().copied()).collect();
    for kind in ["command.fused","shell.session","activity.session","state.interval","activity.span","metric.figure",
        "rhythm.deviation","rest.fused","health.figure","conversation.day","input.bout","input.workload","reference.about"]{
        assert!(kinds.contains(kind),"missing default kind {kind}");
    }
}
