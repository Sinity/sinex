use super::*;
fn native(kind:&str,seconds:i64,mut payload:Value)->Event{
    payload["host"]=json!("box");payload["cursor"]=json!(format!("{kind}/{seconds}"));
    payload["logged_at"]=json!(at(seconds));payload["started_at"]=json!(at(seconds));
    (sinex_model::event_types::find(kind).unwrap().build)(&payload,"synthetic.journal",Origin::Record{
        location:RecordLocation{material:MaterialId::new(),anchor:seconds.max(0)as u64},output_index:0},ReadingOrder(0),[0;32],Class::Open).unwrap()
}
fn unit(kind:&str,seconds:i64,boot:&str,invocation:&str)->Event{native(kind,seconds,json!({"boot_id":boot,
    "unit":"example.service","manager":"system","uid":0,"invocation_id":invocation,"outcome":"stopped"}))}
fn machine(kind:&str,events:&[Event])->Vec<StateInterval>{
    let env=Env::at(at(10*86400), sinex_model::time_settings::TimeSettings::new("Europe/Warsaw", 4, 0).expect("fixture day setting"));let day=env.time_settings.day(at(0)).unwrap();
    StateIntervals.compute(&json!({"state":kind,"day":day.to_string(),"host":"box"}),events,&env)
        .outputs.iter().map(|event|event.payload_as::<StateInterval>().unwrap()).collect()
}
#[devkit::test]
fn native_unit_pairing_keeps_boot_and_invocation_identity(){
    let start=unit("unit.started",0,"boot-a","run-a");
    let wrong=unit("unit.stopped",5,"boot-a","run-b");
    let other_boot=unit("unit.stopped",6,"boot-b","run-a");
    let stop=unit("unit.stopped",10,"boot-a","run-a");
    let result=machine("machine.unit",&[start.clone(),wrong,other_boot]);assert_eq!(result.len(),1);assert_eq!(result[0].ended_at,None);
    let result=machine("machine.unit",&[start,stop]);assert_eq!(result[0].ended_at,Some(at(10)));
}
#[devkit::test]
fn boot_fences_unknown_unit_end_without_inventing_continuous_uptime(){
    let start=unit("unit.started",0,"boot-a","run-a");
    let boot=native("boot.started",3*86400,json!({"boot_id":"boot-b"}));
    let result=machine("machine.unit",&[start,boot]);assert_eq!(result.len(),1);
    assert_eq!(result[0].ended_at,None);assert_eq!(result[0].fenced_at,Some(at(3*86400)));
    assert_eq!(result[0].end_event_type.as_deref(),Some("boot.started"));
}
#[devkit::test]
fn a_late_start_job_completion_does_not_reopen_an_already_stopped_oneshot(){
    let result=machine("machine.unit",&[unit("unit.stopped",0,"boot-a","run-a"),unit("unit.started",1,"boot-a","run-a")]);
    assert!(result.is_empty());
}
#[devkit::test]
fn separate_native_hosts_cannot_close_each_others_units(){
    let start=unit("unit.started",0,"boot-a","run-a");let mut other=unit("unit.stopped",10,"boot-a","run-a");
    other.payload["host"]=json!("other-box");assert_eq!(machine("machine.unit",&[start,other])[0].ended_at,None);
}
