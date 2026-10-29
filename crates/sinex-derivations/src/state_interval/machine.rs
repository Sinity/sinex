//! Native machine transitions share the state-interval product and admission path.
use super::*;
use crate::activity::stable;
const UNIT:&[&str]=&["unit.started","unit.stopped","boot.started"];
const SLEEP:&[&str]=&["sleep.started","sleep.ended","boot.started"];

pub(super) fn keys(input:&KeyView<'_>,env:&Env)->Vec<Value>{
    let states:&[&str]=match input.event_type {"unit.started"|"unit.stopped"=>&["machine.unit"],
        "boot.started"=>&["machine.boot","machine.unit","machine.suspend"],
        "sleep.started"|"sleep.ended"=>&["machine.suspend"],_=>return vec![]};
    let Some(at)=input.ts_orig else{return vec![]};let Ok(day)=env.time_settings.day(at)else{return vec![]};
    states.iter().map(|state|json!({"state":state,"day":day.to_string(),"host":input.payload["host"],
        "unknown_record":if input.payload["host"].is_string(){Value::Null}else{input.payload["cursor"].clone()}})).collect()
}
pub(super) fn inputs(key:&Value,env:&Env)->Option<InputQuery>{
    let day=key["day"].as_str()?.parse().ok()?;let start=env.time_settings.day_start(day).ok()?;
    let end=env.time_settings.day_end(day).ok()?;
    let kinds=match key["state"].as_str()?{"machine.unit"=>UNIT,"machine.suspend"=>SLEEP,"machine.boot"=>&["boot.started"],_=>return None};
    let mut query=InputQuery::window(kinds,start,end);
    if let Some(host)=key["host"].as_str(){
        query=query.only("host",json!(host));
        query.range=Some(InputRange::UntilNext{start,after:Some(end),kinds:vec![Kind{event_type:"boot.started".into(),equal:vec![("host".into(),json!(host))]}],field:"started_at"});
        query.after.push(Neighbour{kinds:vec![Kind{event_type:"boot.started".into(),equal:vec![("host".into(),json!(host))]}],per:None,count:1});
        query.before.push(Neighbour{kinds:kinds.iter().map(|kind|Kind{event_type:(*kind).into(),equal:vec![("host".into(),json!(host))]}).collect(),
            per:if key["state"]=="machine.unit"{Some("unit")}else{None},count:1});
    }else if let Some(cursor)=key["unknown_record"].as_str(){query=query.only("cursor",json!(cursor));}
    Some(query)
}
fn unit_scope(event:&Event)->String{json!([event.payload["host"],event.payload["boot_id"],event.payload["manager"],event.payload["uid"],event.payload["unit"]]).to_string()}
fn same_invocation(start:&Event,stop:&Event)->bool{
    unit_scope(start)==unit_scope(stop) && match (start.payload["invocation_id"].as_str(),stop.payload["invocation_id"].as_str()){
        (Some(a),Some(b))=>a==b,(None,None)=>true,_=>false}
}
fn interval(start:&Event,end:Option<&Event>,fence:Option<&Event>,state:&str)->Event{
    let mut attributes=BTreeMap::new();
    for field in ["host","boot_id","manager","uid","unit","invocation_id","outcome","state"]{
        if let Some(value)=scalar(&start.payload,field){attributes.insert(field.into(),value);}
    }
    if start.payload["host"].is_null(){attributes.insert("scope_quality".into(),"unknown".into());}
    let subject=if state=="machine.unit"{Some(unit_scope(start))}else{start.payload["host"].as_str().map(str::to_owned)};
    let label=if state=="machine.unit"{start.payload["unit"].as_str().map(str::to_owned)}else if state=="machine.boot"{Some("booted".into())}else{Some("suspended".into())};
    derived(&StateInterval{interval_id:format!("{state}:{}",stable(start)),state_kind:state.into(),subject_id:subject,label,
        started_at:start.ts_orig.at.unwrap(),ended_at:end.and_then(|event|event.ts_orig.at),fenced_at:fence.and_then(|event|event.ts_orig.at),
        start_event_type:start.event_type.clone(),end_event_type:end.or(fence).map(|event|event.event_type.clone()),attributes},
        StateIntervals::NAME,[start].into_iter().chain(end).chain(fence))
}
pub(super) fn compute(key:&Value,inputs:&[Event],env:&Env)->Computed{
    let Some(state)=key["state"].as_str()else{return Computed::default()};
    let Some(day)=key["day"].as_str().and_then(|day|day.parse().ok())else{return Computed::default()};
    let (Ok(from),Ok(until))=(env.time_settings.day_start(day),env.time_settings.day_end(day))else{return Computed::default()};
    let mut events:Vec<_>=inputs.iter().filter(|event|event.ts_orig.at.is_some()
        && (key["host"].is_null()||event.payload["host"]==key["host"]))
        .filter(|event|key["unknown_record"].is_null()||event.payload["cursor"]==key["unknown_record"]).collect();
    events.sort_by_cached_key(|event|(event.ts_orig.at,event.reading_order,stable(event)));
    let mut outputs=vec![];
    if key["host"].is_null(){
        for event in &events {if (state=="machine.unit"&&event.event_type=="unit.started")
            ||(state=="machine.boot"&&event.event_type=="boot.started")||(state=="machine.suspend"&&event.event_type=="sleep.started"){
            outputs.push(interval(event,None,None,state));}}
    }else if state=="machine.unit"{
        let mut open=BTreeMap::<String,&Event>::new();
        for event in &events{
            match event.event_type.as_str(){
                "boot.started"=>{
                    let prior:Vec<_>=open.iter().filter(|(_,start)|start.ts_orig.at<event.ts_orig.at&&start.payload["boot_id"]!=event.payload["boot_id"])
                        .map(|(scope,_)|scope.clone()).collect();
                    for scope in prior{let start=open.remove(&scope).unwrap();outputs.push(interval(start,None,Some(event),state));}
                },
                "unit.started"=>{
                    // Oneshot exits can precede the manager's start-job completion.
                    if event.payload["invocation_id"].is_string()&&events.iter().any(|stop|stop.event_type=="unit.stopped"
                        &&stop.ts_orig.at<=event.ts_orig.at&&same_invocation(event,stop)){continue}
                    let scope=unit_scope(event);
                    if let Some(previous)=open.insert(scope,event){
                        if previous.payload["invocation_id"].is_string()&&previous.payload["invocation_id"]==event.payload["invocation_id"]{
                            open.insert(unit_scope(previous),previous);
                        }else{outputs.push(interval(previous,Some(event),None,state));}
                    }
                },
                "unit.stopped"=>{let scope=unit_scope(event);if open.get(&scope).is_some_and(|start|same_invocation(start,event)){
                    let start=open.remove(&scope).unwrap();outputs.push(interval(start,Some(event),None,state));}},_=>{}
            }
        }
        outputs.extend(open.values().map(|start|interval(start,None,None,state)));
    }else{
        let (begin,finish)=if state=="machine.boot"{("boot.started","boot.started")}else{("sleep.started","sleep.ended")};
        let mut open:Option<&Event>=None;
        for event in &events{
            if event.event_type=="boot.started"&&let Some(previous)=open.take(){
                if previous.ts_orig.at<event.ts_orig.at{outputs.push(interval(previous,None,Some(event),state));}
            }
            if event.event_type==begin{
                if let Some(previous)=open.replace(event){outputs.push(interval(previous,Some(event),None,state));}
            }else if event.event_type==finish&&let Some(previous)=open.take(){
                if previous.payload["boot_id"]==event.payload["boot_id"]{outputs.push(interval(previous,Some(event),None,state));}
                else{open=Some(previous);}
            }
        }
        outputs.extend(open.map(|start|interval(start,None,None,state)));
    }
    outputs.retain(|event|event.ts_orig.at.is_some_and(|at|at>=from&&at<until));
    let waiting=outputs.iter().any(|event|event.payload["ended_at"].is_null()&&event.payload.get("fenced_at").is_none_or(Value::is_null));
    Computed{error:None,outputs,waiting}
}
