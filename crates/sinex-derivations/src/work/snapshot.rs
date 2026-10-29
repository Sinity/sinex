//! Projection of Polylogue's atomic version-3 work snapshot. The full typed
//! producer record stays in the sole parent; this decoder names only fields
//! consumed by work attribution.
use super::*;
use serde::Deserialize;

const REDUCER_VERSION: u32 = 1;

#[derive(Deserialize)]
struct Envelope {
    session: String,
    reducer_version: u32,
    source_position: i64,
    complete: bool,
    #[serde(default)]
    native_unavailable: Vec<Value>,
    record: Product,
}
#[derive(Deserialize)]
struct Product { product: String, snapshot: Snapshot }
#[derive(Deserialize)]
struct Snapshot {
    reducer_version: u32,
    position: i64,
    session_id: String,
    session: Option<Session>,
    retired: bool,
    unavailable: Vec<String>,
    sources: Vec<Source>,
    native: Vec<Value>,
    messages: Vec<Message>,
    calls: Vec<Call>,
    turns: Vec<Turn>,
}
#[derive(Deserialize)]
struct Source {selected:bool,projection:Option<Projection>}
#[derive(Deserialize)]
struct Projection {block_count:u64,block_ordinals:Vec<u64>}
#[derive(Deserialize)]
struct Session { id: String }
#[derive(Deserialize)]
struct Message {
    message: String,
    role: String,
    written_by: String,
    prompt_kind: Option<String>,
    at: Option<Timestamp>,
    #[serde(default)]
    at_evidence: WorkTimeEvidence,
}
#[derive(Deserialize)]
struct Call {
    call: String,
    session: String,
    action: String,
    path: Option<String>,
    at: Option<Timestamp>,
    #[serde(default)]
    at_evidence: WorkTimeEvidence,
    result_at: Option<Timestamp>,
    #[serde(default)]
    result_at_evidence: WorkTimeEvidence,
    outcome: Outcome,
    revision: Option<String>,
    check_name: Option<String>,
    check_passed: Option<bool>,
}
#[derive(Deserialize)]
#[serde(tag="kind",rename_all="snake_case")]
enum Outcome { Ok, Error { #[serde(rename="reason")] _reason: Option<Value> }, NoResult, Unknown { #[serde(rename="reason")] _reason: String } }
#[derive(Deserialize)]
struct Turn { turn:String,session:String,start:Option<Timestamp>,#[serde(default)]start_evidence:WorkTimeEvidence,
    end:Option<Timestamp>,#[serde(default)]end_evidence:WorkTimeEvidence,errors:u64 }

struct Entry {
    kind: &'static str, id: String, at: Option<Timestamp>,at_evidence:WorkTimeEvidence,end:Option<Timestamp>,end_evidence:Option<WorkTimeEvidence>,
    agency: Agency, action: String, path: Option<String>, failed: bool,
    revision: Option<String>, check_name: Option<String>, check_passed: Option<bool>,
}
#[allow(clippy::too_many_arguments)]
fn coverage(parent:&Event, session:&str, reducer:Option<u32>, state:WorkCoverageState,
    eligible:Option<u64>,timed:Option<u64>,untimed:Option<u64>,reasons:BTreeMap<String,u64>)->Event {
    derived(&WorkCoverage { session:session.into(), snapshot:evidence(parent), reducer_version:reducer,
        state,eligible_entries:eligible,timed_entries:timed,untimed_entries:untimed,reasons },WorkActs::NAME,[parent])
}
fn unavailable(parent:&Event,session:&str,reducer:Option<u32>,reason:&str)->Computed {
    Computed {outputs:vec![coverage(parent,session,reducer,WorkCoverageState::Unavailable,None,None,None,
        [(reason.into(),1)].into())],..Default::default()}
}

pub(super) fn compute(key:&Value,inputs:&[Event])->Computed {
    let Some(session)=key["session"].as_str().filter(|id|!id.is_empty()) else {return Computed::default()};
    let mut snapshots:Vec<_>=inputs.iter().filter(|event|event.event_type=="polylogue.work_snapshot"&&event.payload["session"]==session).collect();
    snapshots.sort_by_cached_key(|event|stable(event));
    let Some(parent)=snapshots.first().copied() else{return Computed::default()};
    // Reading all versions is intentional: an unsupported version is visible,
    // rather than silently falling back to a stale supported snapshot.
    if let Some(unsupported)=snapshots.iter().find(|event|event.kind_version!=3||event.payload["reducer_version"].as_u64()!=Some(u64::from(REDUCER_VERSION))) {
        return unavailable(unsupported,session,unsupported.payload["reducer_version"].as_u64().and_then(|v|u32::try_from(v).ok()),"unsupported_reducer_or_kind_version")
    }
    if snapshots.len()!=1{return unavailable(parent,session,Some(REDUCER_VERSION),"conflicting_live_snapshots")}
    let envelope:Envelope=match serde_json::from_value(parent.payload.clone()) {
        Ok(envelope)=>envelope,Err(_)=>return unavailable(parent,session,Some(REDUCER_VERSION),"invalid_snapshot"),
    };
    let snapshot=envelope.record.snapshot;
    if !envelope.complete||envelope.record.product!="work_snapshot"||envelope.session!=session||snapshot.session_id!=session||snapshot.session.as_ref().is_some_and(|metadata|metadata.id!=session)
        ||snapshot.reducer_version!=envelope.reducer_version||snapshot.position!=envelope.source_position {
        return unavailable(parent,session,Some(envelope.reducer_version),"incomplete_or_inconsistent_snapshot")
    }
    let metadata=&parent.payload;
    if snapshot.retired {
        if snapshot.session.is_some()||!snapshot.messages.is_empty()||!snapshot.calls.is_empty()||!snapshot.turns.is_empty()||!snapshot.sources.is_empty()||!snapshot.native.is_empty()
            ||["repo","branch","kind","mode","machine","git_identity","terminal","boot_id"].iter().any(|field|metadata.get(*field).is_some_and(|value|!value.is_null()))
            ||metadata["objectives"].as_array().is_some_and(|values|!values.is_empty()) {
            return unavailable(parent,session,Some(REDUCER_VERSION),"invalid_retired_snapshot")
        }
        return Computed{outputs:vec![coverage(parent,session,Some(REDUCER_VERSION),WorkCoverageState::Complete,Some(0),Some(0),Some(0),[("retired".into(),1)].into())],..Default::default()}
    }
    let sparse=snapshot.sources.iter().filter(|source|source.selected&&source.projection.as_ref().is_some_and(|projection|projection.block_ordinals.len()as u64!=projection.block_count||projection.block_ordinals.iter().any(|ordinal|*ordinal>=projection.block_count)||projection.block_ordinals.iter().collect::<BTreeSet<_>>().len()!=projection.block_ordinals.len())).count();
    let mut entries=Vec::new();let mut uncertain_outcomes=0u64;
    for message in snapshot.messages {
        if message.role=="user"&&message.written_by=="human"&&message.prompt_kind.as_deref()==Some("prose")
            &&metadata["kind"]=="main"&&metadata.get("mode").is_none_or(Value::is_null) {
            entries.push(Entry{kind:"message",id:message.message,at:message.at,at_evidence:message.at_evidence,end:None,end_evidence:None,agency:Agency::Operator,
                action:"prompt".into(),path:None,failed:false,revision:None,check_name:None,check_passed:None});
        }
    }
    for call in snapshot.calls {
        if call.session!=session{return unavailable(parent,session,Some(REDUCER_VERSION),"entry_session_mismatch")}
        uncertain_outcomes+=u64::from(matches!(&call.outcome,Outcome::Unknown{..}));
        let failed=matches!(&call.outcome,Outcome::Error{..})||call.check_passed==Some(false);
        entries.push(Entry{kind:"call",id:call.call,at:call.at,at_evidence:call.at_evidence,end:call.result_at,end_evidence:Some(call.result_at_evidence),agency:Agency::Delegated,
            action:call.action,path:call.path,failed,revision:call.revision,check_name:call.check_name,check_passed:call.check_passed});
    }
    for turn in snapshot.turns {
        if turn.session!=session{return unavailable(parent,session,Some(REDUCER_VERSION),"entry_session_mismatch")}
        entries.push(Entry{kind:"turn",id:turn.turn,at:turn.start,at_evidence:turn.start_evidence,end:turn.end,end_evidence:Some(turn.end_evidence),agency:Agency::Delegated,
            action:"turn".into(),path:None,failed:turn.errors>0,revision:None,check_name:None,check_passed:None});
    }
    let mut identities=BTreeSet::new();
    if entries.iter().any(|entry|entry.id.is_empty()||!identities.insert((entry.kind,entry.id.clone()))) {
        return unavailable(parent,session,Some(REDUCER_VERSION),"duplicate_or_empty_entry_identity")
    }
    let eligible=entries.len()as u64;
    let objective=objective(metadata);
    let episode=objective.as_ref().map(|objective|json!([objective.kind,objective.repository,objective.reference]).to_string());
    let mut outputs=Vec::new();let mut reasons=BTreeMap::<String,u64>::new();let mut untimed=0;
    if !envelope.native_unavailable.is_empty(){reasons.insert("native_metadata_unavailable".into(),envelope.native_unavailable.len()as u64);}
    if !snapshot.unavailable.is_empty(){reasons.insert("snapshot_source_unavailable".into(),snapshot.unavailable.len()as u64);}
    if sparse>0 {reasons.insert("sparse_selected_source".into(),sparse as u64);}
    if uncertain_outcomes>0 {reasons.insert("tool_outcome_unknown".into(),uncertain_outcomes);}
    if snapshot.session.is_none(){reasons.insert("session_metadata_unavailable".into(),1);}
    entries.sort_by(|left,right|(left.at,left.kind,&left.id).cmp(&(right.at,right.kind,&right.id)));
    for entry in entries {
        let Some(at)=entry.at_evidence.instant(entry.at) else{untimed+=1;
            let reason=if entry.at.is_none(){"entry_time_unknown"}else if entry.at_evidence.quality==sinex_model::TimeQuality::Unknown{"entry_time_quality_unknown"}else{"entry_time_not_instant"};
            *reasons.entry(reason.into()).or_default()+=1;continue};
        let ended_at=entry.end_evidence.as_ref().and_then(|evidence|evidence.instant(entry.end)).filter(|end|*end>=at);
        if entry.agency==Agency::Delegated&&ended_at.is_none(){*reasons.entry("entry_duration_unknown".into()).or_default()+=1;}
        let identity=json!([session,entry.kind,entry.id]).to_string();
        let mut citation=evidence(parent);citation.record=format!("{}#{identity}",citation.record);
        outputs.push(derived(&WorkAct {
            act:identity,session:session.into(),episode:episode.clone(),objective:objective.clone(),
            label:objective.as_ref().map(|objective|objective.reference.clone()),label_source:evidence(parent),
            machine:metadata["machine"].as_str().map(str::to_owned),at,at_evidence:entry.at_evidence,ended_at,ended_at_evidence:entry.end_evidence,evidence:citation,agency:entry.agency,
            action:entry.action,path:entry.path,failed:entry.failed,revision:entry.revision,check_name:entry.check_name,
            check_passed:entry.check_passed,cwd:metadata.get("cwd").or_else(||metadata.pointer("/record/snapshot/session/cwd")).and_then(Value::as_array).and_then(|paths|paths.first()).and_then(Value::as_str).map(str::to_owned),
            git_identity:metadata.get("git_identity").cloned().and_then(|value|serde_json::from_value(value).ok()),
            terminal:metadata.get("terminal").cloned().and_then(|value|serde_json::from_value(value).ok()),
        },WorkActs::NAME,[parent]));
    }
    let timed=outputs.len()as u64;
    let state=if reasons.is_empty(){WorkCoverageState::Complete}else{WorkCoverageState::Partial};
    outputs.push(coverage(parent,session,Some(REDUCER_VERSION),state,Some(eligible),Some(timed),Some(untimed),reasons));
    Computed{outputs,..Default::default()}
}
