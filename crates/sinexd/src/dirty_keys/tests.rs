use super::*;
use serde_json::json;

#[test]
fn dirty_selection_uses_the_configured_activity_break() {
    let configuration = sinex_derivations::Configuration {
        activity: sinex_derivations::activity::ActivityProfile {
            break_seconds: 1_800,
        },
        ..Default::default()
    };
    let keys = Registered::new(
        sinex_derivations::keyed_with(&configuration),
        TimeSettings::operator_default(),
    );
    let at = Timestamp::from_unix_secs(1_718_438_400).unwrap();
    let payload = json!({"scope":"host-a", "started_at":at, "ended_at":at});
    let selected = keys.nearby_inputs(&KeyInput {
        event_id: sinex_model::EventId::new(),
        event_type: "activity.window",
        ts_orig: Some(at.0),
        payload: &payload,
    });
    assert_eq!(selected.len(), 1);
    assert_eq!(
        selected[0].start.unwrap().unix_timestamp(),
        at.0.unix_timestamp() - 1_800
    );
    assert_eq!(
        selected[0].end.unwrap().unix_timestamp_nanos(),
        at.unix_ns() + 1_800_000_000_001
    );
}

#[test]
fn dirty_keys_use_the_same_local_hour_as_the_runner() {
    let keys = Registered::new(
        vec![Box::new(sinex_derivations::activity::ActivitySpans)],
        TimeSettings::new("Asia/Kolkata", 6, 30).unwrap(),
    );
    let at = Timestamp::from_unix_secs(1_718_438_400).unwrap();
    let payload = json!({});
    let selected = keys.keys(&KeyInput {
        event_id: sinex_model::EventId::new(),
        event_type: "key.pressed",
        ts_orig: Some(at.0),
        payload: &payload,
    });
    let hour = Timestamp::from_unix_secs(1_718_436_600).unwrap();
    assert!(
        selected
            .iter()
            .any(|(name, key)| *name == "activity.spans" && key["hour"] == json!(hour))
    );
}

struct IdentityKeys(sinex_model::EventId);
impl sinex_derivations::Keyed for IdentityKeys {
    const NAME:&'static str="fixture.identity";const VERSION:u32=1;
    const INPUTS:&'static[&'static str]=&["fixture.reading"];const OUTPUTS:&'static[&'static str]=&[];
    fn keys_for(&self,input:&KeyView<'_>,_:&Env)->Vec<Value>{vec![json!({"event":input.event_id})]}
    fn inputs(&self,_:&Value,_:&Env)->Option<sinex_derivations::InputQuery>{None}
    fn compute(&self,_:&Value,_:&[sinex_model::Event],_:&Env)->sinex_derivations::Computed{Default::default()}
    fn key_state(&self,_:&Value,_:&Env)->sinex_derivations::KeyState{sinex_derivations::KeyState::Closed}
    fn nearby_inputs(&self,input:&KeyView<'_>,_:&Env)->Option<sinex_derivations::InputQuery>{
        let mut kind=sinex_derivations::Kind::all("fixture.reading");kind.equal.push(("event".into(),json!(input.event_id)));
        Some(sinex_derivations::InputQuery::all(vec![kind]))
    }
    fn affected_ranges(&self,input:&KeyView<'_>,_:&Env)->Vec<sinex_derivations::AffectedRange>{
        vec![sinex_derivations::AffectedRange{open_field:None,output_type:if input.event_id==self.0{"fixture.expected"}else{"fixture.other"},start_field:"at",end_field:"at",start:Timestamp::from_unix_secs(1).unwrap(),end:Timestamp::from_unix_secs(2).unwrap()}]
    }
}

#[test]
fn every_key_view_preserves_identity_when_equal_payloads_are_reinterpreted(){
    let first=sinex_model::EventId::new();let second=sinex_model::EventId::new();
    let payload=json!({"event":sinex_model::EventId::new(),"text":"same content"});
    let keys=Registered::new(vec![Box::new(IdentityKeys(first))],TimeSettings::new("UTC",0,0).unwrap());
    for event_id in [first,second]{
        let input=KeyInput{event_id,event_type:"fixture.reading",ts_orig:None,payload:&payload};
        assert_eq!(keys.keys(&input),vec![("fixture.identity".into(),json!({"event":event_id}))]);
        assert_eq!(keys.nearby_inputs(&input)[0].kinds[0].contains["event"],json!(event_id));
        assert_eq!(keys.affected_ranges(&input)[0].output_type,if event_id==first{"fixture.expected"}else{"fixture.other"});
    }
}
