use super::*;
use serde_json::json;

#[test]
fn dirty_selection_uses_the_configured_activity_break() {
    let configuration = sinex_derivations::Configuration {
        activity: sinex_derivations::activity::ActivityProfile { break_seconds: 1_800 },
        ..Default::default()
    };
    let keys = Registered::new(sinex_derivations::keyed_with(&configuration), TimeSettings::operator_default());
    let at = Timestamp::from_unix_secs(1_718_438_400).unwrap();
    let payload = json!({"scope":"host-a", "started_at":at, "ended_at":at});
    let selected = keys.nearby_inputs(&KeyInput { event_type:"activity.window", ts_orig:Some(at.0), payload:&payload });
    assert_eq!(selected.len(), 1);
    assert_eq!(selected[0].start.unwrap().unix_timestamp(), at.0.unix_timestamp()-1_800);
    assert_eq!(selected[0].end.unwrap().unix_timestamp_nanos(), at.unix_ns()+1_800_000_000_001);
}

#[test]
fn dirty_keys_use_the_same_local_hour_as_the_runner() {
    let keys = Registered::new(vec![Box::new(sinex_derivations::activity::ActivitySpans)], TimeSettings::new("Asia/Kolkata",6,30).unwrap());
    let at = Timestamp::from_unix_secs(1_718_438_400).unwrap();
    let payload = json!({});
    let selected = keys.keys(&KeyInput { event_type:"key.pressed", ts_orig:Some(at.0), payload:&payload });
    let hour = Timestamp::from_unix_secs(1_718_436_600).unwrap();
    assert!(selected.iter().any(|(name,key)| *name=="activity.spans" && key["hour"] == json!(hour)));
}
