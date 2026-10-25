use super::*;
use serde_json::json;

#[devkit::test]
fn variants_decode_to_their_data() {
    let v = json!({"urgency": {"type": "y", "data": 1}, "list": {"type": "as", "data": ["a"]},
                   "nested": {"type": "a{sv}", "data": {"k": {"type": "s", "data": "v"}}}});
    assert_eq!(
        decode(&v),
        json!({"urgency": 1, "list": ["a"], "nested": {"k": "v"}})
    );
}

#[devkit::test]
fn a_monitored_call_parses() {
    let line = br#"{"type":"method_call","sender":":1.2","path":"/org/freedesktop/Notifications","interface":"org.freedesktop.Notifications","member":"Notify","payload":{"type":"susssasa{sv}i","data":["app",0,"","s","b",[],{"urgency":{"type":"y","data":1}},1500]}}"#;
    let m = Message::parse(line).unwrap();
    assert_eq!(m.member, "Notify");
    assert_eq!(m.args.len(), 8);
    assert_eq!(m.args[6], json!({"urgency": 1}));
}
