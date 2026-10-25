use super::*;
use serde_json::json;

#[devkit::test]
fn gdpr_threads_key_by_participants() {
    let doc = json!({"participants": ["B", "A"], "threadName": "B_4", "messages": [
        {"senderName": "A", "text": "hi", "timestamp": 1_723_458_102_017i64, "type": "text",
         "isUnsent": false, "media": [], "reactions": [{"reaction": "x", "actor": "B"}]},
        {"senderName": "B", "timestamp": 1_723_462_266_668i64, "type": "media", "isUnsent": true,
         "media": [{"uri": "media/a.webp"}]}]});
    let (out, ()) = MessengerParser
        .parse(doc.as_object().unwrap(), &(), &crate::RecordContext::none())
        .unwrap();
    assert_eq!(out.len(), 2);
    assert_eq!(out[0].thread, "A|B");
    assert_eq!(out[0].reactions.len(), 1);
    assert_eq!(out[1].media, vec!["media/a.webp"]);
    assert_eq!(out[1].unsent_at, Some(out[1].sent_at));
    assert_eq!(out[1].text, None);
}

#[devkit::test]
fn older_export_threads_are_repaired() {
    let doc = json!({"participants": [{"name": "Micha\u{00c5}\u{0082}"}, {"name": "B"}],
        "title": "B", "messages": [{"sender_name": "Micha\u{00c5}\u{0082}",
        "timestamp_ms": 1_500_000_000_000i64, "content": "za\u{00c5}\u{00bc}\u{00c3}\u{00b3}\u{00c5}\u{0082}\u{00c4}\u{0087}",
        "type": "Generic", "photos": [{"uri": "messages/photos/p.jpg"}]}]});
    let (out, ()) = MessengerParser
        .parse(doc.as_object().unwrap(), &(), &crate::RecordContext::none())
        .unwrap();
    assert_eq!(out[0].sender, "Michał");
    assert_eq!(out[0].text.as_deref(), Some("zażółć"));
    assert_eq!(out[0].thread, "B|Michał");
    assert_eq!(out[0].media, vec!["messages/photos/p.jpg"]);
    assert_eq!(repair("plain ascii"), "plain ascii");
    assert_eq!(repair("już"), "już", "real text is left alone");
}
