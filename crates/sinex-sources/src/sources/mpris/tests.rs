use super::*;
use sinex_model::Timestamp;

fn run(lines: &[&str]) -> Vec<PlayerChanged> {
    let mut carry = BTreeMap::new();
    let mut out = Vec::new();
    for l in lines {
        let rec: Map<String, Value> = serde_json::from_slice(&live_stream::frame(
            Timestamp::from_unix_ns(1).unwrap(),
            l.as_bytes(),
        ))
        .unwrap();
        let (e, c) = MprisParser
            .parse(&rec, &carry, &crate::RecordContext::none())
            .unwrap();
        out.extend(e);
        carry = c;
    }
    out
}

#[devkit::test]
fn snapshot_names_the_sender_of_later_changes() {
    let out = run(&[
        r#"{"snapshot":{"name":"org.mpris.MediaPlayer2.spotify","owner":":1.5","properties":{"PlaybackStatus":"Paused","Metadata":{"xesam:title":"Song","xesam:artist":["A"],"mpris:length":200000000}}}}"#,
        r#"{"type":"signal","sender":":1.5","member":"PropertiesChanged","payload":{"type":"sa{sv}as","data":["org.mpris.MediaPlayer2.Player",{"PlaybackStatus":{"type":"s","data":"Playing"}},[]]}}"#,
    ]);
    assert!(
        out[0].snapshot && out[0].title.as_deref() == Some("Song") && out[0].artists == vec!["A"]
    );
    assert_eq!(out[1].player, "org.mpris.MediaPlayer2.spotify");
    assert_eq!(out[1].status.as_deref(), Some("Playing"));
    assert!(!out[1].snapshot);
}

#[devkit::test]
fn owner_changes_follow_players_that_appear_later() {
    let out = run(&[
        r#"{"type":"signal","member":"NameOwnerChanged","payload":{"type":"sss","data":["org.mpris.MediaPlayer2.mpv","",":1.9"]}}"#,
        r#"{"type":"signal","sender":":1.9","member":"PropertiesChanged","payload":{"type":"sa{sv}as","data":["org.mpris.MediaPlayer2.Player",{"Metadata":{"type":"a{sv}","data":{"xesam:url":{"type":"s","data":"file:///x.mkv"}}}},[]]}}"#,
        r#"{"type":"signal","sender":":1.9","member":"PropertiesChanged","payload":{"type":"sa{sv}as","data":["org.mpris.MediaPlayer2",{"Fullscreen":{"type":"b","data":true}},[]]}}"#,
    ]);
    assert_eq!(out.len(), 1);
    assert_eq!(out[0].player, "org.mpris.MediaPlayer2.mpv");
    assert_eq!(out[0].url.as_deref(), Some("file:///x.mkv"));
}
