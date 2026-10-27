use super::*;
use serde_json::json;

fn one(v: Value) -> WykopEvent {
    WykopParser
        .parse(
            v.as_object().unwrap(),
            &(),
            &sinex_source_core::RecordContext::none(),
        )
        .unwrap()
        .0
        .remove(0)
}

#[devkit::test]
fn entries_are_warsaw_time_not_utc() {
    let WykopEvent::Post(p) = one(json!({"kind": "entry", "entry_id": 76315507,
        "entry_created_at": "2024-05-18 06:53:25", "entry_content": "x #tag",
        "entry_tags": ["tag"], "votes_score": 0, "page": 1, "platform": "wykop"}))
    else {
        panic!()
    };
    // 06:53:25 CEST is 04:53:25 UTC.
    assert_eq!(p.created_at, fields::rfc3339("2024-05-18T04:53:25Z").ok());
    assert_eq!(p.native_id, "76315507");
    assert_eq!(p.tags, vec!["tag"]);
    assert!(!p.extra.contains_key("page"));
}

#[devkit::test]
fn activity_feed_repeats_own_posts_and_adds_down_votes_and_favourites() {
    let rows = WykopParser
        .parse(
            json!({"kind": "action", "resource": "entry", "id": 5, "slug": "s",
                "created_at": "2025-10-31 16:50:54", "content": "#a", "tags": ["a"],
                "author": {"username": "Sinity"}, "voted": 0, "favourite": false,
                "votes": {"up": 2, "down": 0}})
            .as_object()
            .unwrap(),
            &(),
            &sinex_source_core::RecordContext::none(),
        )
        .unwrap()
        .0;
    let [WykopEvent::Post(p)] = rows.as_slice() else {
        panic!("{rows:?}")
    };
    assert_eq!(
        (p.kind.as_str(), p.native_id.as_str(), p.score),
        ("entry", "5", Some(2))
    );
    assert_eq!(p.url.as_deref(), Some("https://wykop.pl/wpis/5/s"));
    let rows = WykopParser
        .parse(
            json!({"kind": "action", "resource": "link", "id": 7,
                "author": {"username": "other"}, "voted": -1, "favourite": true})
            .as_object()
            .unwrap(),
            &(),
            &sinex_source_core::RecordContext::none(),
        )
        .unwrap()
        .0;
    assert!(matches!(&rows[0], WykopEvent::Vote(v) if v.direction == "down"));
    assert!(matches!(&rows[1], WykopEvent::Saved(s) if s.native_id == "link:7"));
    // An item the operator only commented on yields nothing here.
    let rows = WykopParser
        .parse(
            json!({"kind": "action", "resource": "entry", "id": 9,
                "author": {"username": "other"}, "voted": 0, "favourite": false})
            .as_object()
            .unwrap(),
            &(),
            &sinex_source_core::RecordContext::none(),
        )
        .unwrap()
        .0;
    assert!(rows.is_empty());
}

#[devkit::test]
fn notifications_and_blacklists() {
    let n = one(
        json!({"kind": "notification_group_item", "id": "6Rj", "type":
        "new_entry_with_observed_tag", "created_at": "2025-12-19 13:51:54", "read": 0,
        "entry": {"id": 8, "slug": "x"}, "tag": {"name": "gielda"},
        "user": {"username": "u"}}),
    );
    let WykopEvent::Notification(n) = n else {
        panic!()
    };
    assert_eq!(n.target.as_deref(), Some("entry:8"));
    assert_eq!(n.channel.as_deref(), Some("gielda"));
    assert_eq!(n.read, Some(false));
    let s = one(
        json!({"kind": "setting_item", "endpoint": "settings/blacklists/users",
        "username": "nioctiB", "created_at": "1970-01-01 01:33:42"}),
    );
    let WykopEvent::Setting(s) = s else { panic!() };
    assert_eq!(
        (s.setting.as_str(), s.value.as_str()),
        ("blacklist/users", "nioctiB")
    );
}

#[devkit::test]
fn votes_have_no_time_of_their_own() {
    let WykopEvent::Vote(v) = one(json!({"kind": "link_zakopane", "link_id": 7,
        "link_created_at": "2024-01-08 17:14:47", "link_title": "t"}))
    else {
        panic!()
    };
    assert_eq!(v.direction, "down");
    assert_eq!(v.cast_at, None);
    assert_eq!(
        v.target_created_at,
        fields::rfc3339("2024-01-08T16:14:47Z").ok(),
        "CET in winter"
    );
    assert!(
        WykopParser
            .parse(
                json!({"kind": "nope"}).as_object().unwrap(),
                &(),
                &sinex_source_core::RecordContext::none()
            )
            .is_err()
    );
}
