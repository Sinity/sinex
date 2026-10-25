use super::*;
use serde_json::json;

fn read(member: &str, rows: &[&[u8]]) -> Vec<RedditEvent> {
    let manifest = sinex_model::Manifest::from_json(json!({"path": member}));
    let ctx = RecordContext {
        manifest: &manifest,
        member: None,
    };
    let mut carry = Carry::default();
    let mut out = vec![];
    for row in rows {
        let (events, next) = RedditParser.parse(row, &carry, &ctx).unwrap();
        out.extend(events);
        carry = next;
    }
    out
}

#[devkit::test]
fn comments_votes_saves_and_messages() {
    let c = read("comments.csv", &[
        b"id,permalink,date,ip,subreddit,gildings,link,parent,body,media\n",
        b"ck1,https://r/x/ck1/,2014-08-27 00:59:46 UTC,1.2.3.4,Futurology,0,https://r/x/,t3_x,hello,\n",
    ]);
    let RedditEvent::Post(p) = &c[0] else {
        panic!()
    };
    assert_eq!(p.kind, "comment");
    assert_eq!(p.community.as_deref(), Some("Futurology"));
    assert_eq!(p.created_at, fields::rfc3339("2014-08-27T00:59:46Z").ok());
    assert!(
        !serde_json::to_string(p).unwrap().contains("1.2.3.4"),
        "ip dropped"
    );
    let v = read(
        "post_votes.csv",
        &[b"id,permalink,direction\n", b"11a,https://r/p/,none\n"],
    );
    let RedditEvent::Vote(v) = &v[0] else {
        panic!()
    };
    assert!(v.withdrawn_at.is_some());
    let s = read(
        "saved_posts.csv",
        &[b"id,permalink\n", b"1cc,https://r/s/\n"],
    );
    assert!(matches!(&s[0], RedditEvent::Saved(b) if b.native_id == "post:1cc"));
    let m = read(
        "messages_archive.csv",
        &[
            b"id,permalink,thread_id,date,ip,from,to,subject,body\n",
            b"4s6,https://r/m,t4_4s5,2016-01-16 13:03:57 UTC,,/u/Sinity,/u/other,re: hi,text\n",
        ],
    );
    let RedditEvent::Message(m) = &m[0] else {
        panic!()
    };
    assert_eq!(m.thread, "t4_4s5");
    assert_eq!(m.sender, "/u/Sinity");
}

#[devkit::test]
fn a_new_member_starts_with_its_header() {
    let manifest = sinex_model::Manifest::from_json(json!({"path": "posts.csv"}));
    let ctx = RecordContext {
        manifest: &manifest,
        member: None,
    };
    let carry = Carry {
        member: "comments.csv".into(),
        header: vec!["id".into()],
    };
    let (events, next) = RedditParser
        .parse(&&b"id,permalink,date\n"[..], &carry, &ctx)
        .unwrap();
    assert!(events.is_empty());
    assert_eq!(next.member, "posts.csv");
}
