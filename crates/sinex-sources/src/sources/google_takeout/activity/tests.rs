use super::super::cells;
use super::*;

fn cell(service: &str, body: &str, time: &str) -> Cell {
    let html = format!(
        r#"<div class="outer-cell"><p class="mdl-typography--title">{service}<br></p></div><div class="content-cell mdl-typography--body-1">{body}<br>{time}<br></div><div class="content-cell mdl-typography--caption"><b>Products:</b><br>&emsp;{service}<br></div>"#
    );
    cells::parse(&html).unwrap()
}

#[devkit::test]
fn linked_and_unlinked_actions() {
    let c = cell(
        "Search",
        r#"Visited <a href="https://example.org/a">A page</a>"#,
        "Aug 26, 2026, 9:34:44 PM CEST",
    );
    let (out, ()) = MyActivityParser
        .parse(&c, &(), &crate::RecordContext::none())
        .unwrap();
    assert_eq!(out[0].verb.as_deref(), Some("Visited"));
    assert_eq!(out[0].title, "A page");
    assert_eq!(out[0].title_url.as_deref(), Some("https://example.org/a"));
    assert_eq!(out[0].unlinked_text, None);
    let c = cell(
        "Takeout",
        "Downloaded a Takeout archive",
        "Dec 15, 2025, 5:15:21 AM CEST",
    );
    let (out, ()) = MyActivityParser
        .parse(&c, &(), &crate::RecordContext::none())
        .unwrap();
    assert_eq!(out[0].title_url, None);
    assert_eq!(
        out[0].unlinked_text.as_deref(),
        Some("Downloaded a Takeout archive")
    );
}

#[devkit::test]
fn watches_keep_unknown_titles_unknown() {
    let c = cell(
        "YouTube",
        r#"Watched <a href="https://www.youtube.com/watch?v=abc123">A talk</a><br><a href="https://www.youtube.com/channel/C">Chan</a>"#,
        "Aug 26, 2026, 9:34:44 PM CEST",
    );
    let (out, ()) = YouTubeParser
        .parse(&c, &(), &crate::RecordContext::none())
        .unwrap();
    let YouTubeEvent::Watched(w) = &out[0] else {
        panic!()
    };
    assert_eq!(w.video_id.as_deref(), Some("abc123"));
    assert_eq!(w.title.as_deref(), Some("A talk"));
    assert_eq!(w.channel.as_deref(), Some("Chan"));
    let gone = cell(
        "YouTube",
        r#"Watched <a href="https://www.youtube.com/watch?v=gone1">https://www.youtube.com/watch?v=gone1</a>"#,
        "Aug 26, 2026, 9:34:44 PM CEST",
    );
    let (out, ()) = YouTubeParser
        .parse(&gone, &(), &crate::RecordContext::none())
        .unwrap();
    let YouTubeEvent::Watched(w) = &out[0] else {
        panic!()
    };
    assert_eq!(w.title, None);
    assert_eq!(w.channel, None);
    let removed = cell(
        "YouTube",
        "Watched a video that has been removed",
        "Aug 26, 2026, 9:34:44 PM CEST",
    );
    let (out, ()) = YouTubeParser
        .parse(&removed, &(), &crate::RecordContext::none())
        .unwrap();
    let YouTubeEvent::Watched(w) = &out[0] else {
        panic!()
    };
    assert_eq!((w.video_id.as_ref(), w.url.as_ref()), (None, None));
    assert_eq!(w.details[0], "Watched a video that has been removed");
}

#[devkit::test]
fn video_ids_from_every_url_form() {
    assert_eq!(
        video_id("https://www.youtube.com/watch?v=x1&t=3").as_deref(),
        Some("x1")
    );
    assert_eq!(video_id("https://youtu.be/x2?t=1").as_deref(), Some("x2"));
    assert_eq!(
        video_id("https://youtube.com/shorts/x3").as_deref(),
        Some("x3")
    );
    assert_eq!(video_id("https://example.org/"), None);
}

#[devkit::test]
fn selects_members() {
    assert!(select_my_activity(
        "Takeout/My Activity/Search/MyActivity.html"
    ));
    assert!(!select_my_activity("Takeout/My Activity/Assistant/x.mp3"));
    assert!(select_youtube(
        "Takeout/YouTube and YouTube Music/history/watch-history.html"
    ));
    assert!(!select_youtube(
        "Takeout/YouTube/my-comments/my-comments.html"
    ));
}

/// An export's cells as one My Activity page, each `(time line)` an entry
/// titled the same.
fn page(times: &[&str]) -> String {
    times
        .iter()
        .map(|time| {
            format!(
                r#"<div class="outer-cell"><p class="mdl-typography--title">Search<br></p></div><div class="content-cell mdl-typography--body-1">Visited <a href="https://example.org/a">A page</a><br>{time}<br></div><div class="content-cell mdl-typography--caption"><b>Products:</b><br>&emsp;Search<br></div>"#
            )
        })
        .collect()
}

/// The events and failure reasons of one export page, drained.
fn drained(html: &str) -> (Vec<(sinex_model::Timestamp, String)>, Vec<String>) {
    let (records, _) = frame(html.as_bytes());
    let chunks = crate::drain::drain(
        &MyActivityParser,
        crate::drain::DrainInput {
            source: "google.takeout.my-activity",
            class: Class::Open,
            material: sinex_model::MaterialId::new(),
            bytes: html.as_bytes().into(),
            records: &records,
            carry: serde_json::Value::Null,
            reading_order: &|_| sinex_model::ReadingOrder(0),
            limits: crate::drain::Limits::default(),
            manifest: crate::RecordContext::none().manifest,
        },
    )
    .unwrap();
    let (mut events, mut failures) = (Vec::new(), Vec::new());
    for outcome in chunks.into_iter().flat_map(|c| c.outcomes) {
        match outcome {
            sinex_model::RecordOutcome::Events { events: e, .. } => {
                events.extend(e.into_iter().map(|e| {
                    (
                        e.ts_orig.at.unwrap(),
                        e.equivalence_key.expect("keyed by content").0,
                    )
                }));
            }
            sinex_model::RecordOutcome::Failure { reason, .. } => failures.push(reason),
        }
    }
    (events, failures)
}

/// Three entries with one title at one wall-clock time under three zone
/// labels are three moments; a label the reader has no rule for is a record
/// failure carrying it, never an entry without a time; the same moments in a
/// second export written in another zone are the same three events.
#[devkit::test]
fn an_unparsed_time_never_merges_records() {
    let (first, failed) = drained(&page(&[
        "Jan 10, 2026, 9:00:00 AM UTC",
        "Jan 10, 2026, 9:00:00 AM CET",
        "Jan 10, 2026, 9:00:00 AM CEST",
        "Jan 10, 2026, 9:00:00 AM XYZT",
    ]));
    let instants: Vec<String> = first.iter().map(|(t, _)| format!("{t:?}")).collect();
    assert_eq!(first.len(), 3, "{instants:?}");
    let mut keys: Vec<&String> = first.iter().map(|(_, k)| k).collect();
    keys.sort();
    keys.dedup();
    assert_eq!(keys.len(), 3, "three moments, three keys");
    assert_eq!(failed.len(), 1);
    assert!(failed[0].contains("XYZT"), "{failed:?}");

    let (second, failed) = drained(&page(&[
        "Jan 10, 2026, 10:00:00 AM CET",
        "Jan 10, 2026, 8:00:00 AM UTC",
        "Jan 10, 2026, 8:00:00 AM CET",
        "Jan 10, 2026, 10:00:00 AM XYZT",
    ]));
    assert_eq!(failed.len(), 1);
    let mut a = first.clone();
    let mut b = second.clone();
    a.sort();
    b.sort();
    assert_eq!(
        a, b,
        "the same moments from an export written in another zone"
    );
}
