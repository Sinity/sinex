//! Google My Activity (`My Activity/<service>/MyActivity.html`) and YouTube
//! history (`YouTube*/history/{watch,search}-history.html`), on
//! `ExportArchive`, one record per activity cell (`cells`).
//!
//! My Activity gives every service's entries as `action.recorded`, keyed by
//! the instant and the link (or, without a link, the first line), never by the
//! title or the time text: Google restates both between exports. The YouTube
//! history gives `video.watched` and `video.searched`.
use super::cells::{CELL, Cell};
use crate::drivers::export_archive::{ExportArchive, Framing, PART_BYTES};
use crate::registry::{Declared, Driver, SourceSpec};
use crate::{Parser, RecordError};
use sinex_model::Class;
use sinex_model::kinds::account::{ActionRecorded, VideoSearched, VideoWatched};
use std::path::PathBuf;

pub const MY_ACTIVITY: SourceSpec = SourceSpec {
    name: "google.my-activity",
    driver: Driver::ExportArchive,
    consumer: "searches, visits and app use recorded by the Google account",
    class: Class::Open,
    parser: "google-my-activity",
    parser_version: 1,
    event_types: &["action.recorded"],
    framer: frame,
};

pub const YOUTUBE: SourceSpec = SourceSpec {
    name: "google.youtube-history",
    driver: Driver::ExportArchive,
    consumer: "videos watched and searched for",
    class: Class::Open,
    parser: "google-youtube-history",
    parser_version: 1,
    event_types: &["video.watched", "video.searched"],
    framer: frame,
};

pub fn frame(bytes: &[u8]) -> (Vec<(usize, usize)>, usize) {
    Framing::Delimited(CELL).frame(bytes, true)
}

pub struct MyActivityParser;

impl Parser for MyActivityParser {
    type Record<'a> = Cell;
    type Carry = ();
    type Payload = ActionRecorded;

    fn parse(&self, cell: &Cell, _: &()) -> Result<(Vec<ActionRecorded>, ()), RecordError> {
        let first = &cell.lines[0];
        let (verb, title, title_url, unlinked_text) = match first.links.first() {
            Some((href, text)) => {
                let verb = first
                    .text
                    .strip_suffix(text.as_str())
                    .or_else(|| first.text.split_once(text.as_str()).map(|(v, _)| v))
                    .map(|v| v.trim().to_owned())
                    .filter(|v| !v.is_empty());
                (verb, text.clone(), Some(href.clone()), None)
            }
            None => (None, first.text.clone(), None, Some(first.text.clone())),
        };
        Ok((
            vec![ActionRecorded {
                service: cell.service.clone(),
                at: cell.at,
                verb,
                title,
                title_url,
                unlinked_text,
                lines: cell.lines[1..].iter().map(|l| l.text.clone()).collect(),
                products: cell.section("Products"),
                locations: cell.section("Locations"),
                details: cell.section("Details"),
                zone_label: cell.zone_label.clone(),
            }],
            (),
        ))
    }
}

sinex_model::payload_enum! {
    #[derive(Clone, Debug, PartialEq)]
    pub enum YouTubeEvent {
        Watched(VideoWatched),
        Searched(VideoSearched),
    }
}

pub struct YouTubeParser;

impl Parser for YouTubeParser {
    type Record<'a> = Cell;
    type Carry = ();
    type Payload = YouTubeEvent;

    fn parse(&self, cell: &Cell, _: &()) -> Result<(Vec<YouTubeEvent>, ()), RecordError> {
        let first = &cell.lines[0];
        let service = "youtube".to_owned();
        if first.text.starts_with("Watched") {
            let (url, link_text) = first
                .links
                .first()
                .cloned()
                .ok_or_else(|| RecordError::new("watch entry without a video link"))?;
            let video_id = video_id(&url)
                .ok_or_else(|| RecordError::new(format!("no video ID in {url:?}")))?;
            // `Watched <url>`: the video is gone and Google omits its title and
            // channel; the field is unknown, not the URL.
            let title = (link_text != url && !link_text.is_empty()).then_some(link_text);
            let channel = cell.lines.get(1).and_then(|l| l.links.first().cloned());
            Ok((
                vec![YouTubeEvent::Watched(VideoWatched {
                    service,
                    video_id,
                    watched_at: cell.at,
                    url,
                    title,
                    channel: channel.as_ref().map(|(_, n)| n.clone()),
                    channel_url: channel.map(|(u, _)| u),
                    products: cell.section("Products"),
                    details: cell.section("Details"),
                })],
                (),
            ))
        } else if first.text.starts_with("Searched for") {
            let (url, query) = match first.links.first() {
                Some((u, q)) => (Some(u.clone()), q.clone()),
                None => (
                    None,
                    first.text["Searched for".len()..].trim().to_owned(),
                ),
            };
            Ok((
                vec![YouTubeEvent::Searched(VideoSearched {
                    service,
                    searched_at: cell.at,
                    query,
                    url,
                    products: cell.section("Products"),
                })],
                (),
            ))
        } else {
            // Other history lines (a visit to the YouTube Music home page) are
            // in My Activity as actions; here they are not a watch or search.
            Ok((vec![], ()))
        }
    }
}

/// The video ID in a YouTube URL: `watch?v=ID`, `youtu.be/ID`, `/shorts/ID`.
pub fn video_id(url: &str) -> Option<String> {
    let id = if let Some((_, query)) = url.split_once("watch?") {
        query
            .split('&')
            .find_map(|p| p.strip_prefix("v="))
            .map(str::to_owned)
    } else if let Some((_, rest)) = url.split_once("youtu.be/") {
        Some(rest.split(['?', '&', '#']).next()?.to_owned())
    } else if let Some((_, rest)) = url.split_once("/shorts/") {
        Some(rest.split(['?', '&', '#', '/']).next()?.to_owned())
    } else {
        None
    };
    id.filter(|i| !i.is_empty())
}

pub fn my_activity() -> Declared<MyActivityParser> {
    Declared {
        spec: MY_ACTIVITY,
        parser: MyActivityParser,
    }
}

pub fn youtube() -> Declared<YouTubeParser> {
    Declared {
        spec: YOUTUBE,
        parser: YouTubeParser,
    }
}

pub fn select_my_activity(member: &str) -> bool {
    member.contains("/My Activity/") && member.ends_with("/MyActivity.html")
}

pub fn select_youtube(member: &str) -> bool {
    member.contains("/history/")
        && member.contains("YouTube")
        && (member.ends_with("watch-history.html") || member.ends_with("search-history.html"))
}

pub fn my_activity_driver(path: PathBuf, host: String) -> ExportArchive {
    ExportArchive {
        path,
        host,
        select: select_my_activity,
        framing: Framing::Delimited(CELL),
        part_bytes: PART_BYTES,
    }
}

pub fn youtube_driver(path: PathBuf, host: String) -> ExportArchive {
    ExportArchive {
        path,
        host,
        select: select_youtube,
        framing: Framing::Delimited(CELL),
        part_bytes: PART_BYTES,
    }
}

#[cfg(test)]
mod tests {
    use super::super::cells;
    use super::*;

    fn cell(service: &str, body: &str, time: &str) -> Cell {
        let html = format!(
            r#"<div class="outer-cell"><p class="mdl-typography--title">{service}<br></p></div><div class="content-cell mdl-typography--body-1">{body}<br>{time}<br></div><div class="content-cell mdl-typography--caption"><b>Products:</b><br>&emsp;{service}<br></div>"#
        );
        cells::parse(&html).unwrap()
    }

    #[test]
    fn linked_and_unlinked_actions() {
        let c = cell(
            "Search",
            r#"Visited <a href="https://example.org/a">A page</a>"#,
            "Aug 26, 2026, 9:34:44 PM CEST",
        );
        let (out, ()) = MyActivityParser.parse(&c, &()).unwrap();
        assert_eq!(out[0].verb.as_deref(), Some("Visited"));
        assert_eq!(out[0].title, "A page");
        assert_eq!(out[0].title_url.as_deref(), Some("https://example.org/a"));
        assert_eq!(out[0].unlinked_text, None);
        let c = cell("Takeout", "Downloaded a Takeout archive", "Dec 15, 2025, 5:15:21 AM CEST");
        let (out, ()) = MyActivityParser.parse(&c, &()).unwrap();
        assert_eq!(out[0].title_url, None);
        assert_eq!(out[0].unlinked_text.as_deref(), Some("Downloaded a Takeout archive"));
    }

    #[test]
    fn watches_keep_unknown_titles_unknown() {
        let c = cell(
            "YouTube",
            r#"Watched <a href="https://www.youtube.com/watch?v=abc123">A talk</a><br><a href="https://www.youtube.com/channel/C">Chan</a>"#,
            "Aug 26, 2026, 9:34:44 PM CEST",
        );
        let (out, ()) = YouTubeParser.parse(&c, &()).unwrap();
        let YouTubeEvent::Watched(w) = &out[0] else { panic!() };
        assert_eq!(w.video_id, "abc123");
        assert_eq!(w.title.as_deref(), Some("A talk"));
        assert_eq!(w.channel.as_deref(), Some("Chan"));
        let gone = cell(
            "YouTube",
            r#"Watched <a href="https://www.youtube.com/watch?v=gone1">https://www.youtube.com/watch?v=gone1</a>"#,
            "Aug 26, 2026, 9:34:44 PM CEST",
        );
        let (out, ()) = YouTubeParser.parse(&gone, &()).unwrap();
        let YouTubeEvent::Watched(w) = &out[0] else { panic!() };
        assert_eq!(w.title, None);
        assert_eq!(w.channel, None);
    }

    #[test]
    fn video_ids_from_every_url_form() {
        assert_eq!(video_id("https://www.youtube.com/watch?v=x1&t=3").as_deref(), Some("x1"));
        assert_eq!(video_id("https://youtu.be/x2?t=1").as_deref(), Some("x2"));
        assert_eq!(video_id("https://youtube.com/shorts/x3").as_deref(), Some("x3"));
        assert_eq!(video_id("https://example.org/"), None);
    }

    #[test]
    fn selects_members() {
        assert!(select_my_activity("Takeout/My Activity/Search/MyActivity.html"));
        assert!(!select_my_activity("Takeout/My Activity/Assistant/x.mp3"));
        assert!(select_youtube(
            "Takeout/YouTube and YouTube Music/history/watch-history.html"
        ));
        assert!(!select_youtube("Takeout/YouTube/my-comments/my-comments.html"));
    }
}
