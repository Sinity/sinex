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
use crate::registry::{DeclaredSource, Driver, SourceSpec};
use crate::{Parser, RecordError};
use sinex_model::Class;
use sinex_model::event_types::account::{ActionRecorded, VideoSearched, VideoWatched};
use std::path::PathBuf;

pub const MY_ACTIVITY: SourceSpec = SourceSpec {
    name: "google.my-activity",
    driver: Driver::ExportArchive,
    consumer: "searches, visits and app use recorded by the Google account",
    class: Class::Open,
    parser: "google-my-activity",
    parser_version: 1,
    event_types: &["action.recorded"],
    framer: crate::Framer::export(
        frame,
        crate::drivers::export_archive::Framing::Delimited(CELL),
    ),
};

pub const YOUTUBE: SourceSpec = SourceSpec {
    name: "google.youtube-history",
    driver: Driver::ExportArchive,
    consumer: "videos watched and searched for",
    class: Class::Open,
    parser: "google-youtube-history",
    parser_version: 2,
    event_types: &["video.watched", "video.searched"],
    framer: crate::Framer::export(
        frame,
        crate::drivers::export_archive::Framing::Delimited(CELL),
    ),
};

pub fn frame(bytes: &[u8]) -> (Vec<(usize, usize)>, usize) {
    Framing::Delimited(CELL).frame(bytes, true)
}

pub struct MyActivityParser;

impl Parser for MyActivityParser {
    type Record<'a> = Cell;
    type Carry = ();
    type Payload = ActionRecorded;

    fn parse(
        &self,
        cell: &Cell,
        _: &(),
        _context: &crate::RecordContext<'_>,
    ) -> Result<(Vec<ActionRecorded>, ()), RecordError> {
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

    fn parse(
        &self,
        cell: &Cell,
        _: &(),
        _context: &crate::RecordContext<'_>,
    ) -> Result<(Vec<YouTubeEvent>, ()), RecordError> {
        let first = &cell.lines[0];
        let service = "youtube".to_owned();
        if first.text.starts_with("Watched") {
            // A removed video is listed as text without a link; a link can
            // also point at something other than a video (a post, a
            // playlist). Both keep what is known and leave the rest unknown.
            let link = first.links.first().cloned();
            let video_id = link.as_ref().and_then(|(url, _)| video_id(url));
            let title = match &link {
                // `Watched <url>`: the video is gone and Google omits its
                // title and channel; the field is unknown, not the URL.
                Some((url, text)) => (text != url && !text.is_empty()).then(|| text.clone()),
                None => None,
            };
            let channel = cell.lines.get(1).and_then(|l| l.links.first().cloned());
            let mut details = cell.section("Details");
            if link.is_none() {
                details.insert(0, first.text.trim().to_owned());
            }
            Ok((
                vec![YouTubeEvent::Watched(VideoWatched {
                    service,
                    video_id,
                    watched_at: cell.at,
                    url: link.map(|(u, _)| u),
                    title,
                    channel: channel.as_ref().map(|(_, n)| n.clone()),
                    channel_url: channel.map(|(u, _)| u),
                    products: cell.section("Products"),
                    details,
                })],
                (),
            ))
        } else if first.text.starts_with("Searched for") {
            let (url, query) = match first.links.first() {
                Some((u, q)) => (Some(u.clone()), q.clone()),
                None => (None, first.text["Searched for".len()..].trim().to_owned()),
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

pub fn my_activity() -> DeclaredSource<MyActivityParser> {
    DeclaredSource {
        spec: MY_ACTIVITY,
        parser: MyActivityParser,
    }
}

pub fn youtube() -> DeclaredSource<YouTubeParser> {
    DeclaredSource {
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
mod tests;
