//! Spotify streaming history, on `ExportArchive`: the extended history
//! (`Streaming_History_Audio_*.json`, `..._Video_*.json`) and the older
//! account-data history (`StreamingHistory*.json`), each a JSON array.
//!
//! Spotify records when playback *stopped*: `ts` (RFC 3339) in the extended
//! export, `endTime` (`YYYY-MM-DD HH:MM`, UTC) in the account-data export. The
//! start is the stop time less the milliseconds played. (The predecessor read
//! `ts` as the start, which shifted every play by its length.)
use super::fields;
use crate::drivers::export_archive::{ExportArchive, Framing, PART_BYTES};
use crate::registry::{Declared, Driver, SourceSpec};
use crate::{Parser, RecordError};
use serde_json::{Map, Value};
use sinex_model::event_types::media::MusicPlayed;
use sinex_model::{Class, Timestamp};
use std::path::PathBuf;

pub const SPEC: SourceSpec = SourceSpec {
    name: "media.spotify-history",
    driver: Driver::ExportArchive,
    consumer: "listening history and the attention stream",
    class: Class::Open,
    parser: "spotify-streaming-history",
    parser_version: 1,
    event_types: &["music.played"],
    framer: crate::drivers::export_archive::frame_json_array,
};

const EXTENDED: &[&str] = &[
    "ts",
    "ms_played",
    "platform",
    "conn_country",
    "master_metadata_track_name",
    "master_metadata_album_artist_name",
    "master_metadata_album_album_name",
    "spotify_track_uri",
    "episode_name",
    "episode_show_name",
    "spotify_episode_uri",
    "audiobook_title",
    "audiobook_chapter_title",
    "reason_start",
    "reason_end",
    "shuffle",
    "skipped",
    "offline",
    "incognito_mode",
];

const BASIC: &[&str] = &[
    "endTime",
    "msPlayed",
    "artistName",
    "trackName",
    "podcastName",
    "episodeName",
];

pub struct SpotifyParser;

impl Parser for SpotifyParser {
    type Record<'a> = Map<String, Value>;
    type Carry = ();
    type Payload = MusicPlayed;

    fn parse(
        &self,
        row: &Map<String, Value>,
        _: &(),
        _context: &crate::RecordContext<'_>,
    ) -> Result<(Vec<MusicPlayed>, ()), RecordError> {
        let payload = if row.contains_key("ts") {
            extended(row)?
        } else if row.contains_key("endTime") {
            basic(row)?
        } else {
            return Err(RecordError::new(
                "neither ts nor endTime: not a Spotify play",
            ));
        };
        Ok((vec![payload], ()))
    }
}

fn started(ended: Timestamp, played_ms: i64) -> Result<Timestamp, RecordError> {
    Timestamp::from_unix_ns(ended.unix_ns() - i128::from(played_ms) * 1_000_000)
        .map_err(|_| RecordError::new("start time out of range"))
}

fn extended(row: &Map<String, Value>) -> Result<MusicPlayed, RecordError> {
    let ended_at = fields::rfc3339(&fields::text(row, "ts")?)?;
    let played_ms = fields::int(row, "ms_played")?;
    let t = |name| fields::optional_text(row, name);
    Ok(MusicPlayed {
        started_at: started(ended_at, played_ms)?,
        ended_at,
        played_ms,
        track_uri: t("spotify_track_uri"),
        track_name: t("master_metadata_track_name"),
        artist_name: t("master_metadata_album_artist_name"),
        album_name: t("master_metadata_album_album_name"),
        episode_uri: t("spotify_episode_uri"),
        episode_name: t("episode_name"),
        show_name: t("episode_show_name"),
        audiobook_title: t("audiobook_title"),
        audiobook_chapter_title: t("audiobook_chapter_title"),
        platform: t("platform"),
        conn_country: t("conn_country"),
        reason_start: t("reason_start"),
        reason_end: t("reason_end"),
        shuffle: fields::optional_bool(row, "shuffle"),
        skipped: fields::optional_bool(row, "skipped"),
        offline: fields::optional_bool(row, "offline"),
        incognito_mode: fields::optional_bool(row, "incognito_mode"),
        extra: fields::extra(row, EXTENDED),
    })
}

fn basic(row: &Map<String, Value>) -> Result<MusicPlayed, RecordError> {
    let raw = fields::text(row, "endTime")?;
    let format = time::macros::format_description!("[year]-[month]-[day] [hour]:[minute]");
    let ended_at = time::PrimitiveDateTime::parse(&raw, format)
        .map(|t| Timestamp(t.assume_utc()))
        .map_err(|e| RecordError::new(format!("invalid endTime {raw:?}: {e}")))?;
    let played_ms = fields::int(row, "msPlayed")?;
    let t = |name| fields::optional_text(row, name);
    Ok(MusicPlayed {
        started_at: started(ended_at, played_ms)?,
        ended_at,
        played_ms,
        track_uri: None,
        track_name: t("trackName"),
        artist_name: t("artistName"),
        album_name: None,
        episode_uri: None,
        episode_name: t("episodeName"),
        show_name: t("podcastName"),
        audiobook_title: None,
        audiobook_chapter_title: None,
        platform: None,
        conn_country: None,
        reason_start: None,
        reason_end: None,
        shuffle: None,
        skipped: None,
        offline: None,
        incognito_mode: None,
        extra: fields::extra(row, BASIC),
    })
}

pub fn source() -> Declared<SpotifyParser> {
    Declared {
        spec: SPEC,
        parser: SpotifyParser,
    }
}

pub fn select(member: &str) -> bool {
    let name = member.rsplit('/').next().unwrap_or(member);
    name.ends_with(".json")
        && (name.starts_with("Streaming_History_") || name.starts_with("StreamingHistory"))
}

pub fn driver(path: PathBuf, host: String) -> ExportArchive {
    ExportArchive {
        path,
        host,
        select,
        framing: Framing::JsonArray(""),
        part_bytes: PART_BYTES,
    }
}

/// `--source spotify=PATH`: each Spotify export archive in PATH.
pub const KIND: crate::binding::Kind = crate::binding::Kind {
    name: "spotify",
    specs: || vec![&SPEC],
    plan: Some(|input, settings| {
        Ok(crate::binding::per_file(
            &SPEC,
            input,
            settings,
            false,
            crate::binding::any,
        ))
    }),
    source: |_| Ok(Box::new(source())),
    reader: Some(|b| {
        Ok(crate::binding::plain(driver(
            b.input.clone(),
            b.settings.host.clone(),
        )))
    }),
    capture: None,
};

#[cfg(test)]
mod tests;
