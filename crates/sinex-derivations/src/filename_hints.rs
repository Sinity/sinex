//! Filename hints use only the path and content recorded by a library scan.
use crate::{Env, Input, PerEvent};
use sinex_model::event_types::documents::FilenameHints;
use sinex_model::{Event, RecordState};

pub struct Hints;
impl PerEvent for Hints {
    const NAME: &'static str = "library.filename-hints";
    const VERSION: u32 = 1;
    const INPUTS: &'static [&'static str] = &["library.copy"];
    const OUTPUTS: &'static [&'static str] = &["library.filename_hints"];
    fn derive(&self, input: Input<'_>, _: &Env) -> Result<Vec<Event>, String> {
        let event = input.event;
        if event.record_state == RecordState::Removed {
            return Ok(vec![]);
        }
        let path = event
            .payload
            .get("path")
            .and_then(serde_json::Value::as_str)
            .ok_or("library copy has no path")?;
        let content = event
            .payload
            .get("content")
            .and_then(serde_json::Value::as_str)
            .ok_or("library copy has no content")?;
        let name = path.rsplit('/').next().unwrap_or(path);
        let stem = name.rsplit_once('.').map_or(name, |(stem, _)| stem);
        let year = isolated(stem, 4, |c| c.is_ascii_digit()).and_then(|s| s.parse().ok());
        let external = isolated(stem, 32, |c| c.is_ascii_hexdigit()).map(str::to_owned);
        let (author, title) = stem.split_once(" - ").map_or((None, None), |(a, t)| {
            let title = t
                .rfind(" (")
                .filter(|&i| {
                    t.ends_with(')')
                        && t[i + 2..t.len() - 1].len() == 4
                        && t[i + 2..t.len() - 1].bytes().all(|b| b.is_ascii_digit())
                })
                .map_or(t, |i| &t[..i]);
            (nonempty(a), nonempty(title))
        });
        let payload = FilenameHints {
            copy: event.id,
            path: path.into(),
            content: content.into(),
            author_hint: author,
            title_hint: title,
            year_hint: year,
            external_id_hint: external,
            rule_version: Self::VERSION,
        };
        Ok(vec![crate::derived_event::derived(
            &payload,
            Self::NAME,
            [event],
        )])
    }
}
fn nonempty(s: &str) -> Option<String> {
    let s = s.trim();
    (!s.is_empty()).then(|| s.to_owned())
}
fn isolated(text: &str, length: usize, accept: impl Fn(u8) -> bool) -> Option<&str> {
    let bytes = text.as_bytes();
    let mut start = 0;
    while start < bytes.len() {
        if !accept(bytes[start]) {
            start += 1;
            continue;
        }
        let mut end = start + 1;
        while end < bytes.len() && accept(bytes[end]) {
            end += 1;
        }
        if end - start == length {
            return text.get(start..end);
        }
        start = end;
    }
    None
}
#[cfg(test)]
mod tests;
