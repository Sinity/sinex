//! Text normalization shared by indexing saved events and searching them.
use crate::kinds::{Search, SearchFields};
use serde_json::Value;
use unicode_normalization::UnicodeNormalization;
use unicode_segmentation::UnicodeSegmentation;

pub fn normalize(text: &str) -> String {
    caseless::default_case_fold_str(text).nfc().collect()
}

pub fn words(text: &str) -> impl Iterator<Item = String> + '_ {
    text.unicode_words().map(normalize)
}

pub fn fields(event_type: &str, declared: Search) -> Option<SearchFields> {
    match crate::event_types::find(event_type) {
        Some(kind) => kind.search_fields,
        None if declared != Search::None => Some(SearchFields {
            mode: declared,
            fields: &[],
        }),
        None => None,
    }
}

/// An event's searchable strings, with normalized words separated by spaces.
pub fn text(payload: &Value, fields: &[&str]) -> String {
    let mut text = String::new();
    if fields.is_empty() {
        collect(payload, &mut text);
    } else {
        for value in fields.iter().filter_map(|field| payload.get(field)) {
            collect(value, &mut text);
        }
    }
    text
}

fn collect(value: &Value, text: &mut String) {
    match value {
        Value::String(value) => {
            for word in words(value) {
                if !text.is_empty() {
                    text.push(' ');
                }
                text.push_str(&word);
            }
        }
        Value::Array(items) => items.iter().for_each(|value| collect(value, text)),
        Value::Object(map) => map.values().for_each(|value| collect(value, text)),
        _ => {}
    }
}

/// Keep each word vector small and within PostgreSQL's positional range.
/// Splitting at spaces preserves the complete text when pieces are joined.
pub fn pieces(mut text: &str) -> impl Iterator<Item = &str> {
    const BYTES: usize = 128 * 1024;
    const WORDS: usize = 4_096;
    std::iter::from_fn(move || {
        if text.is_empty() {
            return None;
        }
        let mut spaces = text.match_indices(' ').map(|(at, _)| at);
        let mut previous = 0;
        let mut end = text.len();
        for (index, at) in spaces.by_ref().enumerate() {
            if at > BYTES {
                end = if previous == 0 { at } else { previous };
                break;
            }
            if index + 1 == WORDS {
                end = at;
                break;
            }
            previous = at;
        }
        if end == text.len() && end > BYTES && previous != 0 {
            end = previous;
        }
        let part = &text[..end];
        text = text.get(end + 1..).unwrap_or_default();
        Some(part)
    })
}
