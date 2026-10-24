//! Note structure and lexical body references. The original text stays intact.
use regex::Regex;
use serde_json::Value;
use sinex_model::event_types::media::{NoteFrontMatter, NoteFrontMatterError};
use std::{collections::BTreeSet, path::Path, sync::OnceLock};

mod yaml;

#[derive(Default)]
pub(super) struct Metadata {
    pub title: Option<String>,
    pub note_id: Option<String>,
    pub tags: Option<Vec<String>>,
    pub wikilinks: Option<Vec<String>>,
    pub front_matter: Option<NoteFrontMatter>,
}

pub(super) fn parse(text: &str, path: &str) -> Metadata {
    let mut lines = text.split_inclusive('\n');
    let first = lines.next().unwrap_or("");
    let (front_matter, body) = if first.trim_end_matches(['\r', '\n']) == "---" {
        let mut offset = first.len();
        let mut closing = None;
        for line in lines {
            if line.trim_end_matches(['\r', '\n', ' ', '\t']) == "---" {
                closing = Some((offset, offset + line.len()));
                break;
            }
            offset += line.len();
        }
        match closing {
            Some((end, body)) => (yaml::parse(&text[first.len()..end]), Some(&text[body..])),
            None => (
                NoteFrontMatter::Invalid {
                    kind: NoteFrontMatterError::Unclosed,
                    line: Some(1),
                    column: Some(1),
                },
                None,
            ),
        }
    } else {
        (NoteFrontMatter::Absent, Some(text))
    };
    let value = match &front_matter {
        NoteFrontMatter::Parsed { value } => Some(value),
        _ => None,
    };
    let note_id = value
        .and_then(|v| v.get("id"))
        .and_then(Value::as_str)
        .map(str::to_owned);
    let title = value
        .and_then(|v| v.get("title"))
        .and_then(Value::as_str)
        .filter(|s| !s.trim().is_empty())
        .map(str::to_owned)
        .or_else(|| {
            note_id
                .as_deref()
                .and_then(|id| id.rsplit('.').next())
                .filter(|s| !s.trim().is_empty())
                .map(display_name)
        })
        .unwrap_or_else(|| {
            display_name(
                Path::new(path)
                    .file_stem()
                    .and_then(|s| s.to_str())
                    .unwrap_or(path),
            )
        });
    let tags = body.map(|body| {
        let mut tags = BTreeSet::new();
        if let Some(values) = value.and_then(|v| v.get("tags")).and_then(Value::as_array) {
            tags.extend(
                values
                    .iter()
                    .filter_map(Value::as_str)
                    .map(|s| s.trim_start_matches('#'))
                    .filter(|s| !s.is_empty())
                    .map(str::to_owned),
            );
        }
        static TAGS: OnceLock<Regex> = OnceLock::new();
        let re = TAGS.get_or_init(|| {
            Regex::new(r"(?:^|\s)#([A-Za-z][A-Za-z0-9_/-]*)").expect("tag expression")
        });
        for line in body.lines() {
            let heading = line.trim_start().trim_start_matches('#');
            if line.trim_start().starts_with('#')
                && (heading.is_empty() || heading.starts_with(char::is_whitespace))
            {
                continue;
            }
            tags.extend(re.captures_iter(line).map(|c| c[1].to_owned()));
        }
        tags.into_iter().collect()
    });
    let wikilinks = body.map(|body| {
        static LINKS: OnceLock<Regex> = OnceLock::new();
        LINKS
            .get_or_init(|| {
                Regex::new(r"\[\[([^\]|#\r\n]+)(?:#[^\]|\r\n]*)?(?:\|[^\]\r\n]*)?\]\]")
                    .expect("wikilink expression")
            })
            .captures_iter(body)
            .map(|c| c[1].trim().to_owned())
            .filter(|s| !s.is_empty())
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect()
    });
    Metadata {
        title: Some(title),
        note_id,
        tags,
        wikilinks,
        front_matter: Some(front_matter),
    }
}

fn display_name(value: &str) -> String {
    value.replace(['-', '_'], " ")
}
