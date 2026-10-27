//! Declared rules attach tags to captured events. Every matching tag is an
//! attributed derived record; rules never change the captured payload.
use crate::derived_event::{derived, record_identity};
use crate::{Env, Input, PerEvent};
use sinex_model::event_types::derived::TagApplied;
use sinex_model::{Event, EventPayload, Origin};
use std::collections::BTreeMap;
use std::sync::OnceLock;

/// The part of an event a rule reads.
#[derive(Clone, Copy)]
pub enum Field {
    Source,
    Payload(&'static str),
}

/// A literal comparison, with no inference about what text means.
#[derive(Clone, Copy)]
pub enum Match {
    Prefix(&'static str),
    Extension(&'static str),
    Equal(&'static str),
}

pub struct Rule {
    pub name: &'static str,
    pub field: Field,
    pub matches: Match,
    pub tag: &'static str,
}

/// Source, file extension and MIME rules. Their names are recorded with tags.
pub const RULES: &[Rule] = &[
    Rule {
        name: "terminal source",
        field: Field::Source,
        matches: Match::Prefix("terminal."),
        tag: "sys.source.terminal",
    },
    Rule {
        name: "browser source",
        field: Field::Source,
        matches: Match::Prefix("browser."),
        tag: "sys.source.browser",
    },
    Rule {
        name: "desktop source",
        field: Field::Source,
        matches: Match::Prefix("desktop."),
        tag: "sys.source.desktop",
    },
    Rule {
        name: "file source",
        field: Field::Source,
        matches: Match::Equal("machine.file-watch"),
        tag: "sys.source.file",
    },
    Rule {
        name: "Rust file",
        field: Field::Payload("path"),
        matches: Match::Extension("rs"),
        tag: "inferred.file-type.rust",
    },
    Rule {
        name: "Nix file",
        field: Field::Payload("path"),
        matches: Match::Extension("nix"),
        tag: "inferred.file-type.nix",
    },
    Rule {
        name: "Markdown file",
        field: Field::Payload("path"),
        matches: Match::Extension("md"),
        tag: "inferred.file-type.markdown",
    },
    Rule {
        name: "Python file",
        field: Field::Payload("path"),
        matches: Match::Extension("py"),
        tag: "inferred.file-type.python",
    },
    Rule {
        name: "JavaScript file",
        field: Field::Payload("path"),
        matches: Match::Extension("js"),
        tag: "inferred.file-type.javascript",
    },
    Rule {
        name: "TypeScript file",
        field: Field::Payload("path"),
        matches: Match::Extension("ts"),
        tag: "inferred.file-type.javascript",
    },
    Rule {
        name: "JSX file",
        field: Field::Payload("path"),
        matches: Match::Extension("jsx"),
        tag: "inferred.file-type.javascript",
    },
    Rule {
        name: "TSX file",
        field: Field::Payload("path"),
        matches: Match::Extension("tsx"),
        tag: "inferred.file-type.javascript",
    },
    Rule {
        name: "JSON file",
        field: Field::Payload("path"),
        matches: Match::Extension("json"),
        tag: "inferred.file-type.json",
    },
    Rule {
        name: "TOML file",
        field: Field::Payload("path"),
        matches: Match::Extension("toml"),
        tag: "inferred.file-type.toml",
    },
    Rule {
        name: "YAML file",
        field: Field::Payload("path"),
        matches: Match::Extension("yaml"),
        tag: "inferred.file-type.yaml",
    },
    Rule {
        name: "YML file",
        field: Field::Payload("path"),
        matches: Match::Extension("yml"),
        tag: "inferred.file-type.yaml",
    },
    Rule {
        name: "HTML file",
        field: Field::Payload("path"),
        matches: Match::Extension("html"),
        tag: "inferred.file-type.html",
    },
    Rule {
        name: "HTM file",
        field: Field::Payload("path"),
        matches: Match::Extension("htm"),
        tag: "inferred.file-type.html",
    },
    Rule {
        name: "CSS file",
        field: Field::Payload("path"),
        matches: Match::Extension("css"),
        tag: "inferred.file-type.css",
    },
    Rule {
        name: "SQL file",
        field: Field::Payload("path"),
        matches: Match::Extension("sql"),
        tag: "inferred.file-type.sql",
    },
    Rule {
        name: "Shell file",
        field: Field::Payload("path"),
        matches: Match::Extension("sh"),
        tag: "inferred.file-type.shell",
    },
    Rule {
        name: "Bash file",
        field: Field::Payload("path"),
        matches: Match::Extension("bash"),
        tag: "inferred.file-type.shell",
    },
    Rule {
        name: "Zsh file",
        field: Field::Payload("path"),
        matches: Match::Extension("zsh"),
        tag: "inferred.file-type.shell",
    },
    Rule {
        name: "plain text MIME",
        field: Field::Payload("media_type"),
        matches: Match::Equal("text/plain"),
        tag: "sys.mime.text-plain",
    },
    Rule {
        name: "HTML MIME",
        field: Field::Payload("media_type"),
        matches: Match::Equal("text/html"),
        tag: "sys.mime.text-html",
    },
    Rule {
        name: "CSS MIME",
        field: Field::Payload("media_type"),
        matches: Match::Equal("text/css"),
        tag: "sys.mime.text-css",
    },
    Rule {
        name: "CSV MIME",
        field: Field::Payload("media_type"),
        matches: Match::Equal("text/csv"),
        tag: "sys.mime.text-csv",
    },
    Rule {
        name: "text XML MIME",
        field: Field::Payload("media_type"),
        matches: Match::Equal("text/xml"),
        tag: "sys.mime.xml",
    },
    Rule {
        name: "application XML MIME",
        field: Field::Payload("media_type"),
        matches: Match::Equal("application/xml"),
        tag: "sys.mime.xml",
    },
    Rule {
        name: "JSON MIME",
        field: Field::Payload("media_type"),
        matches: Match::Equal("application/json"),
        tag: "sys.mime.json",
    },
    Rule {
        name: "PDF MIME",
        field: Field::Payload("media_type"),
        matches: Match::Equal("application/pdf"),
        tag: "sys.mime.pdf",
    },
    Rule {
        name: "Word MIME",
        field: Field::Payload("media_type"),
        matches: Match::Equal(
            "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
        ),
        tag: "sys.mime.docx",
    },
    Rule {
        name: "Excel MIME",
        field: Field::Payload("media_type"),
        matches: Match::Equal("application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"),
        tag: "sys.mime.xlsx",
    },
    Rule {
        name: "PowerPoint MIME",
        field: Field::Payload("media_type"),
        matches: Match::Equal(
            "application/vnd.openxmlformats-officedocument.presentationml.presentation",
        ),
        tag: "sys.mime.pptx",
    },
    Rule {
        name: "Markdown MIME",
        field: Field::Payload("media_type"),
        matches: Match::Equal("text/markdown"),
        tag: "sys.mime.text-markdown",
    },
];

fn matches(rule: &Rule, event: &Event) -> bool {
    let value = match rule.field {
        Field::Source => Some(event.source.as_str()),
        Field::Payload(field) => event.payload.get(field).and_then(|v| v.as_str()),
    };
    let Some(value) = value else { return false };
    match rule.matches {
        Match::Prefix(prefix) => value.starts_with(prefix),
        Match::Equal(expected) => value == expected,
        Match::Extension(extension) => std::path::Path::new(value)
            .extension()
            .is_some_and(|found| found == extension),
    }
}

pub struct Tags;

impl PerEvent for Tags {
    const NAME: &'static str = "tags";
    /// The rules and their meaning are part of this version.
    const VERSION: u32 = 1;
    // The registered kinds are read through `input_types`.
    const INPUTS: &'static [&'static str] = &[];
    const OUTPUTS: &'static [&'static str] = &[TagApplied::EVENT_TYPE];

    fn input_types(&self) -> &'static [&'static str] {
        inputs()
    }

    fn derive(&self, input: Input<'_>, _: &Env) -> Result<Vec<Event>, String> {
        let event = input.event;
        if !matches!(event.origin, Origin::Record { .. }) {
            return Ok(Vec::new());
        }
        let mut tags: BTreeMap<&str, Vec<String>> = BTreeMap::new();
        for rule in RULES.iter().filter(|r| matches(r, event)) {
            tags.entry(rule.tag).or_default().push(rule.name.to_owned());
        }
        Ok(tags
            .into_iter()
            .map(|(tag, rules)| {
                let mut output = derived(
                    &TagApplied {
                        record: record_identity(event),
                        tag: tag.to_owned(),
                        rules,
                        at: event.ts_orig.at,
                    },
                    Self::NAME,
                    [event],
                );
                output.ts_orig = event.ts_orig;
                output
            })
            .collect())
    }
}

/// Registry input types, including future captured kinds, excluding telemetry
/// and this derivation's own output. Derived inputs are ignored by `derive`.
pub fn inputs() -> &'static [&'static str] {
    static TYPES: OnceLock<Vec<&'static str>> = OnceLock::new();
    TYPES.get_or_init(|| {
        let mut types: Vec<_> = sinex_model::event_types::ALL
            .iter()
            .map(|kind| kind.event_type)
            .filter(|kind| {
                *kind != TagApplied::EVENT_TYPE && !sinex_model::event_types::is_telemetry(kind)
            })
            .collect();
        types.sort_unstable();
        types
    })
}

#[cfg(test)]
mod tests;
