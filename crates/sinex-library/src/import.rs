//! Bringing curation in from other library applications and sites, as
//! assignments at the `import` level so an operator override still wins. Hydrus
//! is the reference for features; no Hydrus component is needed: its tag
//! sidecars are plain text.
//!
//! - **gallery-dl metadata** (`<file>.json`, written with
//!   `--write-metadata`): booru, Twitter, Reddit and other extractors. Tags
//!   (plain and namespaced, `tags_<namespace>`), the site's rating, the post's
//!   source and URL, and its MD5 when the site gives one.
//! - **Hydrus tag sidecars** (`<file>.txt`, one tag per line,
//!   `namespace:tag`), as Hydrus's export folders and sidecar exporters write.
use serde_json::Value;

/// One imported value for an item: a tag (`field = "tag"`) or a property.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Imported {
    pub field: String,
    pub value: String,
}

/// What a sidecar says, with where it came from.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Sidecar {
    pub values: Vec<Imported>,
    /// The post or page the file came from, when the sidecar names it.
    pub source_ref: Option<String>,
    /// An MD5 the site gave, to check against the file's own.
    pub md5: Option<String>,
}

fn tag(value: &str) -> Option<Imported> {
    let t = value.trim();
    (!t.is_empty()).then(|| Imported {
        field: "tag".into(),
        value: t.to_owned(),
    })
}

fn texts(v: &Value) -> Vec<String> {
    match v {
        Value::Array(a) => a
            .iter()
            .filter_map(|x| x.as_str().map(str::to_owned))
            .collect(),
        // Space-separated, as Danbooru's `tag_string` fields are.
        Value::String(s) => s.split_whitespace().map(str::to_owned).collect(),
        _ => Vec::new(),
    }
}

/// A gallery-dl metadata file.
fn gallery_dl(json: &Value) -> Sidecar {
    let mut out = Sidecar::default();
    let Some(object) = json.as_object() else {
        return out;
    };
    let site = object.get("category").and_then(Value::as_str);
    for (k, v) in object {
        if k == "tags" || k == "tag_string" {
            out.values.extend(texts(v).iter().filter_map(|t| tag(t)));
        } else if let Some(ns) = k
            .strip_prefix("tags_")
            .or_else(|| k.strip_prefix("tag_string_"))
        {
            out.values
                .extend(texts(v).iter().filter_map(|t| tag(&format!("{ns}:{t}"))));
        }
    }
    if let Some(site) = site {
        out.values.push(Imported {
            field: "site".into(),
            value: site.to_owned(),
        });
    }
    if let Some(r) = object.get("rating").and_then(Value::as_str) {
        out.values.push(Imported {
            field: "content_rating".into(),
            value: match r {
                "s" | "g" | "general" | "safe" => "general",
                "q" | "questionable" | "sensitive" => "questionable",
                "e" | "explicit" => "explicit",
                other => other,
            }
            .to_owned(),
        });
    }
    for key in ["title", "description", "content"] {
        if let Some(s) = object.get(key).and_then(Value::as_str)
            && !s.trim().is_empty()
        {
            out.values.push(Imported {
                field: key.into(),
                value: s.trim().to_owned(),
            });
        }
    }
    if let Some(s) = object.get("source").and_then(Value::as_str)
        && !s.is_empty()
    {
        out.values.push(Imported {
            field: "source".into(),
            value: s.to_owned(),
        });
    }
    out.source_ref = ["post_url", "url", "file_url"]
        .iter()
        .find_map(|k| object.get(*k).and_then(Value::as_str))
        .map(str::to_owned)
        .or_else(|| {
            let id = object.get("id")?;
            Some(format!("{}:{}", site.unwrap_or("post"), id))
        });
    out.md5 = object
        .get("md5")
        .and_then(Value::as_str)
        .map(str::to_ascii_lowercase);
    out
}

/// A Hydrus-style tag sidecar: one tag per line.
fn tag_lines(text: &str) -> Sidecar {
    Sidecar {
        values: text.lines().filter_map(tag).collect(),
        ..Sidecar::default()
    }
}

/// The sidecar files to look for beside `file`, with how to read each.
pub fn sidecars_of(file: &std::path::Path) -> Vec<(std::path::PathBuf, Format)> {
    let mut out = Vec::new();
    let name = file.as_os_str().to_string_lossy();
    out.push((format!("{name}.json").into(), Format::GalleryDl));
    out.push((file.with_extension("json"), Format::GalleryDl));
    out.push((format!("{name}.txt").into(), Format::TagLines));
    out.dedup_by(|a, b| a.0 == b.0);
    out
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Format {
    GalleryDl,
    TagLines,
}

/// Read a sidecar file; `None` when it is absent or not of its format.
pub fn read(path: &std::path::Path, format: Format) -> Option<Sidecar> {
    let bytes = std::fs::read(path).ok()?;
    match format {
        Format::GalleryDl => {
            let v: Value = serde_json::from_slice(&bytes).ok()?;
            v.is_object().then(|| gallery_dl(&v))
        }
        Format::TagLines => Some(tag_lines(std::str::from_utf8(&bytes).ok()?)),
    }
}

#[cfg(test)]
mod tests;
