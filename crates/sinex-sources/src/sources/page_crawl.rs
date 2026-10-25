//! The forward crawl: pages from browser history fetched again so their content
//! is kept. It is opt-in per domain: only URLs whose host is on the operator's
//! allow list are fetched. It is polite: each host's `robots.txt` is honoured
//! for this user agent, one request at a time with a pause between requests to
//! the same host.
//!
//! Each fetch is one stored material, the response body as received, with
//! the request and response facts in its manifest; the parser turns it into
//! `page.fetched`. A failed fetch is a fact too: it is stored with status 0
//! and the error, so the page is not silently missing.
use crate::drivers::{Acquired, DriverError, NewMaterial};
use crate::registry::{DeclaredSource, Driver, SourceSpec};
use crate::{Parser, RecordContext, RecordError};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sinex_model::event_types::session::PageFetched;
use sinex_model::{Class, InputRef, MaterialId, ObservationFacts, RevisionChange, Timestamp};
use std::collections::BTreeMap;
use std::sync::Mutex;
use std::time::{Duration, Instant};

pub const SPEC: SourceSpec = SourceSpec {
    name: "browser.page_crawl",
    driver: Driver::ApiCursor,
    excisable: false,
    consumer: "page content for recall and search over what was read",
    class: Class::Guarded,
    parser: "page-fetch",
    parser_version: 1,
    event_types: &["page.fetched"],
    framer: crate::Framer::export(
        crate::drivers::media::whole,
        crate::drivers::export_archive::Framing::Whole,
    ),
};

pub const USER_AGENT: &str = "sinex-crawl/0.1 (personal archive of the operator's own browsing)";

/// Whether `host` is on the allow list: an entry matches itself and its
/// subdomains.
pub fn allowed(host: &str, allow: &[String]) -> bool {
    let host = host.trim_end_matches('.').to_ascii_lowercase();
    allow.iter().any(|a| {
        let a = a.trim_start_matches('.').to_ascii_lowercase();
        host == a || host.ends_with(&format!(".{a}"))
    })
}

/// The scheme and host of an `http(s)` URL, and its path with query.
pub fn split(url: &str) -> Option<(String, String, String)> {
    let (scheme, rest) = url.split_once("://")?;
    if scheme != "http" && scheme != "https" {
        return None;
    }
    let (user_host_port, path) = match rest.find(['/', '?', '#']) {
        Some(i) => (&rest[..i], &rest[i..]),
        None => (rest, "/"),
    };
    let host = user_host_port
        .rsplit('@')
        .next()?
        .split(':')
        .next()?
        .to_owned();
    let path = path.split('#').next().unwrap_or("/");
    Some((
        scheme.to_owned(),
        host,
        if path.is_empty() {
            "/".into()
        } else {
            path.to_owned()
        },
    ))
}

/// `robots.txt` rules for any agent (`*`) and for `sinex-crawl`: path
/// prefixes allowed and disallowed, the longest match winning.
#[derive(Clone, Debug, Default, PartialEq)]
struct Robots {
    rules: Vec<(bool, String)>,
}

impl Robots {
    pub fn parse(text: &str) -> Self {
        let mut rules = Vec::new();
        let (mut ours, mut in_group, mut seen_rule) = (false, false, false);
        for line in text.lines() {
            let line = line.split('#').next().unwrap_or("").trim();
            let Some((k, v)) = line.split_once(':') else {
                continue;
            };
            let (k, v) = (k.trim().to_ascii_lowercase(), v.trim());
            match k.as_str() {
                "user-agent" => {
                    if seen_rule || !in_group {
                        ours = false;
                    }
                    in_group = true;
                    seen_rule = false;
                    let v = v.to_ascii_lowercase();
                    ours |= v == "*" || v.starts_with("sinex-crawl");
                }
                "allow" | "disallow" => {
                    seen_rule = true;
                    if ours && !v.is_empty() {
                        rules.push((k == "allow", v.to_owned()));
                    }
                }
                _ => {}
            }
        }
        Self { rules }
    }

    pub fn permits(&self, path: &str) -> bool {
        self.rules
            .iter()
            .filter(|(_, prefix)| path.starts_with(prefix.as_str()))
            .max_by_key(|(allow, prefix)| (prefix.len(), *allow))
            .is_none_or(|(allow, _)| *allow)
    }
}

pub struct Crawl {
    pub urls: Vec<String>,
    pub allow: Vec<String>,
    /// Pause between two requests to the same host.
    pub pause: Duration,
    pub host: String,
    robots: Mutex<BTreeMap<String, Robots>>,
    last: Mutex<BTreeMap<String, Instant>>,
    agent: ureq::Agent,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct Cursor {
    /// The next URL in the list to consider.
    pub next: usize,
    pub material: Option<MaterialId>,
    pub part_index: u32,
}

impl Crawl {
    pub fn new(urls: Vec<String>, allow: Vec<String>, pause: Duration, host: String) -> Self {
        let agent = ureq::Agent::config_builder()
            .user_agent(USER_AGENT)
            .timeout_global(Some(Duration::from_secs(60)))
            .http_status_as_error(false)
            .build()
            .into();
        Self {
            urls,
            allow,
            pause,
            host,
            robots: Mutex::new(BTreeMap::new()),
            last: Mutex::new(BTreeMap::new()),
            agent,
        }
    }

    fn input(&self) -> InputRef {
        InputRef {
            kind: "api".into(),
            locator: "crawl:browser-history".into(),
            host: self.host.clone(),
        }
    }

    fn wait_turn(&self, host: &str) {
        let wait = self
            .last
            .lock()
            .expect("lock")
            .get(host)
            .map(|t| self.pause.saturating_sub(t.elapsed()))
            .unwrap_or_default();
        std::thread::sleep(wait);
        self.last
            .lock()
            .expect("lock")
            .insert(host.to_owned(), Instant::now());
    }

    fn robots(&self, scheme: &str, host: &str) -> Robots {
        if let Some(r) = self.robots.lock().expect("lock").get(host) {
            return r.clone();
        }
        self.wait_turn(host);
        let robots = self
            .agent
            .get(&format!("{scheme}://{host}/robots.txt"))
            .call()
            .ok()
            .filter(|r| r.status().is_success())
            .and_then(|mut r| r.body_mut().read_to_string().ok())
            .map(|t| Robots::parse(&t))
            .unwrap_or_default();
        self.robots
            .lock()
            .expect("lock")
            .insert(host.to_owned(), robots.clone());
        robots
    }

    /// Fetch the next permitted URL after the cursor and store it.
    pub fn poll(&self, cursor: &Value) -> Result<Acquired, DriverError> {
        let previous: Cursor = if cursor.is_null() {
            Cursor::default()
        } else {
            serde_json::from_value(cursor.clone())
                .map_err(|e| DriverError::Cursor(e.to_string()))?
        };
        let mut index = previous.next;
        while let Some(url) = self.urls.get(index) {
            index += 1;
            let Some((scheme, host, path)) = split(url) else {
                continue;
            };
            if !allowed(&host, &self.allow) || !self.robots(&scheme, &host).permits(&path) {
                continue;
            }
            self.wait_turn(&host);
            let fetched_at = Timestamp::now();
            let (status, final_url, content_type, body, error) = match self.agent.get(url).call() {
                Ok(mut r) => {
                    use ureq::ResponseExt;
                    let final_url = r.get_uri().to_string();
                    let ct = r
                        .headers()
                        .get("content-type")
                        .and_then(|v| v.to_str().ok())
                        .map(str::to_owned);
                    let status = r.status().as_u16();
                    match r.body_mut().with_config().limit(u64::MAX).read_to_vec() {
                        Ok(b) => (status, final_url, ct, b, None),
                        Err(e) => (status, final_url, ct, Vec::new(), Some(e.to_string())),
                    }
                }
                Err(e) => (0, url.clone(), None, Vec::new(), Some(e.to_string())),
            };
            let manifest = json!({
                "url": url, "final_url": final_url, "status": status, "content_type": content_type,
                "error": error, "fetched_at_ns": fetched_at.unix_ns().to_string(),
                "read_at_ns": i64::try_from(fetched_at.unix_ns()).unwrap_or(i64::MAX),
                "media_type": content_type.clone().unwrap_or_else(|| "application/octet-stream".into()),
            });
            let id = MaterialId::new();
            let part_index = if previous.material.is_some() {
                previous.part_index + 1
            } else {
                0
            };
            let next = Cursor {
                next: index,
                material: Some(id),
                part_index,
            };
            let size = body.len() as u64;
            return Ok(Acquired {
                input: self.input(),
                observation: ObservationFacts {
                    observed_at: Some(fetched_at),
                    size: Some(size),
                    changed: true,
                    ..Default::default()
                },
                revision: RevisionChange::Snapshot,
                material: Some(NewMaterial {
                    id,
                    records: vec![(0, body.len())],
                    bytes: body,
                    stored: None,
                    part_index,
                    previous_part: previous.material,
                    reading_order: crate::drivers::reading_order(&manifest),
                    manifest,
                    cursor_at: Box::new({
                        let cursor =
                            move |_| serde_json::to_value(&next).expect("cursor serializes");
                        move |end| Ok(cursor(end))
                    }),
                    aligned: false,
                    vanished: Vec::new(),
                    memory: None,
                }),
                cursor_unchanged: Value::Null,
            });
        }
        Ok(Acquired {
            input: self.input(),
            observation: ObservationFacts {
                observed_at: Some(Timestamp::now()),
                changed: false,
                ..Default::default()
            },
            revision: RevisionChange::Same,
            material: None,
            cursor_unchanged: serde_json::to_value(Cursor {
                next: index,
                ..previous
            })
            .expect("cursor serializes"),
        })
    }
}

/// The text of the first `<title>` in an HTML body.
pub fn title(body: &[u8]) -> Option<String> {
    let text = String::from_utf8_lossy(body);
    let lower = text.to_ascii_lowercase();
    let start = lower.find("<title")?;
    let open_end = start + lower[start..].find('>')? + 1;
    let close = open_end + lower[open_end..].find("</title")?;
    let t = text[open_end..close]
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ");
    let t = t
        .replace("&amp;", "&")
        .replace("&lt;", "<")
        .replace("&gt;", ">")
        .replace("&quot;", "\"")
        .replace("&#39;", "'");
    (!t.is_empty()).then_some(t)
}

pub struct PageParser;

impl Parser for PageParser {
    type Record<'a> = &'a [u8];
    type Carry = ();
    type Payload = PageFetched;

    fn parse(
        &self,
        body: &&[u8],
        _: &(),
        context: &RecordContext<'_>,
    ) -> Result<(Vec<PageFetched>, ()), RecordError> {
        let m = context.manifest;
        let s = |k: &str| m.get(k).and_then(Value::as_str).map(str::to_owned);
        let fetched_at = s("fetched_at_ns")
            .and_then(|n| n.parse::<i128>().ok())
            .and_then(|n| Timestamp::from_unix_ns(n).ok())
            .ok_or_else(|| RecordError::new("fetch has no time"))?;
        let content_type = s("content_type");
        let html = content_type.as_deref().is_some_and(|c| c.contains("html"));
        let url = s("url").ok_or_else(|| RecordError::new("fetch has no URL"))?;
        Ok((
            vec![PageFetched {
                final_url: s("final_url").unwrap_or_else(|| url.clone()),
                url,
                fetched_at,
                status: m
                    .get("status")
                    .and_then(Value::as_u64)
                    .and_then(|n| u16::try_from(n).ok())
                    .unwrap_or(0),
                content_type,
                bytes: body.len() as u64,
                title: if html { title(body) } else { None },
            }],
            (),
        ))
    }
}

pub fn source() -> DeclaredSource<PageParser> {
    DeclaredSource {
        spec: SPEC,
        parser: PageParser,
    }
}

/// The forward crawl, run by `sinexd admin crawl` over browser history rather
/// than read from a path.
pub const KIND: crate::binding::InputKind = crate::binding::InputKind {
    name: "page-crawl",
    specs: || vec![&SPEC],
    plan: None,
    source: |_| Ok(Box::new(source())),
    reader: None,
    capture: None,
};

#[cfg(test)]
mod tests;

impl crate::binding::StatelessDriver for Crawl {
    fn poll_once(
        &self,
        cursor: &serde_json::Value,
    ) -> Result<crate::drivers::Acquired, crate::drivers::DriverError> {
        self.poll(cursor)
    }
}
