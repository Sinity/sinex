//! Privacy rules run in admission, once (architecture §3.5; spec `privacy.md`
//! §1). A rule names a source, optionally an event type and a payload field by
//! JSON Pointer, one matcher and one action. Rules are insert-only rows: a
//! change inserts a new version and a removal inserts its end, so the rule set
//! in force at any moment can be read back, and every admitted run records the
//! rule-set position it was admitted under. Reads never apply a rule again; a
//! new rule reaches old data by replay.
//!
//! Each source also has a class floor: the class its declaration gives, raised
//! by the operator's per-source setting. No event is admitted below its
//! source's floor, and a derived event is never below the highest class of its
//! parents.
use crate::{Error, Result};
use regex::Regex;
use serde_json::Value;
use sinex_model::{Class, EquivalenceKey, Event};
use sqlx::PgConnection;
use std::collections::HashMap;
use uuid::Uuid;

#[derive(Clone, Debug)]
pub struct Rule {
    pub id: Uuid,
    /// Stable across versions of the same rule.
    pub name: String,
    pub source: String,
    pub event_type: Option<String>,
    /// JSON Pointer into the payload (`/command`); `None` means every string
    /// in the payload, or, with `Matcher::All`, the event as a whole.
    pub field: Option<String>,
    pub matcher: Matcher,
    pub class: Class,
    pub action: Action,
}

#[derive(Clone, Debug)]
pub enum Matcher {
    /// The whole value (or, without a field, the whole event).
    All,
    Pattern(Regex),
    /// Whole words, compared without case.
    Terms(Vec<String>),
    /// Digit runs of 13 to 19 digits (spaces and dashes allowed) passing the
    /// Luhn check: card numbers.
    Luhn,
    /// IBANs passing their mod-97 check.
    Iban,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Action {
    /// Raise the class only.
    Classify,
    /// Replace matched spans with `[masked]`.
    Mask,
    /// Replace the value with a keyed hash, so equal values still join.
    Hash,
    /// Remove the field (with a field) or mask (without one).
    DropField,
}

pub const MASK: &str = "[masked]";

/// The rules in force for some sources, with their position in the rule
/// history.
#[derive(Clone, Debug, Default)]
pub struct RuleSet {
    /// The newest rule row's sequence number when the set was read (0 when
    /// there are none); recorded on every admitted run.
    pub seq: i64,
    pub rules: Vec<Rule>,
    /// Per-source class floors set by the operator.
    pub floors: HashMap<String, Class>,
    hash_key: [u8; 32],
}

/// What applying the rules did to one event.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Applied {
    pub matched: usize,
    pub changed: bool,
}

pub fn class_name(c: Class) -> &'static str {
    match c {
        Class::Open => "open",
        Class::Guarded => "guarded",
        Class::Sealed => "sealed",
    }
}

pub fn parse_class(s: &str) -> Result<Class> {
    match s {
        "open" => Ok(Class::Open),
        "guarded" => Ok(Class::Guarded),
        "sealed" => Ok(Class::Sealed),
        other => Err(Error::Invalid(format!("unknown class {other}"))),
    }
}

impl Action {
    pub fn parse(s: &str) -> Result<Self> {
        match s {
            "classify" => Ok(Self::Classify),
            "mask" => Ok(Self::Mask),
            "hash" => Ok(Self::Hash),
            "drop_field" => Ok(Self::DropField),
            other => Err(Error::Invalid(format!("unknown privacy action {other}"))),
        }
    }
}

impl Matcher {
    /// Build from the stored kind and its argument.
    pub fn parse(kind: &str, arg: Option<&str>) -> Result<Self> {
        let need = || {
            arg.filter(|a| !a.is_empty())
                .ok_or_else(|| Error::Invalid(format!("matcher {kind} needs an argument")))
        };
        match kind {
            "all" => Ok(Self::All),
            "pattern" => Regex::new(need()?)
                .map(Self::Pattern)
                .map_err(|e| Error::Invalid(format!("pattern: {e}"))),
            "terms" => Ok(Self::Terms(
                need()?
                    .split(['\n', ','])
                    .map(|t| t.trim().to_lowercase())
                    .filter(|t| !t.is_empty())
                    .collect(),
            )),
            "luhn" => Ok(Self::Luhn),
            "iban" => Ok(Self::Iban),
            other => Err(Error::Invalid(format!("unknown matcher {other}"))),
        }
    }

    /// Byte ranges of `text` the matcher finds.
    pub fn spans(&self, text: &str) -> Vec<(usize, usize)> {
        match self {
            Self::All => vec![(0, text.len())],
            Self::Pattern(re) => re.find_iter(text).map(|m| (m.start(), m.end())).collect(),
            Self::Terms(terms) => {
                let lower = text.to_lowercase();
                // Lowercasing can change byte lengths; fall back to whole-value
                // matching when it does, so a span never lands mid-character.
                if lower.len() != text.len() {
                    return if terms.iter().any(|t| lower.contains(t.as_str())) {
                        vec![(0, text.len())]
                    } else {
                        vec![]
                    };
                }
                let mut out = Vec::new();
                for t in terms {
                    let mut from = 0;
                    while let Some(i) = lower[from..].find(t.as_str()) {
                        let (s, e) = (from + i, from + i + t.len());
                        let word = |b: Option<u8>| b.is_some_and(|b| b.is_ascii_alphanumeric());
                        if !word(
                            lower
                                .as_bytes()
                                .get(s.wrapping_sub(1))
                                .copied()
                                .filter(|_| s > 0),
                        ) && !word(lower.as_bytes().get(e).copied())
                        {
                            out.push((s, e));
                        }
                        from = e;
                    }
                }
                out.sort_unstable();
                out
            }
            Self::Luhn => digit_runs(text, 13, 19)
                .into_iter()
                .filter(|&(s, e)| luhn(&text[s..e]))
                .collect(),
            Self::Iban => iban_spans(text),
        }
    }
}

/// Runs of digits (with single spaces or dashes between groups) whose digit
/// count is within `min..=max`.
fn digit_runs(text: &str, min: usize, max: usize) -> Vec<(usize, usize)> {
    let b = text.as_bytes();
    let mut out = Vec::new();
    let mut i = 0;
    while i < b.len() {
        if !b[i].is_ascii_digit() {
            i += 1;
            continue;
        }
        let start = i;
        let mut end = i;
        let mut digits = 0;
        while i < b.len() {
            if b[i].is_ascii_digit() {
                digits += 1;
                i += 1;
                end = i;
            } else if (b[i] == b' ' || b[i] == b'-') && b.get(i + 1).is_some_and(u8::is_ascii_digit)
            {
                i += 1;
            } else {
                break;
            }
        }
        if (min..=max).contains(&digits) {
            out.push((start, end));
        }
        i = end.max(i);
    }
    out
}

fn luhn(s: &str) -> bool {
    let digits: Vec<u32> = s.chars().filter_map(|c| c.to_digit(10)).collect();
    let sum: u32 = digits
        .iter()
        .rev()
        .enumerate()
        .map(|(i, &d)| {
            if i % 2 == 1 {
                if d * 2 > 9 { d * 2 - 9 } else { d * 2 }
            } else {
                d
            }
        })
        .sum();
    sum % 10 == 0
}

fn iban_spans(text: &str) -> Vec<(usize, usize)> {
    static IBAN: std::sync::OnceLock<Regex> = std::sync::OnceLock::new();
    let re = IBAN.get_or_init(|| {
        Regex::new(r"\b[A-Z]{2}[0-9]{2}(?: ?[A-Z0-9]{4}){2,7}(?: ?[A-Z0-9]{1,3})?\b").unwrap()
    });
    re.find_iter(text)
        .filter(|m| iban_valid(m.as_str()))
        .map(|m| (m.start(), m.end()))
        .collect()
}

fn iban_valid(s: &str) -> bool {
    let compact: String = s.chars().filter(|c| !c.is_whitespace()).collect();
    if !(15..=34).contains(&compact.len()) {
        return false;
    }
    let (head, tail) = compact.split_at(4);
    let mut rem: u64 = 0;
    for c in tail.chars().chain(head.chars()) {
        let v = match c.to_digit(36) {
            Some(v) => v as u64,
            None => return false,
        };
        rem = if v >= 10 {
            (rem * 100 + v) % 97
        } else {
            (rem * 10 + v) % 97
        };
    }
    rem == 1
}

impl RuleSet {
    /// A rule set held in memory (tests, the shadow audit's candidate rules).
    pub fn new(rules: Vec<Rule>, floors: HashMap<String, Class>, hash_key: [u8; 32]) -> Self {
        Self {
            seq: 0,
            rules,
            floors,
            hash_key,
        }
    }

    fn keyed(&self, value: &str) -> String {
        format!(
            "hash:{}",
            blake3::keyed_hash(&self.hash_key, value.as_bytes()).to_hex()
        )
    }

    /// Apply the class floor and every matching rule to `event`.
    pub fn apply(&self, event: &mut Event) -> Result<Applied> {
        let mut applied = Applied::default();
        if let Some(&floor) = self.floors.get(&event.source) {
            event.class = event.class.max(floor);
        }
        // A sealed payload is unreadable here by design: rules ran where it
        // was sealed, or never could. An event whose class is sealed must
        // arrive sealed; anything else fails closed.
        if crate::sealed::is_sealed(&event.payload) {
            event.class = Class::Sealed;
            return Ok(applied);
        }
        if event.class == Class::Sealed {
            return Err(Error::Unsealed(format!(
                "{} event from {}",
                event.event_type, event.source
            )));
        }
        let source = event.source.clone();
        let event_type = event.event_type;
        for rule in self.rules.iter().filter(|r| {
            r.source == source && r.event_type.as_deref().is_none_or(|t| t == event_type)
        }) {
            if self.apply_rule(rule, event) {
                applied.matched += 1;
                event.class = event.class.max(rule.class);
                applied.changed |= rule.action != Action::Classify;
            }
        }
        // The equivalence key is built from payload values; once a rule changed
        // the payload it becomes a keyed hash, so equal values still match and
        // the key column never holds what the payload no longer shows.
        if applied.changed
            && let Some(key) = &event.equivalence_key
        {
            event.equivalence_key = Some(EquivalenceKey(self.keyed(&key.0)));
        }
        if event.class == Class::Sealed {
            return Err(Error::Unsealed(format!(
                "{} event from {}",
                event.event_type, event.source
            )));
        }
        Ok(applied)
    }

    /// Whether `rule` matched; applies its action.
    fn apply_rule(&self, rule: &Rule, event: &mut Event) -> bool {
        if rule.field.is_none() && matches!(rule.matcher, Matcher::All) {
            return true;
        }
        match &rule.field {
            Some(pointer) => {
                let Some(target) = event.payload.pointer_mut(pointer) else {
                    return false;
                };
                if rule.action == Action::DropField && self.matches_value(rule, target) {
                    remove_pointer(&mut event.payload, pointer);
                    return true;
                }
                self.rewrite(rule, target)
            }
            None => self.rewrite(rule, &mut event.payload),
        }
    }

    fn matches_value(&self, rule: &Rule, value: &Value) -> bool {
        match value {
            Value::String(s) => !rule.matcher.spans(s).is_empty(),
            Value::Array(items) => items.iter().any(|v| self.matches_value(rule, v)),
            Value::Object(map) => map.values().any(|v| self.matches_value(rule, v)),
            Value::Null => false,
            other => {
                matches!(rule.matcher, Matcher::All)
                    || !rule.matcher.spans(&other.to_string()).is_empty()
            }
        }
    }

    /// Rewrite every string under `value` the rule matches. Returns whether any
    /// matched.
    fn rewrite(&self, rule: &Rule, value: &mut Value) -> bool {
        match value {
            Value::String(s) => {
                let spans = rule.matcher.spans(s);
                if spans.is_empty() {
                    return false;
                }
                match rule.action {
                    Action::Classify => {}
                    Action::Hash => *s = self.keyed(s),
                    Action::Mask | Action::DropField => *s = mask_spans(s, &spans),
                }
                true
            }
            Value::Array(items) => {
                let mut any = false;
                for v in items {
                    any |= self.rewrite(rule, v);
                }
                any
            }
            Value::Object(map) => {
                let mut any = false;
                for v in map.values_mut() {
                    any |= self.rewrite(rule, v);
                }
                any
            }
            Value::Number(_) | Value::Bool(_) => {
                let text = value.to_string();
                if rule.matcher.spans(&text).is_empty() {
                    return false;
                }
                match rule.action {
                    Action::Classify => {}
                    Action::Hash => *value = Value::String(self.keyed(&text)),
                    Action::Mask | Action::DropField => *value = Value::String(MASK.into()),
                }
                true
            }
            Value::Null => false,
        }
    }
}

fn mask_spans(s: &str, spans: &[(usize, usize)]) -> String {
    let mut out = String::with_capacity(s.len());
    let mut at = 0;
    for &(start, end) in spans {
        if start < at {
            at = at.max(end);
            continue;
        }
        out.push_str(&s[at..start]);
        out.push_str(MASK);
        at = end;
    }
    out.push_str(&s[at.min(s.len())..]);
    out
}

fn remove_pointer(payload: &mut Value, pointer: &str) {
    let Some((parent, last)) = pointer.rsplit_once('/') else {
        return;
    };
    let last = last.replace("~1", "/").replace("~0", "~");
    match payload.pointer_mut(parent) {
        Some(Value::Object(map)) => {
            map.remove(&last);
        }
        Some(Value::Array(items)) => {
            if let Ok(i) = last.parse::<usize>()
                && i < items.len()
            {
                items[i] = Value::Null;
            }
        }
        _ => {}
    }
}

/// The key that makes hashed values join without revealing them. Created once
/// per database, never rotated (a new key would split every hashed join).
async fn hash_key(conn: &mut PgConnection) -> Result<[u8; 32]> {
    let fresh: [u8; 32] = {
        let mut k = [0u8; 32];
        getrandom::fill(&mut k).map_err(|e| Error::Invalid(format!("random key: {e}")))?;
        k
    };
    sqlx::query!(
        "INSERT INTO privacy.settings (id, hash_key) VALUES (true, $1) ON CONFLICT (id) DO NOTHING",
        &fresh[..]
    )
    .execute(&mut *conn)
    .await?;
    let stored = sqlx::query_scalar!("SELECT hash_key FROM privacy.settings WHERE id")
        .fetch_one(&mut *conn)
        .await?;
    stored
        .try_into()
        .map_err(|_| Error::Invalid("privacy.settings.hash_key is not 32 bytes".into()))
}

/// The rules and floors in force for `sources`.
pub async fn load<'a>(
    conn: &mut PgConnection,
    sources: impl Iterator<Item = &'a str>,
) -> Result<RuleSet> {
    let mut sources: Vec<String> = sources.map(str::to_owned).collect();
    sources.sort();
    sources.dedup();
    let seq = sqlx::query_scalar!(r#"SELECT coalesce(max(seq), 0) AS "seq!" FROM privacy.rules"#)
        .fetch_one(&mut *conn)
        .await?;
    let rows = sqlx::query!(
        r#"SELECT DISTINCT ON (name) id, name, source, event_type, field, matcher, matcher_arg,
                  class, action, ended
           FROM privacy.rules WHERE source = ANY($1)
           ORDER BY name, seq DESC"#,
        &sources
    )
    .fetch_all(&mut *conn)
    .await?;
    let mut rules = Vec::new();
    for r in rows.into_iter().filter(|r| !r.ended) {
        rules.push(Rule {
            id: r.id,
            name: r.name,
            source: r.source,
            event_type: r.event_type,
            field: r.field,
            matcher: Matcher::parse(&r.matcher, r.matcher_arg.as_deref())?,
            class: parse_class(&r.class)?,
            action: Action::parse(&r.action)?,
        });
    }
    let floors = sqlx::query!(
        r#"SELECT DISTINCT ON (source) source, class FROM privacy.source_classes
           WHERE source = ANY($1) ORDER BY source, seq DESC"#,
        &sources
    )
    .fetch_all(&mut *conn)
    .await?
    .into_iter()
    .map(|r| Ok((r.source, parse_class(&r.class)?)))
    .collect::<Result<HashMap<_, _>>>()?;
    Ok(RuleSet {
        seq,
        rules,
        floors,
        hash_key: hash_key(conn).await?,
    })
}

/// A new rule, or a new version of the rule named `name`.
#[derive(Clone, Debug)]
pub struct NewRule {
    pub name: String,
    pub source: String,
    pub event_type: Option<String>,
    pub field: Option<String>,
    pub matcher: String,
    pub matcher_arg: Option<String>,
    pub class: Class,
    pub action: Action,
    pub author: String,
}

fn action_name(a: Action) -> &'static str {
    match a {
        Action::Classify => "classify",
        Action::Mask => "mask",
        Action::Hash => "hash",
        Action::DropField => "drop_field",
    }
}

/// Insert a rule version. Validates the matcher first, so a rule that cannot
/// be applied is never stored.
pub async fn add_rule(conn: &mut PgConnection, rule: &NewRule) -> Result<i64> {
    Matcher::parse(&rule.matcher, rule.matcher_arg.as_deref())?;
    if let Some(f) = &rule.field
        && !f.starts_with('/')
    {
        return Err(Error::Invalid(format!(
            "field {f:?} is not a JSON Pointer (it starts with /)"
        )));
    }
    Ok(sqlx::query_scalar!(
        r#"INSERT INTO privacy.rules (id, name, source, event_type, field, matcher, matcher_arg,
                                      class, action, author)
           VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10) RETURNING seq"#,
        Uuid::now_v7(),
        rule.name,
        rule.source,
        rule.event_type,
        rule.field,
        rule.matcher,
        rule.matcher_arg,
        class_name(rule.class),
        action_name(rule.action),
        rule.author,
    )
    .fetch_one(&mut *conn)
    .await?)
}

/// End the rule named `name` by inserting its end. Ending a rule weakens
/// protection, so callers allow it to the operator only.
pub async fn end_rule(conn: &mut PgConnection, name: &str, author: &str) -> Result<i64> {
    let last = sqlx::query!(
        r#"SELECT source, event_type, field, matcher, matcher_arg, class, action, ended
           FROM privacy.rules WHERE name = $1 ORDER BY seq DESC LIMIT 1"#,
        name
    )
    .fetch_optional(&mut *conn)
    .await?
    .ok_or_else(|| Error::Invalid(format!("no privacy rule named {name}")))?;
    if last.ended {
        return Err(Error::Invalid(format!("privacy rule {name} already ended")));
    }
    Ok(sqlx::query_scalar!(
        r#"INSERT INTO privacy.rules (id, name, source, event_type, field, matcher, matcher_arg,
                                      class, action, author, ended)
           VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, true) RETURNING seq"#,
        Uuid::now_v7(),
        name,
        last.source,
        last.event_type,
        last.field,
        last.matcher,
        last.matcher_arg,
        last.class,
        last.action,
        author,
    )
    .fetch_one(&mut *conn)
    .await?)
}

/// Set a source's class floor (a new row; the newest wins).
pub async fn set_source_class(
    conn: &mut PgConnection,
    source: &str,
    class: Class,
    author: &str,
) -> Result<i64> {
    Ok(sqlx::query_scalar!(
        "INSERT INTO privacy.source_classes (source, class, author) VALUES ($1, $2, $3) RETURNING seq",
        source,
        class_name(class),
        author
    )
    .fetch_one(&mut *conn)
    .await?)
}

/// One row of the rule history, as `privacy rules` lists it.
#[derive(Clone, Debug, serde::Serialize)]
pub struct RuleRow {
    pub seq: i64,
    pub name: String,
    pub source: String,
    pub event_type: Option<String>,
    pub field: Option<String>,
    pub matcher: String,
    pub matcher_arg: Option<String>,
    pub class: String,
    pub action: String,
    pub author: String,
    pub written_at: String,
    pub ended: bool,
}

/// Rules in force (the newest version of each, not ended), or with `history`
/// every row ever written.
pub async fn list_rules(conn: &mut PgConnection, history: bool) -> Result<Vec<RuleRow>> {
    let rows = sqlx::query!(
        r#"SELECT seq, name, source, event_type, field, matcher, matcher_arg, class, action,
                  author, written_at, ended,
                  seq = max(seq) OVER (PARTITION BY name) AS "newest!"
           FROM privacy.rules ORDER BY seq"#
    )
    .fetch_all(&mut *conn)
    .await?;
    Ok(rows
        .into_iter()
        .filter(|r| history || (r.newest && !r.ended))
        .map(|r| RuleRow {
            seq: r.seq,
            name: r.name,
            source: r.source,
            event_type: r.event_type,
            field: r.field,
            matcher: r.matcher,
            matcher_arg: r.matcher_arg,
            class: r.class,
            action: r.action,
            author: r.author,
            written_at: r
                .written_at
                .format(&time::format_description::well_known::Rfc3339)
                .unwrap_or_default(),
            ended: r.ended,
        })
        .collect())
}

/// Events whose class is below what their source's floor or the rules in
/// force now give them (admitted before a rule or floor changed): the class
/// verification (old `rkv.12`). A replay brings them up.
#[derive(Clone, Debug, Default, serde::Serialize)]
pub struct Verification {
    pub checked: u64,
    pub below_floor: u64,
    pub by_source: Vec<(String, u64)>,
}

pub async fn verify_classes(conn: &mut PgConnection) -> Result<Verification> {
    let rows = sqlx::query!(
        r#"WITH floors AS (
             SELECT DISTINCT ON (source) source, class FROM privacy.source_classes
             ORDER BY source, seq DESC),
           rank(c, r) AS (VALUES ('open', 0), ('guarded', 1), ('sealed', 2))
           SELECT e.source AS "source!", count(*) AS "n!"
           FROM core.events e
           JOIN (SELECT event_id FROM core.live_coordinates
                 UNION SELECT event_id FROM core.live_equivalence) lk ON lk.event_id = e.id
           JOIN floors f ON f.source = e.source
           JOIN rank re ON re.c = e.class JOIN rank rf ON rf.c = f.class
           WHERE re.r < rf.r
           GROUP BY e.source ORDER BY e.source"#
    )
    .fetch_all(&mut *conn)
    .await?;
    let checked = sqlx::query_scalar!(
        r#"SELECT count(*) AS "n!" FROM (SELECT event_id FROM core.live_coordinates
                                         UNION SELECT event_id FROM core.live_equivalence) l"#
    )
    .fetch_one(&mut *conn)
    .await?;
    let by_source: Vec<(String, u64)> = rows.into_iter().map(|r| (r.source, r.n as u64)).collect();
    Ok(Verification {
        checked: checked as u64,
        below_floor: by_source.iter().map(|(_, n)| n).sum(),
        by_source,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use sinex_model::{
        Event, MaterialId, Origin, ReadingOrder, RecordLocation, Timestamp, kinds::CommandImported,
    };

    fn event(command: &str) -> Event {
        Event::build(
            &CommandImported {
                command: command.into(),
                hostname: "h".into(),
                started_at: Timestamp::from_unix_secs(1).unwrap(),
                header_elapsed: 0,
                n: 0,
            },
            "terminal.zsh-history",
            Origin::Record {
                location: RecordLocation {
                    material: MaterialId::new(),
                    anchor: 0,
                },
                ordinal: 0,
            },
            ReadingOrder(0),
            [0; 32],
            Class::Open,
        )
        .unwrap()
    }

    fn rule(field: Option<&str>, matcher: Matcher, class: Class, action: Action) -> Rule {
        Rule {
            id: Uuid::nil(),
            name: "r".into(),
            source: "terminal.zsh-history".into(),
            event_type: Some("command.imported".into()),
            field: field.map(str::to_owned),
            matcher,
            class,
            action,
        }
    }

    fn set(rules: Vec<Rule>) -> RuleSet {
        RuleSet::new(rules, HashMap::new(), [7; 32])
    }

    #[test]
    fn masks_only_the_matched_span_and_raises_class() {
        let mut e = event("export TOKEN=hunter2 && ls");
        let r = rule(
            Some("/command"),
            Matcher::parse("pattern", Some("TOKEN=\\S+")).unwrap(),
            Class::Guarded,
            Action::Mask,
        );
        let applied = set(vec![r]).apply(&mut e).unwrap();
        assert_eq!(
            applied,
            Applied {
                matched: 1,
                changed: true
            }
        );
        assert_eq!(e.payload["command"], json!("export [masked] && ls"));
        assert_eq!(e.class, Class::Guarded);
    }

    #[test]
    fn hashes_join_without_revealing() {
        let mut a = event("ssh alice@host");
        let mut b = event("ssh alice@host");
        let r = rule(Some("/command"), Matcher::All, Class::Open, Action::Hash);
        let rs = set(vec![r]);
        rs.apply(&mut a).unwrap();
        rs.apply(&mut b).unwrap();
        let h = a.payload["command"].as_str().unwrap();
        assert!(h.starts_with("hash:") && !h.contains("alice"));
        assert_eq!(a.payload, b.payload);
    }

    #[test]
    fn drop_field_removes_it_and_rekeys() {
        let mut e = event("pass show bank");
        let before = e.equivalence_key.clone();
        let r = rule(
            Some("/command"),
            Matcher::parse("terms", Some("pass")).unwrap(),
            Class::Open,
            Action::DropField,
        );
        set(vec![r]).apply(&mut e).unwrap();
        assert!(e.payload.get("command").is_none());
        if let (Some(b), Some(a)) = (before, &e.equivalence_key) {
            assert_ne!(&b, a);
            assert!(a.0.starts_with("hash:"));
        }
    }

    #[test]
    fn terms_are_whole_words_without_case() {
        let m = Matcher::parse("terms", Some("Secret, pass")).unwrap();
        assert_eq!(m.spans("my SECRET plan").len(), 1);
        assert!(m.spans("secretary passport").is_empty());
    }

    #[test]
    fn structural_detectors() {
        let luhn = Matcher::Luhn;
        assert_eq!(luhn.spans("card 4111 1111 1111 1111 ok").len(), 1);
        assert!(luhn.spans("card 4111 1111 1111 1112").is_empty());
        let iban = Matcher::Iban;
        assert_eq!(iban.spans("to GB82 WEST 1234 5698 7654 32 now").len(), 1);
        assert!(iban.spans("GB82 WEST 1234 5698 7654 33").is_empty());
    }

    #[test]
    fn floors_raise_and_other_sources_untouched() {
        let mut e = event("ls");
        let mut floors = HashMap::new();
        floors.insert("terminal.zsh-history".to_string(), Class::Guarded);
        RuleSet::new(vec![], floors, [0; 32]).apply(&mut e).unwrap();
        assert_eq!(e.class, Class::Guarded);
        let mut other = rule(None, Matcher::All, Class::Guarded, Action::Classify);
        other.source = "x".into();
        let mut e = event("ls");
        set(vec![other]).apply(&mut e).unwrap();
        assert_eq!(e.class, Class::Open);
    }

    #[test]
    fn an_unsealed_event_of_a_sealed_class_fails_closed() {
        let mut e = event("ls");
        let r = rule(None, Matcher::All, Class::Sealed, Action::Classify);
        assert!(matches!(
            set(vec![r]).apply(&mut e),
            Err(Error::Unsealed(_))
        ));
        let mut sealed = event("ls");
        let key = crate::sealed::new_key().unwrap();
        crate::sealed::seal_event(&mut sealed, &key).unwrap();
        let mask = rule(None, Matcher::All, Class::Open, Action::Mask);
        let before = sealed.payload.clone();
        set(vec![mask]).apply(&mut sealed).unwrap();
        assert_eq!(
            sealed.payload, before,
            "rules never rewrite a sealed payload"
        );
    }
}
