//! Posting syntax is captured without inferring amounts or checking balances.
use super::{Posting, RecordError};
use sinex_model::event_types::finance::PostingAmount;

pub(super) fn parse(raw: &str) -> Result<Posting, RecordError> {
    let body = raw.trim_start();
    let (status, body) = match body.strip_prefix('*').or_else(|| body.strip_prefix('!')) {
        Some(rest) if rest.starts_with(char::is_whitespace) => {
            (Some(body[..1].into()), rest.trim_start())
        }
        _ => (None, body),
    };
    let (before_comment, account_comment) = super::split_comment(body);
    let separator = before_comment
        .as_bytes()
        .iter()
        .enumerate()
        .find_map(|(i, b)| {
            (*b == b'\t' || (*b == b' ' && before_comment.as_bytes().get(i + 1) == Some(&b' ')))
                .then_some(i)
        });
    let (account, amounts) = separator.map_or((before_comment.trim(), ""), |i| {
        (body[..i].trim(), body[i..].trim())
    });
    let (account, virtual_account) = if account.starts_with('(') && account.ends_with(')') {
        (&account[1..account.len() - 1], Some("()".into()))
    } else if account.starts_with('[') && account.ends_with(']') {
        (&account[1..account.len() - 1], Some("[]".into()))
    } else {
        (account, None)
    };
    if account.is_empty() {
        return Err(RecordError::new("posting account is empty"));
    }
    let (amounts, annotations, comment) = annotations(amounts)?;
    let marks = outside_marks(&amounts);
    let amount_end = marks.first().map_or(amounts.len(), |(at, _)| *at);
    let base = amounts[..amount_end].trim();
    let (amount, currency) = if base.is_empty() {
        (None, None)
    } else {
        let (number, currency) = quantity(base)?;
        (Some(number), currency)
    };
    let mut price = None;
    let mut assertion = None;
    let mut assertion_price = None;
    for (index, &(at, end)) in marks.iter().enumerate() {
        let operator = &amounts[at..end];
        let until = marks.get(index + 1).map_or(amounts.len(), |(at, _)| *at);
        let (amount, currency) = quantity(amounts[end..until].trim())?;
        let value = PostingAmount {
            operator: operator.into(),
            amount,
            currency,
        };
        let slot = if operator.contains('@') {
            if assertion.is_some() {
                &mut assertion_price
            } else {
                &mut price
            }
        } else {
            &mut assertion
        };
        if slot.replace(value).is_some() {
            return Err(RecordError::new("repeated posting price or assertion"));
        }
    }
    if amount.is_none() && (price.is_some() || !annotations.is_empty()) {
        return Err(RecordError::new(
            "posting annotations and prices require an amount",
        ));
    }
    Ok(Posting {
        account: account.into(),
        amount,
        currency,
        comment: if separator.is_none() {
            account_comment
        } else {
            comment
        },
        status,
        virtual_account,
        price,
        assertion,
        assertion_price,
        annotations,
    })
}

fn annotations(raw: &str) -> Result<(String, Vec<String>, Option<String>), RecordError> {
    let mut body = String::new();
    let mut annotations = Vec::new();
    let mut seen = [false; 4];
    let mut quoted = false;
    let mut escaped = false;
    let mut assertion = false;
    let mut at = 0;
    while at < raw.len() {
        let rest = &raw[at..];
        let c = rest.chars().next().unwrap();
        if escaped {
            escaped = false;
        } else if quoted && c == '\\' {
            escaped = true;
        } else if c == '"' {
            quoted = !quoted;
        } else if !quoted {
            if c == ';' {
                return Ok((body, annotations, Some(rest[1..].trim().into())));
            }
            if let Some(operator) = ["(@@)", "(@)"].into_iter().find(|s| rest.starts_with(s)) {
                body.push_str(operator);
                at += operator.len();
                continue;
            }
            let annotation = if rest.starts_with("((") {
                Some((3, "((", "))"))
            } else if rest.starts_with("{{") {
                Some((0, "{{", "}}"))
            } else {
                match c {
                    '{' => Some((0, "{", "}")),
                    '[' => Some((1, "[", "]")),
                    '(' => Some((2, "(", ")")),
                    _ => None,
                }
            };
            if let Some((kind, open, close)) = annotation {
                if assertion || seen[kind] {
                    return Err(RecordError::new(
                        "repeated posting annotation or annotation after assertion",
                    ));
                }
                let end = annotation_end(rest, open, close)?;
                let value = rest[open.len()..end - close.len()].trim();
                if kind == 0 {
                    let value = value.strip_prefix('=').unwrap_or(value).trim();
                    if !value.is_empty() {
                        quantity(value)?;
                    }
                } else if kind == 1 {
                    super::resolve_date(value, None)?;
                }
                annotations.push(rest[..end].into());
                seen[kind] = true;
                body.push(' ');
                at += end;
                continue;
            }
            if c == '=' {
                assertion = true;
            }
        }
        body.push(c);
        at += c.len_utf8();
    }
    Ok((body, annotations, None))
}

fn annotation_end(raw: &str, open: &str, close: &str) -> Result<usize, RecordError> {
    let mut quoted = false;
    let mut escaped = false;
    for (at, c) in raw.char_indices().skip(open.len()) {
        // Notes and valuation expressions end at their first closing parenthesis.
        if open.starts_with('(') && c == ')' {
            return if raw[at..].starts_with(close) {
                Ok(at + close.len())
            } else {
                Err(RecordError::new("unfinished posting annotation"))
            };
        }
        if escaped {
            escaped = false;
            continue;
        }
        if quoted && c == '\\' {
            escaped = true;
            continue;
        }
        if c == '"' {
            quoted = !quoted;
        }
        if !quoted && raw[at..].starts_with(close) {
            return Ok(at + close.len());
        }
    }
    Err(RecordError::new("unfinished posting annotation"))
}

fn outside_marks(raw: &str) -> Vec<(usize, usize)> {
    let mut out = Vec::new();
    let mut quoted = false;
    let mut escaped = false;
    let mut at = 0;
    while at < raw.len() {
        let rest = &raw[at..];
        let c = rest.chars().next().unwrap();
        if escaped {
            escaped = false;
        } else if quoted && c == '\\' {
            escaped = true;
        } else if c == '"' {
            quoted = !quoted;
        } else if !quoted
            && let Some(operator) = ["(@@)", "(@)", "@@", "@", "==*", "==", "=*", "="]
                .into_iter()
                .find(|s| rest.starts_with(s))
        {
            out.push((at, at + operator.len()));
            at += operator.len();
            continue;
        }
        at += c.len_utf8();
    }
    out
}

fn quantity(raw: &str) -> Result<(String, Option<String>), RecordError> {
    static AMOUNT: std::sync::LazyLock<regex::Regex> = std::sync::LazyLock::new(|| {
        regex::Regex::new(r#"^(?P<sign>[+-])?\s*(?:(?P<leading>"(?:[^"\\]|\\.)*"|[^\s0-9.,+\-;|*!@=()]+)\s*)?(?P<amount>[+-]?(?:[0-9]+(?:[.,\s][0-9]+)*[.,]?|[.,][0-9]+))(?:\s*(?P<trailing>"(?:[^"\\]|\\.)*"|[^\s0-9.,+\-;|*!@=()]+))?$"#).expect("fixed journal amount expression")
    });
    let fields = AMOUNT
        .captures(raw)
        .ok_or_else(|| RecordError::new("invalid journal amount"))?;
    let number = fields.name("amount").unwrap().as_str();
    let punctuation = number.matches([',', '.']).collect::<Vec<_>>();
    if punctuation.len() > 1 && punctuation.iter().all(|p| *p == punctuation[0]) {
        let groups = number.split(punctuation[0]).collect::<Vec<_>>();
        if !groups
            .last()
            .is_some_and(|group| group.len() == 3 || group.is_empty())
        {
            return Err(RecordError::new("invalid journal digit groups"));
        }
    }
    let currency = match (fields.name("leading"), fields.name("trailing")) {
        (Some(_), Some(_)) => return Err(RecordError::new("two posting commodities")),
        (leading, trailing) => leading.or(trailing).map(|m| m.as_str().to_owned()),
    };
    let sign = fields.name("sign").map_or("", |m| m.as_str());
    if !sign.is_empty() && number.starts_with(['+', '-']) {
        return Err(RecordError::new("two amount signs"));
    }
    Ok((format!("{sign}{number}"), currency))
}
