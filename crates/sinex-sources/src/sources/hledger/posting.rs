//! Posting syntax is captured without inferring amounts or checking balances.
use super::{Posting, RecordError};
use sinex_model::event_types::finance::PostingAmount;

pub(super) fn parse(raw: &str) -> Result<Posting, RecordError> {
    let (body, comment) = super::split_comment(raw);
    let body = body.trim();
    let (status, body) = match body.strip_prefix('*').or_else(|| body.strip_prefix('!')) {
        Some(rest) if rest.starts_with(char::is_whitespace) => {
            (Some(body[..1].into()), rest.trim_start())
        }
        _ => (None, body),
    };
    let separator = body.as_bytes().iter().enumerate().find_map(|(i, b)| {
        (*b == b'\t' || (*b == b' ' && body.as_bytes().get(i + 1) == Some(&b' '))).then_some(i)
    });
    let (account, amounts) = separator.map_or((body, ""), |i| (body[..i].trim(), body[i..].trim()));
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
    let marks = outside_marks(amounts);
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
    for (index, &(at, end)) in marks.iter().enumerate() {
        let operator = &amounts[at..end];
        let until = marks.get(index + 1).map_or(amounts.len(), |(at, _)| *at);
        let (amount, currency) = quantity(amounts[end..until].trim())?;
        let value = PostingAmount {
            operator: operator.into(),
            amount,
            currency,
        };
        let slot = if operator.starts_with('@') {
            &mut price
        } else {
            &mut assertion
        };
        if slot.replace(value).is_some() {
            return Err(RecordError::new("repeated posting price or assertion"));
        }
    }
    Ok(Posting {
        account: account.into(),
        amount,
        currency,
        comment,
        status,
        virtual_account,
        price,
        assertion,
    })
}

fn outside_marks(raw: &str) -> Vec<(usize, usize)> {
    let mut out = Vec::new();
    let mut quoted = false;
    let mut escaped = false;
    let mut chars = raw.char_indices().peekable();
    while let Some((at, c)) = chars.next() {
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
        if quoted || !matches!(c, '@' | '=') {
            continue;
        }
        let mut end = at + 1;
        if chars.peek().is_some_and(|(_, next)| *next == c) {
            end = chars.next().unwrap().0 + 1;
        }
        if c == '=' && chars.peek().is_some_and(|(_, next)| *next == '*') {
            end = chars.next().unwrap().0 + 1;
        }
        out.push((at, end));
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
