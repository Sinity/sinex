//! Product commands carry ordinary query relations through the shared grammar.
//! Authored text is evidence; a template is the executable definition.
use super::*;
use query_language::ast as syntax;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct Template {
    pub relation: syntax::Query,
    pub operation: Option<Operation>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum Operation {
    Recall {
        subject: Subject,
    },
    Walk {
        direction: Direction,
        reference: String,
    },
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum Direction {
    Trace,
    Parents,
    Descendants,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum Subject {
    Time {
        value: Option<syntax::Value>,
        radius: Option<syntax::Value>,
    },
    Last {
        selector: Box<syntax::Query>,
    },
    Mark {
        number: syntax::Value,
        radius: Option<syntax::Value>,
    },
}

impl Template {
    pub fn query(relation: syntax::Query) -> Self {
        Self {
            relation,
            operation: None,
        }
    }

    pub fn bind(&self, ctx: &Context) -> Result<Query, QueryError> {
        self.bind_mode(ctx, false)
    }

    pub fn validate(&self, ctx: &Context) -> Result<Query, QueryError> {
        self.bind_mode(ctx, true)
    }

    fn bind_mode(&self, ctx: &Context, validating: bool) -> Result<Query, QueryError> {
        let bind = |relation| {
            if validating {
                super::parse::validate_syntax(relation, ctx)
            } else {
                super::parse::bind_syntax(relation, ctx)
            }
        };
        let source = match &self.operation {
            None => return bind(self.relation.clone()),
            Some(Operation::Walk {
                direction,
                reference,
            }) => {
                if !self.relation.transforms.is_empty()
                    || !self.relation.attach.is_empty()
                    || !matches!(&self.relation.source, syntax::Source::Unit { unit, condition: None, .. } if unit == "events")
                {
                    return Err(QueryError::new(
                        "provenance walks do not support query transforms",
                    ));
                }
                Source::Walk {
                    walk: match direction {
                        Direction::Trace => Walk::Trace,
                        Direction::Parents => Walk::Parents,
                        Direction::Descendants => Walk::Descendants,
                    },
                    reference: parse_reference(reference)?,
                }
            }
            Some(Operation::Recall {
                subject: Subject::Last { selector },
            }) => {
                let selector = bind((**selector).clone())?;
                if selector.source.alias().map(|alias| alias.name) != Some("shell-sessions")
                    || !selector.stages.is_empty()
                {
                    return Err(QueryError::new(
                        "last selects a shell-sessions relation without transforms",
                    ));
                }
                Source::RecallLast {
                    filter: selector.filter,
                }
            }
            Some(Operation::Recall {
                subject: Subject::Mark { number, radius },
            }) => {
                let number = scalar(number)?
                    .parse::<u64>()
                    .map_err(|_| QueryError::new("mark number must be a positive integer"))?;
                if number == 0 {
                    return Err(QueryError::new("mark number starts at one"));
                }
                Source::RecallMark {
                    number,
                    window: seconds(radius.as_ref())?,
                }
            }
            Some(Operation::Recall {
                subject: Subject::Time { value, radius },
            }) => {
                let (from, to) = if let Some(value) = value
                    && scalar(value)?.starts_with("sinex:window/")
                {
                    match parse_reference(scalar(value)?)? {
                        Reference::Window { from, to } => (from, to),
                        _ => unreachable!(),
                    }
                } else {
                    let resolved = value
                        .as_ref()
                        .map(|value| {
                            query_language::time::resolve(scalar(value)?, &ctx.clock()?)
                                .map_err(super::syntax::error)
                        })
                        .transpose()?;
                    if radius.is_none()
                        && let Some(range) = resolved.as_ref().and_then(|value| value.value.range())
                    {
                        (range.start, range.end)
                    } else {
                        let centre = resolved.map_or(ctx.now, |value| value.value.start());
                        let radius = jiff::SignedDuration::from_secs_f64(seconds(radius.as_ref())?);
                        (
                            centre
                                .checked_sub(radius)
                                .map_err(|e| QueryError::new(e.to_string()))?,
                            centre
                                .checked_add(radius)
                                .map_err(|e| QueryError::new(e.to_string()))?,
                        )
                    }
                };
                Source::Recall { from, to }
            }
        };
        super::parse::bind_native_syntax(self.relation.clone(), ctx, source, validating)
    }

    /// Spans locate authored text, and do not identify a question.
    pub fn identity(&self) -> Result<serde_json::Value, QueryError> {
        let mut value = serde_json::to_value(self).map_err(|e| QueryError::new(e.to_string()))?;
        fn strip(value: &mut serde_json::Value) {
            match value {
                serde_json::Value::Object(fields) => {
                    fields.remove("span");
                    for value in fields.values_mut() {
                        strip(value);
                    }
                }
                serde_json::Value::Array(values) => {
                    for value in values {
                        strip(value);
                    }
                }
                _ => {}
            }
        }
        strip(&mut value);
        Ok(value)
    }
}

fn scalar(value: &syntax::Value) -> Result<&str, QueryError> {
    match value {
        syntax::Value::Scalar { text, .. } => Ok(text),
        _ => Err(QueryError::new("command argument needs a bound scalar")),
    }
}
fn seconds(value: Option<&syntax::Value>) -> Result<f64, QueryError> {
    let Some(value) = value else {
        return Ok(1800.0);
    };
    let duration = query_language::time::duration(scalar(value)?)
        .ok_or_else(|| QueryError::new("recall radius needs a positive duration"))?;
    let seconds = duration.as_secs_f64();
    if seconds <= 0.0 || !seconds.is_finite() {
        return Err(QueryError::new("recall radius needs a positive duration"));
    }
    Ok(seconds)
}

pub fn is_command(text: &str) -> bool {
    text.split_whitespace().next().is_some_and(|word| {
        ["recall", "trace", "parents", "descendants"]
            .iter()
            .any(|command| word.eq_ignore_ascii_case(command))
    })
}

/// The command owns its subject. The shared parser owns the entire relation.
pub fn parse(text: &str, ctx: &Context) -> Result<Template, QueryError> {
    let text = text.trim();
    let (command, arguments) = text.split_once(char::is_whitespace).unwrap_or((text, ""));
    if !is_command(text) {
        return Ok(Template::query(super::syntax::parse(text, ctx)?));
    }
    if !command.eq_ignore_ascii_case("recall") {
        let direction = if command.eq_ignore_ascii_case("trace") {
            Direction::Trace
        } else if command.eq_ignore_ascii_case("parents") {
            Direction::Parents
        } else {
            Direction::Descendants
        };
        let reference = arguments.trim();
        parse_reference(reference)?;
        return Ok(Template {
            relation: super::syntax::parse("events", ctx)?,
            operation: Some(Operation::Walk {
                direction,
                reference: reference.into(),
            }),
        });
    }
    let (subject, tail) = split_subject(arguments.trim());
    let source = if subject.trim().eq_ignore_ascii_case("last") {
        "shell-sessions"
    } else {
        "events"
    };
    let mut relation = super::syntax::parse(&format!("{source} {tail}"), ctx)?;
    let (subject, radius) = subject
        .split_once('±')
        .map_or((subject.trim(), None), |(subject, radius)| {
            (subject.trim(), Some(argument(radius.trim())))
        });
    let subject = if subject.eq_ignore_ascii_case("last") {
        let mut selector = super::syntax::parse("shell-sessions", ctx)?;
        if let (
            syntax::Source::Unit { condition, .. },
            syntax::Source::Unit {
                condition: selected,
                ..
            },
        ) = (&mut relation.source, &mut selector.source)
        {
            *selected = condition.take();
        }
        relation.source = super::syntax::parse("events", ctx)?.source;
        Subject::Last {
            selector: Box::new(selector),
        }
    } else if subject.eq_ignore_ascii_case("mark")
        || subject.to_ascii_lowercase().starts_with("mark ")
    {
        let number = subject.split_whitespace().nth(1).unwrap_or("1");
        Subject::Mark {
            number: argument(number),
            radius,
        }
    } else {
        Subject::Time {
            value: (!subject.is_empty()).then(|| argument(subject)),
            radius,
        }
    };
    Ok(Template {
        relation,
        operation: Some(Operation::Recall { subject }),
    })
}

fn argument(text: &str) -> syntax::Value {
    text.strip_prefix('$').map_or_else(
        || syntax::Value::scalar(text),
        |name| syntax::Value::Parameter { name: name.into() },
    )
}

// Command subjects have no quoted query expressions. Once a relation delimiter
// starts, its bytes pass untouched to the shared parser, including quoted values.
fn split_subject(text: &str) -> (&str, &str) {
    for (index, character) in text.char_indices() {
        if character == '|' {
            return (&text[..index], &text[index..]);
        }
        if (index == 0 || text[..index].ends_with(char::is_whitespace))
            && !character.is_whitespace()
        {
            let tail = &text[index..];
            let word = tail.split_whitespace().next().unwrap_or("");
            if [
                "where", "sort", "limit", "offset", "fields", "group", "count", "fill", "distinct",
                "as",
            ]
            .iter()
            .any(|keyword| word.eq_ignore_ascii_case(keyword))
            {
                return (&text[..index], tail);
            }
        }
    }
    (text, "")
}

#[cfg(test)]
mod tests;
