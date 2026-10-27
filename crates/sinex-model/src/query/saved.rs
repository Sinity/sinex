//! Saved queries bind scalar parameters as tokens before the ordinary type check.
use super::lex::{Kind, Token, tokens};
use super::*;
use std::collections::{BTreeMap, BTreeSet};

#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize, schemars::JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct Parameter {
    pub name: String,
    pub field_type: ParameterType,
}
#[derive(
    Clone, Copy, Debug, PartialEq, serde::Serialize, serde::Deserialize, schemars::JsonSchema,
)]
#[serde(rename_all = "snake_case")]
pub enum ParameterType {
    Text,
    Integer,
    Number,
    Bool,
    Time,
    Duration,
}

#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize, schemars::JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct SavedQuery {
    pub name: String,
    pub query: String,
    pub parameters: Vec<Parameter>,
}

pub fn valid_name(name: &str) -> bool {
    !name.is_empty()
        && name
            .bytes()
            .all(|c| c.is_ascii_alphanumeric() || c == b'_' || c == b'-')
}

pub fn parameter(text: &str) -> Result<Parameter, QueryError> {
    let (name, ty) = text.split_once(':').ok_or_else(|| {
        QueryError::new(
            "parameter declaration: name:text (or integer, number, bool, time, duration)",
        )
    })?;
    if !valid_name(name) {
        return Err(QueryError::new(
            "parameter name uses letters, numbers, _ or -",
        ));
    }
    let field_type = match ty {
        "text" => ParameterType::Text,
        "integer" => ParameterType::Integer,
        "number" => ParameterType::Number,
        "bool" => ParameterType::Bool,
        "time" => ParameterType::Time,
        "duration" => ParameterType::Duration,
        _ => {
            return Err(QueryError::new(
                "parameter type: text, integer, number, bool, time, duration",
            ));
        }
    };
    Ok(Parameter {
        name: name.into(),
        field_type,
    })
}

fn check_value(token: &Token, ty: ParameterType) -> Result<(), QueryError> {
    let value = &token.text;
    let valid = match ty {
        ParameterType::Text => true,
        ParameterType::Integer => value.parse::<i64>().is_ok(),
        ParameterType::Number => value.parse::<f64>().is_ok_and(f64::is_finite),
        ParameterType::Bool => matches!(value.as_str(), "true" | "false"),
        ParameterType::Time => value.parse::<jiff::Timestamp>().is_ok(),
        ParameterType::Duration => time::duration(value).is_some(),
    };
    if valid {
        Ok(())
    } else {
        Err(QueryError::at(
            token.at,
            format!("parameter must be {}", format!("{ty:?}").to_lowercase()),
        ))
    }
}

impl SavedQuery {
    pub fn validate(&self, ctx: &Context) -> Result<(), QueryError> {
        if !valid_name(&self.name) {
            return Err(QueryError::new(
                "saved query name uses letters, numbers, _ or -",
            ));
        }
        let mut names = BTreeSet::new();
        let mut arguments = BTreeMap::new();
        for p in &self.parameters {
            if !valid_name(&p.name) || !names.insert(&p.name) {
                return Err(QueryError::new(
                    "parameter names must be valid and distinct",
                ));
            }
            let text = match p.field_type {
                ParameterType::Text => "/synthetic",
                ParameterType::Integer | ParameterType::Number => "1",
                ParameterType::Bool => "true",
                ParameterType::Time => "2026-01-01T00:00:00Z",
                ParameterType::Duration => "1h",
            };
            arguments.insert(
                p.name.clone(),
                Token {
                    kind: if p.field_type == ParameterType::Text {
                        Kind::Quoted
                    } else {
                        Kind::Word
                    },
                    text: text.into(),
                    at: 0,
                },
            );
        }
        let input = substitute(self, &arguments)?;
        if self.parameters.is_empty() {
            super::parse::bind_tokens(input, ctx, self.query.len()).map(|_| ())
        } else {
            // Parameter values decide valid enum values and time ranges. The
            // whole question is checked when those values are supplied.
            Ok(())
        }
    }
}

fn substitute(
    saved: &SavedQuery,
    arguments: &BTreeMap<String, Token>,
) -> Result<Vec<Token>, QueryError> {
    for name in arguments.keys() {
        if !saved.parameters.iter().any(|p| p.name == *name) {
            return Err(QueryError::new(format!(
                "query:{} has no parameter {name}",
                saved.name
            )));
        }
    }
    for p in &saved.parameters {
        let token = arguments.get(&p.name).ok_or_else(|| {
            QueryError::new(format!("query:{} needs parameter {}", saved.name, p.name))
        })?;
        check_value(token, p.field_type)?;
    }
    let mut input = tokens(&saved.query)?;
    let mut used = BTreeSet::new();
    let mut list_depth = 0_usize;
    for i in 0..input.len() {
        if input[i].is_symbol("[") {
            list_depth += 1;
        }
        if input[i].is_symbol("]") {
            list_depth = list_depth.saturating_sub(1);
        }
        if input[i].kind != Kind::Word {
            continue;
        }
        if let Some(name) = input[i].text.strip_prefix('$') {
            let name = name.to_owned();
            if i == 2 && input[0].is_word("query") && input[1].is_symbol(":") {
                return Err(QueryError::at(
                    input[i].at,
                    "parameters stand only in scalar value positions",
                ));
            }
            let preceding = i.checked_sub(1).and_then(|j| input.get(j));
            if !preceding.is_some_and(|t| {
                ["=", "!=", "<", "<=", ">", ">=", "~", "^", ":", "["]
                    .iter()
                    .any(|s| t.is_symbol(s))
                    || (list_depth > 0 && t.is_symbol(","))
                    || [
                        "since", "until", "within", "limit", "of", "recall", "overlaps",
                    ]
                    .iter()
                    .any(|s| t.is_word(s))
            }) {
                return Err(QueryError::at(
                    input[i].at,
                    "parameters stand only in scalar value positions",
                ));
            }
            input[i] = arguments
                .get(&name)
                .ok_or_else(|| QueryError::new(format!("undeclared parameter {name}")))?
                .clone();
            if saved
                .parameters
                .iter()
                .any(|p| p.name == name && p.field_type == ParameterType::Text)
            {
                input[i].kind = Kind::Quoted;
            }
            used.insert(name);
        }
    }
    for p in &saved.parameters {
        if !used.contains(&p.name) {
            return Err(QueryError::new(format!("parameter {} is unused", p.name)));
        }
    }
    Ok(input)
}

/// The named query at the start of this text, using the ordinary lexer.
pub fn reference_name(text: &str) -> Option<String> {
    let input = tokens(text).ok()?;
    if input.first().is_some_and(|t| t.is_word("query"))
        && input.get(1).is_some_and(|t| t.is_symbol(":"))
    {
        input.get(2).map(|t| t.text.clone())
    } else {
        None
    }
}

/// `query:name(root="/src/tide", days=7d)` reads a saved question. Later
/// clauses operate on its result, including grouped result columns.
pub fn bind_saved(
    text: &str,
    ctx: &Context,
    saved: &BTreeMap<String, SavedQuery>,
) -> Result<Query, QueryError> {
    let mut input = tokens(text)?;
    let mut seen = BTreeSet::new();
    while input.first().is_some_and(|t| t.is_word("query"))
        && input.get(1).is_some_and(|t| t.is_symbol(":"))
    {
        let name = input
            .get(2)
            .ok_or_else(|| QueryError::new("query: needs a saved query name"))?
            .text
            .clone();
        if !seen.insert(name.clone()) {
            return Err(QueryError::new(format!("saved query cycle at {name}")));
        }
        let definition = saved
            .get(&name)
            .ok_or_else(|| QueryError::new(format!("no saved query named {name}")))?;
        let mut at = 3;
        let mut arguments = BTreeMap::new();
        if input.get(at).is_some_and(|t| t.is_symbol("(")) {
            at += 1;
            while !input.get(at).is_some_and(|t| t.is_symbol(")")) {
                let name = input
                    .get(at)
                    .filter(|t| t.kind == Kind::Word)
                    .ok_or_else(|| QueryError::new("expected parameter name"))?
                    .text
                    .clone();
                if !input.get(at + 1).is_some_and(|t| t.is_symbol("=")) {
                    return Err(QueryError::new("parameter argument: name=value"));
                }
                let value = input
                    .get(at + 2)
                    .filter(|t| t.kind != Kind::Symbol)
                    .ok_or_else(|| QueryError::new("parameter needs one scalar value"))?
                    .clone();
                if arguments.insert(name.clone(), value).is_some() {
                    return Err(QueryError::new(format!("parameter {name} was given twice")));
                }
                at += 3;
                if input.get(at).is_some_and(|t| t.is_symbol(",")) {
                    at += 1;
                } else if !input.get(at).is_some_and(|t| t.is_symbol(")")) {
                    return Err(QueryError::new("expected , or ) after parameter"));
                }
            }
            at += 1;
        }
        let mut bound = substitute(definition, &arguments)?;
        bound.extend(input.into_iter().skip(at));
        input = bound;
    }
    super::parse::bind_tokens(input, ctx, text.len()).map_err(|mut error| {
        // Expanded tokens name positions in several texts, not the invocation.
        if !seen.is_empty() {
            error.at = None;
        }
        error
    })
}
