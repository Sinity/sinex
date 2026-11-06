//! Saved definitions expand scalar nodes of the shared syntax tree.
use super::*;
use query_language::ast as syntax;
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
    pub template: Definition,
    pub parameters: Vec<Parameter>,
}

/// Current writes use Current. FormatOne is constructed only by the immutable
/// historical source reader and numbered stored-data conversion.
#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize, schemars::JsonSchema)]
#[serde(tag = "format", rename_all = "snake_case", deny_unknown_fields)]
pub enum Definition {
    Current {
        body: native::Template,
    },
    FormatOne {
        body: historical_query_format_v1::ast::Query,
        origin_text_hash: String,
        origin_body_hash: String,
    },
}
impl From<native::Template> for Definition {
    fn from(body: native::Template) -> Self {
        Self::Current { body }
    }
}
impl Definition {
    pub(super) fn from_format_one(
        body: historical_query_format_v1::ast::Query,
        authored: &str,
    ) -> Self {
        let origin_body_hash = Self::body_hash(&body);
        Self::FormatOne {
            body,
            origin_text_hash: blake3::hash(authored.as_bytes()).to_hex().to_string(),
            origin_body_hash,
        }
    }
    fn body_hash(body: &historical_query_format_v1::ast::Query) -> String {
        blake3::hash(&serde_json::to_vec(body).expect("decoded historical syntax serializes"))
            .to_hex()
            .to_string()
    }
    pub fn check_source(&self, authored: &str) -> Result<(), QueryError> {
        if let Self::FormatOne {
            body,
            origin_text_hash,
            origin_body_hash,
        } = self
            && (*origin_text_hash != blake3::hash(authored.as_bytes()).to_hex().as_str()
                || *origin_body_hash != Self::body_hash(body))
        {
            return Err(QueryError::new(
                "historical saved definition does not match its source proof",
            ));
        }
        Ok(())
    }
    pub fn reference_name(&self) -> Option<String> {
        match self {
            Self::Current { body } => match &body.relation.source {
                syntax::Source::Saved { name, .. } => Some(name.clone()),
                _ => None,
            },
            Self::FormatOne { body, .. } => match &body.source {
                historical_query_format_v1::ast::Source::Saved { name, .. } => Some(name.clone()),
                _ => None,
            },
        }
    }
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

fn check_value(value: &syntax::Value, ty: ParameterType) -> Result<(), QueryError> {
    if matches!(value, syntax::Value::Parameter { .. }) {
        return Ok(());
    }
    let raw = value
        .text()
        .ok_or_else(|| QueryError::new("parameter needs one scalar value"))?;
    let valid = match ty {
        ParameterType::Text => true,
        ParameterType::Integer => query_language::number::is_integer(raw),
        ParameterType::Number => query_language::number::is_decimal(raw),
        ParameterType::Bool => matches!(raw, "true" | "false"),
        ParameterType::Time => raw.parse::<jiff::Timestamp>().is_ok(),
        ParameterType::Duration => time::duration(raw).is_some(),
    };
    if valid {
        Ok(())
    } else {
        Err(QueryError::new(format!("parameter must be {ty:?}")))
    }
}
fn sample(p: &Parameter) -> syntax::Value {
    match p.field_type {
        ParameterType::Text => syntax::Value::Parameter {
            name: p.name.clone(),
        },
        ParameterType::Integer | ParameterType::Number => syntax::Value::scalar("1"),
        ParameterType::Bool => syntax::Value::scalar("true"),
        ParameterType::Time => syntax::Value::scalar("2000-01-01T00:00:00Z"),
        ParameterType::Duration => syntax::Value::scalar("1h"),
    }
}
impl SavedQuery {
    pub fn from_text(
        name: String,
        query: String,
        parameters: Vec<Parameter>,
        ctx: &Context,
    ) -> Result<Self, QueryError> {
        let template = native::parse(&query, ctx)?;
        Ok(Self {
            name,
            query,
            template: template.into(),
            parameters,
        })
    }

    pub fn validate(&self, ctx: &Context) -> Result<(), QueryError> {
        self.validate_with(ctx, &BTreeMap::new())
    }
    pub fn validate_with(
        &self,
        ctx: &Context,
        definitions: &BTreeMap<String, SavedQuery>,
    ) -> Result<(), QueryError> {
        let input = self.validation_ast(ctx)?;
        let input = expand_definition(
            input,
            ctx,
            definitions,
            &mut BTreeSet::new(),
            &mut false,
            true,
        )?;
        input.validate(ctx).map(|_| ())
    }
    pub fn validate_parameters(&self) -> Result<(), QueryError> {
        // Syntax validation has no time interpretation. A context supplies only
        // the same bundled field catalog used by public query discovery.
        let ctx = Context::utc(jiff::Timestamp::UNIX_EPOCH);
        self.validation_ast(&ctx).map(|_| ())
    }
    fn validation_ast(&self, ctx: &Context) -> Result<Definition, QueryError> {
        if !valid_name(&self.name) {
            return Err(QueryError::new(
                "saved query name uses letters, numbers, _ or -",
            ));
        }
        let mut args = BTreeMap::new();
        for p in &self.parameters {
            if !valid_name(&p.name) || args.insert(p.name.clone(), sample(p)).is_some() {
                return Err(QueryError::new(
                    "parameter names must be valid and distinct",
                ));
            }
        }
        substitute(self, &args, ctx, true)
    }
}
fn substitute(
    saved: &SavedQuery,
    args: &BTreeMap<String, syntax::Value>,
    ctx: &Context,
    validating: bool,
) -> Result<Definition, QueryError> {
    for name in args.keys() {
        if !saved.parameters.iter().any(|p| p.name == *name) {
            return Err(QueryError::new(format!(
                "query:{} has no parameter {name}",
                saved.name
            )));
        }
    }
    let mut arguments = BTreeMap::new();
    for p in &saved.parameters {
        let value = args.get(&p.name).ok_or_else(|| {
            QueryError::new(format!("query:{} needs parameter {}", saved.name, p.name))
        })?;
        check_value(value, p.field_type)?;
        let mut value = value.clone();
        if let syntax::Value::Scalar { quoted, .. } = &mut value {
            *quoted = p.field_type == ParameterType::Text;
        }
        arguments.insert(p.name.clone(), value);
    }
    saved.template.check_source(&saved.query)?;
    let parsed = saved.template.clone();
    let mut wire = serde_json::to_value(parsed).map_err(|e| QueryError::new(e.to_string()))?;
    let mut used = BTreeSet::new();
    replace_parameters(&mut wire, &arguments, &mut used, validating, false)?;
    for p in &saved.parameters {
        if !used.contains(&p.name) {
            return Err(QueryError::new(format!("parameter {} is unused", p.name)));
        }
    }
    serde_json::from_value(wire).map_err(|e| QueryError::new(e.to_string()))
}
fn replace_parameters(
    value: &mut serde_json::Value,
    args: &BTreeMap<String, syntax::Value>,
    used: &mut BTreeSet<String>,
    validating: bool,
    upper: bool,
) -> Result<(), QueryError> {
    if value.get("kind").and_then(serde_json::Value::as_str) == Some("parameter") {
        let name = value["name"]
            .as_str()
            .ok_or_else(|| QueryError::new("invalid scalar parameter"))?
            .to_owned();
        let replacement = args
            .get(&name)
            .ok_or_else(|| QueryError::new(format!("undeclared parameter {name}")))?;
        used.insert(name);
        let mut replacement = replacement.clone();
        if validating
            && upper
            && matches!(&replacement,syntax::Value::Scalar{text,..} if text=="2000-01-01T00:00:00Z")
        {
            replacement = syntax::Value::scalar("2000-01-02T00:00:00Z");
        }
        *value = serde_json::to_value(replacement).map_err(|e| QueryError::new(e.to_string()))?;
        return Ok(());
    }
    match value {
        serde_json::Value::Array(values) => {
            for v in values {
                replace_parameters(v, args, used, validating, upper)?;
            }
        }
        serde_json::Value::Object(fields) => {
            let upper = upper
                || matches!(
                    fields.get("op").and_then(serde_json::Value::as_str),
                    Some("lt" | "le")
                );
            for (name, v) in fields {
                replace_parameters(v, args, used, validating, upper || name == "to")?;
            }
        }
        _ => {}
    }
    Ok(())
}
pub fn reference_name(text: &str) -> Option<String> {
    let ctx = Context::utc(jiff::Timestamp::UNIX_EPOCH);
    match super::syntax::parse(text, &ctx).ok()?.source {
        syntax::Source::Saved { name, .. } => Some(name),
        _ => None,
    }
}
pub fn template_saved(
    text: &str,
    ctx: &Context,
    saved: &BTreeMap<String, SavedQuery>,
) -> Result<native::Template, QueryError> {
    resolve_template(native::parse(text, ctx)?, ctx, saved)
}

pub fn resolve_template(
    input: native::Template,
    ctx: &Context,
    saved: &BTreeMap<String, SavedQuery>,
) -> Result<native::Template, QueryError> {
    let mut expanded_definition = false;
    let result = expand(
        input,
        ctx,
        saved,
        &mut BTreeSet::new(),
        &mut expanded_definition,
        false,
    );
    result.map_err(|mut error| {
        // Definition spans belong to the stored question, not its invocation.
        if expanded_definition {
            error.at = None;
        }
        error
    })
}
pub fn bind_saved(
    text: &str,
    ctx: &Context,
    saved: &BTreeMap<String, SavedQuery>,
) -> Result<Query, QueryError> {
    template_saved(text, ctx, saved)?.bind(ctx)
}

fn expand_definition(
    input: Definition,
    ctx: &Context,
    saved: &BTreeMap<String, SavedQuery>,
    seen: &mut BTreeSet<String>,
    expanded_definition: &mut bool,
    validating: bool,
) -> Result<native::Template, QueryError> {
    match input {
        Definition::Current { body } => {
            expand(body, ctx, saved, seen, expanded_definition, validating)
        }
        Definition::FormatOne { body, .. } => {
            expand_historical(body, ctx, saved, seen, expanded_definition, validating)
        }
    }
}

pub(super) fn resolve_historical(
    input: historical_query_format_v1::ast::Query,
    ctx: &Context,
    saved: &BTreeMap<String, SavedQuery>,
) -> Result<native::Template, QueryError> {
    expand_historical(input, ctx, saved, &mut BTreeSet::new(), &mut false, false)
}

pub(super) fn expand_historical(
    mut input: historical_query_format_v1::ast::Query,
    ctx: &Context,
    saved: &BTreeMap<String, SavedQuery>,
    seen: &mut BTreeSet<String>,
    expanded_definition: &mut bool,
    validating: bool,
) -> Result<native::Template, QueryError> {
    use historical_query_format_v1::ast as old;
    if let old::Source::Saved {
        name,
        arguments,
        condition,
        ..
    } = input.source.clone()
    {
        *expanded_definition = true;
        if !seen.insert(name.clone()) {
            return Err(QueryError::new(format!("saved query cycle at {name}")));
        }
        let definition = saved
            .get(&name)
            .ok_or_else(|| QueryError::new(format!("no saved query named {name}")))?;
        let mut args = BTreeMap::new();
        for argument in arguments {
            let value = serde_json::from_value(
                serde_json::to_value(argument.value)
                    .map_err(|error| QueryError::new(error.to_string()))?,
            )
            .map_err(|error| QueryError::new(error.to_string()))?;
            if args.insert(argument.parameter.clone(), value).is_some() {
                return Err(QueryError::new(format!(
                    "parameter {} was given twice",
                    argument.parameter
                )));
            }
        }
        let target = substitute(definition, &args, ctx, validating)?;
        let result = match target {
            Definition::FormatOne { mut body, .. } => {
                if let Some(condition) = condition {
                    body.stages.push(old::Stage {
                        span: condition.span,
                        kind: old::StageKind::Where { condition },
                    });
                }
                body.stages.append(&mut input.stages);
                body.attach.append(&mut input.attach);
                expand_historical(body, ctx, saved, seen, expanded_definition, validating)
            }
            Definition::Current { body } => {
                let base = expand(body, ctx, saved, seen, expanded_definition, validating)?;
                super::historical::convert_around_current(
                    input,
                    base,
                    ctx,
                    saved,
                    seen,
                    expanded_definition,
                    validating,
                )
            }
        };
        seen.remove(&name);
        return result;
    }
    // Nested relations have independent output grain and are expanded before
    // the format converter sees their temporal/projection semantics.
    super::historical::convert_expanded(input, ctx, saved, seen, expanded_definition, validating)
}

fn expand(
    mut input: native::Template,
    ctx: &Context,
    saved: &BTreeMap<String, SavedQuery>,
    seen: &mut BTreeSet<String>,
    expanded_definition: &mut bool,
    validating: bool,
) -> Result<native::Template, QueryError> {
    if let syntax::Source::Saved {
        name,
        arguments,
        condition,
        ..
    } = input.relation.source.clone()
    {
        *expanded_definition = true;
        if !seen.insert(name.clone()) {
            return Err(QueryError::new(format!("saved query cycle at {name}")));
        }
        let definition = saved
            .get(&name)
            .ok_or_else(|| QueryError::new(format!("no saved query named {name}")))?;
        let mut args = BTreeMap::new();
        for a in &arguments {
            if args.insert(a.parameter.clone(), a.value.clone()).is_some() {
                return Err(QueryError::new(format!(
                    "parameter {} was given twice",
                    a.parameter
                )));
            }
        }
        let definition = substitute(definition, &args, ctx, validating)?;
        let mut expanded = expand_definition(
            definition,
            ctx,
            saved,
            seen,
            expanded_definition,
            validating,
        )?;
        if let Some(condition) = condition {
            expanded.relation.transforms.push(syntax::Transform {
                span: condition.span,
                kind: syntax::TransformKind::Where { condition },
            });
        }
        expanded
            .relation
            .transforms
            .append(&mut input.relation.transforms);
        expanded.relation.attach.append(&mut input.relation.attach);
        let out = expand(expanded, ctx, saved, seen, expanded_definition, validating);
        seen.remove(&name);
        return out;
    }
    for transform in &mut input.relation.transforms {
        if let syntax::TransformKind::Combine { query, .. }
        | syntax::TransformKind::Join { query, .. }
        | syntax::TransformKind::Diff { query, .. } = &mut transform.kind
        {
            let inner = expand(
                native::Template::query((**query).clone()),
                ctx,
                saved,
                seen,
                expanded_definition,
                validating,
            )?;
            if inner.operation.is_some() {
                return Err(QueryError::new(
                    "native commands cannot be nested as query relations",
                ));
            }
            **query = inner.relation;
        }
    }
    Ok(input)
}
