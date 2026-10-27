//! Payload fields come from the same Rust schemas that admission registers.
use super::*;
use serde_json::Value as Json;
use std::collections::BTreeSet;
use std::sync::OnceLock;

#[derive(Clone, Debug)]
struct PayloadField {
    event_type: &'static str,
    path: Vec<String>,
    ty: FieldType,
    values: Option<Vec<String>>,
}

fn fields() -> &'static [PayloadField] {
    static FIELDS: OnceLock<Vec<PayloadField>> = OnceLock::new();
    FIELDS.get_or_init(|| {
        let mut out = Vec::new();
        for event in crate::event_types::ALL {
            let root = (event.json_schema)();
            collect(
                &root,
                &root,
                event.event_type,
                &mut Vec::new(),
                &mut BTreeSet::new(),
                &mut out,
            );
        }
        out
    })
}

fn collect(
    root: &Json,
    schema: &Json,
    event_type: &'static str,
    path: &mut Vec<String>,
    refs: &mut BTreeSet<String>,
    out: &mut Vec<PayloadField>,
) {
    if let Some(reference) = schema["$ref"].as_str().and_then(|s| s.strip_prefix('#')) {
        if refs.insert(reference.into()) {
            if let Some(target) = root.pointer(reference) {
                collect(root, target, event_type, path, refs, out);
            }
            refs.remove(reference);
        }
        return;
    }
    for name in ["anyOf", "oneOf", "allOf"] {
        if let Some(branches) = schema[name].as_array() {
            for branch in branches {
                collect(root, branch, event_type, path, refs, out);
            }
            return;
        }
    }
    let ty = schema["type"].as_str().or_else(|| {
        schema["type"]
            .as_array()?
            .iter()
            .filter_map(Json::as_str)
            .find(|s| *s != "null")
    });
    let ty = match ty {
        Some("integer") => FieldType::Integer,
        Some("number") => FieldType::Number,
        Some("boolean") => FieldType::Bool,
        Some("string") if schema["format"] == "date-time" => FieldType::Time,
        Some("string") => FieldType::Text,
        Some("array") if schema["items"]["type"] == "string" => FieldType::TextList,
        Some("null") | None => return,
        _ => FieldType::Json,
    };
    if !path.is_empty() {
        out.push(PayloadField {
            event_type,
            path: path.clone(),
            ty,
            values: schema["enum"].as_array().map(|vs| {
                vs.iter()
                    .filter_map(Json::as_str)
                    .map(str::to_owned)
                    .collect()
            }),
        });
    }
    if let Some(properties) = schema["properties"].as_object() {
        for (name, field) in properties {
            path.push(name.clone());
            collect(root, field, event_type, path, refs, out);
            path.pop();
        }
    }
}

/// A field name offered by every query surface, with its schema type.
#[derive(Clone, Debug, serde::Serialize, PartialEq, Eq)]
pub struct Completion {
    pub name: String,
    pub field_type: String,
}

pub fn complete(text: &str) -> Vec<Completion> {
    let start = text
        .rfind(|c: char| c.is_whitespace() || c == ',')
        .map_or(0, |i| i + 1);
    let prefix = &text[start..];
    let first = text.split_whitespace().next().unwrap_or("");
    let alias = Alias::from_name(first);
    let table = Table::from_name(first).unwrap_or(Table::Events);
    let mut kinds: Option<BTreeSet<String>> =
        alias.map(|a| a.kinds.iter().map(|s| (*s).into()).collect());
    if let Ok(tokens) = super::lex::tokens(&text[..start]) {
        let mut mentioned = BTreeSet::new();
        for (i, token) in tokens.iter().enumerate() {
            if !token.is_word("event_type")
                || !tokens
                    .get(i + 1)
                    .is_some_and(|t| t.is_symbol("=") || t.is_symbol(":"))
            {
                continue;
            }
            let values = &tokens[i + 2..];
            let list = values.first().is_some_and(|t| t.is_symbol("["));
            for value in values.iter().skip(usize::from(list)) {
                if value.is_symbol("]") {
                    break;
                }
                if value.kind != super::lex::Kind::Symbol {
                    let value = if value.kind == super::lex::Kind::Word && value.text.contains('*')
                    {
                        Value::Glob(value.text.clone())
                    } else {
                        Value::Text(value.text.clone())
                    };
                    if let Some(selected) = selected_value(&value) {
                        mentioned.extend(selected);
                    }
                }
                if !list {
                    break;
                }
            }
        }
        if !mentioned.is_empty() {
            kinds = Some(kinds.map_or(mentioned.clone(), |alias| {
                alias.intersection(&mentioned).cloned().collect()
            }));
        }
    }
    let mut choices = std::collections::BTreeMap::new();
    for field in table
        .fields()
        .iter()
        .chain(alias.into_iter().flat_map(|a| a.fields))
    {
        choices.insert(
            field.name.to_owned(),
            format!("{:?}", field.ty).to_lowercase(),
        );
    }
    if table.is_events() {
        for field in fields()
            .iter()
            .filter(|f| kinds.as_ref().is_none_or(|ks| ks.contains(f.event_type)))
        {
            let name = format!("payload.{}", field.path.join("."));
            let field_type = format!("{:?}", field.ty).to_lowercase();
            choices
                .entry(name)
                .and_modify(|prior| {
                    if prior != &field_type {
                        if [prior.as_str(), field_type.as_str()]
                            .iter()
                            .all(|t| matches!(*t, "integer" | "number"))
                        {
                            *prior = "number".into();
                        } else {
                            *prior = "ambiguous".into();
                        }
                    }
                })
                .or_insert(field_type);
        }
    }
    choices
        .into_iter()
        .filter(|(name, _)| name.starts_with(prefix))
        .map(|(name, field_type)| Completion { name, field_type })
        .collect()
}

fn glob_matches(pattern: &str, text: &str) -> bool {
    let (pattern, text) = (pattern.as_bytes(), text.as_bytes());
    let (mut p, mut t, mut star, mut retry) = (0, 0, None, 0);
    while t < text.len() {
        if p < pattern.len() && pattern[p] == text[t] {
            p += 1;
            t += 1;
        } else if p < pattern.len() && pattern[p] == b'*' {
            star = Some(p);
            p += 1;
            retry = t;
        } else if let Some(last) = star {
            p = last + 1;
            retry += 1;
            t = retry;
        } else {
            return false;
        }
    }
    while p < pattern.len() && pattern[p] == b'*' {
        p += 1;
    }
    p == pattern.len()
}

fn selected_value(value: &Value) -> Option<BTreeSet<String>> {
    match value {
        Value::Text(s) => Some(BTreeSet::from([s.clone()])),
        Value::Glob(pattern) => Some(
            crate::event_types::ALL
                .iter()
                .filter(|event| glob_matches(pattern, event.event_type))
                .map(|event| event.event_type.into())
                .collect(),
        ),
        Value::List(values) => {
            let mut out = BTreeSet::new();
            for value in values {
                out.extend(selected_value(value)?);
            }
            Some(out)
        }
        _ => None,
    }
}

fn selected(e: &Expr) -> Option<BTreeSet<String>> {
    match e {
        Expr::Compare {
            lhs: Operand::Field(FieldRef::Field(f)),
            op: Op::Eq,
            rhs,
        } if f.name == "event_type" => selected_value(rhs),
        Expr::And(a, b) => match (selected(a), selected(b)) {
            (Some(a), Some(b)) => Some(a.intersection(&b).cloned().collect()),
            (a, b) => a.or(b),
        },
        Expr::Or(a, b) => Some(selected(a)?.union(&selected(b)?).cloned().collect()),
        _ => None,
    }
}

fn field_type(path: &[String], kinds: &Option<BTreeSet<String>>) -> Result<FieldType, QueryError> {
    let mut types = fields()
        .iter()
        .filter(|f| f.path == path && kinds.as_ref().is_none_or(|ks| ks.contains(f.event_type)))
        .map(|f| f.ty);
    let Some(mut ty) = types.next() else {
        return Err(QueryError::new(format!(
            "payload.{} is not declared by the selected event schemas",
            path.join(".")
        )));
    };
    for other in types {
        if matches!(
            (ty, other),
            (FieldType::Number, FieldType::Integer) | (FieldType::Integer, FieldType::Number)
        ) {
            ty = FieldType::Number;
        } else if ty != other {
            return Err(QueryError::new(format!(
                "payload.{} has different types; select an event_type",
                path.join(".")
            )));
        }
    }
    Ok(ty)
}

fn field(f: &mut FieldRef, kinds: &Option<BTreeSet<String>>) -> Result<(), QueryError> {
    if let FieldRef::Payload(path) = f {
        *f = FieldRef::TypedPayload {
            ty: field_type(path, kinds)?,
            path: path.clone(),
        };
    }
    Ok(())
}
fn operand(o: &mut Operand, kinds: &Option<BTreeSet<String>>) -> Result<(), QueryError> {
    match o {
        Operand::Field(f) => field(f, kinds),
        Operand::TimeFn(_, f) => {
            field(f, kinds)?;
            if let FieldRef::TypedPayload { ty, .. } = f
                && *ty != FieldType::Time
            {
                return Err(QueryError::new("time functions require a time field"));
            }
            Ok(())
        }
    }
}
fn validate_value(v: &mut Value, ty: FieldType, ctx: &Context) -> Result<(), QueryError> {
    if let Value::List(values) = v {
        for value in values {
            validate_value(value, ty, ctx)?;
        }
        return Ok(());
    }
    if ty == FieldType::Time
        && let Value::Text(text) = v
    {
        *v = Value::Time(
            time::instant(ctx, text)
                .ok_or_else(|| QueryError::new("a payload time requires an absolute timestamp"))?,
        );
    }
    let valid = matches!(
        (ty, &*v),
        (FieldType::Integer, Value::Integer(_))
            | (FieldType::Number, Value::Integer(_) | Value::Number(_))
            | (
                FieldType::Text | FieldType::TextList,
                Value::Text(_) | Value::Glob(_)
            )
            | (FieldType::Time, Value::Time(_))
            | (FieldType::Bool, Value::Bool(_))
    );
    if valid {
        Ok(())
    } else {
        Err(QueryError::new(format!(
            "payload value must be {}",
            format!("{ty:?}").to_lowercase()
        )))
    }
}
fn expr(e: &mut Expr, kinds: &Option<BTreeSet<String>>, ctx: &Context) -> Result<(), QueryError> {
    let narrowed = selected(e).map(|selected| {
        kinds.as_ref().map_or(selected.clone(), |ks| {
            ks.intersection(&selected).cloned().collect()
        })
    });
    let kinds = if narrowed.is_some() { &narrowed } else { kinds };
    match e {
        Expr::And(a, b) | Expr::Or(a, b) => {
            expr(a, kinds, ctx)?;
            expr(b, kinds, ctx)?;
        }
        Expr::Not(e) => expr(e, kinds, ctx)?,
        Expr::Compare { lhs, op, rhs } => {
            operand(lhs, kinds)?;
            if let Operand::Field(FieldRef::TypedPayload { ty, .. }) = lhs {
                if matches!(op, Op::Contains | Op::Prefix)
                    && !matches!(ty, FieldType::Text | FieldType::TextList)
                {
                    return Err(QueryError::new("~ and ^ require a text payload field"));
                }
                if op.ordering()
                    && matches!(ty, FieldType::Bool | FieldType::Json | FieldType::TextList)
                {
                    return Err(QueryError::new("this payload field cannot be ordered"));
                }
                validate_value(rhs, *ty, ctx)?;
                if let Operand::Field(FieldRef::TypedPayload { path, .. }) = lhs {
                    let definitions: Vec<_> = fields()
                        .iter()
                        .filter(|f| {
                            f.path == *path
                                && kinds.as_ref().is_none_or(|ks| ks.contains(f.event_type))
                        })
                        .collect();
                    if !definitions.is_empty() && definitions.iter().all(|f| f.values.is_some()) {
                        let values: BTreeSet<&str> = definitions
                            .iter()
                            .flat_map(|f| {
                                f.values.as_ref().into_iter().flatten().map(String::as_str)
                            })
                            .collect();
                        let check =
                            |v: &Value| matches!(v,Value::Text(s) if values.contains(s.as_str()));
                        let valid = if let Value::List(items) = rhs {
                            items.iter().all(check)
                        } else {
                            check(rhs)
                        };
                        if !valid {
                            return Err(QueryError::new(format!(
                                "payload.{} is one of: {}",
                                path.join("."),
                                values.into_iter().collect::<Vec<_>>().join(", ")
                            )));
                        }
                    }
                }
            }
        }
        Expr::Within { lhs, .. } => {
            operand(lhs, kinds)?;
            if let Operand::Field(FieldRef::TypedPayload { ty, .. }) = lhs
                && *ty != FieldType::Time
            {
                return Err(QueryError::new("in requires a time field"));
            }
        }
        Expr::Exists(o) => operand(o, kinds)?,
        Expr::TimeDiff { a, b, .. } => {
            field(a, kinds)?;
            field(b, kinds)?;
        }
        Expr::Words(_) | Expr::Overlaps { .. } => {}
    }
    Ok(())
}

pub(super) fn check(q: &mut Query, ctx: &Context) -> Result<(), QueryError> {
    if let Source::RecallLast { filter: Some(e) } = &mut q.source {
        let alias = Alias::from_name("shell-sessions").expect("shell sessions declared");
        let kinds = Some(alias.kinds.iter().map(|s| (*s).into()).collect());
        expr(e, &kinds, ctx)?;
    }
    let mut kinds: Option<BTreeSet<String>> = q
        .source
        .alias()
        .map(|a| a.kinds.iter().map(|s| (*s).into()).collect());
    for e in q.filter.iter().chain(
        q.stages
            .iter()
            .take_while(|s| !matches!(s, Stage::Group(_)))
            .filter_map(|s| {
                if let Stage::Where(e) = s {
                    Some(e)
                } else {
                    None
                }
            }),
    ) {
        if let Some(selected) = selected(e) {
            kinds = Some(kinds.map_or(selected.clone(), |ks| {
                ks.intersection(&selected).cloned().collect()
            }));
        }
    }
    if let Some(e) = &mut q.filter {
        expr(e, &kinds, ctx)?;
    }
    for stage in &mut q.stages {
        match stage {
            Stage::Where(e) => expr(e, &kinds, ctx)?,
            Stage::Join(j) => check(&mut j.inner, ctx)?,
            Stage::Fields(fs) => {
                for f in fs {
                    field(f, &kinds)?;
                }
            }
            Stage::Sort(keys) => {
                for k in keys {
                    if let SortBy::Operand(o) = &mut k.key {
                        operand(o, &kinds)?;
                    }
                }
            }
            Stage::Group(g) => {
                for k in &mut g.keys {
                    operand(k, &kinds)?;
                }
                for a in &mut g.aggregates {
                    if let Some(f) = &mut a.field {
                        field(f, &kinds)?;
                        let ty = match f {
                            FieldRef::TypedPayload { ty, .. } => Some(*ty),
                            FieldRef::Field(f) => Some(f.ty),
                            _ => None,
                        };
                        if matches!(a.function, AggFn::Min | AggFn::Max)
                            && ty.is_some_and(|ty| {
                                matches!(
                                    ty,
                                    FieldType::Bool
                                        | FieldType::Json
                                        | FieldType::TextList
                                        | FieldType::PathList
                                        | FieldType::IdList
                                        | FieldType::Id
                                )
                            })
                        {
                            return Err(QueryError::new(
                                "min and max require an ordered scalar field",
                            ));
                        }
                        if matches!(a.function, AggFn::Sum | AggFn::Avg)
                            && ty.is_some_and(|ty| {
                                !matches!(ty, FieldType::Integer | FieldType::Number)
                            })
                        {
                            return Err(QueryError::new(format!(
                                "{} requires a numeric field declared by the event schema",
                                a.function.name()
                            )));
                        }
                    }
                }
            }
            _ => {}
        }
    }
    Ok(())
}
