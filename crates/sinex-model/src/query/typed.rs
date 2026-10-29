//! Payload fields come from the same Rust schemas that admission registers.
use super::*;
use serde_json::Value as Json;
use std::collections::BTreeSet;
use std::sync::OnceLock;

#[derive(Clone, Debug)]
struct PayloadField {
    event_type: String,
    path: Vec<String>,
    ty: FieldType,
    values: Option<Vec<String>>,
}

fn payload_fields(catalog: &QueryCatalog) -> Vec<PayloadField> {
    catalog
        .kinds
        .iter()
        .flat_map(|kind| {
            kind.fields
                .iter()
                .flat_map(|fields| fields.values())
                .map(move |field| PayloadField {
                    event_type: kind.kind.name.clone(),
                    path: field.path.clone(),
                    ty: match &field.ty {
                        catalog::DataType::Text => FieldType::Text,
                        catalog::DataType::Integer => FieldType::Integer,
                        catalog::DataType::Number => FieldType::Number,
                        catalog::DataType::Bool => FieldType::Bool,
                        catalog::DataType::Time => FieldType::Time,
                        catalog::DataType::Array(item) if **item == catalog::DataType::Text => {
                            FieldType::TextList
                        }
                        _ => FieldType::Json,
                    },
                    values: (!field.values.is_empty() && field.values.iter().all(Json::is_string))
                        .then(|| {
                            field
                                .values
                                .iter()
                                .filter_map(Json::as_str)
                                .map(str::to_owned)
                                .collect()
                        }),
                })
        })
        .collect()
}

fn fields() -> &'static [PayloadField] {
    static FIELDS: OnceLock<Vec<PayloadField>> = OnceLock::new();
    FIELDS.get_or_init(|| {
        payload_fields(&QueryCatalog::new(
            crate::event_types::declarations().iter().cloned(),
        ))
    })
}

fn definitions(ctx: &Context) -> Vec<PayloadField> {
    match &ctx.catalog {
        Some(catalog) => payload_fields(catalog),
        None => fields().to_vec(),
    }
}

/// A field name offered by every query surface, with its schema type.
#[derive(Clone, Debug, serde::Serialize, PartialEq, Eq)]
pub struct Completion {
    pub name: String,
    pub field_type: String,
}

pub fn complete(text: &str) -> Vec<Completion> {
    let first = text.split_whitespace().next().unwrap_or("");
    let alias = Alias::from_name(first);
    let table = Table::from_name(first).unwrap_or(Table::Events);
    let mut kinds: Option<BTreeSet<String>> =
        alias.map(|a| a.kinds.iter().map(|s| (*s).into()).collect());
    let ctx = Context::utc(jiff::Timestamp::UNIX_EPOCH);
    let shared_catalog = super::syntax::catalog(&ctx);
    let offered = query_language::complete::complete(text, text.len(), &shared_catalog);
    let field_names: BTreeSet<String> = offered
        .iter()
        .filter(|c| c.kind == query_language::complete::CompletionKind::Field)
        .map(|c| c.value.clone())
        .collect();
    // Complete one field-exists atom to inspect the already parsed kind filter.
    // The shared completer owns the unfinished-token replacement span.
    if let Some(candidate) = offered
        .iter()
        .find(|c| c.kind == query_language::complete::CompletionKind::Field)
    {
        let probe = format!(
            "{}{} exists{}",
            &text[..candidate.replace.start],
            candidate.value,
            &text[candidate.replace.end..]
        );
        if let Ok(parsed) = super::syntax::parse(&probe, &ctx) {
            fn mentioned(c: &query_language::ast::Condition, out: &mut BTreeSet<String>) {
                use query_language::ast::{ConditionKind as C, Operand as O, Value as V};
                match &c.kind {
                    C::And { conditions } | C::Or { conditions } => {
                        for c in conditions {
                            mentioned(c, out);
                        }
                    }
                    C::Compare {
                        operand: O::Field { name },
                        op: Op::Eq,
                        value,
                    }
                    | C::Match { field: name, value }
                        if name == "event_type" =>
                    {
                        fn add(v: &V, out: &mut BTreeSet<String>) {
                            match v {
                                V::Set { values } => {
                                    for v in values {
                                        add(v, out);
                                    }
                                }
                                V::Scalar { text, quoted } => {
                                    let v = if !quoted && text.contains('*') {
                                        Value::Glob(text.clone())
                                    } else {
                                        Value::Text(text.clone())
                                    };
                                    if let Some(selected) = selected_value(&v, None) {
                                        out.extend(selected);
                                    }
                                }
                                _ => {}
                            }
                        }
                        add(value, out);
                    }
                    _ => {}
                }
            }
            let mut selected = BTreeSet::new();
            if let query_language::ast::Source::Unit {
                condition: Some(c), ..
            } = parsed.source
            {
                mentioned(&c, &mut selected);
            }
            if !selected.is_empty() {
                kinds = Some(kinds.map_or(selected.clone(), |alias| {
                    alias.intersection(&selected).cloned().collect()
                }));
            }
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
            .filter(|f| kinds.as_ref().is_none_or(|ks| ks.contains(&f.event_type)))
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
        .filter(|(name, _)| field_names.contains(name))
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

fn selected_value(value: &Value, ctx: Option<&Context>) -> Option<BTreeSet<String>> {
    match value {
        Value::Text(s) => Some(BTreeSet::from([s.clone()])),
        Value::Glob(pattern) => Some(
            match ctx.and_then(|ctx| ctx.catalog.as_ref()) {
                Some(catalog) => catalog
                    .declarations
                    .iter()
                    .map(|(kind, _)| kind.name.clone())
                    .collect::<BTreeSet<_>>(),
                None => crate::event_types::ALL
                    .iter()
                    .map(|kind| kind.event_type.to_owned())
                    .collect(),
            }
            .into_iter()
            .filter(|name| glob_matches(pattern, name))
            .collect(),
        ),
        Value::List(values) => {
            let mut out = BTreeSet::new();
            for value in values {
                out.extend(selected_value(value, ctx)?);
            }
            Some(out)
        }
        _ => None,
    }
}

fn selected(e: &Expr, ctx: &Context) -> Option<BTreeSet<String>> {
    match e {
        Expr::Compare {
            lhs: Operand::Field(FieldRef::Field(f)),
            op: Op::Eq,
            rhs,
        } if f.name == "event_type" => selected_value(rhs, Some(ctx)),
        Expr::And(a, b) => match (selected(a, ctx), selected(b, ctx)) {
            (Some(a), Some(b)) => Some(a.intersection(&b).cloned().collect()),
            (a, b) => a.or(b),
        },
        Expr::Or(a, b) => Some(
            selected(a, ctx)?
                .union(&selected(b, ctx)?)
                .cloned()
                .collect(),
        ),
        _ => None,
    }
}

fn field_type(
    path: &[String],
    kinds: &Option<BTreeSet<String>>,
    ctx: &Context,
) -> Result<FieldType, QueryError> {
    if ctx.catalog.as_ref().is_some_and(|catalog| {
        catalog.kinds.iter().any(|kind| {
            kind.fields.is_none()
                && kinds
                    .as_ref()
                    .is_none_or(|names| names.contains(&kind.kind.name))
        })
    }) {
        return Ok(FieldType::Json);
    }
    let definitions = definitions(ctx);
    let mut types = definitions
        .iter()
        .filter(|f| f.path == path && kinds.as_ref().is_none_or(|ks| ks.contains(&f.event_type)))
        .map(|f| f.ty);
    let Some(mut ty) = types.next() else {
        if ctx.catalog.as_ref().is_some_and(|c| c.schemaless) {
            return Ok(FieldType::Json);
        }
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
            if ctx.catalog.is_some() {
                return Ok(FieldType::Json);
            }
            return Err(QueryError::new(format!(
                "payload.{} has different types; select an event_type",
                path.join(".")
            )));
        }
    }
    Ok(ty)
}

fn field(
    f: &mut FieldRef,
    kinds: &Option<BTreeSet<String>>,
    ctx: &Context,
) -> Result<(), QueryError> {
    if let FieldRef::Payload(path) | FieldRef::TypedPayload { path, .. } = f {
        *f = FieldRef::TypedPayload {
            ty: field_type(path, kinds, ctx)?,
            path: path.clone(),
        };
    }
    Ok(())
}
fn operand(
    o: &mut Operand,
    kinds: &Option<BTreeSet<String>>,
    ctx: &Context,
) -> Result<(), QueryError> {
    match o {
        Operand::Field(f) => field(f, kinds, ctx),
        Operand::TimeFn(_, f) => {
            field(f, kinds, ctx)?;
            if let FieldRef::TypedPayload { ty, .. } = f
                && *ty == FieldType::Json
            {
                *ty = FieldType::Time;
            }
            if let FieldRef::TypedPayload { ty, .. } = f
                && *ty != FieldType::Time
                && !(ctx.catalog.is_some() && *ty == FieldType::Json)
            {
                return Err(QueryError::new("time functions require a time field"));
            }
            Ok(())
        }
    }
}
fn validate_value(v: &mut Value, ty: FieldType, ctx: &Context) -> Result<(), QueryError> {
    if matches!(v, Value::Unknown) {
        return Ok(());
    }
    if ty == FieldType::Json {
        return Ok(());
    }
    if let Value::List(values) = v {
        for value in values {
            validate_value(value, ty, ctx)?;
        }
        return Ok(());
    }
    if ty == FieldType::Text && matches!(v, Value::Integer(_) | Value::Number(_) | Value::Bool(_)) {
        *v = Value::Text(match &*v {
            Value::Integer(v) => v.to_string(),
            Value::Number(v) => v.to_string(),
            Value::Bool(v) => v.to_string(),
            _ => unreachable!(),
        });
    }
    if ty == FieldType::Time
        && let Value::Text(text) = v
    {
        *v = Value::Time(
            time::instant(ctx, text)
                .ok_or_else(|| QueryError::new("a payload time requires an absolute timestamp"))?,
        );
    }
    if ty == FieldType::Json {
        return Ok(());
    }
    let valid = matches!(
        (ty, &*v),
        (FieldType::Integer, Value::Integer(_))
            | (FieldType::Number, Value::Integer(_) | Value::Number(_))
            | (
                FieldType::Text | FieldType::TextList,
                Value::Text(_) | Value::Glob(_) | Value::PlaceholderText(_)
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
    let narrowed = selected(e, ctx).map(|selected| {
        kinds.as_ref().map_or(selected.clone(), |ks| {
            ks.intersection(&selected).cloned().collect()
        })
    });
    let kinds = if narrowed.is_some() { &narrowed } else { kinds };
    match e {
        Expr::Truth { operand: field, .. } => operand(field, kinds, ctx)?,
        Expr::And(a, b) | Expr::Or(a, b) => {
            expr(a, kinds, ctx)?;
            expr(b, kinds, ctx)?;
        }
        Expr::Not(e) => expr(e, kinds, ctx)?,
        Expr::Compare { lhs, op, rhs } => {
            operand(lhs, kinds, ctx)?;
            if let Operand::Field(FieldRef::TypedPayload { ty, .. }) = lhs {
                if matches!(op, Op::Contains | Op::Prefix)
                    && !matches!(ty, FieldType::Text | FieldType::TextList)
                    && !(ctx.catalog.is_some() && *ty == FieldType::Json)
                {
                    return Err(QueryError::new("~ and ^ require a text payload field"));
                }
                if op.ordering()
                    && matches!(ty, FieldType::Bool | FieldType::Json | FieldType::TextList)
                    && !(ctx.catalog.is_some() && *ty == FieldType::Json)
                {
                    return Err(QueryError::new("this payload field cannot be ordered"));
                }
                validate_value(rhs, *ty, ctx)?;
                if let Operand::Field(FieldRef::TypedPayload { path, .. }) = lhs {
                    let available = definitions(ctx);
                    let definitions: Vec<_> = available
                        .iter()
                        .filter(|f| {
                            f.path == *path
                                && kinds.as_ref().is_none_or(|ks| ks.contains(&f.event_type))
                        })
                        .collect();
                    if !definitions.is_empty() && definitions.iter().all(|f| f.values.is_some()) {
                        let values: BTreeSet<&str> = definitions
                            .iter()
                            .flat_map(|f| {
                                f.values.as_ref().into_iter().flatten().map(String::as_str)
                            })
                            .collect();
                        let check = |v: &Value| match v {
                            Value::PlaceholderText(_) | Value::Unknown => true,
                            Value::Text(s) => values.contains(s.as_str()),
                            _ => false,
                        };
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
            operand(lhs, kinds, ctx)?;
            if let Operand::Field(FieldRef::TypedPayload { ty, .. }) = lhs
                && *ty == FieldType::Json
            {
                *ty = FieldType::Time;
            }
            if let Operand::Field(FieldRef::TypedPayload { ty, .. }) = lhs
                && *ty != FieldType::Time
                && !(ctx.catalog.is_some() && *ty == FieldType::Json)
            {
                return Err(QueryError::new("in requires a time field"));
            }
        }
        Expr::Exists(o) => operand(o, kinds, ctx)?,
        Expr::TimeDiff { a, b, .. } => {
            field(a, kinds, ctx)?;
            field(b, kinds, ctx)?;
            for field in [&mut *a, &mut *b] {
                if let FieldRef::TypedPayload { ty, .. } = field
                    && *ty == FieldType::Json
                {
                    *ty = FieldType::Time;
                }
            }
            if [a, b].iter().any(|field| matches!(field, FieldRef::TypedPayload { ty, .. } if *ty != FieldType::Time)) {
                return Err(QueryError::new("time differences require time fields"));
            }
        }
        Expr::Words(_) | Expr::Overlaps { .. } | Expr::Similar { .. } => {}
    }
    Ok(())
}

fn select_parameter(expression: &mut Expr, name: &str) -> bool {
    fn select_value(input: &mut Value, name: &str) -> bool {
        match input {
            Value::PlaceholderText(_) => {
                *input = Value::Text(name.to_owned());
                true
            }
            Value::List(values) => {
                let mut changed = false;
                for item in values {
                    changed |= select_value(item, name);
                }
                changed
            }
            _ => false,
        }
    }
    match expression {
        Expr::And(a, b) | Expr::Or(a, b) => select_parameter(a, name) | select_parameter(b, name),
        Expr::Not(e) => select_parameter(e, name),
        Expr::Compare {
            lhs: Operand::Field(FieldRef::Field(f)),
            rhs,
            ..
        } if f.name == "event_type" => select_value(rhs, name),
        _ => false,
    }
}

fn select_query_parameter(query: &mut Query, name: &str) -> bool {
    let mut changed = false;
    if let Some(filter) = &mut query.filter {
        changed |= select_parameter(filter, name);
    }
    if let Source::RecallLast {
        filter: Some(filter),
    } = &mut query.source
    {
        changed |= select_parameter(filter, name);
    }
    for stage in &mut query.stages {
        if let Stage::Where(filter) = stage {
            changed |= select_parameter(filter, name);
        }
    }
    changed
}

pub(super) fn check(q: &mut Query, ctx: &Context) -> Result<(), QueryError> {
    // A template's event-type argument is checked when an invocation supplies it.
    // Its definition must be valid for at least one declared event type.
    if ctx.catalog.is_none() && select_query_parameter(&mut q.clone(), "") {
        let mut first_error = None;
        for kind in crate::event_types::ALL {
            let mut trial = q.clone();
            select_query_parameter(&mut trial, kind.event_type);
            match check(&mut trial, ctx) {
                Ok(()) => {
                    *q = trial;
                    return Ok(());
                }
                Err(error) => {
                    first_error.get_or_insert(error);
                }
            }
        }
        return Err(first_error.unwrap_or_else(|| QueryError::new("no event type is declared")));
    }
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
        if let Some(selected) = selected(e, ctx) {
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
                    field(f, &kinds, ctx)?;
                }
            }
            Stage::Sort(keys) => {
                for k in keys {
                    if let SortBy::Operand(o) = &mut k.key {
                        operand(o, &kinds, ctx)?;
                    }
                }
            }
            Stage::Group(g) => {
                for k in &mut g.keys {
                    operand(k, &kinds, ctx)?;
                }
                for a in &mut g.aggregates {
                    if let Some(f) = &mut a.field {
                        field(f, &kinds, ctx)?;
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
                                    && !(ctx.catalog.is_some() && ty == FieldType::Json)
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
