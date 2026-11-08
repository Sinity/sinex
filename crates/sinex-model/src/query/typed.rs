//! Payload fields come from the same Rust schemas that admission registers.
use super::*;
use query_language::scope::{FieldInfo, Origin, TypeRule};
use serde_json::Value as Json;
use std::collections::{BTreeMap, BTreeSet};
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
    // Complete one known-value atom to inspect the already parsed kind filter.
    // The shared completer owns the unfinished-token replacement span.
    if let Some(candidate) = offered
        .iter()
        .find(|c| c.kind == query_language::complete::CompletionKind::Field)
    {
        let probe = format!(
            "{}{}:known{}",
            &text[..candidate.replace.start],
            candidate.value,
            &text[candidate.replace.end..]
        );
        if let Ok(parsed) = super::syntax::parse(&probe, &ctx) {
            fn mentioned(c: &query_language::ast::Condition, out: &mut BTreeSet<String>) {
                use query_language::ast::{ConditionKind as C, Expression as O, Value as V};
                match &c.kind {
                    C::And { conditions } | C::Or { conditions } => {
                        for c in conditions {
                            mentioned(c, out);
                        }
                    }
                    C::Compare {
                        left: O::Function { name, arguments },
                        op: Op::Eq,
                        right:
                            O::Value {
                                value: V::Scalar { text, .. },
                            },
                    } if name == "glob" && text == "true" => {
                        if let [
                            O::Field { name },
                            O::Value {
                                value: V::Scalar { text: pattern, .. },
                            },
                        ] = arguments.as_slice()
                        {
                            if name == "event_type" {
                                out.extend(pattern_kinds(pattern, None));
                            }
                        }
                    }
                    C::Compare {
                        left: O::Field { name },
                        op: Op::Eq,
                        right: O::Value { value },
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
                                V::Scalar { text, .. } => {
                                    let v = Value::Text(text.clone());
                                    if let Some(selected) = selected_value(&v) {
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

fn pattern_kinds(pattern: &str, ctx: Option<&Context>) -> BTreeSet<String> {
    let names = match ctx.and_then(|ctx| ctx.catalog.as_ref()) {
        Some(catalog) => catalog
            .declarations
            .iter()
            .map(|(kind, _)| kind.name.clone())
            .collect::<BTreeSet<_>>(),
        None => crate::event_types::ALL
            .iter()
            .map(|kind| kind.event_type.to_owned())
            .collect(),
    };
    names
        .into_iter()
        .filter(|name| query_language::glob::matches(name, pattern))
        .collect()
}

fn selected_value(value: &Value) -> Option<BTreeSet<String>> {
    match value {
        Value::Text(s) => Some(BTreeSet::from([s.clone()])),
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

fn selected(e: &Expr, ctx: &Context) -> Option<BTreeSet<String>> {
    match e {
        Expr::Compare {
            lhs: Operand::Glob { left, right },
            op: Op::Eq,
            rhs: Value::Bool(true),
        } => match (&**left, &**right) {
            (
                Operand::Field(FieldRef::Field(field)),
                Operand::Literal {
                    value: Value::Text(pattern),
                    ..
                },
            ) if field.name == "event_type" => Some(pattern_kinds(pattern, Some(ctx))),
            _ => None,
        },
        Expr::Compare {
            lhs: Operand::Field(FieldRef::Field(f)),
            op: Op::Eq,
            rhs,
        } if f.name == "event_type" => selected_value(rhs),
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

/// The first scope types the source filter; each following scope types one stage.
/// Later filters can narrow a row selection's output without changing its inputs.
fn relation_kinds(query: &Query, ctx: &Context) -> Vec<Option<BTreeSet<String>>> {
    fn narrow(kinds: &mut Option<BTreeSet<String>>, expression: &Expr, ctx: &Context) {
        if let Some(selected) = selected(expression, ctx) {
            *kinds = Some(kinds.as_ref().map_or(selected.clone(), |kinds| {
                kinds.intersection(&selected).cloned().collect()
            }));
        }
    }
    let mut kinds = query
        .source
        .alias()
        .map(|alias| alias.kinds.iter().map(|kind| (*kind).into()).collect());
    if let Some(filter) = &query.filter {
        narrow(&mut kinds, filter, ctx);
    }
    let mut scopes = Vec::with_capacity(query.stages.len() + 1);
    let mut grouped = false;
    for stages in query.stages.split_inclusive(|stage| {
        matches!(
            stage,
            Stage::Limit(_) | Stage::GroupFirst { .. } | Stage::Group(_)
        )
    }) {
        if !grouped {
            for stage in stages {
                if let Stage::Where(expression) = stage {
                    narrow(&mut kinds, expression, ctx);
                }
            }
        }
        if scopes.is_empty() {
            scopes.push(kinds.clone());
        }
        scopes.extend(std::iter::repeat_n(kinds.clone(), stages.len()));
        grouped |= matches!(stages.last(), Some(Stage::Group(_)));
    }
    if scopes.is_empty() {
        scopes.push(kinds);
    }
    scopes
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
    match f {
        FieldRef::Payload(path) | FieldRef::TypedPayload { path, .. } => {
            *f = FieldRef::TypedPayload {
                ty: field_type(path, kinds, ctx)?,
                path: path.clone(),
            };
        }
        FieldRef::TypedGroup(column) => {
            if column.field_type().is_none() {
                let ty = physical_type(column.info(), kinds, ctx)?;
                column.bind(ty);
            }
        }
        FieldRef::Group(_) => {
            return Err(QueryError::new(
                "output column has no authoritative bound scope",
            ));
        }
        _ => {}
    }
    Ok(())
}

fn physical_kind(kind: &query_language::catalog::FieldKind) -> FieldType {
    use query_language::catalog::FieldKind as K;
    match kind {
        K::Integer => FieldType::Integer,
        K::Number | K::Money | K::Duration => FieldType::Number,
        K::Bool => FieldType::Bool,
        K::Time => FieldType::Time,
        K::Reference => FieldType::Id,
        K::Json => FieldType::Json,
        K::List(item) => match &**item {
            K::Reference => FieldType::IdList,
            K::Text | K::Exact | K::Substring | K::Words | K::Subtree => FieldType::TextList,
            _ => FieldType::Json,
        },
        _ => FieldType::Text,
    }
}

fn physical_type(
    info: &FieldInfo,
    kinds: &Option<BTreeSet<String>>,
    ctx: &Context,
) -> Result<FieldType, QueryError> {
    match &info.origin {
        Origin::Catalog { unit, field } => {
            let alias = Alias::from_name(unit);
            let table = Table::from_name(unit)
                .or(alias.map(|_| Table::Events))
                .ok_or_else(|| QueryError::new(format!("unsupported bound row domain {unit}")))?;
            if let Some(path) = field.strip_prefix("payload.") {
                if !table.is_events() {
                    return Err(QueryError::new("bound relation has no payload"));
                }
                return field_type(
                    &path.split('.').map(str::to_owned).collect::<Vec<_>>(),
                    kinds,
                    ctx,
                );
            }
            alias
                .and_then(|alias| alias.field(field))
                .or_else(|| table.field(field))
                .map(|field| field.ty)
                .or_else(|| NativeField::from_name(field).map(NativeField::field_type))
                .ok_or_else(|| {
                    QueryError::new(format!(
                        "bound field {unit}.{field} has no physical adapter"
                    ))
                })
        }
        Origin::Group { input, .. } => physical_type(input, kinds, ctx),
        Origin::Aggregate {
            inputs,
            rule: TypeRule::Input(index),
            ..
        }
        | Origin::Function {
            arguments: inputs,
            rule: TypeRule::Input(index),
            ..
        } => physical_type(
            inputs
                .get(*index)
                .ok_or_else(|| QueryError::new("bound input lineage is missing"))?,
            kinds,
            ctx,
        ),
        Origin::Function {
            name,
            rule: TypeRule::Fixed,
            ..
        } => {
            // These are the existing SQL calendar columns, not another scope rule.
            match TimeFn::from_name(name) {
                Some(TimeFn::Hour | TimeFn::Year) => Ok(FieldType::Integer),
                Some(_) => Ok(FieldType::Text),
                None => Ok(physical_kind(&info.field.kind)),
            }
        }
        Origin::Matched { input } => physical_type(input, &None, ctx),
        _ => Ok(physical_kind(&info.field.kind)),
    }
}

fn output_type(field: &FieldRef) -> Result<FieldType, QueryError> {
    match field {
        FieldRef::Native(field) => Ok(field.field_type()),
        FieldRef::Field(field) => Ok(field.ty),
        FieldRef::TypedPayload { ty, .. } => Ok(*ty),
        FieldRef::TypedGroup(column) => column
            .field_type()
            .ok_or_else(|| QueryError::new("bound column has no physical type")),
        _ => Err(QueryError::new(
            "field needs its authoritative type binding",
        )),
    }
}

pub(super) fn operand_type(value: &Operand) -> Result<FieldType, QueryError> {
    Ok(match value {
        Operand::Field(field) => output_type(field)?,
        Operand::TimeFn(TimeFn::Hour | TimeFn::Year, _) | Operand::Length(_) => FieldType::Integer,
        Operand::TimeFn(_, _) => FieldType::Text,
        Operand::Literal { kind, .. } | Operand::Arithmetic { kind, .. } => physical_kind(kind),
        Operand::Absolute(value)
            if matches!(
                &**value,
                Operand::Literal {
                    value: Value::Unknown,
                    ..
                }
            ) =>
        {
            FieldType::Number
        }
        Operand::CaseEqual { value, .. } | Operand::Absolute(value) => operand_type(value)?,
        Operand::Glob { .. } => FieldType::Bool,
        Operand::Text(_) => FieldType::Text,
        Operand::Named { operand, .. } => operand_type(operand)?,
    })
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
            let ty = output_type(f)?;
            let bucket = matches!(f, FieldRef::TypedGroup(column) if matches!(column.time_function(), Some(TimeFn::Day | TimeFn::Week | TimeFn::Month)));
            if ty != FieldType::Time && !bucket && !(ctx.catalog.is_some() && ty == FieldType::Json)
            {
                return Err(QueryError::new("time functions require a time field"));
            }
            Ok(())
        }
        Operand::Literal { .. } => Ok(()),
        Operand::Named { operand: inner, .. } => operand(inner, kinds, ctx),
        Operand::Text(inner) => {
            operand(inner, kinds, ctx)?;
            if !matches!(
                operand_type(inner)?,
                FieldType::Text
                    | FieldType::Path
                    | FieldType::Id
                    | FieldType::Enum(_)
                    | FieldType::Json
            ) {
                return Err(QueryError::new("text requires text or an identifier"));
            }
            Ok(())
        }
        Operand::Absolute(inner) => {
            operand(inner, kinds, ctx)?;
            if !matches!(
                &**inner,
                Operand::Literal {
                    value: Value::Unknown,
                    ..
                }
            ) && !matches!(operand_type(inner)?, FieldType::Integer | FieldType::Number)
            {
                return Err(QueryError::new("abs requires a number, money or duration"));
            }
            Ok(())
        }
        Operand::Length(inner) => {
            operand(inner, kinds, ctx)?;
            if !matches!(
                operand_type(inner)?,
                FieldType::Text
                    | FieldType::TextList
                    | FieldType::Path
                    | FieldType::PathList
                    | FieldType::IdList
                    | FieldType::Enum(_)
                    | FieldType::Json
            ) {
                return Err(QueryError::new("len requires text or a list"));
            }
            Ok(())
        }
        Operand::Arithmetic {
            operator,
            left,
            right,
            kind,
        } => {
            operand(left, kinds, ctx)?;
            operand(right, kinds, ctx)?;
            *kind = query_language::scope::arithmetic_kind(*operator, &left.kind(), &right.kind())
                .ok_or_else(|| {
                    QueryError::new("arithmetic operands have incompatible physical dimensions")
                })?;
            Ok(())
        }
        Operand::Glob { left, right } => {
            for value in [left, right] {
                operand(value, kinds, ctx)?;
                if !matches!(
                    operand_type(value)?,
                    FieldType::Text
                        | FieldType::Path
                        | FieldType::Id
                        | FieldType::Enum(_)
                        | FieldType::Json
                ) {
                    return Err(QueryError::new("glob requires text values"));
                }
            }
            Ok(())
        }
        Operand::CaseEqual { left, right, value } => {
            operand(left, kinds, ctx)?;
            operand(right, kinds, ctx)?;
            operand(value, kinds, ctx)?;
            let expected = operand_type(left)?;
            if let Operand::Literal { value, kind } = &mut **right {
                validate_value(value, expected, ctx)?;
                *kind = super::syntax::field_kind(expected);
            }
            Ok(())
        }
    }
}
fn validate_value(v: &mut Value, ty: FieldType, ctx: &Context) -> Result<(), QueryError> {
    if let Value::Operand(value) = v {
        let actual = operand_type(value)?;
        if actual == ty
            || matches!(
                (actual, ty),
                (
                    FieldType::Integer | FieldType::Number,
                    FieldType::Integer | FieldType::Number
                )
            )
            || actual == FieldType::Json
            || ty == FieldType::Json
        {
            return Ok(());
        }
        return Err(QueryError::new(
            "comparison expressions have incompatible physical types",
        ));
    }
    if matches!(v, Value::Unknown) {
        return Ok(());
    }
    if matches!(v, Value::Number(number) if !number.is_finite()) {
        return Err(QueryError::new("a numeric value must be finite"));
    }
    if let Value::List(values) = v {
        for value in values {
            validate_value(value, ty, ctx)?;
        }
        return Ok(());
    }
    if ty == FieldType::Json {
        // Only the projected row can determine a dynamic JSON scalar's type.
        return Ok(());
    }
    if let Value::Text(text) = v {
        let parsed = match ty {
            FieldType::Integer => {
                if query_language::number::is_integer(text) {
                    return Ok(());
                }
                None
            }
            FieldType::Number => {
                if query_language::number::is_decimal(text) {
                    return Ok(());
                }
                None
            }
            FieldType::Bool => text.parse().ok().map(Value::Bool),
            FieldType::Time => time::instant(ctx, text).map(Value::Time),
            FieldType::Id | FieldType::IdList => text
                .strip_prefix("sinex:ev/")
                .unwrap_or(text)
                .parse()
                .ok()
                .map(Value::Id),
            FieldType::Enum(values) => values
                .contains(&text.as_str())
                .then(|| Value::Text(text.clone())),
            FieldType::Text | FieldType::TextList | FieldType::Path | FieldType::PathList => {
                return Ok(());
            }
            FieldType::Json => return Ok(()),
        };
        *v = parsed.ok_or_else(|| {
            QueryError::new(format!(
                "value must be {}",
                format!("{ty:?}").to_lowercase()
            ))
        })?;
    }
    let valid = matches!(
        (ty, &*v),
        (FieldType::Integer, Value::Integer(_))
            | (FieldType::Number, Value::Integer(_) | Value::Number(_))
            | (
                FieldType::Text
                    | FieldType::TextList
                    | FieldType::Path
                    | FieldType::PathList
                    | FieldType::Enum(_),
                Value::Text(_) | Value::PlaceholderText(_)
            )
            | (FieldType::Time, Value::Time(_))
            | (FieldType::Bool, Value::Bool(_))
            | (FieldType::Id | FieldType::IdList, Value::Id(_))
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
fn normalize_output_literal(
    value: &mut Value,
    info: &FieldInfo,
    ctx: &Context,
) -> Result<(), QueryError> {
    if let Value::List(values) = value {
        for value in values {
            normalize_output_literal(value, info, ctx)?;
        }
        return Ok(());
    }
    match &info.origin {
        Origin::Group { input, .. } => return normalize_output_literal(value, input, ctx),
        Origin::Aggregate {
            inputs,
            rule: TypeRule::Input(index),
            ..
        }
        | Origin::Function {
            arguments: inputs,
            rule: TypeRule::Input(index),
            ..
        } => {
            if let Some(input) = inputs.get(*index) {
                return normalize_output_literal(value, input, ctx);
            }
        }
        Origin::Function { name, .. } => {
            if let (Some(function), Value::Text(text)) = (TimeFn::from_name(name), &*value) {
                *value =
                    super::parse::time_fn_value(ctx, function, text, 0).map_err(|mut error| {
                        error.at = None;
                        error
                    })?;
                return Ok(());
            }
        }
        _ => {}
    }
    if info.field.kind == query_language::catalog::FieldKind::Duration
        && let Value::Text(text) = value
    {
        if !query_language::number::is_decimal(text) {
            let (negative, magnitude) = text
                .strip_prefix('-')
                .map_or((false, text.as_str()), |text| (true, text));
            let duration = query_language::time::duration(magnitude)
                .ok_or_else(|| QueryError::new("expected a duration or seconds"))?;
            let nanos = duration.as_nanos();
            let sign = if negative && nanos != 0 { "-" } else { "" };
            let fraction = format!("{:09}", nanos % 1_000_000_000);
            let fraction = fraction.trim_end_matches('0');
            *text = if fraction.is_empty() {
                format!("{sign}{}", nanos / 1_000_000_000)
            } else {
                format!("{sign}{}.{fraction}", nanos / 1_000_000_000)
            };
        }
    }
    Ok(())
}

fn check_join(
    join: &mut Join,
    kinds: &Option<BTreeSet<String>>,
    ctx: &Context,
) -> Result<(), QueryError> {
    let inner_kinds = relation_kinds(&join.inner, ctx)
        .pop()
        .expect("the source has a kind scope");
    fn candidate_fields(value: &Operand, out: &mut BTreeSet<String>) {
        match value {
            Operand::Field(field) | Operand::TimeFn(_, field) => {
                let name = field.name();
                if !matches!(
                    name.as_str(),
                    "match.id" | "match.coarse" | "match.distance"
                ) {
                    out.insert(name);
                }
            }
            Operand::Arithmetic { left, right, .. } | Operand::Glob { left, right } => {
                candidate_fields(left, out);
                candidate_fields(right, out);
            }
            Operand::CaseEqual { left, right, value } => {
                candidate_fields(left, out);
                candidate_fields(right, out);
                candidate_fields(value, out);
            }
            Operand::Text(value)
            | Operand::Absolute(value)
            | Operand::Length(value)
            | Operand::Named { operand: value, .. } => candidate_fields(value, out),
            Operand::Literal {
                value: Value::Operand(value),
                ..
            } => candidate_fields(value, out),
            _ => {}
        }
    }
    let mut pick_fields = BTreeSet::new();
    for key in &join.pick {
        if let SortBy::Operand(value) = &key.key {
            candidate_fields(value, &mut pick_fields);
        }
    }
    for name in pick_fields {
        if !join.required.contains(&name) {
            join.required.push(name);
        }
    }
    let inner_schema = check_schema(&mut join.inner, ctx, &join.required)?;
    fn implicit_column(
        column: &mut Operand,
        schema: &PhysicalSchema,
        kinds: &Option<BTreeSet<String>>,
        ctx: &Context,
        required: bool,
    ) -> Result<(), QueryError> {
        match column {
            Operand::Named {
                operand: column, ..
            } => implicit_column(column, schema, kinds, ctx, required),
            Operand::Field(field @ (FieldRef::Payload(_) | FieldRef::TypedPayload { .. })) => {
                let path = match field {
                    FieldRef::Payload(path) | FieldRef::TypedPayload { path, .. } => path.clone(),
                    _ => unreachable!(),
                };
                // The shared catalog is the union of declared payload paths.
                // Implicit matched columns absent in this selected kind remain
                // dynamic JSON; explicit inner projections were checked above.
                let ty = if required {
                    field_type(&path, kinds, ctx)?
                } else {
                    field_type(&path, kinds, ctx).unwrap_or(FieldType::Json)
                };
                *field = FieldRef::TypedPayload { path, ty };
                Ok(())
            }
            _ => {
                seed_operand(column, schema);
                operand(column, kinds, ctx)
            }
        }
    }
    for column in &mut join.columns {
        let name = column.column();
        if !join.required.contains(&name) && !inner_schema.contains_key(&name) {
            if let Operand::Named { operand, .. } = column {
                if let Operand::Field(
                    value @ (FieldRef::Payload(_) | FieldRef::TypedPayload { .. }),
                ) = &mut **operand
                {
                    let path = match value {
                        FieldRef::Payload(path) | FieldRef::TypedPayload { path, .. } => {
                            path.clone()
                        }
                        _ => unreachable!(),
                    };
                    *value = FieldRef::TypedPayload {
                        path,
                        ty: FieldType::Json,
                    };
                    continue;
                }
            }
        }
        implicit_column(
            column,
            &inner_schema,
            &inner_kinds,
            ctx,
            join.required.contains(&name),
        )?;
    }
    let mut candidates = inner_schema.clone();
    for column in &join.columns {
        candidates
            .entry(column.column())
            .or_insert(operand_type(column)?);
    }
    candidates.insert("match.id".into(), FieldType::Id);
    candidates.insert("match.distance".into(), FieldType::Number);
    candidates.insert("match.coarse".into(), FieldType::Bool);
    for key in &mut join.pick {
        if let SortBy::Operand(value) = &mut key.key {
            seed_operand(value, &candidates);
            operand(value, &inner_kinds, ctx)?;
        }
    }
    if let JoinOn::Key { left, right } = &mut join.on {
        field(left, kinds, ctx)?;
        if let FieldRef::TypedGroup(column) = right {
            if let Some(ty) = inner_schema.get(column.name()) {
                column.bind(*ty);
            }
        }
        field(right, &inner_kinds, ctx)?;
    }
    Ok(())
}

fn expr(e: &mut Expr, kinds: &Option<BTreeSet<String>>, ctx: &Context) -> Result<(), QueryError> {
    let narrowed = selected(e, ctx).map(|selected| {
        kinds.as_ref().map_or(selected.clone(), |ks| {
            ks.intersection(&selected).cloned().collect()
        })
    });
    let kinds = if narrowed.is_some() { &narrowed } else { kinds };
    match e {
        Expr::Order(join) => check_join(join, kinds, ctx)?,
        Expr::Truth { operand: field, .. } => operand(field, kinds, ctx)?,
        Expr::And(a, b) | Expr::Or(a, b) => {
            expr(a, kinds, ctx)?;
            expr(b, kinds, ctx)?;
        }
        Expr::Not(e) => expr(e, kinds, ctx)?,
        Expr::Compare { lhs, op, rhs } => {
            operand(lhs, kinds, ctx)?;
            if let Value::Operand(value) = rhs {
                operand(value, kinds, ctx)?;
            }
            if let Operand::Field(
                FieldRef::Native(_)
                | FieldRef::TypedPayload { .. }
                | FieldRef::TypedGroup(_)
                | FieldRef::Field(_),
            ) = lhs
            {
                let ty = output_type(match lhs {
                    Operand::Field(field) => field,
                    _ => unreachable!(),
                })?;
                if matches!(op, Op::Contains | Op::Prefix)
                    && !matches!(
                        ty,
                        FieldType::Text
                            | FieldType::TextList
                            | FieldType::Path
                            | FieldType::PathList
                            | FieldType::Enum(_)
                    )
                    && !(ctx.catalog.is_some() && ty == FieldType::Json)
                {
                    return Err(QueryError::new("~ and ^ require a text payload field"));
                }
                if op.ordering()
                    && matches!(ty, FieldType::Bool | FieldType::Json | FieldType::TextList)
                    && !(ctx.catalog.is_some() && ty == FieldType::Json)
                {
                    return Err(QueryError::new("this payload field cannot be ordered"));
                }
                if let Operand::Field(FieldRef::TypedGroup(column)) = lhs {
                    normalize_output_literal(rhs, column.info(), ctx)?;
                }
                validate_value(rhs, ty, ctx)?;
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
            if let Operand::Field(field) = lhs {
                let ty = output_type(field)?;
                if ty != FieldType::Time && !(ctx.catalog.is_some() && ty == FieldType::Json) {
                    return Err(QueryError::new("in requires a time field"));
                }
            }
        }
        Expr::Exists(o) | Expr::FieldWords { operand: o, .. } => operand(o, kinds, ctx)?,
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

fn mark_join_requirements(query: &mut Query, extra: &[String]) {
    fn operand(value: &Operand, out: &mut BTreeSet<String>) {
        match value {
            Operand::Field(field) | Operand::TimeFn(_, field) => {
                if let Some(name) = field.name().strip_prefix("match.") {
                    out.insert(name.to_owned());
                }
            }
            Operand::Arithmetic { left, right, .. } | Operand::Glob { left, right } => {
                operand(left, out);
                operand(right, out);
            }
            Operand::CaseEqual { left, right, value } => {
                operand(left, out);
                operand(right, out);
                operand(value, out);
            }
            Operand::Text(value)
            | Operand::Absolute(value)
            | Operand::Length(value)
            | Operand::Named { operand: value, .. } => operand(value, out),
            Operand::Literal {
                value: Value::Operand(value),
                ..
            } => operand(value, out),
            _ => {}
        }
    }
    fn expression(value: &Expr, out: &mut BTreeSet<String>) {
        match value {
            Expr::And(left, right) | Expr::Or(left, right) => {
                expression(left, out);
                expression(right, out);
            }
            Expr::Not(value) => expression(value, out),
            Expr::Compare { lhs, rhs, .. } => {
                operand(lhs, out);
                if let Value::Operand(rhs) = rhs {
                    operand(rhs, out);
                }
            }
            Expr::Exists(value)
            | Expr::Within { lhs: value, .. }
            | Expr::Truth { operand: value, .. }
            | Expr::FieldWords { operand: value, .. } => operand(value, out),
            _ => {}
        }
    }
    fn clear_orders(value: &mut Expr) {
        match value {
            Expr::Order(join) => join.required.clear(),
            Expr::And(left, right) | Expr::Or(left, right) => {
                clear_orders(left);
                clear_orders(right);
            }
            Expr::Not(value) => clear_orders(value),
            _ => {}
        }
    }
    fn assign(join: &mut Join, names: &mut BTreeSet<String>, pair: bool) {
        let mut produced = join
            .columns
            .iter()
            .map(Operand::column)
            .collect::<BTreeSet<_>>();
        if matches!(
            join.on,
            JoinOn::Order {
                within_secs: Some(_),
                ..
            }
        ) || (pair && matches!(join.on, JoinOn::Order { .. }))
        {
            produced.insert("distance".into());
        }
        join.required = names.intersection(&produced).cloned().collect();
        names.retain(|name| !produced.contains(name));
        if let JoinOn::Key { right, .. } = &join.on {
            join.required.push(right.name());
        }
    }
    if let Some(value) = &mut query.filter {
        clear_orders(value);
    }
    for stage in &mut query.stages {
        if let Stage::Where(value) = stage {
            clear_orders(value);
        }
    }
    let mut names = extra
        .iter()
        .filter_map(|name| name.strip_prefix("match.").map(str::to_owned))
        .collect::<BTreeSet<_>>();
    // Work back to the producer of each matched column. A later join
    // overwrites the names it exposes, while other input fields survive.
    for stage in query.stages.iter_mut().rev() {
        match stage {
            Stage::Where(value) => {
                if let Some(join) = value.exposed_order_mut() {
                    assign(join, &mut names, false);
                }
                // Conditions read the input's matched row, before this filter exposes
                // a replacement. Their requirements belong to earlier producers.
                expression(value, &mut names);
            }
            Stage::Join(join) => {
                assign(join, &mut names, true);
                if let JoinOn::Key { left, .. } = &join.on {
                    if let Some(name) = left.name().strip_prefix("match.") {
                        names.insert(name.to_owned());
                    }
                }
            }
            Stage::GroupFirst { keys, .. } | Stage::Fields(keys) => {
                for key in keys {
                    operand(key, &mut names);
                }
            }
            Stage::Sort(values) => {
                for value in values {
                    if let SortBy::Operand(value) = &value.key {
                        operand(value, &mut names);
                    }
                }
            }
            Stage::Group(value) => {
                for value in &value.keys {
                    operand(value, &mut names);
                }
                for value in &value.aggregates {
                    if let Some(value) = &value.argument {
                        operand(value, &mut names);
                    }
                }
            }
            _ => {}
        }
    }
    if let Some(value) = &mut query.filter {
        if let Some(join) = value.exposed_order_mut() {
            assign(join, &mut names, false);
        }
    }
}

type PhysicalSchema = BTreeMap<String, FieldType>;

fn seed_operand(value: &mut Operand, schema: &PhysicalSchema) {
    match value {
        Operand::Field(FieldRef::TypedGroup(column)) => {
            if let Some(ty) = schema.get(column.name()) {
                column.bind(*ty);
            }
        }
        Operand::TimeFn(_, FieldRef::TypedGroup(column)) => {
            if let Some(ty) = schema.get(column.name()) {
                column.bind(*ty);
            }
        }
        Operand::Arithmetic { left, right, .. } | Operand::Glob { left, right } => {
            seed_operand(left, schema);
            seed_operand(right, schema);
        }
        Operand::CaseEqual { left, right, value } => {
            seed_operand(left, schema);
            seed_operand(right, schema);
            seed_operand(value, schema);
        }
        Operand::Text(value)
        | Operand::Absolute(value)
        | Operand::Length(value)
        | Operand::Named { operand: value, .. } => seed_operand(value, schema),
        Operand::Literal {
            value: Value::Operand(value),
            ..
        } => seed_operand(value, schema),
        _ => {}
    }
}
fn seed_expr(value: &mut Expr, schema: &PhysicalSchema) {
    match value {
        Expr::And(left, right) | Expr::Or(left, right) => {
            seed_expr(left, schema);
            seed_expr(right, schema);
        }
        Expr::Not(value) => seed_expr(value, schema),
        Expr::Compare { lhs, rhs, .. } => {
            seed_operand(lhs, schema);
            if let Value::Operand(rhs) = rhs {
                seed_operand(rhs, schema);
            }
        }
        Expr::Exists(value)
        | Expr::FieldWords { operand: value, .. }
        | Expr::Within { lhs: value, .. }
        | Expr::Truth { operand: value, .. } => seed_operand(value, schema),
        Expr::Order(join) => {
            if let JoinOn::Key {
                left: FieldRef::TypedGroup(column),
                ..
            } = &mut join.on
            {
                if let Some(ty) = schema.get(column.name()) {
                    column.bind(*ty);
                }
            }
        }
        _ => {}
    }
}
fn expose_join(schema: &mut PhysicalSchema, join: &Join) -> Result<(), QueryError> {
    for column in &join.columns {
        schema.insert(format!("match.{}", column.column()), operand_type(column)?);
    }
    if matches!(
        join.on,
        JoinOn::Order {
            within_secs: Some(_),
            ..
        }
    ) {
        schema.insert("match.distance".into(), FieldType::Number);
    }
    Ok(())
}
fn expose_order(schema: &mut PhysicalSchema, value: &Expr) -> Result<(), QueryError> {
    if let Some(join) = value.exposed_order() {
        expose_join(schema, join)?;
    }
    Ok(())
}

/// Erase only the derived physical cache. Every admission rechecks it against
/// the current declaration snapshot, including programmatically reused plans.
fn clear_query_bindings(query: &mut Query) {
    fn field(value: &mut FieldRef) {
        if let FieldRef::TypedGroup(column) = value {
            column.clear_binding();
        }
    }
    fn operand(value: &mut Operand) {
        match value {
            Operand::Field(value) | Operand::TimeFn(_, value) => field(value),
            Operand::Arithmetic { left, right, .. } | Operand::Glob { left, right } => {
                operand(left);
                operand(right);
            }
            Operand::CaseEqual { left, right, value } => {
                operand(left);
                operand(right);
                operand(value);
            }
            Operand::Text(value)
            | Operand::Absolute(value)
            | Operand::Length(value)
            | Operand::Named { operand: value, .. } => operand(value),
            Operand::Literal {
                value: Value::Operand(value),
                ..
            } => operand(value),
            _ => {}
        }
    }
    fn join(value: &mut Join) {
        clear_query_bindings(&mut value.inner);
        for key in &mut value.pick {
            if let SortBy::Operand(value) = &mut key.key {
                operand(value);
            }
        }
        for value in &mut value.columns {
            operand(value);
        }
        if let JoinOn::Key { left, right } = &mut value.on {
            field(left);
            field(right);
        }
    }
    fn expression(value: &mut Expr) {
        match value {
            Expr::And(left, right) | Expr::Or(left, right) => {
                expression(left);
                expression(right);
            }
            Expr::Not(value) => expression(value),
            Expr::Order(value) => join(value),
            Expr::Compare { lhs, rhs, .. } => {
                operand(lhs);
                if let Value::Operand(value) = rhs {
                    operand(value);
                }
            }
            Expr::Exists(value)
            | Expr::FieldWords { operand: value, .. }
            | Expr::Within { lhs: value, .. }
            | Expr::Truth { operand: value, .. } => operand(value),
            _ => {}
        }
    }
    if let Some(value) = &mut query.filter {
        expression(value);
    }
    if let Source::RecallLast {
        filter: Some(value),
    } = &mut query.source
    {
        expression(value);
    }
    for stage in &mut query.stages {
        match stage {
            Stage::Where(value) => expression(value),
            Stage::Join(value) => join(value),
            Stage::GroupFirst { keys, .. } => {
                for key in keys {
                    operand(key);
                }
            }
            Stage::Fields(values) => {
                for value in values {
                    operand(value);
                }
            }
            Stage::Sort(values) => {
                for value in values {
                    if let SortBy::Operand(value) = &mut value.key {
                        operand(value);
                    }
                }
            }
            Stage::Group(value) => {
                for value in &mut value.keys {
                    operand(value);
                }
                for value in &mut value.aggregates {
                    if let Some(value) = &mut value.argument {
                        operand(value);
                    }
                }
            }
            _ => {}
        }
    }
}

pub(super) fn check(q: &mut Query, ctx: &Context) -> Result<(), QueryError> {
    check_schema(q, ctx, &[]).map(|_| ())
}

fn check_schema(
    q: &mut Query,
    ctx: &Context,
    extra: &[String],
) -> Result<PhysicalSchema, QueryError> {
    mark_join_requirements(q, extra);
    clear_query_bindings(q);
    // A template's event-type argument is checked when an invocation supplies it.
    // Its definition must be valid for at least one declared event type.
    if ctx.catalog.is_none() && select_query_parameter(&mut q.clone(), "") {
        let mut first_error = None;
        for kind in crate::event_types::ALL {
            let mut trial = q.clone();
            select_query_parameter(&mut trial, kind.event_type);
            match check_schema(&mut trial, ctx, extra) {
                Ok(schema) => {
                    *q = trial;
                    return Ok(schema);
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
    let kind_scopes = relation_kinds(q, ctx);
    let mut schema = q
        .source
        .table()
        .fields()
        .iter()
        .map(|field| (field.name.to_owned(), field.ty))
        .collect::<PhysicalSchema>();
    if let Some(alias) = q.source.alias() {
        schema.extend(
            alias
                .fields
                .iter()
                .map(|field| (field.name.to_owned(), field.ty)),
        );
    }
    if let Some(e) = &mut q.filter {
        seed_expr(e, &schema);
        expr(e, &kind_scopes[0], ctx)?;
        expose_order(&mut schema, e)?;
    }
    for (stage, kinds) in q.stages.iter_mut().zip(&kind_scopes[1..]) {
        match stage {
            Stage::Where(e) => {
                seed_expr(e, &schema);
                expr(e, kinds, ctx)?;
                expose_order(&mut schema, e)?;
            }
            Stage::Join(join) => {
                if let JoinOn::Key {
                    left: FieldRef::TypedGroup(column),
                    ..
                } = &mut join.on
                {
                    if let Some(ty) = schema.get(column.name()) {
                        column.bind(*ty);
                    }
                }
                check_join(join, kinds, ctx)?;
                expose_join(&mut schema, join)?;
                if matches!(join.on, JoinOn::Order { .. }) {
                    schema.insert("match.distance".into(), FieldType::Number);
                }
            }
            Stage::GroupFirst { keys, .. } => {
                for key in keys {
                    seed_operand(key, &schema);
                    operand(key, kinds, ctx)?;
                }
            }
            Stage::Fields(fs) => {
                let mut projected = PhysicalSchema::new();
                for f in fs {
                    seed_operand(f, &schema);
                    operand(f, kinds, ctx)?;
                    projected.insert(f.column(), operand_type(f)?);
                }
                schema = projected;
            }
            Stage::Sort(keys) => {
                for k in keys {
                    if let SortBy::Operand(o) = &mut k.key {
                        seed_operand(o, &schema);
                        operand(o, kinds, ctx)?;
                    }
                }
            }
            Stage::Group(g) => {
                for k in &mut g.keys {
                    seed_operand(k, &schema);
                    operand(k, kinds, ctx)?;
                }
                for a in &mut g.aggregates {
                    if let Some(argument) = &mut a.argument {
                        seed_operand(argument, &schema);
                        operand(argument, kinds, ctx)?;
                        let ty = operand_type(argument)?;
                        if matches!(a.function, AggFn::Min | AggFn::Max)
                            && matches!(
                                ty,
                                FieldType::Bool
                                    | FieldType::Json
                                    | FieldType::TextList
                                    | FieldType::PathList
                                    | FieldType::IdList
                                    | FieldType::Id
                            )
                        {
                            return Err(QueryError::new(
                                "min and max require an ordered scalar field",
                            ));
                        }
                        if matches!(a.function, AggFn::Sum | AggFn::Avg)
                            && !matches!(ty, FieldType::Integer | FieldType::Number)
                            && !(ctx.catalog.is_some() && ty == FieldType::Json)
                        {
                            return Err(QueryError::new(format!(
                                "{} requires a numeric field declared by the event schema",
                                a.function.name()
                            )));
                        }
                    }
                }
                let mut grouped = PhysicalSchema::new();
                for key in &g.keys {
                    grouped.insert(key.column(), operand_type(key)?);
                }
                for aggregate in &g.aggregates {
                    let ty = match aggregate.function {
                        AggFn::Count | AggFn::CountDistinct => FieldType::Integer,
                        AggFn::Avg => FieldType::Number,
                        _ => aggregate
                            .argument
                            .as_ref()
                            .map(operand_type)
                            .transpose()?
                            .ok_or_else(|| QueryError::new("aggregate argument is missing"))?,
                    };
                    grouped.insert(aggregate.column(), ty);
                }
                if g.fill.is_some() {
                    grouped.insert("coverage".into(), FieldType::Text);
                }
                schema = grouped;
            }
            _ => {}
        }
    }
    Ok(schema)
}
