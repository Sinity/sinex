//! Explicit reader for immutable query-request format 1 and source-owned read projections.
//! Ordinary command admission never calls this decoder.
use super::{Context, QueryError, native};
use historical_query_format_v1::ast as old;
use query_language::ast;
use serde_json::{Value, json};
use std::collections::{BTreeMap, BTreeSet};

mod formats;

/// The exact historical admitted schema expected by this owner converter.
pub fn original_declaration(reference: &crate::KindRef) -> Option<crate::KindDeclaration> {
    formats::original(reference)
}

/// The original request material header owned by these historical query kinds.
pub struct MaterialFormat {
    pub parser: &'static str,
    pub version: i32,
    pub path: &'static str,
    pub media_type: &'static str,
}
pub fn original_material_format(kind: &crate::KindRef) -> Option<MaterialFormat> {
    match (kind.name.as_str(), kind.version) {
        ("query.saved", 2) => Some(MaterialFormat {
            parser: "query-request",
            version: 1,
            path: "query",
            media_type: "application/x-sinex-query-request",
        }),
        ("analysis.request", 2) => Some(MaterialFormat {
            parser: "analysis-request",
            version: 1,
            path: "analysis",
            media_type: "application/x-sinex-analysis-request",
        }),
        _ => None,
    }
}

pub fn validate_original_payload(kind: &crate::KindRef, payload: &Value) -> Result<(), QueryError> {
    formats::validate(kind, payload)
}

pub const GRAMMAR_REVISION: &str = "8c22e127bb1f5c10d8c1740de4c4f0bfe53ffdb7";

fn historical_catalog(ctx: &Context) -> historical_query_format_v1::catalog::Catalog {
    use super::{FieldType, QueryCatalog, Table};
    use historical_query_format_v1::catalog::{Catalog, Field, FieldKind, ReferenceSyntax, Unit};

    // Format 1's field vocabulary is part of its source format. Rebuild it
    // from the original physical fields, rather than serializing the evolving
    // current catalog (Exact, generated fields and functions did not exist).
    fn field_kind(ty: FieldType) -> FieldKind {
        match ty {
            FieldType::Integer => FieldKind::Integer,
            FieldType::Number => FieldKind::Number,
            FieldType::Bool => FieldKind::Bool,
            FieldType::Time => FieldKind::Time,
            FieldType::Id => FieldKind::Reference,
            FieldType::IdList => FieldKind::List(Box::new(FieldKind::Reference)),
            FieldType::Path => FieldKind::Subtree,
            FieldType::TextList => FieldKind::List(Box::new(FieldKind::Text)),
            FieldType::PathList => FieldKind::List(Box::new(FieldKind::Subtree)),
            FieldType::Json => FieldKind::Json,
            FieldType::Text | FieldType::Enum(_) => FieldKind::Text,
        }
    }
    let declarations = ctx
        .catalog
        .clone()
        .unwrap_or_else(|| QueryCatalog::new(crate::event_types::declarations().iter().cloned()));
    let fields = |table: Table, extra: &[super::Field]| {
        let mut fields = table
            .fields()
            .iter()
            .chain(extra)
            .map(|field| Field {
                name: field.name.into(),
                kind: field_kind(field.ty),
                values: match field.ty {
                    FieldType::Enum(values) => values.iter().map(|value| (*value).into()).collect(),
                    _ => vec![],
                },
                help: field.phrase.into(),
                ..Field::default()
            })
            .collect::<Vec<_>>();
        if table.is_events() {
            fields.extend(declarations.fields.values().map(|field| Field {
                name: format!("payload.{}", field.path.join(".")),
                kind: FieldKind::Json,
                ..Field::default()
            }));
        }
        fields
    };
    let mut units = Table::ALL
        .into_iter()
        .map(|table| Unit {
            name: table.name().into(),
            fields: fields(table, &[]),
            main_time: Some(table.main_time().into()),
            identity: vec![
                if table == Table::Sources {
                    "binding"
                } else {
                    "id"
                }
                .into(),
            ],
            default_order: vec![table.main_time().into()],
            help: table.phrase().into(),
            ..Unit::default()
        })
        .collect::<Vec<_>>();
    units.extend(super::ALIASES.iter().map(|alias| Unit {
        name: alias.name.into(),
        fields: fields(Table::Events, alias.fields),
        main_time: Some("when".into()),
        identity: vec!["id".into()],
        default_order: vec!["when".into()],
        help: alias.phrase.into(),
        ..Unit::default()
    }));
    Catalog {
        default_unit: "events".into(),
        recall_last_unit: Some("shell-sessions".into()),
        units,
        commands: [
            "recall",
            "panel",
            "compute",
            "brief",
            "context",
            "related",
            "follow",
            "trace",
            "descendants",
            "parents",
            "walk",
            "duplicates",
        ]
        .into_iter()
        .map(String::from)
        .collect(),
        references: vec![ReferenceSyntax {
            prefix: "sinex:".into(),
            marker: Some("/".into()),
        }],
        unpiped_stages: true,
        ..Catalog::default()
    }
}

pub fn parse_format_one(text: &str, ctx: &Context) -> Result<old::Query, QueryError> {
    historical_query_format_v1::parse::parse(text, &historical_catalog(ctx))
        .map_err(|e| QueryError::at(e.position, e.message))
}

pub fn definition_format_one(
    text: &str,
    ctx: &Context,
) -> Result<super::saved::Definition, QueryError> {
    Ok(super::saved::Definition::from_format_one(
        parse_format_one(text, ctx)?,
        text,
    ))
}

pub fn decode_format_one(text: &str, ctx: &Context) -> Result<native::Template, QueryError> {
    decode_with_identity(text, ctx).map(|(template, _)| template)
}

pub fn decode_with_identity(
    text: &str,
    ctx: &Context,
) -> Result<(native::Template, Value), QueryError> {
    let (parsed, identity) = decode_with_identity_for_source(text, ctx)?;
    Ok((
        super::saved::resolve_historical(parsed, ctx, &BTreeMap::new())?,
        identity,
    ))
}

fn decode_with_identity_for_source(
    text: &str,
    ctx: &Context,
) -> Result<(old::Query, Value), QueryError> {
    let parsed = parse_format_one(text, ctx)?;
    let identity =
        historical_query_format_v1::identity::normal_form(&parsed, &historical_catalog(ctx))
            .map_err(|error| QueryError::new(error.to_string()))?;
    Ok((parsed, identity))
}

pub(super) fn convert_expanded(
    mut query: old::Query,
    ctx: &Context,
    saved: &BTreeMap<String, super::saved::SavedQuery>,
    seen: &mut BTreeSet<String>,
    expanded: &mut bool,
    validating: bool,
) -> Result<native::Template, QueryError> {
    let operation = match &query.source {
        old::Source::Recall { time, window } => Some(native::Operation::Recall {
            subject: native::Subject::Time {
                value: time.clone().map(current).transpose()?,
                radius: window.clone().map(current).transpose()?,
            },
        }),
        old::Source::RecallLast { unit, condition } => {
            let selector = old::Query {
                source: old::Source::Unit {
                    unit: unit.clone(),
                    implicit: false,
                    condition: condition.clone(),
                },
                attach: vec![],
                stages: vec![],
            };
            Some(native::Operation::Recall {
                subject: native::Subject::Last {
                    selector: Box::new(simple_relation(selector)?),
                },
            })
        }
        old::Source::RecallMark { number, window } => Some(native::Operation::Recall {
            subject: native::Subject::Mark {
                number: ast::Value::scalar(number.to_string()),
                radius: window.clone().map(current).transpose()?,
            },
        }),
        old::Source::Walk {
            direction,
            reference,
        } => Some(native::Operation::Walk {
            direction: match direction {
                old::WalkDirection::Trace => native::Direction::Trace,
                old::WalkDirection::Parents => native::Direction::Parents,
                old::WalkDirection::Descendants => native::Direction::Descendants,
            },
            reference: reference.clone(),
        }),
        _ => None,
    };
    if operation.is_some() {
        query.source = old::Source::Unit {
            unit: "events".into(),
            implicit: false,
            condition: None,
        };
    }
    let relation = if matches!(operation, Some(native::Operation::Walk { .. })) {
        simple_relation(query)?
    } else {
        convert_relation(query, None, ctx, saved, seen, expanded, validating)?
    };
    Ok(native::Template {
        relation,
        operation,
    })
}

pub(super) fn convert_around_current(
    query: old::Query,
    base: native::Template,
    ctx: &Context,
    saved: &BTreeMap<String, super::saved::SavedQuery>,
    seen: &mut BTreeSet<String>,
    expanded: &mut bool,
    validating: bool,
) -> Result<native::Template, QueryError> {
    let relation = convert_relation(
        query,
        Some(base.relation),
        ctx,
        saved,
        seen,
        expanded,
        validating,
    )?;
    Ok(native::Template {
        relation,
        operation: base.operation,
    })
}

fn convert_relation(
    query: old::Query,
    base: Option<ast::Query>,
    ctx: &Context,
    saved: &BTreeMap<String, super::saved::SavedQuery>,
    seen: &mut BTreeSet<String>,
    expanded: &mut bool,
    validating: bool,
) -> Result<ast::Query, QueryError> {
    let source_unit = match &query.source {
        old::Source::Unit { unit, .. } => Some(unit.clone()),
        _ => None,
    };
    let inherited_snippet = base
        .as_ref()
        .map(|base| {
            available_fields(base, ctx).map(|fields| fields.iter().any(|field| field == "snippet"))
        })
        .transpose()?
        .unwrap_or(true);
    let inherited_output = base
        .as_ref()
        .map(|base| default_output(base, ctx))
        .transpose()?;
    let mut explicit = None;
    let mut generated = vec![];
    let grouped = query.stages.iter().any(|stage| {
        matches!(
            stage.kind,
            old::StageKind::Group { .. } | old::StageKind::Aggregate { .. }
        )
    });
    let payload_dependencies = referenced_payload_fields(&query)?;
    let source_fields = source_identifiers(&query.source, ctx);
    let mut relation = if let Some(mut base) = base {
        if let old::Source::Saved {
            condition: Some(condition),
            ..
        } = &query.source
        {
            let mut wire = serde_json::to_value(condition).map_err(error)?;
            promote_globs(&mut wire, &identifier_fields(&base, ctx)?);
            evolve(&mut wire)?;
            base.transforms.push(ast::Transform {
                span: current(condition.span)?,
                kind: ast::TransformKind::Where {
                    condition: serde_json::from_value(wire).map_err(error)?,
                },
            });
        }
        base.attach.extend(
            query
                .attach
                .into_iter()
                .map(current)
                .collect::<Result<Vec<_>, _>>()?,
        );
        base
    } else {
        let mut wire = serde_json::to_value(old::Query {
            source: query.source,
            attach: query.attach,
            stages: vec![],
        })
        .map_err(error)?;
        promote_globs(&mut wire, &source_fields);
        evolve(&mut wire)?;
        serde_json::from_value(wire).map_err(error)?
    };
    let mut temporal = 0usize;
    for stage in query.stages {
        let span = current(stage.span)?;
        match stage.kind {
            old::StageKind::Fields { fields } => {
                if explicit.is_none() {
                    explicit = Some(fields);
                }
            }
            old::StageKind::Temporal {
                relation: kind,
                query: inner,
                at,
                within,
                all,
            } => {
                let inner = super::saved::expand_historical(
                    *inner, ctx, saved, seen, expanded, validating,
                )?;
                if inner.operation.is_some() {
                    return Err(QueryError::new(
                        "historical temporal relation contains a native command",
                    ));
                }
                let mut outer_fields = available_fields(&relation, ctx)?;
                let materialized = relation.transforms.iter().any(|transform| {
                    matches!(
                        transform.kind,
                        ast::TransformKind::Fields { .. } | ast::TransformKind::Aggregate { .. }
                    )
                });
                if !materialized {
                    // A source catalog includes queryable JSON paths, not
                    // independent emitted columns. Carry the payload object
                    // and only paths this outer question actually references.
                    outer_fields.retain(|name| {
                        !name.starts_with("payload.") || payload_dependencies.contains(name)
                    });
                }
                let suffix = if temporal == 0 {
                    String::new()
                } else {
                    format!("_{}", temporal + 1)
                };
                temporal += 1;
                let relation_kind = current(kind)?;
                let inner_table = match &inner.relation.source {
                    ast::Source::Unit { unit, .. } => super::Table::from_name(unit)
                        .or(super::Alias::from_name(unit).map(|_| super::Table::Events)),
                    _ => None,
                }
                .ok_or_else(|| {
                    QueryError::new("historical temporal input has no native row domain")
                })?;
                let span_start = if inner_table == super::Table::Runs {
                    "started"
                } else {
                    "start"
                };
                let span_end = if inner_table == super::Table::Runs {
                    "ended"
                } else {
                    "end"
                };
                let anchor = match at {
                    old::TimePoint::When => inner_table.main_time(),
                    old::TimePoint::Start => span_start,
                    old::TimePoint::End => span_end,
                };
                let key = |name: &str, descending| ast::SortKey {
                    expression: ast::Expression::field(name),
                    descending,
                };
                let mut choice = match kind {
                    old::TemporalRelation::Near => vec![
                        ast::SortKey {
                            expression: ast::Expression::Function {
                                name: "abs".into(),
                                arguments: vec![ast::Expression::field("match.distance")],
                            },
                            descending: false,
                        },
                        key(anchor, false),
                    ],
                    old::TemporalRelation::Before | old::TemporalRelation::Containing => {
                        vec![key(anchor, false)]
                    }
                    old::TemporalRelation::After => vec![key(anchor, true)],
                    old::TemporalRelation::During => vec![key(span_start, false)],
                };
                choice.push(key("match.id", false));
                let at = match at {
                    old::TimePoint::When => ast::TimePoint::MatchWhen,
                    old::TimePoint::Start => ast::TimePoint::MatchStart,
                    old::TimePoint::End => ast::TimePoint::MatchEnd,
                };
                let within = within
                    .map(|duration| {
                        current(duration).map(|duration| ast::Distance::Time { duration })
                    })
                    .transpose()?;
                let transform = if all {
                    ast::TransformKind::Join {
                        query: Box::new(inner.relation),
                        on: ast::JoinOn::Order {
                            relation: relation_kind,
                            within,
                            at,
                            scope: None,
                        },
                    }
                } else {
                    ast::TransformKind::Where {
                        condition: ast::Condition {
                            span,
                            kind: ast::ConditionKind::Order {
                                relation: relation_kind,
                                query: Box::new(inner.relation),
                                within,
                                at,
                                scope: None,
                                pick: ast::WitnessChoice::FirstBy { keys: choice },
                            },
                        },
                    }
                };
                relation.transforms.push(ast::Transform {
                    span,
                    kind: transform,
                });
                let (name, distance) = match kind {
                    old::TemporalRelation::Near
                    | old::TemporalRelation::Before
                    | old::TemporalRelation::After => (format!("near{suffix}"), true),
                    old::TemporalRelation::During => (format!("during{suffix}"), false),
                    old::TemporalRelation::Containing => (format!("containing{suffix}"), false),
                };
                let offset_name = format!("offset{suffix}");
                // Frozen temporal outputs shadow equally named input fields:
                // in particular, numeric seconds `offset` replaces the event's
                // integer byte offset. Emit one current projection per name.
                let mut columns = outer_fields
                    .into_iter()
                    .filter(|field| field != &name && (!distance || field != &offset_name))
                    .map(ast::Column::field)
                    .collect::<Vec<_>>();
                columns.push(ast::Column {
                    expression: ast::Expression::field("match.id"),
                    name: Some(name.clone()),
                });
                generated.push(name);
                if distance {
                    let name = offset_name;
                    // The old offset is numeric seconds with the opposite
                    // sign to match.distance. Divide first so the expression
                    // also has a spelling in the current unparenthesized grammar.
                    let expression = ast::Expression::Arithmetic {
                        operator: ast::Arithmetic::Subtract,
                        left: Box::new(ast::Expression::Value {
                            value: ast::Value::scalar("0"),
                        }),
                        right: Box::new(ast::Expression::Arithmetic {
                            operator: ast::Arithmetic::Divide,
                            left: Box::new(ast::Expression::field("match.distance")),
                            right: Box::new(ast::Expression::Value {
                                value: ast::Value::scalar("1s"),
                            }),
                        }),
                    };
                    columns.push(ast::Column {
                        expression,
                        name: Some(name.clone()),
                    });
                    generated.push(name);
                }
                relation.transforms.push(ast::Transform {
                    span,
                    kind: ast::TransformKind::Fields { columns },
                });
            }
            kind => {
                let mut wire = serde_json::to_value(old::Stage {
                    span: current(span)?,
                    kind,
                })
                .map_err(error)?;
                expand_nested(&mut wire, ctx, saved, seen, expanded, validating)?;
                promote_globs(&mut wire, &identifier_fields(&relation, ctx)?);
                evolve(&mut wire)?;
                relation
                    .transforms
                    .push(serde_json::from_value(wire).map_err(error)?);
            }
        }
    }
    let mut fields = if let Some(fields) = explicit {
        Some(fields)
    } else if temporal == 0 {
        None
    } else if !grouped && inherited_output.is_some() {
        let mut fields = inherited_output.unwrap();
        for field in generated {
            if !fields.contains(&field) {
                fields.push(field);
            }
        }
        Some(fields)
    } else if !grouped {
        let unit = source_unit.or_else(|| match &relation.source {
            ast::Source::Unit { unit, .. } => Some(unit.clone()),
            _ => None,
        });
        unit.and_then(|unit| {
            let alias = super::Alias::from_name(&unit);
            let table = super::Table::from_name(&unit).or(alias.map(|_| super::Table::Events))?;
            let mut fields = table
                .default_fields()
                .iter()
                .filter(|name| alias.is_none() || **name != "payload")
                .map(|name| (*name).to_owned())
                .collect::<Vec<_>>();
            if let Some(alias) = alias {
                fields.extend(alias.fields.iter().map(|field| field.name.to_owned()));
            }
            for field in generated {
                if !fields.contains(&field) {
                    fields.push(field);
                }
            }
            Some(fields)
        })
    } else {
        None
    };
    if !grouped
        && inherited_snippet
        && let Some(fields) = &mut fields
    {
        if ranked_relation(&relation, ctx, validating)?
            && !fields.iter().any(|name| name == "snippet")
        {
            fields.push("snippet".into());
        }
    }

    if let Some(fields) = fields {
        relation.transforms.push(ast::Transform {
            span: Default::default(),
            kind: ast::TransformKind::Fields {
                columns: fields.into_iter().map(ast::Column::field).collect(),
            },
        });
    }
    Ok(relation)
}

fn ranked_relation(
    relation: &ast::Query,
    ctx: &Context,
    validating: bool,
) -> Result<bool, QueryError> {
    let template = native::Template::query(relation.clone());
    Ok(if validating {
        template.validate(ctx)?
    } else {
        template.bind(ctx)?
    }
    .ranked)
}

fn default_output(query: &ast::Query, ctx: &Context) -> Result<Vec<String>, QueryError> {
    if let Some(ast::Transform {
        kind: ast::TransformKind::Fields { columns },
        ..
    }) = query.transforms.last()
    {
        return Ok(columns.iter().map(ast::Column::column).collect());
    }
    if query.transforms.iter().any(|stage| {
        matches!(
            stage.kind,
            ast::TransformKind::Group { .. } | ast::TransformKind::Aggregate { .. }
        )
    }) {
        return available_fields(query, ctx);
    }
    let ast::Source::Unit { unit, .. } = &query.source else {
        return available_fields(query, ctx);
    };
    let alias = super::Alias::from_name(unit);
    let table = super::Table::from_name(unit)
        .or(alias.map(|_| super::Table::Events))
        .ok_or_else(|| QueryError::new("historical wrapper has no native row domain"))?;
    let mut fields = table
        .default_fields()
        .iter()
        .filter(|name| alias.is_none() || **name != "payload")
        .map(|name| (*name).to_owned())
        .collect::<Vec<_>>();
    if let Some(alias) = alias {
        fields.extend(alias.fields.iter().map(|field| field.name.to_owned()));
    }
    Ok(fields)
}

fn referenced_payload_fields(query: &old::Query) -> Result<BTreeSet<String>, QueryError> {
    fn visit(value: &Value, fields: &mut BTreeSet<String>) {
        match value {
            Value::Object(object) => {
                // These predicates introduce another row domain. Its fields
                // are not dependencies of the outer temporal carrier.
                if matches!(
                    object.get("kind").and_then(Value::as_str),
                    Some("exists" | "count" | "sequence" | "scope")
                ) {
                    return;
                }
                if object.get("kind").and_then(Value::as_str) == Some("field") {
                    if let Some(name) = object.get("name").and_then(Value::as_str) {
                        if name.starts_with("payload.") {
                            fields.insert(name.into());
                        }
                    }
                }
                if let Some(name) = object.get("field").and_then(Value::as_str) {
                    if name.starts_with("payload.") {
                        fields.insert(name.into());
                    }
                }
                if let Some(names) = object.get("fields").and_then(Value::as_array) {
                    for name in names.iter().filter_map(Value::as_str) {
                        if name.starts_with("payload.") {
                            fields.insert(name.into());
                        }
                    }
                }
                for (name, value) in object {
                    if name != "query" && name != "attach" {
                        visit(value, fields);
                    }
                }
            }
            Value::Array(values) => {
                for value in values {
                    visit(value, fields);
                }
            }
            _ => {}
        }
    }
    let mut fields = BTreeSet::new();
    visit(&serde_json::to_value(query).map_err(error)?, &mut fields);
    Ok(fields)
}

fn available_fields(query: &ast::Query, ctx: &Context) -> Result<Vec<String>, QueryError> {
    let bound = query_language::bind::bind(query, &super::syntax::catalog(ctx))
        .map_err(super::syntax::error)?;
    let scope = bound
        .typing()
        .query(&Default::default())
        .ok_or_else(|| QueryError::new("historical relation has no bound output"))?;
    let output = scope
        .transforms
        .last()
        .map_or(&scope.source, |stage| &stage.after);
    Ok(output
        .fields
        .iter()
        .map(|field| field.field.name.clone())
        .collect())
}

fn expand_nested(
    value: &mut Value,
    ctx: &Context,
    saved: &BTreeMap<String, super::saved::SavedQuery>,
    seen: &mut BTreeSet<String>,
    expanded: &mut bool,
    validating: bool,
) -> Result<(), QueryError> {
    if value.get("source").is_some() && value.get("stages").is_some() {
        let input = serde_json::from_value(value.clone()).map_err(error)?;
        let template =
            super::saved::expand_historical(input, ctx, saved, seen, expanded, validating)?;
        if template.operation.is_some() {
            return Err(QueryError::new(
                "historical native command occurs inside a relation",
            ));
        }
        *value = serde_json::to_value(template.relation).map_err(error)?;
        return Ok(());
    }
    match value {
        Value::Array(values) => {
            for value in values {
                expand_nested(value, ctx, saved, seen, expanded, validating)?;
            }
        }
        Value::Object(fields) => {
            for value in fields.values_mut() {
                expand_nested(value, ctx, saved, seen, expanded, validating)?;
            }
        }
        _ => {}
    }
    Ok(())
}

fn simple_relation(query: old::Query) -> Result<ast::Query, QueryError> {
    let fields = BTreeSet::from(["source".to_owned(), "event_type".to_owned()]);
    let mut wire = serde_json::to_value(query).map_err(error)?;
    promote_globs(&mut wire, &fields);
    evolve(&mut wire)?;
    serde_json::from_value(wire).map_err(error)
}

fn current<T: serde::Serialize, U: serde::de::DeserializeOwned>(value: T) -> Result<U, QueryError> {
    serde_json::from_value(serde_json::to_value(value).map_err(error)?).map_err(error)
}
fn error(error: serde_json::Error) -> QueryError {
    QueryError::new(error.to_string())
}

/// Project an owned historical payload while preserving original selector/hash evidence.
pub fn upgrade_payload(payload: &mut Value, fallback: &Context) -> Result<(), QueryError> {
    upgrade_value(payload, fallback, true)
}
fn upgrade_value(
    payload: &mut Value,
    fallback: &Context,
    saved_definition: bool,
) -> Result<(), QueryError> {
    let Some(fields) = payload.as_object_mut() else {
        return Ok(());
    };
    if fields.contains_key("query")
        && fields.contains_key("selected")
        && fields.contains_key("declarations")
        && fields.contains_key("now")
    {
        if fields.contains_key("template") {
            return Ok(());
        }
        let timestamp: crate::Timestamp =
            serde_json::from_value(fields["now"].clone()).map_err(error)?;
        let now = jiff::Timestamp::from_nanosecond(timestamp.unix_ns())
            .map_err(|error| QueryError::new(error.to_string()))?;
        let zone = jiff::tz::TimeZone::get(
            fields["zone"]
                .as_str()
                .ok_or_else(|| QueryError::new("stored selection has no zone"))?,
        )
        .map_err(|error| QueryError::new(error.to_string()))?;
        let day_start = fields["day_start"]
            .as_str()
            .ok_or_else(|| QueryError::new("stored selection has no day start"))?
            .parse()
            .map_err(|error: jiff::Error| QueryError::new(error.to_string()))?;
        let mut ctx = Context::new(now, zone, day_start);
        ctx.catalog = fallback.catalog.clone();
        let mut definitions = fields["selected"]
            .get("_saved")
            .cloned()
            .unwrap_or(json!({}));
        if let Some(definitions) = definitions.as_object_mut() {
            for definition in definitions.values_mut() {
                upgrade_value(definition, &ctx, true)?;
            }
        }
        let definitions: std::collections::BTreeMap<String, super::saved::SavedQuery> =
            serde_json::from_value(definitions).map_err(error)?;
        let text = fields["query"]
            .as_str()
            .ok_or_else(|| QueryError::new("stored selection has no query text"))?;
        let parsed = parse_format_one(text, &ctx)?;
        let (_, original_question) = decode_with_identity_for_source(text, &ctx)?;
        let template = super::saved::resolve_historical(parsed, &ctx, &definitions)?;
        fields.insert(
            "template".into(),
            serde_json::to_value(template).map_err(error)?,
        );
        fields.insert(
            "definitions".into(),
            serde_json::to_value(definitions).map_err(error)?,
        );
        fields.insert("original_question".into(), original_question);
        return Ok(());
    }
    if saved_definition
        && fields.contains_key("query")
        && fields.contains_key("parameters")
        && fields.contains_key("name")
    {
        if !fields.contains_key("template") {
            let text = fields["query"]
                .as_str()
                .ok_or_else(|| QueryError::new("stored saved question has no authored text"))?;
            let template = definition_format_one(text, fallback)?;
            fields.insert(
                "template".into(),
                serde_json::to_value(template).map_err(error)?,
            );
        }
        return Ok(());
    }
    for value in fields.values_mut() {
        match value {
            Value::Object(_) => upgrade_value(value, fallback, false)?,
            Value::Array(values) => {
                for value in values {
                    upgrade_value(value, fallback, false)?;
                }
            }
            _ => {}
        }
    }
    Ok(())
}

/// The old decoder owns its old syntax; this conversion owns the storage change.
fn evolve(value: &mut Value) -> Result<(), QueryError> {
    match value {
        Value::Array(values) => {
            for value in values {
                evolve(value)?;
            }
        }
        Value::Object(fields) => {
            if fields.contains_key("source") && fields.contains_key("transforms") {
                return Ok(());
            }
            // A query is identified structurally, independent of its embedding.
            if fields.contains_key("source") && fields.contains_key("stages") {
                if let Some(stages) = fields.remove("stages") {
                    fields.insert("transforms".into(), stages);
                }
            }
            let kind = fields
                .get("kind")
                .and_then(Value::as_str)
                .map(str::to_owned);
            match kind.as_deref() {
                Some("walk" | "recall" | "recall_last" | "recall_mark") => {
                    return Err(QueryError::new(
                        "historical native operation occurs inside a relation",
                    ));
                }
                Some("saved" | "result") => {
                    if fields.get("output").and_then(Value::as_str) == Some("deferred") {
                        fields.insert("output".into(), json!("unknown"));
                    }
                }
                Some("compare") => {
                    if let Some(operand) = fields.remove("operand") {
                        fields.insert("left".into(), operand);
                    }
                    if let Some(value) = fields.remove("value") {
                        fields.insert("right".into(), json!({"kind":"value", "value":value}));
                    }
                }
                Some("since" | "until") => {
                    if let Some(operand) = fields.remove("operand") {
                        fields.insert("field".into(), operand["name"].clone());
                    }
                }
                Some("between" | "time_range" | "in") => {
                    if let Some(operand) = fields.remove("operand") {
                        fields.insert("left".into(), operand);
                    }
                }
                Some("field_exists") => {
                    let left = fields.remove("operand").unwrap_or(Value::Null);
                    *value = json!({"kind":"compare","left":left,"op":"eq","right":{"kind":"value","value":{"kind":"known"}}});
                    return evolve(value);
                }
                Some("time_difference") => {
                    let left = fields.remove("left").unwrap_or(Value::Null);
                    let right = fields.remove("right").unwrap_or(Value::Null);
                    let op = fields.remove("op").unwrap_or(Value::Null);
                    let rhs = fields.remove("value").unwrap_or(Value::Null);
                    *value = json!({"kind":"compare","left":{"kind":"arithmetic","operator":"subtract","left":left,"right":right},"op":op,"right":{"kind":"value","value":rhs}});
                    return evolve(value);
                }
                Some("similar") => {
                    fields.insert("kind".into(), json!("rank"));
                    fields.insert("list".into(), json!("similar"));
                }
                Some("overlaps") => {
                    let from = fields.remove("from").unwrap_or(Value::Null);
                    let to = fields.remove("to").unwrap_or(Value::Null);
                    *value = json!({"kind":"extension","name":"overlaps","arguments":[{"value":from},{"optional":if to.is_null(){json!([])}else{json!([{"keyword":".."},{"value":to}])}}]});
                    return evolve(value);
                }
                Some("fields") => {
                    if let Some(Value::Array(names)) = fields.remove("fields") {
                        fields.insert("columns".into(), Value::Array(names.into_iter().map(|name|json!({"expression":{"kind":"field","name":name},"name":null})).collect()));
                    }
                }
                Some("group") => {
                    if let Some(Value::Array(keys)) = fields.remove("keys") {
                        fields.insert(
                            "keys".into(),
                            Value::Array(
                                keys.into_iter()
                                    .map(|expression| {
                                        let operand: old::Operand =
                                            serde_json::from_value(expression.clone())
                                                .expect("decoded historical group key");
                                        json!({"expression":expression,"name":operand.column()})
                                    })
                                    .collect(),
                            ),
                        );
                    }
                }
                Some("temporal") => {
                    return Err(QueryError::new(
                        "historical temporal stage was not converted in its relation scope",
                    ));
                }
                Some("measure") => {
                    let name = fields.remove("name").unwrap_or(Value::Null);
                    let by = fields.remove("by").unwrap_or(json!([]));
                    let by = by.as_array().ok_or_else(|| {
                        QueryError::new("historical measure grouping is not an array")
                    })?;
                    *value = json!({"kind":"extension","name":"measure","arguments":[{"name":name},{"list":by.iter().map(|expression| json!({"expression":expression})).collect::<Vec<_>>()}]});
                    return evolve(value);
                }
                _ => {}
            }
            if fields.contains_key("descending")
                && let Some(operand) = fields.remove("operand")
            {
                fields.insert("expression".into(), operand);
            }
            if fields.contains_key("function")
                && let Some(Value::Array(arguments)) = fields.remove("fields")
            {
                let aggregate: old::Aggregate = serde_json::from_value(
                    json!({"function":fields["function"],"fields":arguments}),
                )
                .map_err(error)?;
                fields.insert("name".into(), json!(aggregate.column()));
                fields.insert(
                    "arguments".into(),
                    Value::Array(
                        aggregate
                            .fields
                            .into_iter()
                            .map(|name| json!({"kind":"field","name":name}))
                            .collect(),
                    ),
                );
            }
            for value in fields.values_mut() {
                evolve(value)?;
            }
        }
        _ => {}
    }
    Ok(())
}

#[cfg(test)]
mod tests;

fn source_identifiers(source: &old::Source, ctx: &Context) -> BTreeSet<String> {
    let old::Source::Unit { unit, .. } = source else {
        return BTreeSet::new();
    };
    super::syntax::catalog(ctx)
        .units
        .into_iter()
        .find(|declared| declared.name == *unit)
        .map(|unit| {
            unit.fields
                .into_iter()
                .filter(|field| matches!(field.name.as_str(), "source" | "event_type"))
                .map(|field| field.name)
                .collect()
        })
        .unwrap_or_default()
}
fn identifier_fields(query: &ast::Query, ctx: &Context) -> Result<BTreeSet<String>, QueryError> {
    use query_language::scope::{FieldInfo, Origin, TypeRule};
    fn identifier(info: &FieldInfo) -> bool {
        match &info.origin {
            Origin::Catalog { field, .. } => matches!(field.as_str(), "source" | "event_type"),
            Origin::Group { input, .. } | Origin::Matched { input } => identifier(input),
            Origin::Function {
                arguments,
                rule: TypeRule::Input(index),
                ..
            } => arguments.get(*index).is_some_and(identifier),
            Origin::Aggregate {
                inputs,
                rule: TypeRule::Input(index),
                ..
            } => inputs.get(*index).is_some_and(identifier),
            _ => false,
        }
    }
    let bound = query_language::bind::bind(query, &super::syntax::catalog(ctx))
        .map_err(super::syntax::error)?;
    let scope = bound
        .typing()
        .query(&Default::default())
        .ok_or_else(|| QueryError::new("historical relation has no bound output"))?;
    Ok(scope
        .transforms
        .last()
        .map_or(&scope.source, |stage| &stage.after)
        .fields
        .iter()
        .filter(|field| identifier(field))
        .map(|field| field.field.name.clone())
        .collect())
}
fn promote_globs(value: &mut Value, fields: &BTreeSet<String>) {
    if value.get("source").is_some() && value.get("transforms").is_some() {
        return;
    }
    let kind = value.get("kind").and_then(Value::as_str);
    if kind == Some("compare") {
        let identifier = value
            .get("operand")
            .and_then(|operand| operand.get("name"))
            .and_then(Value::as_str);
        let op = value.get("op").and_then(Value::as_str);
        if identifier.is_some_and(|name| fields.contains(name))
            && matches!(op, Some("eq" | "ne"))
            && value.get("value").is_some_and(contains_glob)
        {
            *value = glob_condition(
                value["operand"].clone(),
                value["value"].clone(),
                op == Some("ne"),
            );
            return;
        }
    } else if kind == Some("match")
        && value
            .get("field")
            .and_then(Value::as_str)
            .is_some_and(|name| fields.contains(name))
        && value.get("value").is_some_and(contains_glob)
    {
        *value = glob_condition(
            json!({"kind":"field","name":value["field"]}),
            value["value"].clone(),
            false,
        );
        return;
    }
    match value {
        Value::Object(values) => {
            for value in values.values_mut() {
                promote_globs(value, fields);
            }
        }
        Value::Array(values) => {
            for value in values {
                promote_globs(value, fields);
            }
        }
        _ => {}
    }
}

fn contains_glob(value: &Value) -> bool {
    match value.get("kind").and_then(Value::as_str) {
        Some("scalar") => value
            .get("text")
            .and_then(Value::as_str)
            .is_some_and(|text| text.contains('*')),
        Some("set") => value
            .get("values")
            .and_then(Value::as_array)
            .is_some_and(|values| values.iter().any(contains_glob)),
        _ => false,
    }
}
fn glob_condition(operand: Value, value: Value, negative: bool) -> Value {
    if value.get("kind").and_then(Value::as_str) == Some("set") {
        let conditions=value["values"].as_array().expect("decoded historical set").iter().cloned().map(|value|json!({"span":{"start":0,"end":0},"kind":glob_condition(operand.clone(),value,negative)})).collect::<Vec<_>>();
        return json!({"kind":if negative {"and"}else{"or"},"conditions":conditions});
    }
    if contains_glob(&value) {
        json!({"kind":"compare","left":{"kind":"function","name":"glob","arguments":[operand,{"kind":"value","value":value}]},"op":if negative {"ne"}else{"eq"},"right":{"kind":"value","value":{"kind":"scalar","text":"true","quoted":false}}})
    } else {
        json!({"kind":"compare","left":operand,"op":if negative {"ne"}else{"eq"},"right":{"kind":"value","value":value}})
    }
}

#[test]
fn historical_catalog_preserves_its_frozen_vocabulary() {
    use historical_query_format_v1::catalog::FieldKind;
    let catalog = historical_catalog(&Context::utc(jiff::Timestamp::UNIX_EPOCH));
    assert_eq!(
        catalog.field("events", "source").unwrap().kind,
        FieldKind::Text
    );
    assert_eq!(
        catalog.field("events", "text").unwrap().kind,
        FieldKind::Text
    );
    assert_eq!(
        catalog.field("events", "id").unwrap().kind,
        FieldKind::Reference
    );
    assert!(catalog.field("events", "snippet").is_none());
    assert!(catalog.field("events", "start").is_none());
    assert!(catalog.field("sources", "id").is_none());
    assert!(catalog.extensions.is_empty());
    assert!(catalog.source_outputs.is_empty());
    assert!(catalog.commands.iter().any(|word| word == "recall"));
    assert!(
        parse_format_one(
            "events where glob(source, 'x*') = true",
            &Context::utc(jiff::Timestamp::UNIX_EPOCH)
        )
        .is_err()
    );
}
