//! Sinex vocabulary for the shared grammar. Storage binding lives in `parse`.
use super::{ALIASES, Context, FieldType, QueryCatalog, QueryError, Table};
use query_language::catalog::{Catalog, Field, FieldKind, ReferenceForm, Unit};

pub fn catalog(ctx: &Context) -> Catalog {
    let declarations = ctx
        .catalog
        .clone()
        .unwrap_or_else(|| QueryCatalog::new(crate::event_types::declarations().iter().cloned()));
    let payload = || {
        declarations
            .fields
            .values()
            .map(|f| Field {
                name: format!("payload.{}", f.path.join(".")),
                // Kind narrowing and conflicting payload schemas are checked by the
                // Sinex binder, after the whole condition has selected its kinds.
                kind: FieldKind::Json,
                ..Field::default()
            })
            .collect::<Vec<_>>()
    };
    let fields = |table: Table, extra: &[super::Field]| {
        let mut fields = table
            .fields()
            .iter()
            .chain(extra)
            .map(|f| Field {
                name: f.name.into(),
                kind: if matches!(
                    f.name,
                    "source" | "event_type" | "host" | "binding" | "producer"
                ) {
                    FieldKind::Exact
                } else if f.name == "text" {
                    FieldKind::Words
                } else {
                    field_kind(f.ty)
                },
                values: if let FieldType::Enum(v) = f.ty {
                    v.iter().map(|v| (*v).into()).collect()
                } else {
                    vec![]
                },
                help: f.phrase.into(),
                ..Field::default()
            })
            .collect::<Vec<_>>();
        if table.is_events() {
            fields.extend(payload());
        }
        for name in if table.is_events() {
            &["snippet", "start", "end"][..]
        } else if table == Table::Runs {
            &["start", "end"][..]
        } else {
            &[][..]
        } {
            if !fields.iter().any(|field| field.name == *name) {
                fields.push(Field {
                    name: (*name).into(),
                    kind: if *name == "snippet" {
                        FieldKind::Text
                    } else {
                        FieldKind::Time
                    },
                    ..Field::default()
                });
            }
        }
        if matches!(table, Table::Sources | Table::Gaps)
            && !fields.iter().any(|field| field.name == "id")
        {
            fields.push(Field {
                name: "id".into(),
                kind: FieldKind::Exact,
                ..Field::default()
            });
        }
        fields
    };
    let mut units = Table::ALL
        .into_iter()
        .map(|t| Unit {
            name: t.name().into(),
            fields: fields(t, &[]),
            main_time: Some(t.main_time().into()),
            identity: vec![if t == Table::Sources { "binding" } else { "id" }.into()],
            default_order: vec![t.main_time().into()],
            help: t.phrase().into(),
            ..Unit::default()
        })
        .collect::<Vec<_>>();
    units.extend(ALIASES.iter().map(|a| Unit {
        name: a.name.into(),
        fields: fields(Table::Events, a.fields),
        main_time: Some("when".into()),
        identity: vec!["id".into()],
        default_order: vec!["when".into()],
        help: a.phrase.into(),
        ..Unit::default()
    }));
    Catalog {
        default_unit: "events".into(),
        namespace: Some("sx".into()),
        units,
        extensions: {
            use query_language::catalog::{Extension, ExtensionPosition as P, ExtensionSlot as S};
            vec![
                Extension {
                    name: "holding".into(),
                    position: P::Atom,
                    arguments: vec![S::Name, S::Value],
                    help: "State intervals holding at an instant or during a period".into(),
                },
                Extension {
                    name: "overlaps".into(),
                    position: P::Atom,
                    arguments: vec![
                        S::Value,
                        S::Optional(vec![S::Keyword("..".into()), S::Value]),
                    ],
                    help: "Intersect state intervals with an instant or interval".into(),
                },
                Extension {
                    name: "measure".into(),
                    position: P::Transform,
                    arguments: vec![S::Name, S::List(Box::new(S::Expression))],
                    help:
                        "Stored historical measure syntax; execution requires an analysis operation"
                            .into(),
                },
            ]
        },
        references: vec![ReferenceForm::Prefix {
            prefix: "sinex:".into(),
            marker: Some("/".into()),
        }],
        ..Catalog::default()
    }
}

pub fn field_kind(ty: FieldType) -> FieldKind {
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
pub fn parse(text: &str, ctx: &Context) -> Result<query_language::ast::Query, QueryError> {
    let catalog = catalog(ctx);
    let input = text.trim_start();
    let first = input.split_whitespace().next().unwrap_or("");
    // Command routing belongs to the product. A quoted command word remains
    // ordinary search text, and a declared relation wins an overlapping name.
    if catalog.unit(first).is_none()
        && super::tables::COMMAND_WORDS
            .iter()
            .any(|word| first.eq_ignore_ascii_case(word))
    {
        return Err(QueryError::not_built(
            text.len() - input.len(),
            format!("`{first}` is a Sinex operation, not a relation query"),
        ));
    }
    query_language::parse::parse(text, &catalog).map_err(error)
}
pub fn error(e: query_language::Error) -> QueryError {
    let not_built = e.kind == query_language::ErrorKind::NotAQuery;
    let mut message = e.message;
    if let Some(suggestion) = e.suggestion {
        message.push_str(&format!("; try `{suggestion}`"));
    }
    QueryError {
        message,
        at: Some(e.position),
        not_built,
    }
}
