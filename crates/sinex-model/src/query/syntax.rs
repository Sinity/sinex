//! Sinex vocabulary for the shared grammar. Storage binding lives in `parse`.
use super::{ALIASES, Context, FieldType, QueryCatalog, QueryError, Table};
use query_language::catalog::{Catalog, Field, FieldKind, ReferenceSyntax, Unit};

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
                kind: field_kind(f.ty),
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
        recall_last_unit: Some("shell-sessions".into()),
        units,
        commands: super::tables::COMMAND_WORDS
            .iter()
            .map(|s| (*s).into())
            .collect(),
        references: vec![ReferenceSyntax {
            prefix: "sinex:".into(),
            marker: Some("/".into()),
        }],
        unpiped_stages: true,
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
    query_language::parse::parse(text, &catalog(ctx)).map_err(error)
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
