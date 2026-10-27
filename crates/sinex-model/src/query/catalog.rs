//! Owned field descriptions from one immutable declaration snapshot.
use super::{Context, Query, QueryError};
use crate::{KindDeclaration, KindRef};
use serde::{Deserialize, Serialize};
use serde_json::Value as Json;
use std::collections::{BTreeMap, BTreeSet};

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DataType {
    Text,
    Integer,
    Number,
    Bool,
    Time,
    Array(Box<DataType>),
    Json,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct FieldDescription {
    pub path: Vec<String>,
    pub ty: DataType,
    pub nullable: bool,
    pub values: Vec<Json>,
}

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct QueryCatalog {
    pub declarations: Vec<(KindRef, String)>,
    pub fields: BTreeMap<String, FieldDescription>,
    pub schemaless: bool,
}

impl QueryCatalog {
    pub fn new(declarations: impl IntoIterator<Item = KindDeclaration>) -> Self {
        let mut catalog = Self::default();
        let mut counts = BTreeMap::<String, usize>::new();
        let mut total = 0;
        for d in declarations {
            total += 1;
            catalog
                .declarations
                .push((d.kind.clone(), d.definition_hash()));
            let Some(schema) = d.payload_schema else {
                catalog.schemaless = true;
                continue;
            };
            let fields = describe(&schema, &schema, vec![], false, &mut BTreeSet::new());
            for (name, field) in fields {
                *counts.entry(name.clone()).or_default() += 1;
                match catalog.fields.get_mut(&name) {
                    None => {
                        catalog.fields.insert(name, field);
                    }
                    Some(previous) => merge(previous, field),
                }
            }
        }
        catalog.declarations.sort();
        for (name, field) in &mut catalog.fields {
            field.nullable |= counts[name] < total;
        }
        if catalog.schemaless {
            for f in catalog.fields.values_mut() {
                f.ty = DataType::Json;
                f.nullable = true;
                f.values.clear();
            }
        }
        catalog
    }

    pub fn field(&self, path: &[String]) -> Option<&FieldDescription> {
        self.fields.get(&path_name(path))
    }

    pub fn bind(&self, query: &mut Query, ctx: &Context) -> Result<(), QueryError> {
        let mut ctx = ctx.clone();
        ctx.catalog = Some(self.clone());
        super::typed::check(query, &ctx)
    }
}

fn merge(a: &mut FieldDescription, b: FieldDescription) {
    if matches!(
        (&a.ty, &b.ty),
        (DataType::Integer, DataType::Number) | (DataType::Number, DataType::Integer)
    ) {
        a.ty = DataType::Number;
        a.values.clear();
    } else if a.ty != b.ty {
        a.ty = DataType::Json;
        a.values.clear();
    } else if a.values.is_empty() || b.values.is_empty() {
        a.values.clear();
    } else {
        for value in b.values {
            if !a.values.contains(&value) {
                a.values.push(value);
            }
        }
    }
    a.nullable |= b.nullable;
}

fn datatype(schema: &Json, root: &Json, active: &mut BTreeSet<String>) -> DataType {
    if let Some(reference) = schema["$ref"].as_str() {
        if !active.insert(reference.into()) {
            return DataType::Json;
        }
        let ty = reference
            .strip_prefix('#')
            .and_then(|r| root.pointer(r))
            .map(|s| datatype(s, root, active))
            .unwrap_or(DataType::Json);
        active.remove(reference);
        return ty;
    }
    if let Some(branches) = schema
        .get("anyOf")
        .or_else(|| schema.get("oneOf"))
        .and_then(Json::as_array)
    {
        let types: Vec<_> = branches
            .iter()
            .filter(|s| s["type"] != "null")
            .map(|s| datatype(s, root, active))
            .collect();
        return types
            .first()
            .filter(|t| types.iter().all(|other| other == *t))
            .cloned()
            .unwrap_or(DataType::Json);
    }
    let ty = if let Some(ty) = schema["type"].as_str() {
        Some(ty)
    } else {
        let types: Vec<_> = schema["type"]
            .as_array()
            .into_iter()
            .flatten()
            .filter_map(Json::as_str)
            .filter(|s| *s != "null")
            .collect();
        if types.len() > 1 {
            return DataType::Json;
        }
        types.first().copied()
    };
    match ty {
        Some("string") if schema["format"] == "date-time" => DataType::Time,
        Some("string") => DataType::Text,
        Some("integer") => DataType::Integer,
        Some("number") => DataType::Number,
        Some("boolean") => DataType::Bool,
        Some("array") => DataType::Array(Box::new(datatype(&schema["items"], root, active))),
        None if schema["enum"]
            .as_array()
            .is_some_and(|a| !a.is_empty() && a.iter().all(Json::is_string)) =>
        {
            DataType::Text
        }
        None if schema["enum"]
            .as_array()
            .is_some_and(|a| !a.is_empty() && a.iter().all(Json::is_boolean)) =>
        {
            DataType::Bool
        }
        _ => DataType::Json,
    }
}

fn describe(
    schema: &Json,
    root: &Json,
    path: Vec<String>,
    nullable: bool,
    active: &mut BTreeSet<String>,
) -> BTreeMap<String, FieldDescription> {
    if let Some(reference) = schema.get("$ref").and_then(Json::as_str) {
        if !active.insert(reference.into()) {
            return leaf(path, DataType::Json, true, vec![]);
        }
        let mut described = reference
            .strip_prefix('#')
            .and_then(|r| root.pointer(r))
            .map(|s| describe(s, root, path.clone(), nullable, active))
            .unwrap_or_else(|| leaf(path.clone(), DataType::Json, true, vec![]));
        active.remove(reference);
        add_properties(&mut described, schema, root, &path, nullable, active);
        return described;
    }
    if let Some(branches) = schema
        .get("anyOf")
        .or_else(|| schema.get("oneOf"))
        .or_else(|| schema.get("allOf"))
        .and_then(Json::as_array)
    {
        let nullable = nullable || branches.iter().any(|s| s["type"] == "null");
        let mut out: BTreeMap<String, FieldDescription> = BTreeMap::new();
        let mut counts: BTreeMap<String, usize> = BTreeMap::new();
        let branch_count = branches.iter().filter(|s| s["type"] != "null").count();
        for branch in branches.iter().filter(|s| s["type"] != "null") {
            for (name, field) in describe(branch, root, path.clone(), nullable, active) {
                *counts.entry(name.clone()).or_default() += 1;
                if let Some(previous) = out.get_mut(&name) {
                    merge(previous, field);
                } else {
                    out.insert(name, field);
                }
            }
        }
        if schema.get("allOf").is_none() {
            for (name, field) in &mut out {
                field.nullable |= counts[name] < branch_count;
            }
        }
        add_properties(&mut out, schema, root, &path, nullable, active);
        return out;
    }
    let nullable = nullable
        || schema["type"]
            .as_array()
            .is_some_and(|t| t.iter().any(|v| v == "null"));
    let values = schema["enum"].as_array().cloned().unwrap_or_default();
    let mut out = leaf(
        path.clone(),
        datatype(schema, root, &mut BTreeSet::new()),
        nullable,
        values,
    );
    add_properties(&mut out, schema, root, &path, nullable, active);
    out
}

fn add_properties(
    out: &mut BTreeMap<String, FieldDescription>,
    schema: &Json,
    root: &Json,
    path: &[String],
    nullable: bool,
    active: &mut BTreeSet<String>,
) {
    if let Some(properties) = schema["properties"].as_object() {
        for (name, property) in properties {
            let mut child = path.to_vec();
            child.push(name.clone());
            let required = schema["required"]
                .as_array()
                .is_some_and(|r| r.contains(&Json::String(name.clone())));
            for (name, field) in describe(property, root, child, nullable || !required, active) {
                if let Some(previous) = out.get_mut(&name) {
                    merge(previous, field);
                } else {
                    out.insert(name, field);
                }
            }
        }
    }
}

fn path_name(path: &[String]) -> String {
    path.iter()
        .map(|p| p.replace('\\', "\\\\").replace('.', "\\."))
        .collect::<Vec<_>>()
        .join(".")
}

fn leaf(
    path: Vec<String>,
    ty: DataType,
    nullable: bool,
    values: Vec<Json>,
) -> BTreeMap<String, FieldDescription> {
    if path.is_empty() {
        BTreeMap::new()
    } else {
        BTreeMap::from([(
            path_name(&path),
            FieldDescription {
                path,
                ty,
                nullable,
                values,
            },
        )])
    }
}

#[cfg(test)]
mod tests;
