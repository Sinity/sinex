//! Stored query definitions embedded in event payloads.
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
}

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
    pub template: Template,
    pub parameters: Vec<Parameter>,
}

impl SavedQuery {
    /// The saved query this definition reads from, when it is built on one.
    pub fn reference_name(&self) -> Option<String> {
        match &self.template.relation.source {
            syntax::Source::Saved { name, .. } => Some(name.clone()),
            _ => None,
        }
    }
}
