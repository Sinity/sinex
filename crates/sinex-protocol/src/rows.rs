//! One resolved selection followed by typed steps. CLI spelling is a boundary;
//! these requests carry the same fields over MCP and HTTP.
#[cfg(feature = "surface")]
use command_surface::{ArgumentForm, ArgumentSpec, StepDeclarations};
use command_surface::{Failure, FailureKind};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
#[cfg(feature = "surface")]
use std::sync::LazyLock;

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct Rows {
    /// A query, time, path, URL or reference, as one argument.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub selection: Option<String>,
    /// An ordered explicit population, instead of selection text.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub references: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub steps: Vec<Step>,
    /// Authorize a write over every row a query matched; not a paging option.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub all: bool,
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub dry_run: bool,
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub explain: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub page_size: Option<u64>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub fields: Vec<String>,
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub why: bool,
}

impl Rows {
    /// Population constraints are independent of transport and verb spelling.
    pub fn validate(&self) -> Result<(), Failure> {
        if self.selection.is_some() && !self.references.is_empty() {
            return Err(Failure::new(
                FailureKind::BadRequest,
                "use selection or references, not both",
            ));
        }
        if self
            .selection
            .as_ref()
            .is_some_and(|text| text.trim().is_empty())
        {
            return Err(Failure::new(FailureKind::BadRequest, "selection is empty"));
        }
        if self
            .references
            .iter()
            .any(|reference| reference.trim().is_empty())
        {
            return Err(Failure::new(
                FailureKind::BadRequest,
                "a reference is empty",
            ));
        }
        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "verb", rename_all = "snake_case")]
pub enum Step {
    Read(Read),
}

/// Views with an existing reader. Future domain views join this type with
/// their implementation, so discovery cannot advertise an empty substitute.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum ReadView {
    #[default]
    Full,
    Trace,
}

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct Read {
    #[serde(default)]
    pub view: ReadView,
    /// Number of rows in this read page; the continuation reaches the rest.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub page_size: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cursor: Option<String>,
}

/// Step argument ownership is shared by all parsers. The CLI's eventual
/// splitter passes already-separated words to this declaration owner.
#[cfg(feature = "surface")]
pub fn steps() -> &'static StepDeclarations {
    static STEPS: LazyLock<StepDeclarations> = LazyLock::new(|| {
        let mut steps = StepDeclarations::default().cli_selection("selection", "references");
        steps
            .declare::<Read>(
                "read",
                vec![
                    ArgumentSpec::new(
                        "view",
                        ArgumentForm::Named,
                        "full content or provenance trace",
                    ),
                    ArgumentSpec::new("page_size", ArgumentForm::Named, "rows in this page"),
                    ArgumentSpec::new("cursor", ArgumentForm::Named, "continue this read"),
                ],
            )
            .expect("read step fields match their typed declaration");
        steps
    });
    &STEPS
}

#[cfg(test)]
mod tests;

/// The accepted population and the results of steps run against it.
#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema)]
pub struct RowsReply {
    pub population: Option<sinex_model::query_reply::QueryPage>,
    pub references: Vec<String>,
    pub steps: Vec<StepReport>,
}

#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema)]
pub struct StepReport {
    pub verb: String,
    pub outcome: StepOutcome,
}

#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "status", rename_all = "snake_case")]
pub enum StepOutcome {
    Done { reply: ReadReply },
    Failed { error: Failure },
    NotRun,
}

#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "kind", content = "reply", rename_all = "snake_case")]
pub enum ReadReply {
    Records(Vec<serde_json::Value>),
    Trace(Vec<sinex_model::query_reply::QueryPage>),
}
