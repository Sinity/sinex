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
    /// One query or source reference, as one argument.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub selection: Option<String>,
    /// An ordered explicit population, instead of selection text.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub references: Option<Vec<String>>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub steps: Vec<Step>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub page_size: Option<u64>,
}

impl Rows {
    /// Population constraints are independent of transport and verb spelling.
    pub fn validate(&self) -> Result<(), Failure> {
        if self.selection.is_some() && self.references.is_some() {
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
        if self.references.as_ref().is_some_and(|references| {
            references
                .iter()
                .any(|reference| reference.trim().is_empty())
        }) {
            return Err(Failure::new(
                FailureKind::BadRequest,
                "a reference is empty",
            ));
        }
        if self.selection.is_none() && self.references.is_none() {
            return Err(Failure::new(
                FailureKind::BadRequest,
                "read needs a selection or references",
            ));
        }
        for step in &self.steps {
            let Step::Read(read) = step;
            if read.view == ReadView::Full && (read.page_size.is_some() || read.cursor.is_some()) {
                return Err(Failure::new(
                    FailureKind::BadRequest,
                    "read page_size and cursor belong to the trace view; a full read continues its population page",
                ));
            }
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
    /// Number of rows in a trace page; its continuation reaches the rest.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub page_size: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cursor: Option<String>,
    /// Exact accepted commit position for a continued provenance read.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub at: Option<String>,
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
                    ArgumentSpec::new("page_size", ArgumentForm::Named, "rows in a trace page"),
                    ArgumentSpec::new("cursor", ArgumentForm::Named, "continue a trace page"),
                    ArgumentSpec::new("at", ArgumentForm::Named, "exact accepted commit position"),
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

impl RowsReply {
    /// A failed step keeps prior results, while clients still report failure.
    pub fn failure(&self) -> Option<&Failure> {
        self.steps.iter().find_map(|step| match &step.outcome {
            StepOutcome::Failed { error } => Some(error),
            StepOutcome::Done { .. } | StepOutcome::NotRun => None,
        })
    }
}
