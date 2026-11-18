//! One resolved selection followed by typed steps. CLI spelling is a boundary;
//! these requests carry the same fields over MCP and HTTP.
use command_surface::{Failure, FailureKind};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

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
    /// Continue a read from an accepted write request's original question.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub accepted: Option<sinex_model::OperationId>,
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
        if self.selection.is_none()
            && self.references.is_none()
            && (self.steps.is_empty()
                || self.steps.iter().any(|step| !matches!(step, Step::Note(_))))
        {
            return Err(Failure::new(
                FailureKind::BadRequest,
                "this line needs a selection or references",
            ));
        }
        if self.accepted.is_some() && self.steps.iter().any(|step| !matches!(step, Step::Read(_))) {
            return Err(Failure::new(
                FailureKind::BadRequest,
                "an accepted read continuation contains only reads",
            ));
        }
        if self.steps.iter().any(|step| matches!(step, Step::Save(_))) && self.selection.is_none() {
            return Err(Failure::new(
                FailureKind::BadRequest,
                "save needs a question in selection",
            ));
        }
        if self.steps.iter().any(|step| matches!(step, Step::Note(_)))
            && (self.selection.is_some() || self.references.is_some())
        {
            return Err(Failure::new(
                FailureKind::BadRequest,
                "a moment note takes no selection",
            ));
        }
        for step in &self.steps {
            let Step::Read(read) = step else { continue };
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
    Save(Save),
    Note(Note),
    Archive(Lifecycle),
    Restore(Lifecycle),
}

impl Step {
    pub fn verb(&self) -> &'static str {
        match self {
            Self::Read(_) => "read",
            Self::Save(_) => "save",
            Self::Note(_) => "note",
            Self::Archive(_) => "archive",
            Self::Restore(_) => "restore",
        }
    }
}

/// Move the selected readings or materials with their provenance chains.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct Lifecycle {
    /// Authorize every selected row, beyond the displayed page.
    #[serde(default)]
    pub all: bool,
    /// Compute the same cascade and refusals without recording a request or move.
    #[serde(default)]
    pub dry_run: bool,
}

/// A moment note with context explicitly observed by its client.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct Note {
    pub text: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub at: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cwd: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub command: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub app: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub title: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub command_record: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub focus_record: Option<String>,
}

/// Store the selected question without requiring its current data to exist.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct Save {
    pub name: String,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub parameter: Vec<String>,
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
    Done {
        reply: StepReply,
    },
    Failed {
        error: Failure,
    },
    StoppedEarly {
        error: Failure,
        reply: serde_json::Value,
    },
    NotRun,
}

#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "kind", content = "reply", rename_all = "snake_case")]
pub enum StepReply {
    Records(Vec<serde_json::Value>),
    Trace(Vec<sinex_model::query_reply::QueryPage>),
    Written(serde_json::Value),
}

impl RowsReply {
    /// A failed step keeps prior results, while clients still report failure.
    pub fn failure(&self) -> Option<&Failure> {
        self.steps.iter().find_map(|step| match &step.outcome {
            StepOutcome::Failed { error } | StepOutcome::StoppedEarly { error, .. } => Some(error),
            StepOutcome::Done { .. } | StepOutcome::NotRun => None,
        })
    }
}
