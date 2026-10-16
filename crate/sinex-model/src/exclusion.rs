//! One retained Atuin occurrence, separate from its interpretation IDs.
use serde::{Deserialize, Serialize};
use uuid::Uuid;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Request {
    #[serde(rename = "ref")]
    pub reference: String,
    pub action_id: Uuid,
    #[serde(default)]
    pub expected_key: Option<String>,
}
impl Request {
    pub fn event_id(&self) -> Result<Uuid, &'static str> {
        Uuid::parse_str(
            self.reference
                .strip_prefix("sinex:ev/")
                .unwrap_or(&self.reference),
        )
        .map_err(|_| "delete requires one raw Atuin event UUID or sinex:ev/UUID")
    }
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Action {
    Exclude,
    Undo,
}
impl Action {
    pub fn method(self) -> &'static str {
        match self {
            Self::Exclude => "delete.run",
            Self::Undo => "delete.undo",
        }
    }
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum State {
    Completed,
    Noop,
    Refused,
    NotFound,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Occurrence {
    pub source: String,
    pub event_type: String,
    pub equivalence_key: String,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Outcome {
    pub operation_id: Uuid,
    pub action_id: Uuid,
    pub method: String,
    pub state: State,
    pub target: Uuid,
    pub occurrence: Option<Occurrence>,
    /// The exclusion binding immediately after this action, not a current-state claim on retry.
    pub exclusion_operation: Option<Uuid>,
    pub removed_exclusion_operation: Option<Uuid>,
    pub archived: Vec<Uuid>,
    pub reason: Option<String>,
    /// A canonical refusal may identify its one raw parent, without applying to it.
    pub raw_ref: Option<String>,
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn exact_reference_and_typed_action_boundary() {
        let id = Uuid::now_v7();
        let mut r = Request {
            reference: format!("sinex:ev/{id}"),
            action_id: Uuid::now_v7(),
            expected_key: None,
        };
        assert_eq!(r.event_id().unwrap(), id);
        r.reference = "events where kind = command.executed".into();
        assert!(r.event_id().is_err());
        assert!(
            serde_json::from_value::<Request>(
                serde_json::json!({"ref":id,"action_id":Uuid::now_v7(),"purge":true})
            )
            .is_err()
        );
    }
}
