//! Input event filtering helpers for `AutomatonRuntime`.
//!
//! Carved out of `adapter/mod.rs` as part of #697. Pure mechanical move; the
//! methods, control flow, and instrumentation are unchanged.

use super::AutomatonRuntime;

use crate::runtime::SinexError;
use crate::runtime::automaton::traits::{Automaton, InputProvenanceFilter};

use sinex_primitives::events::Event;
use sinex_primitives::{EventType, JsonValue};

impl<N> AutomatonRuntime<N>
where
    N: Automaton,
{
    pub(super) fn input_event_type_matches(&self, event: &Event<JsonValue>) -> bool {
        let input_type = self.automaton.input_event_type();
        input_type == "*" || event.event_type.as_ref() == input_type
    }

    pub(super) fn input_provenance_filter(&self) -> InputProvenanceFilter {
        self.automaton.input_provenance_filter()
    }

    pub(super) fn input_query_has_lineage(&self) -> Option<bool> {
        self.input_provenance_filter().query_has_lineage()
    }

    pub(super) fn input_query_event_types(&self) -> Result<Vec<EventType>, SinexError> {
        let input_type = self.automaton.input_event_type();
        if input_type == "*" {
            Ok(Vec::new())
        } else {
            Ok(vec![EventType::new(input_type)?])
        }
    }

    pub(super) fn event_matches_input(&self, event: &Event<JsonValue>) -> bool {
        self.input_event_type_matches(event) && self.input_provenance_filter().matches_event(event)
    }

    pub(super) fn filter_matching_events(
        &self,
        events: Vec<Event<JsonValue>>,
    ) -> Vec<Event<JsonValue>> {
        events
            .into_iter()
            .filter(|event| self.event_matches_input(event))
            .collect()
    }
}
