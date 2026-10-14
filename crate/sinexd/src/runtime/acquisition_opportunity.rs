//! Process-local source acquisition uncertainty observed by source runtimes.

use sinex_primitives::sources::opportunity::AcquisitionOpportunity;
use std::collections::HashMap;
use std::sync::{Arc, OnceLock};

type Store = Arc<parking_lot::RwLock<HashMap<String, AcquisitionOpportunity>>>;

static STORE: OnceLock<Store> = OnceLock::new();

fn store() -> &'static Store {
    STORE.get_or_init(|| Arc::new(parking_lot::RwLock::new(HashMap::new())))
}

/// Retain the latest bounded acquisition report for its logical source.
pub fn report(opportunity: AcquisitionOpportunity) {
    store()
        .write()
        .insert(opportunity.source_id.clone(), opportunity);
}

/// Remove synthetic fixture state; production drains cannot prove that a
/// sibling binding or query scope was reconciled.
#[cfg(test)]
pub(crate) fn clear_test_observation(source_id: &str) {
    store().write().remove(source_id);
}

/// Return the latest acquisition report for a logical source, if any.
#[must_use]
pub fn latest(source_id: &str) -> Option<AcquisitionOpportunity> {
    store().read().get(source_id).cloned()
}

#[cfg(test)]
mod tests {
    use super::*;
    use sinex_primitives::sources::opportunity::OpportunityCompleteness;

    #[test]
    fn latest_report_is_source_scoped_and_replaced() {
        report(AcquisitionOpportunity {
            source_id: "test.source".into(),
            completeness: OpportunityCompleteness::Unknown,
            reason: "synthetic query failure".into(),
            affected_paths: Vec::new(),
            observed_at_unix_nanos: None,
            cursor_before: Some("before".into()),
            cursor_after: None,
        });
        let current = latest("test.source").expect("report retained");
        assert_eq!(current.completeness, OpportunityCompleteness::Unknown);
        assert_eq!(current.reason, "synthetic query failure");
        assert!(current.cursor_after.is_none());
        clear_test_observation("test.source");
        assert!(latest("test.source").is_none());
    }
}
