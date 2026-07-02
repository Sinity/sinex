use super::*;
use crate::events::EventPayload;
use xtask::sandbox::prelude::*;

#[sinex_test]
async fn declares_source_and_event_type() -> TestResult<()> {
    assert_eq!(LedgerTransactionPayload::SOURCE.as_static_str(), "ledger");
    assert_eq!(
        LedgerTransactionPayload::EVENT_TYPE.as_static_str(),
        "transaction.posted"
    );
    Ok(())
}
