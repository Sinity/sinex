use super::*;
use crate::events::EventPayload;
use xtask::sandbox::prelude::*;

#[sinex_test]
async fn declares_source_and_event_type() -> TestResult<()> {
    assert_eq!(
        SleepSessionPayload::SOURCE.as_static_str(),
        "samsung-health"
    );
    assert_eq!(
        SleepSessionPayload::EVENT_TYPE.as_static_str(),
        "sleep.session"
    );
    assert_eq!(
        HealthSubstanceIntakeRecordedPayload::EVENT_TYPE.as_static_str(),
        "health.substance.intake_recorded"
    );
    assert_eq!(
        HealthEffectObservationRecordedPayload::EVENT_TYPE.as_static_str(),
        "health.effect.observed"
    );
    Ok(())
}
