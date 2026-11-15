use super::*;
use serde_json::json;
use sqlx::Connection;

fn day() -> DaySettingWire {
    serde_json::from_value(json!({"zone":"UTC","start":"04:00:00"})).unwrap()
}
fn config() -> crate::signals::Config {
    serde_json::from_value(json!({"interrupt_budget_per_hour":2,"quiet_hours":{"from":1320,"to":480},"notifications":null,"outlet_timeout_ms":1000})).unwrap()
}
#[test]
fn identity_tracks_day_and_public_policy_without_private_outlets() {
    let day = day();
    let mut config = config();
    let baseline = settings_identity(&day, "host-a", &config).unwrap();
    config.outlet_timeout_ms = 2000;
    assert_eq!(
        baseline,
        settings_identity(&day, "host-a", &config).unwrap()
    );
    config.interrupt_budget_per_hour = 3;
    assert_ne!(
        baseline,
        settings_identity(&day, "host-a", &config).unwrap()
    );
    let mut other_day = day.clone();
    other_day.start = jiff::civil::Time::constant(5, 0, 0, 0);
    assert_ne!(
        settings_identity(&day, "host-a", &config).unwrap(),
        settings_identity(&other_day, "host-a", &config).unwrap()
    );
    assert_ne!(
        settings_identity(&day, "host-a", &config).unwrap(),
        settings_identity(&day, "host-b", &config).unwrap()
    );
}
#[devkit::test]
async fn unconfigured_and_uninitialized_context_stay_explicit(db: devkit::pg::TestDatabase) {
    let mut conn = sqlx::PgConnection::connect(&db.url(None)).await.unwrap();
    let at = Timestamp::from_unix_secs(1800000000).unwrap();
    let provider = Provider::new(day(), "host-a".into(), None).unwrap();
    assert!(matches!(
        provider.snapshot(&mut conn, at).await.unwrap(),
        PublicContext::Unavailable { .. }
    ));
    let provider = Provider::new(day(), "host-a".into(), Some(&config())).unwrap();
    assert!(matches!(
        provider.snapshot(&mut conn, at).await.unwrap(),
        PublicContext::Pending { .. }
    ));
}
