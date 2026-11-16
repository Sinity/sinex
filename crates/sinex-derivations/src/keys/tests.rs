use crate::{AnyKeyed, Keyed};
use serde_json::{Value, json};

fn round_trip<K: Keyed>(literal: Value) {
    let key: K::Key = serde_json::from_value(literal.clone()).unwrap();
    let encoded = serde_json::to_value(key).unwrap();
    assert_eq!(encoded, literal, "{} key", K::NAME);
    assert_eq!(
        canonical_json::canonical_json(&encoded).unwrap(),
        canonical_json::canonical_json(&literal).unwrap(),
        "{} key bytes",
        K::NAME
    );
}

#[devkit::test]
fn stored_keys_round_trip() {
    let hour = json!({"hour":"2024-06-15T01:00:00Z"});
    let day = json!({"day":"2024-06-15"});
    let metric = json!({"metric":"shell.commands","day":"2024-06-15"});
    let scope = json!({"day":"2024-06-15","scope":"seat"});
    round_trip::<crate::library::LibraryItems>(json!("item"));
    round_trip::<crate::session::ShellSessions>(json!({"host":"box","day":"2024-06-15"}));
    round_trip::<crate::activity::InputCoverage>(json!({"session":"keyboard","scope":"seat"}));
    round_trip::<crate::portfolio::input::InputWorkloads>(scope.clone());
    round_trip::<crate::portfolio::rhythm::RhythmDeviations>(scope);
    round_trip::<crate::work::WorkActs>(json!({"session":"s"}));
    round_trip::<crate::activity::ActivitySpans>(hour.clone());
    round_trip::<crate::attribution::ProjectSpans>(hour.clone());
    round_trip::<crate::activity::ActivityWindows>(hour.clone());
    round_trip::<crate::activity::ActivitySessions>(
        json!({"at":"2024-06-15T01:00:00Z","scope":"seat"}),
    );
    round_trip::<crate::conversation::ConversationDays>(day.clone());
    round_trip::<crate::state_interval::StateIntervals>(
        json!({"state":"desktop.focus","day":"2024-06-15"}),
    );
    round_trip::<crate::state_interval::StateIntervals>(
        json!({"state":"machine.unit","day":"2024-06-15","host":"box","unknown_record":null}),
    );
    round_trip::<crate::state_interval::StateIntervals>(
        json!({"state":"machine.unit","day":"2024-06-15","host":null,"unknown_record":"record"}),
    );
    round_trip::<crate::figures::HourlyMetrics>(metric.clone());
    round_trip::<crate::figures::DailyMetrics>(metric);
    round_trip::<crate::presence::Presence>(hour.clone());
    round_trip::<crate::work::WorkEpisodes>(json!({"episode":"e"}));
    round_trip::<crate::work::ContextPacks>(json!({"episode":"e"}));
    round_trip::<crate::work::ActivityDays>(json!({"start":"2024-06-15T01:00:00Z"}));
    round_trip::<crate::health::HourlyFigures>(hour);
    round_trip::<crate::health::DailyFigures>(day.clone());
    round_trip::<crate::calendar::Instances>(json!({"calendar":"calendar","uid":"event"}));
    round_trip::<crate::portfolio::marks::MarkLinks>(day);
    for family in crate::fusion::families() {
        let literal = if family.keyed_by.is_empty() {
            json!({"hour":477313})
        } else {
            Value::Object(
                family
                    .keyed_by
                    .iter()
                    .map(|field| ((*field).to_owned(), json!(format!("sample-{field}"))))
                    .collect(),
            )
        };
        let key: crate::fusion::FusionKey = serde_json::from_value(literal.clone()).unwrap();
        let encoded = serde_json::to_value(key).unwrap();
        assert_eq!(encoded, literal, "{} key", family.derivation);
        assert_eq!(
            canonical_json::canonical_json(&encoded).unwrap(),
            canonical_json::canonical_json(&literal).unwrap()
        );
    }
}

#[devkit::test]
fn an_undecodable_key_reports_the_stored_key_error() {
    let env = crate::Env::at(
        sinex_model::Timestamp::from_unix_secs(1_718_452_800).unwrap(),
        sinex_model::time_settings::TimeSettings::new("UTC", 0, 0).unwrap(),
    );
    let derivation: &dyn AnyKeyed = &crate::session::ShellSessions;
    let key = json!({"host":"box","day":"not-a-day"});
    assert!(derivation.inputs(&key, &env).is_none());
    assert_eq!(derivation.key_state(&key, &env), crate::KeyState::Closed);
    let result = derivation.compute(&key, &[], &env);
    assert!(matches!(
        result.error,
        Some(sinex_model::BuildError::Key(_))
    ));
    assert!(result.outputs.is_empty());
    assert!(!result.waiting);
}
