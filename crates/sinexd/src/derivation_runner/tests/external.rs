//! Synthetic outside records follow stored producer declarations and normal
//! admission validation. Sinex owns no duplicate Polylogue payload types.
use super::*;

fn declarations() -> Vec<sinex_model::KindDeclaration> {
    [("session", "session", "started"), ("message", "message", "at")]
        .into_iter()
        .map(|(name, key, time)| {
            let mut declaration = sinex_model::KindDeclaration::schemaless(
                sinex_model::KindRef { name: format!("polylogue.{name}"), version: 2 },
                "polylogue".into(),
            );
            declaration.key_rule = Some(sinex_model::kinds::KeyRule::Fields { fields: vec![key.into()] });
            declaration.time_rule = Some(sinex_model::kinds::TimeRule::FirstPresent {
                fields: vec![time.into()], quality: sinex_model::TimeQuality::Intrinsic,
            });
            declaration.replace_rule = Some(sinex_model::ReplaceRule::ReplaceWhenNewer);
            if name == "message" {
                declaration.field_hints = vec![sinex_model::kinds::FieldHint {
                    field: "/text".into(), class: Class::Guarded,
                }];
            }
            declaration
        })
        .collect()
}

pub(super) async fn register(url: &str) {
    let mut conn = PgConnection::connect(url).await.unwrap();
    sinex_schema::kinds::register(&mut conn, "polylogue", &declarations()).await.unwrap();
}

pub(super) fn record(kind: &str, payload: Value, origin: Origin) -> Event {
    assert!(matches!(kind, "polylogue.session" | "polylogue.message"));
    let hash = *blake3::hash(&serde_json::to_vec(&payload).unwrap()).as_bytes();
    Event::submitted(
        sinex_model::KindRef { name: kind.into(), version: 2 },
        payload, "polylogue".into(), origin, ReadingOrder(0), hash, Class::Open,
    )
}
