use super::*;
use sinex_model::event_types::embedding::EmbeddingSpace;
use sinex_model::{Class, EventId, EventPayload, ReadingOrder};
fn supplied(vector: Vec<f64>) -> Event {
    let target = EventId::new();
    Event::build(
        &SuppliedEmbedding {
            target,
            input_record: "synthetic:document".into(),
            input_hash: blake3::hash(b"text").to_hex().to_string(),
            space: EmbeddingSpace {
                provider: "supplied".into(),
                model: "neutral".into(),
                revision: "1".into(),
                settings_hash: blake3::hash(b"settings").to_hex().to_string(),
                dimensions: vector.len() as u64,
            },
            vector,
            embedded_at: None,
        },
        "fixture.vectors",
        Origin::Derived {
            parents: vec![target],
        },
        ReadingOrder(1),
        [1; 32],
        Class::Guarded,
    )
    .unwrap()
}
fn env() -> Env {
    Env::at(
        sinex_model::Timestamp::from_unix_secs(0).unwrap(),
        sinex_model::time_settings::TimeSettings::new("UTC", 0, 0).unwrap(),
    )
}
#[devkit::test]
fn supplied_vectors_keep_original_precision_dimensions_parents_and_unknown_time() {
    let vector = vec![f64::MAX, 1.0e-250, 0.0];
    let event = supplied(vector.clone());
    let outputs = Embeddings
        .derive(
            Input {
                event: &event,
                bytes: None,
            },
            &env(),
        )
        .unwrap();
    let stored = outputs[0].payload_as::<TextEmbedding>().unwrap();
    assert_eq!(stored.vector, vector);
    assert_eq!(stored.space.dimensions, 3);
    assert_eq!(
        outputs[0].origin,
        Origin::Derived {
            parents: vec![event.id]
        }
    );
    assert_eq!(outputs[0].class, Class::Guarded);
    assert!(outputs[0].ts_orig.at.is_none());
    let large = supplied(vec![0.25; 16_001]);
    assert_eq!(
        Embeddings
            .derive(
                Input {
                    event: &large,
                    bytes: None
                },
                &env()
            )
            .unwrap()[0]
            .payload_as::<TextEmbedding>()
            .unwrap()
            .vector
            .len(),
        16_001
    );
}
#[devkit::test]
fn vector_validation_checks_supplied_identity_and_count_without_calling_a_provider() {
    let mut event = supplied(vec![1.0, 0.0]);
    event.payload["space"]["dimensions"] = 2.into();
    assert!(
        Embeddings
            .derive(
                Input {
                    event: &event,
                    bytes: None
                },
                &env()
            )
            .is_ok()
    );
    event.payload["space"]["dimensions"] = 3.into();
    assert!(
        Embeddings
            .derive(
                Input {
                    event: &event,
                    bytes: None
                },
                &env()
            )
            .unwrap_err()
            .contains("dimension")
    );
    event.payload["space"]["dimensions"] = 2.into();
    event.payload["input_hash"] = "unverified".into();
    assert!(
        Embeddings
            .derive(
                Input {
                    event: &event,
                    bytes: None
                },
                &env()
            )
            .unwrap_err()
            .contains("BLAKE3")
    );
    assert_eq!(SuppliedEmbedding::VERSION, 2);
}
