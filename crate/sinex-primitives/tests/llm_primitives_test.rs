//! Tests for model-effect cache primitives — ReplayPolicy, can_replay,
//! ModelEffectRequest composite_key determinism, and record creation.

use sinex_primitives::llm::{
    ModelEffectRecord, ModelEffectRequest, ReplayPolicy, can_replay, hash_model_input,
};

#[test]
fn composite_key_is_deterministic() {
    let req = ModelEffectRequest {
        provider: "test".into(),
        model: "test-model".into(),
        prompt_hash: "abc123".into(),
        schema_hash: Some("schema_hash".into()),
        input_hash: "input_hash".into(),
    };
    let key1 = req.composite_key();
    let key2 = req.composite_key();
    assert_eq!(key1, key2, "composite key must be deterministic");
    assert!(!key1.is_empty());
}

#[test]
fn composite_key_differs_on_input_hash() {
    let req1 = ModelEffectRequest {
        provider: "test".into(),
        model: "test-model".into(),
        prompt_hash: "abc".into(),
        schema_hash: None,
        input_hash: "input_a".into(),
    };
    let req2 = ModelEffectRequest {
        provider: "test".into(),
        model: "test-model".into(),
        prompt_hash: "abc".into(),
        schema_hash: None,
        input_hash: "input_b".into(),
    };
    assert_ne!(req1.composite_key(), req2.composite_key());
}

#[test]
fn can_replay_reuse_recorded_matching() {
    let req = ModelEffectRequest {
        provider: "p".into(), model: "m".into(),
        prompt_hash: "ph".into(), schema_hash: None, input_hash: "ih".into(),
    };
    let record = ModelEffectRecord::new(
        req.clone(), "output", ReplayPolicy::ReuseRecorded, "test",
    );
    assert!(can_replay(&req, &record, ReplayPolicy::ReuseRecorded));
}

#[test]
fn can_replay_fail_if_missing_with_matching_record() {
    let req = ModelEffectRequest {
        provider: "p".into(), model: "m".into(),
        prompt_hash: "ph".into(), schema_hash: None, input_hash: "ih".into(),
    };
    let record = ModelEffectRecord::new(
        req.clone(), "output", ReplayPolicy::FailIfMissing, "test",
    );
    assert!(can_replay(&req, &record, ReplayPolicy::FailIfMissing));
}

#[test]
fn can_replay_explicit_reevaluate_always_false() {
    let req = ModelEffectRequest {
        provider: "p".into(), model: "m".into(),
        prompt_hash: "ph".into(), schema_hash: None, input_hash: "ih".into(),
    };
    let record = ModelEffectRecord::new(
        req.clone(), "output", ReplayPolicy::ReuseRecorded, "test",
    );
    assert!(!can_replay(&req, &record, ReplayPolicy::ExplicitReevaluate));
}

#[test]
fn can_replay_mismatched_inputs_return_false() {
    let req1 = ModelEffectRequest {
        provider: "p".into(), model: "m".into(),
        prompt_hash: "ph".into(), schema_hash: None, input_hash: "ih1".into(),
    };
    let req2 = ModelEffectRequest {
        provider: "p".into(), model: "m".into(),
        prompt_hash: "ph".into(), schema_hash: None, input_hash: "ih2".into(),
    };
    let record = ModelEffectRecord::new(
        req2, "output", ReplayPolicy::ReuseRecorded, "test",
    );
    assert!(!can_replay(&req1, &record, ReplayPolicy::ReuseRecorded));
}

#[test]
fn hash_model_input_is_deterministic() {
    let h1 = hash_model_input("hello");
    let h2 = hash_model_input("hello");
    assert_eq!(h1, h2);
}

#[test]
fn hash_model_input_differs_on_input() {
    assert_ne!(hash_model_input("a"), hash_model_input("b"));
}

#[test]
fn model_effect_record_output_hash_matches() {
    let req = ModelEffectRequest {
        provider: "p".into(), model: "m".into(),
        prompt_hash: "ph".into(), schema_hash: None, input_hash: "ih".into(),
    };
    let record = ModelEffectRecord::new(req, "test output", ReplayPolicy::ReuseRecorded, "test-node");
    assert!(!record.effect_id.is_empty());
    assert!(!record.output_hash.is_empty());
    assert_eq!(record.recorded_by, "test-node");
    assert_eq!(record.recorded_policy, ReplayPolicy::ReuseRecorded);
}
