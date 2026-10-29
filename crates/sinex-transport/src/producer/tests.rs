use super::*;

fn receipt(frame: &Ingress) -> Acknowledgment {
    Acknowledgment {
        number: Some(1),
        frame: frame.frame().into(),
        material: None,
        material_extents: vec![],
        admitted: vec![],
        outcome: serde_json::json!({"frame_content_hash":content_hash(frame).unwrap()}),
        refusal: None,
        commit_seq: Some(1),
    }
}

#[devkit::test]
fn conflict_keeps_the_prior_owned_receipt_distinct_from_the_offered_body() {
    let prior = Ingress::RegisterKinds {
        frame: "same-frame".into(),
        declarations: vec![],
    };
    let offered = Ingress::Events {
        frame: "same-frame".into(),
        source: None,
        coverage: None,
        events: vec![],
        failures: vec![],
    };
    let saved = receipt(&prior);
    assert_eq!(check_receipt(&prior, saved.clone()).unwrap(), saved);
    let SubmitError::ReceiptConflict {
        frame,
        offered_hash,
        receipt: actual,
    } = check_receipt(&offered, saved.clone()).unwrap_err()
    else {
        panic!("typed conflict evidence")
    };
    assert_eq!(frame, offered.frame());
    assert_eq!(offered_hash, content_hash(&offered).unwrap());
    assert_eq!(*actual, saved);
    assert_ne!(actual.content_hash(), Some(offered_hash.as_str()));
}

#[devkit::test]
fn foreign_frame_or_missing_durable_identity_never_exposes_a_prior_receipt_as_conflict_evidence() {
    let offered = Ingress::RegisterKinds {
        frame: "owned-frame".into(),
        declarations: vec![],
    };
    let mut foreign = receipt(&offered);
    foreign.frame = "foreign-frame".into();
    let error = check_receipt(&offered, foreign).unwrap_err();
    assert!(matches!(
        error,
        SubmitError::Refused {
            kind: sinex_model::FrameRefusal::Conflict,
            ..
        }
    ));
    assert!(!error.to_string().contains("foreign-frame"));
    let mut unknown = receipt(&offered);
    unknown.number = None;
    unknown.outcome = serde_json::json!({});
    assert!(matches!(
        check_receipt(&offered, unknown),
        Err(SubmitError::NoReceipt(_))
    ));
}

#[devkit::test]
fn malformed_receipt_binds_the_exact_offered_envelope_and_logical_body() {
    for frame in [
        Ingress::RegisterKinds {
            frame: "".into(),
            declarations: vec![],
        },
        Ingress::LargeFrame {
            frame: "broken\0name".into(),
            blob: "blake3:logical-body".into(),
            length: 42,
        },
    ] {
        let mut saved = receipt(&frame);
        saved.frame = format!(
            "malformed/{}",
            blake3::hash(&serde_json::to_vec(&frame).unwrap()).to_hex()
        );
        saved.refusal = Some("invalid frame name".into());
        saved.outcome["refusal_kind"] = serde_json::json!("malformed");
        assert_eq!(check_receipt(&frame, saved.clone()).unwrap(), saved);
        let mut foreign = saved.clone();
        foreign.frame.push('x');
        assert!(check_receipt(&frame, foreign).is_err());
        let mut wrong_body = saved.clone();
        wrong_body.outcome["frame_content_hash"] = serde_json::json!("blake3:different");
        assert!(check_receipt(&frame, wrong_body).is_err());
        let mut not_refused = saved.clone();
        not_refused.refusal = None;
        assert!(check_receipt(&frame, not_refused).is_err());
        let mut undurable = saved;
        undurable.number = None;
        assert!(check_receipt(&frame, undurable).is_err());
    }
}
