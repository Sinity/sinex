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
fn malformed_receipt_requires_an_invalid_name_and_exact_transmitted_envelope() {
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
        let bytes = serde_json::to_vec(&frame).unwrap();
        let malformed = format!("malformed/{}", blake3::hash(&bytes).to_hex());
        let mut saved = receipt(&frame);
        saved.frame = malformed.clone();
        saved.refusal = Some("invalid frame name".into());
        saved.outcome["refusal_kind"] = serde_json::json!("malformed");
        let check = |receipt| {
            check_receipt_for(
                &frame,
                receipt,
                content_hash(&frame).unwrap(),
                Some(&malformed),
            )
        };
        assert_eq!(check(saved.clone()).unwrap(), saved);
        // The public recovery helper has no transmitted-envelope evidence.
        assert!(check_receipt(&frame, saved.clone()).is_err());
        let other_envelope = format!(
            "malformed/{}",
            blake3::hash(b"different transmitted bytes").to_hex()
        );
        assert!(
            check_receipt_for(
                &frame,
                saved.clone(),
                content_hash(&frame).unwrap(),
                Some(&other_envelope)
            )
            .is_err()
        );
        let mut foreign = saved.clone();
        foreign.frame.push('x');
        assert!(check(foreign).is_err());
        let mut wrong_body = saved.clone();
        wrong_body.outcome["frame_content_hash"] = serde_json::json!("blake3:different");
        assert!(check(wrong_body).is_err());
        let mut not_refused = saved.clone();
        not_refused.refusal = None;
        assert!(check(not_refused).is_err());
        let mut undurable = saved;
        undurable.number = None;
        assert!(check(undurable).is_err());
    }
    let valid = Ingress::RegisterKinds {
        frame: "valid-offered-name".into(),
        declarations: vec![],
    };
    let malformed = format!(
        "malformed/{}",
        blake3::hash(&serde_json::to_vec(&valid).unwrap()).to_hex()
    );
    let mut saved = receipt(&valid);
    saved.frame = malformed.clone();
    saved.refusal = Some("invalid content".into());
    saved.outcome["refusal_kind"] = serde_json::json!("malformed");
    assert!(
        check_receipt_for(
            &valid,
            saved,
            content_hash(&valid).unwrap(),
            Some(&malformed)
        )
        .is_err()
    );
}
