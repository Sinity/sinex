use super::*;

fn decision(root: &str, rev: &str, author: &str, decision: Option<Decision>) -> Duplicate {
    Duplicate {
        root: root.into(),
        rev: rev.into(),
        author: author.into(),
        via: "api".into(),
        at: 0,
        revises: vec![],
        withdraws: vec![],
        pair: vec!["a:1:0".into(), "b:2:0".into()],
        decision,
        pair_at: None,
        family: Some("command".into()),
        family_key: None,
    }
}

fn current(records: &[Duplicate]) -> Vec<String> {
    let mut history = authored::History::new();
    for record in records {
        history.push(record.header().unwrap(), record.slots());
    }
    history
        .current()
        .unwrap()
        .into_iter()
        .map(|r| r.to_string())
        .collect()
}

#[devkit::test]
fn a_revision_supersedes_and_a_withdrawal_takes_it_back_by_the_descendant_rule() {
    let first = decision("r1", "r1", "operator", Some(Decision::Same));
    let mut second = decision("r1", "r2", "operator", Some(Decision::Distinct));
    second.revises = vec!["r1@r1".into()];
    assert_eq!(current(&[first.clone(), second.clone()]), ["r1@r2"]);
    // Withdrawing the latest revision makes the earlier one current again.
    let mut back = decision("w1", "w1", "operator", None);
    back.withdraws = vec!["r1@r2".into()];
    let mut now = current(&[first.clone(), second.clone(), back.clone()]);
    now.retain(|r| r.starts_with("r1"));
    assert_eq!(now, ["r1@r1"]);
    // In r1 <- r2 <- r3, withdrawing the middle revision leaves r3 current
    // and r1 not: a withdrawn revision is transparent.
    let mut third = decision("r1", "r3", "operator", Some(Decision::Same));
    third.revises = vec!["r1@r2".into()];
    let mut middle = decision("w3", "w3", "operator", None);
    middle.withdraws = vec!["r1@r2".into()];
    let mut now = current(&[first.clone(), second.clone(), third, middle]);
    now.retain(|r| r.starts_with("r1"));
    assert_eq!(now, ["r1@r3"]);
    // Withdrawing the root takes the whole decision back.
    let mut all = decision("w2", "w2", "operator", None);
    all.withdraws = vec!["r1".into()];
    let mut none = current(&[first, second, all]);
    none.retain(|r| r.starts_with("r1"));
    assert!(none.is_empty());
}

#[devkit::test]
fn one_slot_per_unordered_pair_and_author() {
    let one = decision("r1", "r1", "operator", Some(Decision::Same));
    let header = one.header().unwrap();
    assert_eq!(header.at, Some(0));
    assert_eq!(serde_json::to_value(&header).unwrap()["at"], 0);
    let mut swapped = one.clone();
    swapped.pair.reverse();
    assert_eq!(one.slots(), swapped.slots());
    let other = decision("r2", "r2", "agent", Some(Decision::Same));
    assert_ne!(one.slots(), other.slots());
}
