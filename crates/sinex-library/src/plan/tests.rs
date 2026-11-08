use super::*;

fn scale(range: Option<(f64, f64)>, choices: Option<&[&str]>) -> Scale {
    Scale {
        name: "liking".into(),
        question: "How much do you like it?".into(),
        range,
        choices: choices.map(|c| c.iter().map(|s| (*s).to_owned()).collect()),
    }
}

#[devkit::test]
fn ratings_outside_the_scale_are_refused_and_zero_is_a_value() {
    let stars = scale(Some((0.0, 5.0)), None);
    assert!(stars.check(&json!(0)).is_ok());
    assert!(stars.check(&json!(2.5)).is_ok());
    assert!(stars.check(&json!(5)).is_ok());
    for bad in [json!(5.5), json!(-1), json!("4"), Value::Null] {
        assert!(stars.check(&bad).is_err(), "{bad}");
    }
    let mood = scale(None, Some(&["low", "high"]));
    assert!(mood.check(&json!("low")).is_ok());
    assert!(mood.check(&json!("medium")).is_err());
    assert!(mood.check(&json!(1)).is_err());
    assert_eq!(Scale::from_value("liking", &stars.value()), Some(stars));
}

#[devkit::test]
fn a_redefinition_only_widens_a_scale() {
    let stars = scale(Some((0.0, 5.0)), None);
    assert!(scale(Some((0.0, 10.0)), None).accepts_all_of(&stars));
    assert!(!scale(Some((1.0, 5.0)), None).accepts_all_of(&stars));
    assert!(!scale(None, Some(&["a"])).accepts_all_of(&stars));
    let mood = scale(None, Some(&["low", "high"]));
    assert!(scale(None, Some(&["low", "mid", "high"])).accepts_all_of(&mood));
    assert!(!scale(None, Some(&["low"])).accepts_all_of(&mood));
}

#[devkit::test]
fn request_ids_cannot_collide_with_write_ids_or_revisions() {
    assert!(check_request("0192-ab_C").is_ok());
    for bad in ["", "a.1", "a@b", "a b", "ä"] {
        assert!(check_request(bad).is_err(), "{bad}");
    }
    assert!(check_request(&new_request()).is_ok());
}

#[devkit::test]
fn ten_thousand_insertions_between_the_same_neighbours_stay_distinct_and_ordered() {
    let first = (OrderKey::parse("V").unwrap(), "first".to_owned());
    let last = (OrderKey::parse("W").unwrap(), "last".to_owned());
    // Each new entry goes right after `first`, so it lands between `first`
    // and the entry inserted just before: every key is strictly below the
    // previous one and above `first`, so all are distinct and ordered.
    let mut nearest = last.clone();
    for n in 0..10_000 {
        let entries = [first.clone(), nearest.clone(), last.clone()];
        let window: &[(OrderKey, String)] = if nearest.1 == "last" {
            &entries[..2]
        } else {
            &entries
        };
        let key = Resolver::position_in(0, window, Some("first"), None, None).unwrap();
        assert!(first.0 < key && key < nearest.0, "insertion {n}: {key}");
        nearest = (key, format!("e{n}"));
    }
    // And on the other side: always just before `last`.
    let mut nearest = first.clone();
    for n in 0..10_000 {
        let entries = [first.clone(), nearest.clone(), last.clone()];
        let window: &[(OrderKey, String)] = if nearest.1 == "first" {
            &entries[1..]
        } else {
            &entries
        };
        let key = Resolver::position_in(0, window, None, Some("last"), None).unwrap();
        assert!(nearest.0 < key && key < last.0, "insertion {n}: {key}");
        nearest = (key, format!("f{n}"));
    }
}

#[devkit::test]
fn a_moved_entry_is_placed_among_the_others_only() {
    let entries: Vec<(OrderKey, String)> = OrderKey::evenly(3)
        .into_iter()
        .zip(["a", "b", "c"])
        .map(|(key, root)| (key, root.to_owned()))
        .collect();
    // Moving `a` after `c` puts it last; naming itself is refused.
    let key = Resolver::position_in(0, &entries, Some("c"), None, Some("a")).unwrap();
    assert!(key > entries[2].0);
    assert!(Resolver::position_in(0, &entries, Some("a"), None, Some("a")).is_err());
    assert!(Resolver::position_in(0, &entries, Some("a"), Some("b"), None).is_err());
}

#[devkit::test]
fn any_label_with_text_is_accepted() {
    for text in ["-", "!x", "-cat", "?", "a b"] {
        assert_eq!(label(0, text).unwrap(), text);
    }
    assert_eq!(label(0, "  padded ").unwrap(), "padded");
    assert!(label(0, "   ").is_err());
}

#[devkit::test]
fn an_implication_that_closes_a_cycle_is_refused() {
    let edges = vec![
        ("a".to_owned(), "b".to_owned()),
        ("b".to_owned(), "c".to_owned()),
    ];
    assert!(closes_cycle(&edges, "c", "a"));
    assert!(closes_cycle(&edges, "b", "a"));
    assert!(closes_cycle(&edges, "a", "a"));
    assert!(!closes_cycle(&edges, "a", "c"));
    assert!(!closes_cycle(&edges, "d", "a"));
}

#[devkit::test]
fn a_declared_property_accepts_only_its_type() {
    let number = json!({"value_type": "number"});
    assert!(check_property("p", Some(&number), &json!(3)).is_ok());
    assert!(check_property("p", Some(&number), &json!("3")).is_err());
    let choice = json!({"value_type": "choice", "choices": ["a", "b"]});
    assert!(check_property("p", Some(&choice), &json!("a")).is_ok());
    assert!(check_property("p", Some(&choice), &json!("c")).is_err());
    assert!(check_property("p", None, &json!(true)).is_ok());
}
