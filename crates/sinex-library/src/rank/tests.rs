use super::*;

fn j(w: &str, l: &str) -> (String, String) {
    (w.into(), l.into())
}

#[devkit::test]
fn a_consistent_order_is_recovered() {
    let judgements = vec![
        j("a", "b"),
        j("b", "c"),
        j("a", "c"),
        j("c", "d"),
        j("b", "d"),
        j("a", "b"),
    ];
    let r = bradley_terry(&judgements);
    let order: Vec<&str> = r.iter().map(|x| x.item.as_str()).collect();
    assert_eq!(order, ["a", "b", "c", "d"]);
    assert!(r.iter().all(|x| x.strength.is_finite() && x.strength > 0.0));
    assert_eq!(r[0].wins, 3);
    assert_eq!(r[3].losses, 2);
}

#[devkit::test]
fn an_unbeaten_item_stays_finite_and_ties_are_repeatable() {
    let r = bradley_terry(&[j("x", "y")]);
    assert_eq!(r[0].item, "x");
    assert!(r[0].strength.is_finite());
    let tie = bradley_terry(&[j("p", "q"), j("q", "p")]);
    assert_eq!(tie[0].item, "p");
    assert!((tie[0].strength - tie[1].strength).abs() < 1e-9);
    assert!(bradley_terry(&[]).is_empty());
}

#[devkit::test]
fn the_next_pair_is_the_least_compared_neighbours() {
    let judgements = vec![j("a", "b"), j("a", "b"), j("b", "c")];
    let r = bradley_terry(&judgements);
    assert_eq!(next_pair(&r, &judgements), Some(("b".into(), "c".into())));
}
