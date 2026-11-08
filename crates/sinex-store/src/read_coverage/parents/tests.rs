use super::*;
#[devkit::test]
fn same_frame_ancestry_requires_certified_roots_and_dependency_order() {
    let (root, first, second, foreign) = (
        uuid::Uuid::now_v7(),
        uuid::Uuid::now_v7(),
        uuid::Uuid::now_v7(),
        uuid::Uuid::now_v7(),
    );
    let referenced = BTreeSet::from([first, second]);
    assert!(
        dag(
            [(first, vec![root]), (second, vec![first])],
            &mut BTreeSet::from([root]),
            &referenced
        )
        .is_ok()
    );
    assert!(
        dag(
            [(second, vec![first]), (first, vec![root])],
            &mut BTreeSet::from([root]),
            &referenced
        )
        .is_err()
    );
    assert!(
        dag(
            [(first, vec![second]), (second, vec![first])],
            &mut BTreeSet::from([root]),
            &referenced
        )
        .is_err()
    );
    assert!(
        dag(
            [(first, vec![foreign]), (second, vec![first])],
            &mut BTreeSet::from([root]),
            &referenced
        )
        .is_err()
    );
}
