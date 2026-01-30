use sinex_primitives::non_empty::NonEmptyVec;
use xtask::sandbox::sinex_test;

#[sinex_test]
fn non_empty_vec_construction_variants_work() -> TestResult<()> {
    let nev = NonEmptyVec::single(1);
    assert_eq!(nev.len(), 1);
    assert_eq!(*nev.first(), 1);

    let nev = NonEmptyVec::from_head_tail(1, vec![2, 3]);
    assert_eq!(nev.len(), 3);
    assert_eq!(*nev.first(), 1);
    assert_eq!(*nev.last(), 3);
    Ok(())
}

#[sinex_test]
fn non_empty_vec_from_vec_enforces_constraint() -> TestResult<()> {
    assert!(NonEmptyVec::<i32>::from_vec(vec![]).is_none());

    let nev = NonEmptyVec::from_vec(vec![1, 2, 3]).unwrap();
    assert_eq!(nev.len(), 3);
    Ok(())
}

#[sinex_test]
fn non_empty_vec_serializes_and_deserializes() -> TestResult<()> {
    let nev = NonEmptyVec::from_head_tail(1, vec![2, 3]);
    let json = serde_json::to_string(&nev)?;
    assert_eq!(json, "[1,2,3]");

    let nev2: NonEmptyVec<i32> = serde_json::from_str(&json)?;
    assert_eq!(nev, nev2);

    let result: Result<NonEmptyVec<i32>, _> = serde_json::from_str("[]");
    assert!(result.is_err());
    Ok(())
}
