use super::*;

fn balanced() -> Accounting {
    Accounting {
        framed: 11,
        removed: 1,
        read: 8,
        headers: 1,
        failures: 2,
        readings: 9,
        new: 5,
        same: 2,
        revised: 1,
        older: 1,
        superseded: 0,
        first_failure: Some("not a row".into()),
    }
}

/// law: sx.acct.identity
#[devkit::test]
fn acct_identity_holds_and_names_each_side_that_does_not() {
    assert_eq!(balanced().balanced(), Ok(()));

    let mut lost_record = balanced();
    lost_record.read -= 1;
    let error = lost_record.balanced().unwrap_err();
    assert!(error.contains("11 records framed"), "{error}");
    assert!(!error.contains("readings offered"), "{error}");

    let mut lost_reading = balanced();
    lost_reading.same -= 1;
    let error = lost_reading.balanced().unwrap_err();
    assert!(error.contains("9 readings offered"), "{error}");
    assert!(!error.contains("records framed"), "{error}");

    // Removed records archive readings; they are in no new material.
    let mut removed = balanced();
    removed.removed += 4;
    assert_eq!(removed.balanced(), Ok(()));
}

#[devkit::test]
fn adding_keeps_the_first_failure_and_the_balance() {
    let mut total = Accounting::default();
    total.add(&balanced());
    let mut second = balanced();
    second.first_failure = Some("a later failure".into());
    total.add(&second);
    assert_eq!(total.framed, 22);
    assert_eq!(total.new, 10);
    assert_eq!(total.first_failure.as_deref(), Some("not a row"));
    assert_eq!(total.balanced(), Ok(()));
}
