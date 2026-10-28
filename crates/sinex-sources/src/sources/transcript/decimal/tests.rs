use super::*;
#[devkit::test]
fn decimals_compare_before_rounding_and_without_an_exponent_limit() {
    for (left, right) in [
        ("1.0003", "1.0004"),
        ("0", "0.000000001"),
        ("9e99999999999999999999999", "1e100000000000000000000000"),
        ("1e-100000000000000000000000", "9e-99999999999999999999999"),
        ("0.009", "0.01"),
    ] {
        assert_eq!(compare(left, right), Ordering::Less, "{left} < {right}");
        assert_eq!(compare(right, left), Ordering::Greater);
    }
    for (left, right) in [
        ("0.0e99999999999999999999999", "0"),
        ("1.000e2", "100"),
        ("0.001", "1e-3"),
        ("1e+0003", "1000"),
    ] {
        assert_eq!(compare(left, right), Ordering::Equal);
    }
}
