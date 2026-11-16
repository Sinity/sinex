use super::*;

#[test]
fn small_values_keep_significant_digits() {
    assert_eq!(cell_text(&Cell::Observed { value: 0.0031 }), "0.0031");
    assert_eq!(
        cell_text(&Cell::Recorded { value: -0.00001234 }),
        ">=-0.0000123"
    );
}
