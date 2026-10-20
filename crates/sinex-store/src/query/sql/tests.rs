use super::*;

#[test]
fn placeholders_number_in_text_order_across_nesting() {
    let mut inner = Frag::text("x = ");
    inner.param(1i64);
    let mut outer = Frag::text("SELECT ");
    outer
        .param("a")
        .push(" WHERE ")
        .append(inner.parenthesized());
    let (sql, values) = outer.render();
    assert_eq!(sql, "SELECT $1 WHERE (x = $2)");
    assert_eq!(values.len(), 2);
}
