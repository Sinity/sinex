use super::*;

#[devkit::test]
fn placeholders_number_in_text_order_across_nesting() {
    let mut inner = SqlFragment::text("x = ");
    inner.param(1i64);
    let mut outer = SqlFragment::text("SELECT ");
    outer
        .param("a")
        .push(" WHERE ")
        .append(inner.parenthesized());
    let (sql, values) = outer.render();
    assert_eq!(sql, "SELECT $1 WHERE (x = $2)");
    assert_eq!(values, vec![Param::from("a"), Param::from(1i64)]);
}

#[devkit::test]
fn joined_fragments_keep_every_parameter_in_order() {
    let parts = (0..12i64)
        .map(|i| {
            let mut f = SqlFragment::text("c = ");
            f.param(i);
            f
        })
        .collect();
    let (sql, values) = SqlFragment::join(parts, " OR ").parenthesized().render();
    let expected: Vec<String> = (1..=12).map(|n| format!("c = ${n}")).collect();
    assert_eq!(sql, format!("({})", expected.join(" OR ")));
    assert_eq!(values, (0..12i64).map(Param::from).collect::<Vec<_>>());
}
