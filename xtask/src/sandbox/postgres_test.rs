use super::*;
use crate::sandbox::sinex_test;

#[sinex_test]
async fn build_operation_id_stmt_escapes_single_quotes() -> crate::sandbox::TestResult<()> {
    // sinex-3pgp: operation_id used to be interpolated into the ALTER ROLE
    // statement via raw format!(), so a value containing a single quote could
    // break out of the intended string literal and inject arbitrary SQL.
    let malicious = "x'; DROP TABLE core.events; --";
    let stmt = build_operation_id_stmt("sinex_app", malicious)?;

    assert_eq!(
        stmt, "ALTER ROLE sinex_app SET sinex.operation_id = 'x''; DROP TABLE core.events; --';",
        "single quotes in operation_id must be escaped by doubling, not left to break out of the literal"
    );

    // The statement must still be exactly one SET assignment: the escaped
    // value should account for every quote in the input, leaving no
    // unescaped `'` outside the doubled pairs that a SQL parser could treat
    // as the end of the literal.
    let after_literal_start = stmt
        .find("sinex.operation_id = '")
        .map(|i| i + "sinex.operation_id = '".len())
        .expect("statement must contain the SET assignment literal");
    let literal_body = &stmt[after_literal_start..stmt.len() - 2]; // trim trailing "';"
    assert_eq!(
        literal_body.matches('\'').count() % 2,
        0,
        "every quote inside the literal body must be part of a doubled ('') pair: {literal_body}"
    );

    Ok(())
}

#[sinex_test]
async fn build_operation_id_stmt_rejects_invalid_app_user() -> crate::sandbox::TestResult<()> {
    // app_user is still interpolated unquoted and relies on strict identifier
    // validation, not literal-escaping -- a malicious app_user must be
    // rejected outright rather than silently escaped.
    let result = build_operation_id_stmt("sinex_app; DROP TABLE core.events; --", "op-1");
    assert!(
        result.is_err(),
        "an app_user containing SQL metacharacters must be rejected by identifier validation"
    );
    Ok(())
}

#[sinex_test]
async fn build_operation_id_stmt_roundtrips_benign_values() -> crate::sandbox::TestResult<()> {
    let stmt = build_operation_id_stmt("sinex_app", "run-2026-08-11-abc123")?;
    assert_eq!(
        stmt,
        "ALTER ROLE sinex_app SET sinex.operation_id = 'run-2026-08-11-abc123';"
    );
    Ok(())
}
