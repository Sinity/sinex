use serde_json::json;
use sinex_test_utils::{sinex_test, Result, TestContext};

#[sinex_test]
async fn test_context_basic_functionality(ctx: TestContext) -> Result<()> {
    assert!(!ctx.test_name().is_empty());
    assert!(ctx.elapsed().as_nanos() > 0);

    let initial_count = ctx.pool.events().count_all().await?;
    assert_eq!(initial_count, 0);

    Ok(())
}

#[sinex_test]
async fn test_contextual_assertions(ctx: TestContext) -> Result<()> {
    ctx.assert("equality test").eq(&42, &42)?;
    ctx.assert("condition test").that(true, "should be true")?;

    let vec = vec![1, 2, 3];
    ctx.assert("size test").has_size(&vec, 3)?;
    ctx.assert("not empty test").not_empty(&vec)?;

    let some_val = Some(42);
    ctx.assert("option test").some(&some_val)?;

    let none_val: Option<i32> = None;
    ctx.assert("none test").none(&none_val)?;

    Ok(())
}

#[sinex_test]
async fn test_assertion_failures(ctx: TestContext) -> Result<()> {
    let result = ctx.assert("fail test").eq(&1, &2);
    assert!(result.is_err());

    let result = ctx.assert("condition fail").that(false, "should fail");
    assert!(result.is_err());

    let empty: Vec<i32> = vec![];
    let result = ctx.assert("empty fail").not_empty(&empty);
    assert!(result.is_err());

    Ok(())
}

#[sinex_test]
async fn test_log_capture(ctx: TestContext) -> Result<()> {
    ctx.capture_log("test log message".to_string());
    ctx.assert_logged("test log")?;

    let result = ctx.assert_logged("non-existent message");
    assert!(result.is_err());

    Ok(())
}
