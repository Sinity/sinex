// Test Macros - Only for test generation patterns
//
// Most macros have been converted to methods on TestContext.
// We only keep the parameterized macro for data-driven tests.
// For property testing, use standard proptest directly.

// Property testing with sinex_test:
//
// 1. For pure functions (no database): Use standard proptest within #[sinex_test]
//    - Fast: thousands of iterations
//    - Example: testing builders, validators, parsers
//
// 2. For database operations: Use parameterized! macro
//    - Reasonable number of test cases (5-50)
//    - Each case shares the same TestContext
//    - Example: testing different event types, edge cases
//
// Why? Creating a new database connection per property test iteration
// would be prohibitively slow (100s of ms per iteration).

/// Parameterized test macro for running the same test with different inputs
///
/// # Example
/// ```rust
/// #[sinex_test]
/// async fn test_file_operations(ctx: TestContext) -> Result<()> {
///     parameterized!([
///         ("empty", ""),
///         ("normal", "/home/user/file.txt"),
///         ("spaces", "/my documents/file.txt"),
///     ], |(name, path)| {
///         // ctx is available here
///         let event = ctx.event()
///             .filesystem()
///             .created(path)
///             .insert()
///             .await?;
///         assert_eq!(event.payload["path"], path);
///     });
///     Ok(())
/// }
/// ```
// Macro for parameterized tests
#[macro_export]
macro_rules! parameterized {
    ([$($case:expr),* $(,)?], |$param:pat_param| $body:block) => {{
        let cases = vec![$($case),*];

        for (case_idx, $param) in cases.into_iter().enumerate() {
            // Build descriptive name from the case
            let case_name = format!("Case {}", case_idx);
            eprintln!("Running parameterized test: {}", case_name);

            // ctx available from outer scope
            let result: $crate::Result<()> = async { $body }.await;

            // Add context to any errors
            result.map_err(|e| {
                ::sinex_error::SinexError::unknown(format!(
                    "{}: {}", case_name, e
                ))
            })?;
        }
    }};
}
