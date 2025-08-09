// Test Macros - TestContext Integration Helpers

// Re-export rstest macros for convenience

// Helper macro to create rstest case with TestContext
// This allows using rstest with our async TestContext pattern
#[macro_export]
macro_rules! rstest_async {
    (
        #[case($($param:ident = $value:expr),*)]$( #[case($($more_param:ident = $more_value:expr),*)] )*
        async fn $name:ident(ctx: TestContext $(, $arg:ident: $arg_ty:ty)*) -> Result<()> $body:block
    ) => {
        #[rstest]
        #[case($($param = $value),*)]
        $(#[case($($more_param = $more_value),*)])*
        #[tokio::test]
        async fn $name($($arg: $arg_ty),*) -> Result<()> {
            let ctx = TestContext::new().await?;
            $body
        }
    };
}

// Helper for snapshot testing with automatic test name detection
#[macro_export]
macro_rules! assert_snapshot_named {
    ($ctx:expr, $value:expr) => {{
        let test_name = $ctx.test_name();
        let mut settings = insta::Settings::new();
        settings.set_snapshot_path(format!("test/snapshots/{}", test_name));
        settings.bind(|| {
            insta::assert_yaml_snapshot!($value);
        });
    }};
    ($ctx:expr, $name:expr, $value:expr) => {{
        let test_name = $ctx.test_name();
        let mut settings = insta::Settings::new();
        settings.set_snapshot_path(format!("test/snapshots/{}", test_name));
        settings.set_snapshot_suffix($name);
        settings.bind(|| {
            insta::assert_yaml_snapshot!($value);
        });
    }};
}

// Helper for debug snapshot testing (includes debug representation)
#[macro_export]
macro_rules! assert_debug_snapshot {
    ($ctx:expr, $value:expr) => {{
        let test_name = $ctx.test_name();
        let mut settings = insta::Settings::new();
        settings.set_snapshot_path(format!("test/snapshots/{}", test_name));
        settings.bind(|| {
            insta::assert_debug_snapshot!($value);
        });
    }};
}
