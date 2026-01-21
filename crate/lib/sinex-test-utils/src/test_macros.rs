//! Test macros for snapshot testing and assertions
//!
//! # Snapshot Macros
//!
//! - `snapshot!` - Unified macro for creating snapshots
//! - `assert_event_snapshot!` - Snapshot an event with automatic redactions
//! - `assert_payload_snapshot!` - Snapshot just the payload
//! - `assert_debug_event_snapshot!` - Debug snapshot (includes all fields)

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
        #[sinex_test]
        async fn $name($($arg: $arg_ty),*) -> Result<()> {
            let ctx = TestContext::new().await?;
            $body
        }
    };
}

/// Define a pipeline test that provisions a shared-NATS PipelineScope automatically.
#[macro_export]
macro_rules! sinex_pipeline_test {
    (
        $(#[$meta:meta])*
        async fn $name:ident($scope:ident : $scope_ty:ty $(,)?) -> $ret:ty $body:block
    ) => {
        $(#[$meta])*
        #[sinex_test]
        async fn $name(ctx: $crate::TestContext) -> $ret {
            let ctx = ctx.with_nats().shared().await?;
            let $scope = ctx.pipeline_scope().await?;
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

// Snapshot macro for events with automatic redactions
#[macro_export]
macro_rules! assert_event_snapshot {
    ($event:expr) => {{
        insta::assert_json_snapshot!($event, {
            ".id" => "[event-id]",
            ".ts_ingest" => "[timestamp]",
            ".host" => "[hostname]",
        })
    }};
    ($event:expr, $name:expr) => {{
        insta::assert_json_snapshot!($name, $event, {
            ".id" => "[event-id]",
            ".ts_ingest" => "[timestamp]",
            ".host" => "[hostname]",
        })
    }};
}

// Snapshot macro for event payloads only
#[macro_export]
macro_rules! assert_payload_snapshot {
    ($event:expr) => {{
        insta::assert_json_snapshot!(&$event.payload)
    }};
    ($event:expr, $name:expr) => {{
        insta::assert_json_snapshot!($name, &$event.payload)
    }};
}

// Debug snapshot macro for events (includes all fields)
#[macro_export]
macro_rules! assert_debug_event_snapshot {
    ($event:expr) => {{
        insta::assert_debug_snapshot!($event)
    }};
    ($event:expr, $name:expr) => {{
        insta::assert_debug_snapshot!($name, $event)
    }};
}

/// Unified snapshot macro that auto-detects type
#[macro_export]
macro_rules! snapshot {
    // Event with name
    ($event:expr, $name:expr) => {{
        $crate::assert_event_snapshot!($event, $name)
    }};
    // Event without name
    ($event:expr) => {{
        $crate::assert_event_snapshot!($event)
    }};
}
