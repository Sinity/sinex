use super::{
    SerialScope, classify_attrs, expand_async_context_test, expand_rstest_variant,
    expand_simple_async_test, parse_proptest_attr_args, parse_sinex_test_attrs_tokens,
    serial_guard_tokens,
};
use quote::quote;
use syn::parse::Parser as _;
use syn::{ItemFn, parse2};

fn parse_ok(tokens: proc_macro2::TokenStream) -> super::SinexTestConfig {
    parse_sinex_test_attrs_tokens(tokens).expect("attributes should parse")
}

fn parse_err(tokens: proc_macro2::TokenStream) -> String {
    parse_sinex_test_attrs_tokens(tokens)
        .expect_err("attributes should fail")
        .to_string()
}

#[test]
fn sinex_test_attrs_parse_valid_timeout_and_flags() {
    let config = parse_ok(quote!(
        timeout = 45,
        trace = true,
        serial,
        scope = "workspace"
    ));

    assert_eq!(config.timeout, Some(45));
    assert!(config.trace);
    assert!(matches!(config.serial_scope, SerialScope::Workspace));
}

#[test]
fn sinex_test_attrs_reject_invalid_timeout_literal() {
    let error = parse_err(quote!(timeout = "fast"));
    assert!(error.contains("timeout"));
    assert!(error.contains("integer literal"));
}

#[test]
fn sinex_test_attrs_reject_invalid_trace_literal() {
    let error = parse_err(quote!(trace = "yes"));
    assert!(error.contains("trace"));
    assert!(error.contains("boolean literal"));
}

#[test]
fn sinex_test_attrs_reject_unknown_attribute() {
    let error = parse_err(quote!(timout = 30));
    assert!(error.contains("unknown sinex_test attribute"));
    assert!(error.contains("timout"));
}

#[test]
fn sinex_test_attrs_reject_scenario_metadata() {
    let error = parse_err(quote!(scenario = "runtime.restart"));
    assert!(error.contains("unknown sinex_test attribute"));
    assert!(error.contains("scenario"));
}

fn parse_item_fn(tokens: proc_macro2::TokenStream) -> ItemFn {
    parse2(tokens).expect("test function should parse")
}

fn count_occurrences(haystack: &str, needle: &str) -> usize {
    haystack.match_indices(needle).count()
}

#[test]
fn serial_guard_tokens_propagate_lock_acquisition_failures() {
    let rendered = serial_guard_tokens(SerialScope::Workspace).to_string();
    assert!(rendered.contains("acquire_workspace_test_guard"));
    assert!(
        rendered.contains(". await ?"),
        "rendered tokens: {rendered}"
    );
}

#[test]
fn process_serial_guard_tokens_do_not_require_fallible_lock_acquisition() {
    let rendered = serial_guard_tokens(SerialScope::Process).to_string();
    assert!(rendered.contains("acquire_process_test_guard"));
    assert!(
        !rendered.contains(". await ?"),
        "rendered tokens: {rendered}"
    );
}

#[test]
fn async_context_expansion_keeps_single_serial_guard_inside_timed_future() {
    let input = parse_item_fn(quote! {
        async fn serial_context_test(ctx: ::xtask::sandbox::TestContext) -> ::xtask::sandbox::TestResult<()> {
            let _ = ctx;
            Ok(())
        }
    });

    let rendered = expand_async_context_test(&input, &[], &input.block, 30, SerialScope::Workspace)
        .to_string();
    assert_eq!(
        count_occurrences(&rendered, "acquire_workspace_test_guard"),
        1,
        "rendered tokens: {rendered}"
    );
    assert!(
        rendered.contains("let test_future = async"),
        "rendered tokens: {rendered}"
    );
    assert!(rendered.contains("timeout"), "rendered tokens: {rendered}");
}

#[test]
fn simple_async_expansion_keeps_single_serial_guard_inside_timed_future() {
    let input = parse_item_fn(quote! {
        async fn serial_simple_test() -> ::xtask::sandbox::TestResult<()> {
            Ok(())
        }
    });

    let rendered =
        expand_simple_async_test(&input, &[], &input.block, 30, SerialScope::Workspace).to_string();
    assert_eq!(
        count_occurrences(&rendered, "acquire_workspace_test_guard"),
        1,
        "rendered tokens: {rendered}"
    );
    assert!(
        rendered.contains("let test_future = async"),
        "rendered tokens: {rendered}"
    );
    assert!(rendered.contains("timeout"), "rendered tokens: {rendered}");
}

#[test]
fn malformed_proptest_attribute_arguments_return_compile_error_tokens() {
    for source in ["#[cases]", "#[timeout]", "#[seed]"] {
        let attr = syn::Attribute::parse_outer
            .parse_str(source)
            .expect("attribute itself is valid syntax")
            .into_iter()
            .next()
            .expect("one attribute");
        let error =
            parse_proptest_attr_args(&attr).expect_err("missing argument list must be diagnosed");
        assert!(
            error
                .to_compile_error()
                .to_string()
                .contains("compile_error"),
            "malformed {source} should produce compile_error tokens"
        );
    }
}

#[test]
fn rstest_context_uses_original_parameter_identifier() {
    let input = parse_item_fn(quote! {
        #[case(1)]
        async fn rstest_context(actual_ctx: ::xtask::sandbox::Sandbox, value: u8) -> ::anyhow::Result<()> {
            let _ = actual_ctx;
            let _ = value;
            Ok(())
        }
    });
    let attrs = classify_attrs(&input);
    let rendered = expand_rstest_variant(&input, &attrs, 30, SerialScope::None, false).to_string();

    assert!(
        rendered.contains("let actual_ctx = :: xtask :: Sandbox :: with_name"),
        "{rendered}"
    );
    assert!(!rendered.contains("let ctx ="), "{rendered}");
    assert!(
        !rendered.contains("actual_ctx : : xtask"),
        "context parameter should be injected: {rendered}"
    );
    assert!(
        rendered.contains("value : u8"),
        "non-context arguments must be preserved: {rendered}"
    );
}

#[test]
fn rstest_accepts_test_context_reference_and_uses_its_identifier() {
    let input = parse_item_fn(quote! {
        #[case(1)]
        async fn rstest_context(context: &::xtask::sandbox::TestContext) -> ::anyhow::Result<()> {
            let _ = context;
            Ok(())
        }
    });
    let attrs = classify_attrs(&input);
    let rendered = expand_rstest_variant(&input, &attrs, 30, SerialScope::None, false).to_string();

    assert!(
        rendered.contains("let __sinex_test_context = :: xtask :: Sandbox :: with_name"),
        "{rendered}"
    );
    assert!(
        rendered.contains("let context = & __sinex_test_context"),
        "{rendered}"
    );
    assert!(
        !rendered.contains("context : & :: xtask"),
        "context parameter should be injected: {rendered}"
    );
}
