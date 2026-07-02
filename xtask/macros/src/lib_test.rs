use super::{
    SerialScope, expand_async_context_test, expand_simple_async_test,
    parse_sinex_test_attrs_tokens, serial_guard_tokens,
};
use quote::quote;
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

    let rendered =
        expand_async_context_test(&input, &[], &input.block, 30, SerialScope::Workspace)
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
        expand_simple_async_test(&input, &[], &input.block, 30, SerialScope::Workspace)
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
