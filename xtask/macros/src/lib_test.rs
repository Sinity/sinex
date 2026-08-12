use super::{
    SerialScope, expand_async_context_test, expand_simple_async_test,
    parse_sinex_test_attrs_tokens, serial_guard_tokens,
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

// sinex-mqe3: `sinex_proptest!`'s per-attribute loop (lib.rs ~836-850) calls
// `attr.parse_args::<TS>().unwrap()` for `#[cases(..)]`/`#[timeout(..)]`/
// `#[seed(..)]` attributes instead of propagating a `syn::Error` into a
// `compile_error!` like the rest of this crate's parsing does. This proves
// the exact panicking API call pattern at that call site with realistic
// malformed attribute syntax, without invoking the outer `proc_macro`
// entry point directly (constructing `proc_macro::TokenStream` outside of
// an active macro expansion is not supported by this crate's toolchain, so
// no existing test in this file does that either -- see the other tests
// here, which all exercise `proc_macro2`-based internals instead).
#[test]
#[ignore = "sinex-mqe3 open: attr.parse_args().unwrap() panics instead of compile-erroring on malformed #[cases(..)] input"]
fn cases_attr_with_malformed_args_panics_instead_of_compile_erroring() {
    // `#[cases]` with no parenthesized args at all -- the exact typo
    // `sinex_proptest!`'s loop can receive from a test author forgetting
    // the value (`#[cases]` instead of `#[cases(45)]`). The outer
    // attribute syntax is perfectly well-formed (a bare `Meta::Path`), but
    // `parse_args()` requires the `Meta::List` (parenthesized) form and
    // returns `Err` for a bare path -- there is no token stream to
    // extract at all, unlike a merely-unusual-but-still-tokenizable body,
    // which `parse_args::<TokenStream>()` would happily accept.
    let attrs = syn::Attribute::parse_outer
        .parse_str("#[cases]")
        .expect("a bare attribute path is well-formed syntax");
    let attr = attrs.into_iter().next().expect("parsed exactly one attribute");
    let result = std::panic::catch_unwind(|| {
        let _: proc_macro2::TokenStream = attr.parse_args().unwrap();
    });
    assert!(
        result.is_err(),
        "expected attr.parse_args().unwrap() to panic on malformed args, matching the real bug \
         at xtask/macros/src/lib.rs:840 -- if this now passes, the panic has already been fixed \
         (e.g. by switching to `?` and a compile_error!) and this test should be un-ignored and \
         rewritten to assert the graceful-error behavior instead"
    );
    let _ = attr;
}
