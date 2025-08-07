//! Procedural macros for Sinex test infrastructure
//!
//! This macro provides sophisticated test infrastructure for Sinex tests, including:
//! - Automatic TestContext creation and cleanup
//! - Proptest integration with async runtime bridging
//! - Smart timeout handling based on test patterns
//! - Progress indicators for long-running tests
//! - Rich error reporting with timing information

use proc_macro::TokenStream;
use quote::quote;
use syn::{parse_macro_input, visit::Visit, Expr, ItemFn, Lit, Meta};

/// Configuration parsed from sinex_test attributes
struct SinexTestConfig {
    timeout: Option<u64>,
    trace: bool,
}

/// Parse sinex_test attributes
/// Supports: timeout = 30, trace = true
fn parse_sinex_test_attrs(attr: TokenStream) -> SinexTestConfig {
    let mut config = SinexTestConfig {
        timeout: None,
        trace: false,
    };

    if attr.is_empty() {
        return config;
    }

    // Try to parse as a simple integer first (legacy timeout support)
    let attr_str = attr.to_string();
    if let Ok(timeout) = attr_str.trim().parse::<u64>() {
        config.timeout = Some(timeout);
        return config;
    }

    // Parse comma-separated attributes
    if let Ok(meta_list) = syn::parse::<syn::MetaList>(attr.clone()) {
        for nested in meta_list.tokens {
            if let Ok(Meta::NameValue(nv)) = syn::parse2::<Meta>(quote! { #nested }) {
                if nv.path.is_ident("timeout") {
                    if let Expr::Lit(expr_lit) = &nv.value {
                        if let Lit::Int(lit_int) = &expr_lit.lit {
                            config.timeout = lit_int.base10_parse().ok();
                        }
                    }
                } else if nv.path.is_ident("trace") {
                    if let Expr::Lit(expr_lit) = &nv.value {
                        if let Lit::Bool(lit_bool) = &expr_lit.lit {
                            config.trace = lit_bool.value();
                        }
                    }
                }
            }
        }
    }

    // Also try simple name=value parsing
    if let Ok(Meta::NameValue(nv)) = syn::parse::<Meta>(attr) {
        if nv.path.is_ident("timeout") {
            if let Expr::Lit(expr_lit) = &nv.value {
                if let Lit::Int(lit_int) = &expr_lit.lit {
                    config.timeout = lit_int.base10_parse().ok();
                }
            }
        } else if nv.path.is_ident("trace") {
            if let Expr::Lit(expr_lit) = &nv.value {
                if let Lit::Bool(lit_bool) = &expr_lit.lit {
                    config.trace = lit_bool.value();
                }
            }
        }
    }

    config
}

/// Visitor to detect proptest usage in function body
struct ProptestDetector {
    has_proptest: bool,
}

impl ProptestDetector {
    fn new() -> Self {
        Self {
            has_proptest: false,
        }
    }
}

impl<'ast> Visit<'ast> for ProptestDetector {
    fn visit_macro(&mut self, node: &'ast syn::Macro) {
        if let Some(ident) = node.path.get_ident() {
            if ident == "proptest" {
                self.has_proptest = true;
            }
        }
        syn::visit::visit_macro(self, node);
    }
}

/// Detect if the function body contains proptest! macro calls
fn has_proptest_usage(block: &syn::Block) -> bool {
    let mut detector = ProptestDetector::new();
    detector.visit_block(block);
    detector.has_proptest
}

/// Transform proptest! calls to work with async runtime
fn transform_proptest_calls(block: &syn::Block) -> syn::Block {
    use syn::parse_quote;

    // For now, we'll wrap the entire block in a runtime bridge
    // In a more sophisticated implementation, we'd traverse and transform specific proptest! calls
    parse_quote! {
        {
            // Create a runtime handle if not already in async context
            let rt_handle = match tokio::runtime::Handle::try_current() {
                Ok(handle) => handle,
                Err(_) => {
                    // Create a new runtime for proptest execution
                    let rt = tokio::runtime::Runtime::new().expect("Failed to create runtime for proptest");
                    rt.handle().clone()
                }
            };

            // Execute the original block within the runtime context
            #block
        }
    }
}

#[proc_macro_attribute]
pub fn sinex_test(attr: TokenStream, item: TokenStream) -> TokenStream {
    let input = parse_macro_input!(item as ItemFn);
    let fn_name = &input.sig.ident;

    // Check if function is async or sync
    let is_async = input.sig.asyncness.is_some();

    // Check for rstest integration:
    // 1. Look for #[case] attributes on the function
    // 2. Look for #[case] attributes on parameters
    let mut case_attrs = Vec::new();
    let mut other_attrs = Vec::new();
    let mut has_rstest_cases = false;

    // Separate #[case] attributes from others
    for attr in &input.attrs {
        if attr.path().is_ident("case") {
            has_rstest_cases = true;
            case_attrs.push(attr.clone());
        } else {
            other_attrs.push(attr.clone());
        }
    }

    // Also check if any parameters have #[case] attribute
    for arg in &input.sig.inputs {
        if let syn::FnArg::Typed(pat_type) = arg {
            for attr in &pat_type.attrs {
                if attr.path().is_ident("case") {
                    has_rstest_cases = true;
                }
            }
        }
    }

    // Default timeout constants
    const DEFAULT_SYNC_TIMEOUT: u64 = 10; // 10 seconds for sync tests
    const DEFAULT_ASYNC_TIMEOUT: u64 = 30; // 30 seconds for async tests

    // Parse sinex_test configuration
    let config = parse_sinex_test_attrs(attr);
    let timeout_secs = config.timeout.unwrap_or_else(|| {
        if is_async {
            DEFAULT_ASYNC_TIMEOUT
        } else {
            DEFAULT_SYNC_TIMEOUT
        }
    });
    let enable_tracing = config.trace;

    // Detect proptest usage
    let has_proptest = has_proptest_usage(&input.block);

    // Process function body based on proptest usage
    let fn_body = if has_proptest {
        // Transform proptest calls to work with async runtime
        transform_proptest_calls(&input.block)
    } else {
        *input.block.clone()
    };

    let fn_vis = &input.vis;

    // Check return type - must be Result<()> or Result<T>
    let has_result_return = if let syn::ReturnType::Type(_, ref ty) = input.sig.output {
        if let syn::Type::Path(type_path) = ty.as_ref() {
            type_path
                .path
                .segments
                .last()
                .map(|seg| seg.ident == "Result")
                .unwrap_or(false)
        } else {
            false
        }
    } else {
        false
    };

    if !has_result_return {
        return syn::Error::new_spanned(
            &input.sig.output,
            "sinex_test functions must return Result<()> or Result<T>",
        )
        .to_compile_error()
        .into();
    }

    // Check if function takes TestContext parameter
    let takes_context = input.sig.inputs.iter().any(|arg| {
        if let syn::FnArg::Typed(pat_type) = arg {
            if let syn::Type::Path(type_path) = pat_type.ty.as_ref() {
                if let Some(last_segment) = type_path.path.segments.last() {
                    return last_segment.ident == "TestContext";
                }
            }
        }
        false
    });

    // Sync tests can't use TestContext (it requires async)
    if takes_context && !is_async {
        return syn::Error::new_spanned(input.sig.fn_token, "TestContext requires async functions")
            .to_compile_error()
            .into();
    }

    // If we have rstest cases, generate rstest-compatible output
    if has_rstest_cases {
        // For rstest integration, we need to:
        // 1. Add #[rstest] attribute
        // 2. Include #[case] attributes
        // 3. Handle TestContext specially - it gets created per test case

        let fn_vis = &input.vis;
        let _fn_sig = &input.sig;
        let fn_body = &input.block;

        // Build the function signature without TestContext (if present)
        // since we'll create it inside each test case
        let mut filtered_inputs = Vec::new();
        let mut has_ctx_param = false;

        for arg in &input.sig.inputs {
            if let syn::FnArg::Typed(pat_type) = arg {
                if let syn::Type::Path(type_path) = pat_type.ty.as_ref() {
                    if let Some(last_segment) = type_path.path.segments.last() {
                        if last_segment.ident == "TestContext" {
                            has_ctx_param = true;
                            continue; // Skip TestContext parameter
                        }
                    }
                }
            }
            filtered_inputs.push(arg.clone());
        }

        // Create new signature without TestContext
        let mut new_sig = input.sig.clone();
        new_sig.inputs = filtered_inputs.into_iter().collect();

        // Build test body with optional TestContext creation and tracing
        let test_body = match (has_ctx_param, enable_tracing) {
            (true, true) => quote! {
                let _tracing_guard = TestContext::with_name(stringify!(#fn_name))
                    .await?
                    .with_tracing("debug");
                let ctx = TestContext::with_name(stringify!(#fn_name)).await?;
                #fn_body
            },
            (true, false) => quote! {
                let ctx = TestContext::with_name(stringify!(#fn_name)).await?;
                #fn_body
            },
            (false, true) => quote! {
                let _tracing_guard = sinex_test_utils::test_context::TestContext::init_tracing("debug");
                #fn_body
            },
            (false, false) => quote! { #fn_body },
        };

        return quote! {
            #[::rstest::rstest]
            #(#case_attrs)*
            #(#other_attrs)*
            #[tokio::test]
            #fn_vis #new_sig {
                use std::time::Instant;

                let test_name = stringify!(#fn_name);
                let start = Instant::now();
                eprintln!("🔄 {} [rstest case, timeout: {}s]", test_name.replace('_', " "), #timeout_secs);

                let result = tokio::time::timeout(
                    std::time::Duration::from_secs(#timeout_secs),
                    async { #test_body }
                ).await
                .map_err(|_| color_eyre::eyre::eyre!("Test timed out after {} seconds", #timeout_secs))?;

                let elapsed = start.elapsed();
                if result.is_ok() {
                    eprintln!("✅ {} ({:.1?})", test_name.replace('_', " "), elapsed);
                } else {
                    eprintln!("❌ {} ({:.1?})", test_name.replace('_', " "), elapsed);
                }

                result
            }
        }.into();
    }

    let output = if !is_async {
        // Sync test handling
        quote! {
            #[test]
            #fn_vis fn #fn_name() -> color_eyre::eyre::Result<()> {
                use std::thread;
                use std::time::{Duration, Instant};

                let test_name = stringify!(#fn_name);
                let start = Instant::now();
                eprintln!("🔄 {} [sync, timeout: {}s]", test_name.replace('_', " "), #timeout_secs);

                // Start progress thread for longer tests
                let progress_handle = if #timeout_secs > 5 {
                    let test_name_clone = test_name.to_string();
                    let timeout = #timeout_secs;
                    Some(thread::spawn(move || {
                        let mut elapsed = 5;
                        loop {
                            thread::sleep(Duration::from_secs(5));
                            eprintln!("  ⏳ {} still running... ({}s elapsed)",
                                     test_name_clone.replace('_', " "), elapsed);
                            elapsed += 5;
                            if elapsed >= timeout - 5 {
                                break;
                            }
                        }
                    }))
                } else {
                    None
                };

                // Run the test
                let result: color_eyre::eyre::Result<()> = (|| {
                    #fn_body
                })();

                // Clean up progress thread
                if let Some(handle) = progress_handle {
                    // Thread will exit on its own, just don't wait for it
                    drop(handle);
                }

                let elapsed = start.elapsed();
                if result.is_ok() {
                    eprintln!("✅ {} ({:.1?})", test_name.replace('_', " "), elapsed);
                } else {
                    eprintln!("❌ {} ({:.1?})", test_name.replace('_', " "), elapsed);
                }

                // Check if we exceeded timeout (soft warning only)
                if elapsed.as_secs() > #timeout_secs {
                    eprintln!("⚠️  {} exceeded timeout of {}s",
                             test_name.replace('_', " "), #timeout_secs);
                }

                result
            }
        }
    } else if takes_context {
        if has_proptest {
            // Database test with proptest support
            quote! {
                #[tokio::test]
                #fn_vis async fn #fn_name() -> color_eyre::eyre::Result<()> {
                    // Note: TestContext must be in scope

                    // Wrap the entire test in a timeout
                    let test_future = async {
                        // Show test starting (always visible)
                        let test_name = stringify!(#fn_name);
                        let start = std::time::Instant::now();
                        eprintln!("🔄 {} [proptest+async, timeout: {}s]", test_name.replace('_', " "), #timeout_secs);

                        // Create test context with test name
                        let ctx = TestContext::with_name(test_name).await?;

                        // Run the proptest with progress tracking
                        let result: color_eyre::eyre::Result<()> = {
                            // For proptest, spawn a progress indicator
                            let progress_task = tokio::spawn(async {
                                let mut interval = tokio::time::interval(std::time::Duration::from_secs(10));
                                interval.tick().await; // Skip first immediate tick
                                let mut elapsed_secs = 10;
                                loop {
                                    interval.tick().await;
                                    eprintln!("  ⏳ {} [proptest] still running... ({}s elapsed)", test_name.replace('_', " "), elapsed_secs);
                                    elapsed_secs += 10;
                                    if elapsed_secs >= #timeout_secs - 10 {
                                        break;
                                    }
                                }
                            });

                            // Execute the proptest within async context
                            let proptest_result = tokio::task::spawn_blocking(move || {
                                // Create a new runtime for proptest execution
                                let rt = tokio::runtime::Runtime::new().expect("Failed to create runtime for proptest");
                                rt.block_on(async {
                                    // Execute the test body within the runtime
                                    #fn_body
                                })
                            }).await;

                            // Cancel progress task
                            if !progress_task.is_finished() {
                                progress_task.abort();
                                tokio::time::sleep(std::time::Duration::from_millis(50)).await;
                            }

                            match proptest_result {
                                Ok(result) => match result {
                                    Ok(()) => Ok(()),
                                    Err(e) => {
                                        // Convert the non-Send error to anyhow error
                                        Err(color_eyre::eyre::eyre!("Proptest failed: {}", e))
                                    }
                                },
                                Err(e) => Err(color_eyre::eyre::eyre!(e)),
                            }?
                        };

                        // Show result (always visible)
                        let elapsed = start.elapsed();
                        if result.is_ok() {
                            eprintln!("✅ {} [proptest] ({:.1?})", test_name.replace('_', " "), elapsed);
                        } else {
                            eprintln!("❌ {} [proptest] ({:.1?})", test_name.replace('_', " "), elapsed);
                        }

                        result
                    };

                    // Apply timeout
                    tokio::time::timeout(
                        std::time::Duration::from_secs(#timeout_secs),
                        test_future
                    ).await
                    .map_err(|_| color_eyre::eyre::eyre!("Test timed out after {} seconds", #timeout_secs))?
                }
            }
        } else {
            // Regular database test using universal pool system with proper cleanup
            quote! {
                #[tokio::test]
                #fn_vis async fn #fn_name() -> color_eyre::eyre::Result<()> {
                    // Note: TestContext must be in scope

                    // Wrap the entire test in a timeout
                    let test_future = async {
                        // Show test starting (always visible)
                        let test_name = stringify!(#fn_name);
                        let start = std::time::Instant::now();
                        eprintln!("🔄 {} [timeout: {}s]", test_name.replace('_', " "), #timeout_secs);

                        // Create test context with test name
                        let ctx = TestContext::with_name(test_name).await?;

                        // Run the test with progress tracking for long tests
                        let result: color_eyre::eyre::Result<()> = if #timeout_secs > 10 {
                            // For long tests, spawn a progress indicator
                            let progress_task = tokio::spawn(async {
                                let mut interval = tokio::time::interval(std::time::Duration::from_secs(5));
                                interval.tick().await; // Skip first immediate tick
                                let mut elapsed_secs = 5;
                                loop {
                                    interval.tick().await;
                                    eprintln!("  ⏳ {} still running... ({}s elapsed)", test_name.replace('_', " "), elapsed_secs);
                                    elapsed_secs += 5;
                                    if elapsed_secs >= #timeout_secs - 5 {
                                        break;
                                    }
                                }
                            });

                            let test_result = async { #fn_body }.await;
                            // Gracefully cancel progress task to avoid abrupt shutdown
                            if !progress_task.is_finished() {
                                progress_task.abort();
                                // Give a small grace period for cleanup
                                tokio::time::sleep(std::time::Duration::from_millis(50)).await;
                            }
                            test_result
                        } else {
                            async { #fn_body }.await
                        };

                        // Show result (always visible)
                        let elapsed = start.elapsed();
                        if result.is_ok() {
                            eprintln!("✅ {} ({:.1?})", test_name.replace('_', " "), elapsed);
                        } else {
                            eprintln!("❌ {} ({:.1?})", test_name.replace('_', " "), elapsed);
                        }

                        result
                    };

                    // Apply timeout
                    tokio::time::timeout(
                        std::time::Duration::from_secs(#timeout_secs),
                        test_future
                    ).await
                    .map_err(|_| color_eyre::eyre::eyre!("Test timed out after {} seconds", #timeout_secs))?
                }
            }
        }
    } else {
        // Simple test - just timeout wrapper
        quote! {
            #[tokio::test]
            #fn_vis async fn #fn_name() -> color_eyre::eyre::Result<()> {
                let test_name = stringify!(#fn_name);
                let start = std::time::Instant::now();
                eprintln!("🔄 {} [simple, timeout: {}s]", test_name.replace('_', " "), #timeout_secs);

                let result = tokio::time::timeout(
                    std::time::Duration::from_secs(#timeout_secs),
                    async { #fn_body }
                ).await
                .map_err(|_| color_eyre::eyre::eyre!("Test timed out after {} seconds", #timeout_secs))?;

                let elapsed = start.elapsed();
                if result.is_ok() {
                    eprintln!("✅ {} ({:.1?})", test_name.replace('_', " "), elapsed);
                } else {
                    eprintln!("❌ {} ({:.1?})", test_name.replace('_', " "), elapsed);
                }

                result
            }
        }
    };

    output.into()
}

#[proc_macro_attribute]
pub fn sinex_bench(attr: TokenStream, item: TokenStream) -> TokenStream {
    // When building tests (not benchmarks), just remove the function entirely
    // by returning it wrapped in #[cfg(all(test, feature = "bench"))]
    // This prevents divan errors during test compilation

    let input = parse_macro_input!(item as ItemFn);
    let fn_name = &input.sig.ident;

    // Parse optional args parameter
    let args = if !attr.is_empty() {
        // Parse args = [values...] syntax
        let attr_str = attr.to_string();
        if attr_str.starts_with("args") && attr_str.contains('[') {
            Some(proc_macro2::TokenStream::from(attr))
        } else {
            None
        }
    } else {
        None
    };

    // Remove async validation - benchmarks should be synchronous since the macro handles async internally

    // Check function parameters
    let mut takes_context = false;
    let mut takes_args = false;
    let mut arg_type = None;

    for (i, arg) in input.sig.inputs.iter().enumerate() {
        if let syn::FnArg::Typed(pat_type) = arg {
            if let syn::Type::Path(type_path) = pat_type.ty.as_ref() {
                if let Some(last_segment) = type_path.path.segments.last() {
                    if last_segment.ident == "BenchContext" && i == 0 {
                        takes_context = true;
                    } else if i == 1 || (i == 0 && !takes_context) {
                        // This is the args parameter
                        takes_args = true;
                        arg_type = Some(pat_type.ty.clone());
                    }
                }
            }
        }
    }

    let fn_vis = &input.vis;
    let fn_body = &input.block;

    let output = if takes_context && takes_args {
        // Database benchmark with context and args
        if let Some(args_tokens) = args {
            quote! {
                #[divan::bench(#args_tokens)]
                #fn_vis fn #fn_name(bencher: divan::Bencher, arg: #arg_type) {
                    use sinex_test_utils::bench::BENCH_CONTEXT;
                    let ctx = &*BENCH_CONTEXT;

                    bencher.bench_local(|| {
                        ctx.runtime.block_on(async {
                            // The function body contains .await calls, so we wrap it in an async block
                            let result: color_eyre::eyre::Result<()> = async {
                                let ctx = ctx;
                                let arg = arg;
                                #fn_body
                            }.await;
                            result.unwrap()
                        })
                    });
                }
            }
        } else {
            return syn::Error::new_spanned(
                fn_name,
                "Parameterized benchmarks require args attribute",
            )
            .to_compile_error()
            .into();
        }
    } else if takes_context {
        // Database benchmark with only context
        quote! {
            #[divan::bench]
            #fn_vis fn #fn_name(bencher: divan::Bencher) {
                use sinex_test_utils::bench::BENCH_CONTEXT;
                let ctx = &*BENCH_CONTEXT;

                bencher.bench_local(|| {
                    ctx.runtime.block_on(async {
                        // The function body contains .await calls, so we wrap it in an async block
                        let result: color_eyre::eyre::Result<()> = async {
                            let ctx = ctx;
                            #fn_body
                        }.await;
                        result.unwrap()
                    })
                });
            }
        }
    } else if takes_args {
        // Simple benchmark with args
        if let Some(args_tokens) = args {
            quote! {
                #[divan::bench(#args_tokens)]
                #fn_vis fn #fn_name(bencher: divan::Bencher, arg: #arg_type) {
                    let runtime = tokio::runtime::Runtime::new().unwrap();

                    bencher.bench_local(|| {
                        runtime.block_on(async {
                            let result: color_eyre::eyre::Result<()> = async {
                                let arg = arg;
                                #fn_body
                            }.await;
                            result.unwrap()
                        })
                    });
                }
            }
        } else {
            return syn::Error::new_spanned(
                fn_name,
                "Parameterized benchmarks require args attribute",
            )
            .to_compile_error()
            .into();
        }
    } else {
        // Simple benchmark without context or args
        quote! {
            #[divan::bench]
            #fn_vis fn #fn_name(bencher: divan::Bencher) {
                let runtime = tokio::runtime::Runtime::new().unwrap();

                bencher.bench_local(|| {
                    runtime.block_on(async {
                        let result: color_eyre::eyre::Result<()> = async {
                            #fn_body
                        }.await;
                        result.unwrap()
                    })
                });
            }
        }
    };

    output.into()
}
