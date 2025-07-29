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

/// Parse timeout attribute from macro arguments
/// Supports `timeout = 30` syntax
fn parse_timeout_attr(attr: TokenStream) -> Option<u64> {
    if attr.is_empty() {
        return None;
    }

    // Try to parse as a simple integer first
    let attr_str = attr.to_string();
    if let Ok(timeout) = attr_str.trim().parse::<u64>() {
        return Some(timeout);
    }

    // Try to parse as a simple expression like `timeout = 30`
    if let Ok(Meta::NameValue(nv)) = syn::parse::<Meta>(attr) {
        if nv.path.is_ident("timeout") {
            if let Expr::Lit(expr_lit) = &nv.value {
                if let Lit::Int(lit_int) = &expr_lit.lit {
                    return lit_int.base10_parse().ok();
                }
            }
        }
    }

    None
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

    // Default timeout constants
    const DEFAULT_SYNC_TIMEOUT: u64 = 10; // 10 seconds for sync tests
    const DEFAULT_ASYNC_TIMEOUT: u64 = 30; // 30 seconds for async tests

    // Parse timeout attribute or use defaults
    let timeout_secs = parse_timeout_attr(attr).unwrap_or_else(|| {
        if is_async {
            DEFAULT_ASYNC_TIMEOUT
        } else {
            DEFAULT_SYNC_TIMEOUT
        }
    });

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

    let output = if !is_async {
        // Sync test handling
        quote! {
            #[test]
            #fn_vis fn #fn_name() -> anyhow::Result<()> {
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
                let result: anyhow::Result<()> = (|| {
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
                #fn_vis async fn #fn_name() -> anyhow::Result<()> {
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
                        let result: anyhow::Result<()> = {
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
                                        Err(anyhow::anyhow!("Proptest failed: {}", e))
                                    }
                                },
                                Err(e) => Err(anyhow::anyhow!(e)),
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
                    .map_err(|_| anyhow::anyhow!("Test timed out after {} seconds", #timeout_secs))?
                }
            }
        } else {
            // Regular database test using universal pool system with proper cleanup
            quote! {
                #[tokio::test]
                #fn_vis async fn #fn_name() -> anyhow::Result<()> {
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
                        let result: anyhow::Result<()> = if #timeout_secs > 10 {
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
                    .map_err(|_| anyhow::anyhow!("Test timed out after {} seconds", #timeout_secs))?
                }
            }
        }
    } else {
        // Simple test - just timeout wrapper
        quote! {
            #[tokio::test]
            #fn_vis async fn #fn_name() -> anyhow::Result<()> {
                let test_name = stringify!(#fn_name);
                let start = std::time::Instant::now();
                eprintln!("🔄 {} [simple, timeout: {}s]", test_name.replace('_', " "), #timeout_secs);

                let result = tokio::time::timeout(
                    std::time::Duration::from_secs(#timeout_secs),
                    async { #fn_body }
                ).await
                .map_err(|_| anyhow::anyhow!("Test timed out after {} seconds", #timeout_secs))?;

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

    // Validate it's async
    if input.sig.asyncness.is_none() {
        return syn::Error::new_spanned(input.sig.fn_token, "sinex_bench functions must be async")
            .to_compile_error()
            .into();
    }

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
                            let result: anyhow::Result<()> = async { #fn_body }.await;
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
                        let result: anyhow::Result<()> = async { #fn_body }.await;
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
                            let result: anyhow::Result<()> = async { #fn_body }.await;
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
                        let result: anyhow::Result<()> = async { #fn_body }.await;
                        result.unwrap()
                    })
                });
            }
        }
    };

    output.into()
}
