//! Procedural macros for Sinex test infrastructure

use proc_macro::TokenStream;
use quote::quote;
use syn::{parse_macro_input, Expr, ItemFn, Lit, Meta};

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

#[proc_macro_attribute]
pub fn sinex_test(attr: TokenStream, item: TokenStream) -> TokenStream {
    let input = parse_macro_input!(item as ItemFn);
    let fn_name = &input.sig.ident;

    // Parse timeout attribute with smarter defaults
    let timeout_secs = parse_timeout_attr(attr).unwrap_or_else(|| {
        // Smart default based on function name patterns - increased for database operations
        let fn_name_str = fn_name.to_string();
        if fn_name_str.contains("system") || fn_name_str.contains("end_to_end") {
            60 // System tests need more time, especially for template creation
        } else if fn_name_str.contains("adversarial") || fn_name_str.contains("stress") {
            45 // Adversarial tests need moderate time
        } else if fn_name_str.contains("database") || fn_name_str.contains("integration") {
            40 // Database operations need extra time for connection pool
        } else {
            30 // Default timeout for unit tests, increased for safety
        }
    });

    // Validate it's async
    if input.sig.asyncness.is_none() {
        return syn::Error::new_spanned(input.sig.fn_token, "sinex_test functions must be async")
            .to_compile_error()
            .into();
    }
    let fn_body = &input.block;
    let fn_vis = &input.vis;

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

    let output = if takes_context {
        // Database test using universal pool system
        quote! {
            #[tokio::test]
            #fn_vis async fn #fn_name() -> std::result::Result<(), Box<dyn std::error::Error>> {
                use crate::common::test_context::{TestContext, TestConfig};
                use crate::common::database_pool;

                // Wrap the entire test in a timeout
                let test_future = async {
                    // Show test starting (always visible)
                    let test_name = stringify!(#fn_name);
                    let start = std::time::Instant::now();
                    eprintln!("🔄 {} [timeout: {}s]", test_name.replace('_', " "), #timeout_secs);

                    // Acquire database from manager (guaranteed cleanup)
                    let managed_db = database_pool::acquire_test_database().await?;

                    // Create test context
                    let ctx = TestContext::with_managed_database(managed_db, TestConfig {
                        test_name: test_name.to_string(),
                        ..Default::default()
                    }).await?;

                    // Run the test with progress tracking for long tests
                    let result: Result<(), Box<dyn std::error::Error>> = if #timeout_secs > 10 {
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
                .map_err(|_| format!("Test timed out after {} seconds", #timeout_secs))?
            }
        }
    } else {
        // Simple test - just timeout wrapper
        quote! {
            #[tokio::test]
            #fn_vis async fn #fn_name() -> std::result::Result<(), Box<dyn std::error::Error>> {
                tokio::time::timeout(
                    std::time::Duration::from_secs(#timeout_secs),
                    async { #fn_body }
                ).await
                .map_err(|_| format!("Test timed out after {} seconds", #timeout_secs))?
            }
        }
    };

    output.into()
}
