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
        } else if fn_name_str.contains("property") || fn_name_str.contains("proptest") {
            50 // Property tests with proptest need extra time
        } else {
            30 // Default timeout for unit tests, increased for safety
        }
    });

    // Detect proptest usage
    let has_proptest = has_proptest_usage(&input.block);

    // Validate it's async
    if input.sig.asyncness.is_none() {
        return syn::Error::new_spanned(input.sig.fn_token, "sinex_test functions must be async")
            .to_compile_error()
            .into();
    }

    // Process function body based on proptest usage
    let fn_body = if has_proptest {
        // Transform proptest calls to work with async runtime
        transform_proptest_calls(&input.block)
    } else {
        *input.block.clone()
    };

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
        if has_proptest {
            // Database test with proptest support
            quote! {
                #[tokio::test]
                #fn_vis async fn #fn_name() -> std::result::Result<(), Box<dyn std::error::Error + Send + Sync>> {
                    use crate::common::test_context::{TestContext, TestConfig};
                    use crate::common::database_pool;

                    // Wrap the entire test in a timeout
                    let test_future = async {
                        // Show test starting (always visible)
                        let test_name = stringify!(#fn_name);
                        let start = std::time::Instant::now();
                        eprintln!("🔄 {} [proptest+async, timeout: {}s]", test_name.replace('_', " "), #timeout_secs);

                        // Acquire database from manager (guaranteed cleanup)
                        let managed_db = database_pool::acquire_test_database().await?;

                        // Create test context
                        let ctx = TestContext::with_managed_database(managed_db, TestConfig {
                            test_name: test_name.to_string(),
                            ..Default::default()
                        }).await?;

                        // Run the proptest with progress tracking
                        let result: Result<(), Box<dyn std::error::Error + Send + Sync>> = {
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
                                        // Convert the non-Send error to a Send-able String error
                                        Err(format!("Proptest failed: {}", e).into())
                                    }
                                },
                                Err(e) => Err(Box::new(e) as Box<dyn std::error::Error + Send + Sync>),
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
                    .map_err(|_| format!("Test timed out after {} seconds", #timeout_secs))?
                }
            }
        } else {
            // Regular database test using universal pool system with proper cleanup
            quote! {
                #[tokio::test]
                #fn_vis async fn #fn_name() -> std::result::Result<(), Box<dyn std::error::Error + Send + Sync>> {
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
                        let result: Result<(), Box<dyn std::error::Error + Send + Sync>> = if #timeout_secs > 10 {
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
        }
    } else {
        // Simple test - just timeout wrapper
        quote! {
            #[tokio::test]
            #fn_vis async fn #fn_name() -> std::result::Result<(), Box<dyn std::error::Error + Send + Sync>> {
                let test_name = stringify!(#fn_name);
                let start = std::time::Instant::now();
                eprintln!("🔄 {} [simple, timeout: {}s]", test_name.replace('_', " "), #timeout_secs);

                let result = tokio::time::timeout(
                    std::time::Duration::from_secs(#timeout_secs),
                    async { #fn_body }
                ).await
                .map_err(|_| format!("Test timed out after {} seconds", #timeout_secs))?;

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
