//! Procedural macros for Sinex test infrastructure

use proc_macro::TokenStream;
use quote::quote;
use syn::{parse_macro_input, ItemFn, Expr, Lit, Meta};

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
    if let Ok(meta) = syn::parse::<Meta>(attr) {
        if let Meta::NameValue(nv) = meta {
            if nv.path.is_ident("timeout") {
                if let Expr::Lit(expr_lit) = &nv.value {
                    if let Lit::Int(lit_int) = &expr_lit.lit {
                        return lit_int.base10_parse().ok();
                    }
                }
            }
        }
    }
    
    None
}

#[proc_macro_attribute]
pub fn sinex_test(attr: TokenStream, item: TokenStream) -> TokenStream {
    let input = parse_macro_input!(item as ItemFn);
    
    // Parse timeout attribute
    let timeout_secs = parse_timeout_attr(attr).unwrap_or(10); // Default 10 seconds
    
    // Validate it's async
    if input.sig.asyncness.is_none() {
        return syn::Error::new_spanned(&input.sig.fn_token, "sinex_test functions must be async")
            .to_compile_error()
            .into();
    }
    
    let fn_name = &input.sig.ident;
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
        // Database test with perfect isolation
        quote! {
            #[tokio::test]
            #fn_vis async fn #fn_name() -> std::result::Result<(), Box<dyn std::error::Error>> {
                use crate::common::test_context::{TestContext, TestConfig};
                use crate::common::test_database::TestDatabase;
                
                // Wrap the entire test in a timeout
                let test_future = async {
                    // Show test starting (always visible)
                    let test_name = stringify!(#fn_name);
                    let start = std::time::Instant::now();
                    eprintln!("🔄 {}", test_name.replace('_', " "));
                    
                    // Create isolated database for this test
                    let test_db = TestDatabase::create(test_name).await?;
                    
                    // Create test context
                    let ctx = TestContext::with_pool(test_db.pool.clone(), TestConfig {
                        test_name: test_name.to_string(),
                        ..Default::default()
                    }).await?;
                    
                    // Run the test
                    let result: Result<(), Box<dyn std::error::Error>> = async { #fn_body }.await;
                    
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

