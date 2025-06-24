//! Procedural macros for Sinex test infrastructure

use proc_macro::TokenStream;
use quote::quote;
use syn::{parse_macro_input, ItemFn};

#[proc_macro_attribute]
pub fn sinex_test(_attr: TokenStream, item: TokenStream) -> TokenStream {
    let input = parse_macro_input!(item as ItemFn);
    
    // Validate it's async
    if input.sig.asyncness.is_none() {
        return syn::Error::new_spanned(&input.sig.fn_token, "sinex_test functions must be async")
            .to_compile_error()
            .into();
    }
    
    let fn_name = &input.sig.ident;
    let fn_body = &input.block;
    let fn_vis = &input.vis;
    
    // Generate the wrapper function
    let output = quote! {
        #[tokio::test]
        #fn_vis async fn #fn_name() -> std::result::Result<(), Box<dyn std::error::Error>> {
            // Import what we need
            use crate::common::test_context::{TestContext, TestConfig};
            use crate::common::database::{TestPool, CleanupStrategy};
            
            // Get shared pool and start transaction for isolation
            let pool = crate::common::database_helpers::get_shared_test_pool().await?;
            let mut tx = pool.begin().await?;
            
            // Create test context that uses the pool directly
            let ctx = TestContext::with_pool(pool.clone(), TestConfig {
                test_name: stringify!(#fn_name).to_string(),
                ..Default::default()
            }).await?;
            
            // Define and run the original test body
            let test_result: Result<(), Box<dyn std::error::Error>> = async {
                #fn_body
            }.await;
            
            // Always rollback for perfect isolation
            tx.rollback().await?;
            
            // Return the test result
            test_result
        }
    };
    
    output.into()
}

#[proc_macro_attribute]
pub fn sinex_test_no_tx(_attr: TokenStream, item: TokenStream) -> TokenStream {
    let input = parse_macro_input!(item as ItemFn);
    
    if input.sig.asyncness.is_none() {
        return syn::Error::new_spanned(&input.sig.fn_token, "sinex_test_no_tx functions must be async")
            .to_compile_error()
            .into();
    }
    
    let fn_name = &input.sig.ident;
    let fn_body = &input.block;
    let fn_vis = &input.vis;
    
    let output = quote! {
        #[tokio::test]
        #fn_vis async fn #fn_name() -> std::result::Result<(), Box<dyn std::error::Error>> {
            use crate::common::test_context::{TestContext, TestConfig};
            use crate::common::database::{TestPool, CleanupStrategy};
            
            // Create test pool with truncate cleanup (for tests that need commits)
            let test_pool = TestPool::with_strategy(CleanupStrategy::Truncate).await?;
            
            let ctx = TestContext::with_pool(test_pool.pool().clone(), TestConfig {
                test_name: stringify!(#fn_name).to_string(),
                ..Default::default()
            }).await?;
            
            let test_result: Result<(), Box<dyn std::error::Error>> = async {
                #fn_body
            }.await;
            
            // TestPool will handle cleanup on drop
            
            test_result
        }
    };
    
    output.into()
}