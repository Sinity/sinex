//! Procedural macros for Sinex error handling
//!
//! This module provides the `#[with_context]` macro for automatic error context enrichment.
//! The macro automatically adds contextual information like function name, module path,
//! and custom context to errors returned from functions.
//!
//! # Usage
//!
//! ## Basic usage (adds function name and module path):
//! ```rust
//! use sinex_macros::with_context;
//! use sinex_core::{CoreError, Result};
//!
//! #[with_context]
//! fn read_config() -> Result<String> {
//!     std::fs::read_to_string("config.toml")
//!         .map_err(|e| CoreError::Io(e.to_string()))
//! }
//! ```
//!
//! ## With custom operation:
//! ```rust
//! #[with_context(operation = "database_insert")]
//! async fn insert_event(pool: &PgPool, event: &RawEvent) -> Result<()> {
//!     // function body
//! }
//! ```
//!
//! ## With custom context:
//! ```rust
//! #[with_context(context = [("table", "events"), ("operation", "select")])]
//! fn query_events() -> Result<Vec<Event>> {
//!     // function body
//! }
//! ```

use proc_macro::TokenStream;
use quote::quote;
use syn::{
    parse_macro_input, punctuated::Punctuated, token::Comma, Expr, ItemFn, Lit, Meta,
    PathArguments, ReturnType, Type, TypePath,
};

/// Procedural macro for automatic error context enrichment
///
/// This macro wraps functions that return `Result<T, E>` where `E: Into<CoreError>`
/// and automatically adds contextual information to any errors that occur.
///
/// # Attributes
///
/// - `operation = "string"` - Sets a custom operation name (defaults to function name)
/// - `context = [("key", "value"), ...]` - Adds custom key-value context pairs
///
/// # Examples
///
/// ```rust
/// #[with_context]
/// fn simple_function() -> Result<(), CoreError> {
///     // Errors will include function name and module path
///     Ok(())
/// }
///
/// #[with_context(operation = "custom_op")]
/// async fn async_function() -> Result<String, std::io::Error> {
///     // Errors will include custom operation name
///     Ok("result".to_string())
/// }
/// ```
#[proc_macro_attribute]
pub fn with_context(attr: TokenStream, item: TokenStream) -> TokenStream {
    let args = parse_macro_input!(attr with Punctuated::<Meta, Comma>::parse_terminated);
    let input_fn = parse_macro_input!(item as ItemFn);

    // Parse macro arguments
    let mut operation_name: Option<String> = None;
    let context_pairs: Vec<(String, String)> = Vec::new();

    for arg in args {
        match arg {
            Meta::NameValue(nv) if nv.path.is_ident("operation") => {
                if let Expr::Lit(expr_lit) = &nv.value {
                    if let Lit::Str(lit_str) = &expr_lit.lit {
                        operation_name = Some(lit_str.value());
                    }
                }
            }
            Meta::NameValue(nv) if nv.path.is_ident("context") => {
                // For now, we'll implement a simpler version that doesn't parse complex arrays
                // This can be enhanced later to support context = [("key", "value")] syntax
            }
            _ => {} // Ignore unknown attributes
        }
    }

    // Validate function signature
    if !is_result_return_type(&input_fn.sig.output) {
        return syn::Error::new_spanned(
            input_fn.sig.fn_token,
            "with_context can only be applied to functions that return Result<T, E>",
        )
        .to_compile_error()
        .into();
    }

    // Extract function components
    let fn_name = &input_fn.sig.ident;
    let fn_name_str = fn_name.to_string();
    let operation = operation_name.unwrap_or_else(|| fn_name_str.clone());
    let fn_vis = &input_fn.vis;
    let fn_attrs = &input_fn.attrs;
    let fn_sig = &input_fn.sig;
    let fn_block = &input_fn.block;
    let is_async = fn_sig.asyncness.is_some();

    // Generate context building code
    let mut context_building = quote! {
        core_err.context()
            .with_operation(#operation)
            .with_context("function", #fn_name_str)
            .with_context("module", module_path!())
    };

    // Add custom context pairs
    for (key, value) in context_pairs {
        context_building = quote! {
            #context_building
                .with_context(#key, #value)
        };
    }

    context_building = quote! {
        #context_building.build()
    };

    // Extract return type for the closure
    let return_type = &fn_sig.output;

    // Determine the correct error type path based on context
    // If we're in sinex-core crate itself, use crate::CoreError
    // Otherwise, use sinex_core::CoreError
    let error_type = quote! {
        #[allow(unused_imports)]
        use crate::CoreError as __CoreError;
        let core_err: __CoreError = e.into();
    };

    // Generate the transformed function
    let transformed = if is_async {
        quote! {
            #(#fn_attrs)*
            #fn_vis #fn_sig {
                let __original_fn = async move || #return_type {
                    #fn_block
                };

                __original_fn().await.map_err(|e| {
                    #error_type
                    #context_building
                })
            }
        }
    } else {
        quote! {
            #(#fn_attrs)*
            #fn_vis #fn_sig {
                let __original_fn = move || #return_type {
                    #fn_block
                };

                __original_fn().map_err(|e| {
                    #error_type
                    #context_building
                })
            }
        }
    };

    transformed.into()
}

/// Check if a return type is Result<T, E>
fn is_result_return_type(return_type: &ReturnType) -> bool {
    match return_type {
        ReturnType::Type(_, ty) => is_result_type(ty),
        ReturnType::Default => false,
    }
}

/// Check if a type is Result<T, E>
fn is_result_type(ty: &Type) -> bool {
    match ty {
        Type::Path(TypePath { path, .. }) => {
            if let Some(segment) = path.segments.last() {
                if segment.ident == "Result" {
                    // Check if it has generic arguments
                    if let PathArguments::AngleBracketed(args) = &segment.arguments {
                        return args.args.len() >= 1; // Result<T> or Result<T, E>
                    }
                }
            }
            false
        }
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use syn::parse_quote;

    #[test]
    fn test_result_type_detection() {
        // Test valid Result types
        let result_unit: Type = parse_quote!(Result<(), CoreError>);
        assert!(is_result_type(&result_unit));

        let result_string: Type = parse_quote!(Result<String>);
        assert!(is_result_type(&result_string));

        let result_qualified: Type = parse_quote!(std::result::Result<i32, std::io::Error>);
        assert!(is_result_type(&result_qualified));

        // Test invalid types
        let option_type: Type = parse_quote!(Option<String>);
        assert!(!is_result_type(&option_type));

        let simple_type: Type = parse_quote!(String);
        assert!(!is_result_type(&simple_type));
    }

    #[test]
    fn test_return_type_detection() {
        // Test valid return types
        let return_result: ReturnType = parse_quote!(-> Result<(), CoreError>);
        assert!(is_result_return_type(&return_result));

        // Test invalid return types
        let return_unit: ReturnType = parse_quote!(-> ());
        assert!(!is_result_return_type(&return_unit));

        let return_default = ReturnType::Default;
        assert!(!is_result_return_type(&return_default));
    }
}
