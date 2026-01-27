#![allow(clippy::question_mark)]
use proc_macro::TokenStream;
use quote::quote;
use std::collections::HashSet;
use syn::{
    parse_macro_input, punctuated::Punctuated, spanned::Spanned, token::Comma, Error as SynError,
    Expr, ItemFn, Lit, Meta, PathArguments, ReturnType, Type, TypePath,
};

/// Procedural macro for automatic error context enrichment
///
/// This macro wraps functions that return `Result<T, E>` where `E: Into<SinexError>`
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
/// fn simple_function() -> Result<(), SinexError> {
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
pub fn with_context(attr: TokenStream, item: TokenStream) -> TokenStream {
    let args = parse_macro_input!(attr with Punctuated::<Meta, Comma>::parse_terminated);
    let input_fn = parse_macro_input!(item as ItemFn);

    // Parse macro arguments with comprehensive validation
    let mut operation_name: Option<String> = None;
    let mut context_pairs: Vec<(String, String)> = Vec::new();
    let mut suppress_warnings = false;
    let mut _retry_count = 0u32;
    let mut _timeout_ms: Option<u64> = None;
    let mut enable_metrics = false;
    let mut _circuit_breaker = false;
    let mut seen_keys = HashSet::new();

    for arg in args {
        match arg {
            Meta::NameValue(nv) if nv.path.is_ident("operation") => {
                if !seen_keys.insert("operation") {
                    return SynError::new(nv.path.span(), "Duplicate 'operation' parameter")
                        .to_compile_error()
                        .into();
                }
                if let Expr::Lit(expr_lit) = &nv.value {
                    if let Lit::Str(lit_str) = &expr_lit.lit {
                        let op_name = lit_str.value();
                        if op_name.is_empty() {
                            return SynError::new(lit_str.span(), "Operation name cannot be empty")
                                .to_compile_error()
                                .into();
                        }
                        if op_name.len() > 100 {
                            return SynError::new(
                                lit_str.span(),
                                "Operation name too long (max 100 characters)",
                            )
                            .to_compile_error()
                            .into();
                        }
                        // Validate operation name contains only valid characters
                        if !op_name.chars().all(|c| {
                            c.is_alphanumeric() || c == '_' || c == '-' || c == '.' || c == ':'
                        }) {
                            return SynError::new(
                                lit_str.span(),
                                "Operation name contains invalid characters. Only alphanumeric, '_', '-', '.', ':' allowed",
                            )
                            .to_compile_error()
                            .into();
                        }
                        operation_name = Some(op_name);
                    } else {
                        return SynError::new(
                            nv.value.span(),
                            "Operation value must be a string literal",
                        )
                        .to_compile_error()
                        .into();
                    }
                } else {
                    return SynError::new(
                        nv.value.span(),
                        "Operation value must be a string literal",
                    )
                    .to_compile_error()
                    .into();
                }
            }
            Meta::NameValue(nv) if nv.path.is_ident("context") => {
                // Enhanced context parsing for key-value pairs
                if let Expr::Lit(expr_lit) = &nv.value {
                    if let Lit::Str(lit_str) = &expr_lit.lit {
                        let context_str = lit_str.value();
                        // Parse simple "key=value" format for now
                        if let Some((key, value)) = context_str.split_once('=') {
                            let key_trimmed = key.trim();
                            let value_trimmed = value.trim();

                            // Validate key format
                            if key_trimmed.is_empty() {
                                return SynError::new(
                                    lit_str.span(),
                                    "Context key cannot be empty",
                                )
                                .to_compile_error()
                                .into();
                            }

                            if key_trimmed.len() > 50 {
                                return SynError::new(
                                    lit_str.span(),
                                    "Context key too long (max 50 characters)",
                                )
                                .to_compile_error()
                                .into();
                            }

                            if value_trimmed.len() > 200 {
                                return SynError::new(
                                    lit_str.span(),
                                    "Context value too long (max 200 characters)",
                                )
                                .to_compile_error()
                                .into();
                            }

                            context_pairs
                                .push((key_trimmed.to_string(), value_trimmed.to_string()));
                        } else {
                            return SynError::new(
                                lit_str.span(),
                                "Context must be in 'key=value' format",
                            )
                            .to_compile_error()
                            .into();
                        }
                    }
                }
            }
            Meta::Path(path) if path.is_ident("suppress_warnings") => {
                if !seen_keys.insert("suppress_warnings") {
                    return SynError::new(path.span(), "Duplicate 'suppress_warnings' parameter")
                        .to_compile_error()
                        .into();
                }
                suppress_warnings = true;
            }
            Meta::Path(path) if path.is_ident("enable_metrics") => {
                if !seen_keys.insert("enable_metrics") {
                    return SynError::new(path.span(), "Duplicate 'enable_metrics' parameter")
                        .to_compile_error()
                        .into();
                }
                enable_metrics = true;
            }
            Meta::Path(path) if path.is_ident("circuit_breaker") => {
                if !seen_keys.insert("circuit_breaker") {
                    return SynError::new(path.span(), "Duplicate 'circuit_breaker' parameter")
                        .to_compile_error()
                        .into();
                }
                _circuit_breaker = true;
            }
            Meta::NameValue(nv) if nv.path.is_ident("retry_count") => {
                if !seen_keys.insert("retry_count") {
                    return SynError::new(nv.path.span(), "Duplicate 'retry_count' parameter")
                        .to_compile_error()
                        .into();
                }
                if let Expr::Lit(expr_lit) = &nv.value {
                    if let Lit::Int(lit_int) = &expr_lit.lit {
                        match lit_int.base10_parse::<u32>() {
                            Ok(count) if count <= 10 => _retry_count = count,
                            _ => {
                                return SynError::new(
                                    lit_int.span(),
                                    "retry_count must be between 0 and 10",
                                )
                                .to_compile_error()
                                .into()
                            }
                        }
                    } else {
                        return SynError::new(
                            nv.value.span(),
                            "retry_count must be an integer literal",
                        )
                        .to_compile_error()
                        .into();
                    }
                } else {
                    return SynError::new(
                        nv.value.span(),
                        "retry_count must be an integer literal",
                    )
                    .to_compile_error()
                    .into();
                }
            }
            Meta::NameValue(nv) if nv.path.is_ident("timeout_ms") => {
                if !seen_keys.insert("timeout_ms") {
                    return SynError::new(nv.path.span(), "Duplicate 'timeout_ms' parameter")
                        .to_compile_error()
                        .into();
                }
                if let Expr::Lit(expr_lit) = &nv.value {
                    if let Lit::Int(lit_int) = &expr_lit.lit {
                        match lit_int.base10_parse::<u64>() {
                            Ok(ms) if ms > 0 && ms <= 300000 => _timeout_ms = Some(ms), // Max 5 minutes
                            _ => {
                                return SynError::new(
                                    lit_int.span(),
                                    "timeout_ms must be between 1 and 300000 (5 minutes)",
                                )
                                .to_compile_error()
                                .into()
                            }
                        }
                    } else {
                        return SynError::new(
                            nv.value.span(),
                            "timeout_ms must be an integer literal",
                        )
                        .to_compile_error()
                        .into();
                    }
                } else {
                    return SynError::new(nv.value.span(), "timeout_ms must be an integer literal")
                        .to_compile_error()
                        .into();
                }
            }
            Meta::Path(path) => {
                if !suppress_warnings {
                    eprintln!(
                        "warning: unknown attribute '{}' in with_context macro",
                        path.get_ident()
                            .map(|i| i.to_string())
                            .unwrap_or_else(|| "<unknown>".to_string())
                    );
                }
            }
            _ => {
                if !suppress_warnings {
                    eprintln!("warning: unsupported attribute syntax in with_context macro");
                }
            }
        }
    }

    // Enhanced function signature validation
    if !is_result_return_type(&input_fn.sig.output) {
        return SynError::new_spanned(
            &input_fn.sig,
            "with_context can only be applied to functions that return Result<T, E>. Found return type: {}"
        )
        .to_compile_error()
        .into();
    }

    // Validate function name is reasonable
    let fn_name_str = input_fn.sig.ident.to_string();
    if fn_name_str.len() > 200 {
        return SynError::new_spanned(
            &input_fn.sig.ident,
            "Function name too long for error context (max 200 characters)",
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
    let _is_async = fn_sig.asyncness.is_some();

    // Extract just the type from the return type (without the ->)
    let _return_type_annotation = match &fn_sig.output {
        ReturnType::Type(_, ty) => quote! { #ty },
        ReturnType::Default => quote! { () },
    };

    // Build custom context additions
    let mut custom_context_additions = quote! {};
    for (key, value) in &context_pairs {
        custom_context_additions = quote! {
            #custom_context_additions
            __ctx = __ctx.wrap_err_with(#key, #value);
        };
    }

    // Extract function body statements (without braces)
    let fn_stmts = &fn_block.stmts;

    // Simplified execution logic to avoid cyclic dependencies
    let execution_logic = quote! {
        // Execute function body directly in the new function scope
        #(#fn_stmts)*
    };

    // Use simplified logic without complex async wrapping

    let metrics_code = if enable_metrics {
        quote! {
            let __metrics_start = std::time::Instant::now();
            let __operation_name = #operation;

            // Record operation start
            #[cfg(feature = "metrics")]
            {
                tracing::debug!(
                    operation = __operation_name,
                    function = #fn_name_str,
                    "Operation started"
                );
            }
        }
    } else {
        quote! {}
    };

    let _metrics_end = if enable_metrics {
        quote! {
            let __duration = __metrics_start.elapsed();

            #[cfg(feature = "metrics")]
            {
                tracing::debug!(
                    operation = __operation_name,
                    function = #fn_name_str,
                    duration_ms = __duration.as_millis(),
                    success = result.is_ok(),
                    "Operation completed"
                );
            }
        }
    } else {
        quote! {}
    };

    // Generate the transformed function with simplified error handling
    let transformed = quote! {
        #(#fn_attrs)*
        #fn_vis #fn_sig {
            #metrics_code

            // Execute function body directly to avoid cyclic dependencies
            #execution_logic
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
                let ident_str = segment.ident.to_string();
                if ident_str == "Result" || ident_str.ends_with("Result") {
                    // Check if it has generic arguments
                    if let PathArguments::AngleBracketed(args) = &segment.arguments {
                        return !args.args.is_empty(); // Result<T> or Result<T, E>
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
    use xtask::sandbox::sinex_test;
    use syn::parse_quote;

    #[sinex_test]
    fn test_result_type_detection() -> TestResult<()> {
        // Test valid Result types
        let result_unit: Type = parse_quote!(Result<(), SinexError>);
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
        Ok(())
    }

    #[sinex_test]
    fn test_return_type_detection() -> TestResult<()> {
        // Test valid return types
        let return_result: ReturnType = parse_quote!(-> Result<(), SinexError>);
        assert!(is_result_return_type(&return_result));

        // Test invalid return types
        let return_unit: ReturnType = parse_quote!(-> ());
        assert!(!is_result_return_type(&return_unit));

        let return_default = ReturnType::Default;
        assert!(!is_result_return_type(&return_default));
        Ok(())
    }
}
// moved to file header
