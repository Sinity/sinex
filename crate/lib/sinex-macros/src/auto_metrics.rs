//! Automatic metrics generation macros
//!
//! This module provides procedural macros for automatically adding metrics collection
//! to functions and implementations.

use proc_macro::TokenStream;
use quote::quote;
use syn::{parse_macro_input, ItemFn, ItemImpl};

/// Automatic function metrics
pub fn auto_metrics(_attr: TokenStream, item: TokenStream) -> TokenStream {
    let input_fn = parse_macro_input!(item as ItemFn);

    let fn_name = &input_fn.sig.ident;
    let fn_name_str = fn_name.to_string();

    let original_block = &input_fn.block;
    let sig = &input_fn.sig;
    let vis = &input_fn.vis;
    let attrs = &input_fn.attrs;

    let expanded = quote! {
        #(#attrs)*
        #vis #sig {
            let _guard = sinex_db::telemetry::track_function_call(#fn_name_str, module_path!());
            #original_block
        }
    };

    TokenStream::from(expanded)
}

/// Automatic database metrics
pub fn auto_db_metrics(attr: TokenStream, item: TokenStream) -> TokenStream {
    let input_fn = parse_macro_input!(item as ItemFn);

    // Parse attributes to extract operation or use function name as default
    let operation = if attr.is_empty() {
        input_fn.sig.ident.to_string()
    } else {
        let attr_str = attr.to_string();
        // Parse operation = "value" format
        if let Some(start) = attr_str.find("operation = \"") {
            let start_pos = start + "operation = \"".len();
            if let Some(end) = attr_str[start_pos..].find('"') {
                attr_str[start_pos..start_pos + end].to_string()
            } else {
                input_fn.sig.ident.to_string()
            }
        } else {
            input_fn.sig.ident.to_string()
        }
    };

    let original_block = &input_fn.block;
    let sig = &input_fn.sig;
    let vis = &input_fn.vis;
    let attrs = &input_fn.attrs;

    let expanded = quote! {
        #(#attrs)*
        #vis #sig {
            let _guard = sinex_db::telemetry::track_database_query(#operation);
            #original_block
        }
    };

    TokenStream::from(expanded)
}

/// Automatic event metrics
pub fn auto_event_metrics(attr: TokenStream, item: TokenStream) -> TokenStream {
    let input_fn = parse_macro_input!(item as ItemFn);

    // Parse attributes to extract event_type
    let event_type = if attr.is_empty() {
        "unknown".to_string()
    } else {
        let attr_str = attr.to_string();
        // Parse event_type = "value" format
        if let Some(start) = attr_str.find("event_type = \"") {
            let start_pos = start + "event_type = \"".len();
            if let Some(end) = attr_str[start_pos..].find('"') {
                attr_str[start_pos..start_pos + end].to_string()
            } else {
                "unknown".to_string()
            }
        } else {
            "unknown".to_string()
        }
    };

    let original_block = &input_fn.block;
    let sig = &input_fn.sig;
    let vis = &input_fn.vis;
    let attrs = &input_fn.attrs;

    let expanded = quote! {
        #(#attrs)*
        #vis #sig {
            let _guard = sinex_db::telemetry::instrumentation::events::track_event_processing(#event_type);
            #original_block
        }
    };

    TokenStream::from(expanded)
}

/// Automatic resource metrics
pub fn auto_resource_metrics(_attr: TokenStream, item: TokenStream) -> TokenStream {
    let input_fn = parse_macro_input!(item as ItemFn);

    let original_block = &input_fn.block;
    let sig = &input_fn.sig;
    let vis = &input_fn.vis;
    let attrs = &input_fn.attrs;

    let expanded = quote! {
        #(#attrs)*
        #vis #sig {
            let _metrics = sinex_db::telemetry::instrumentation::resources::create_system_metrics();
            _metrics.collect_system_metrics();
            #original_block
        }
    };

    TokenStream::from(expanded)
}

/// Automatic satellite metrics for trait implementations
pub fn auto_satellite_metrics(attr: TokenStream, item: TokenStream) -> TokenStream {
    let input_impl = parse_macro_input!(item as ItemImpl);

    // Parse attributes to extract processor_type and labels
    let attr_str = attr.to_string();
    let _processor_type = if let Some(start) = attr_str.find("processor_type = \"") {
        let start_pos = start + "processor_type = \"".len();
        if let Some(end) = attr_str[start_pos..].find('"') {
            attr_str[start_pos..start_pos + end].to_string()
        } else {
            "unknown".to_string()
        }
    } else {
        "unknown".to_string()
    };

    // For now, we'll enhance each scan method with metrics tracking
    // A full implementation would parse the ItemImpl and wrap each method
    let expanded = quote! {
        #input_impl
    };

    TokenStream::from(expanded)
}
