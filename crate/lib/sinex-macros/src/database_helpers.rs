use proc_macro::TokenStream;
use quote::quote;
use syn::{
    parse_macro_input, punctuated::Punctuated, Block, FnArg, Ident, LitStr, ReturnType, Signature,
    Token, Type,
};

/// Macro for generating database query helpers with automatic ULID/UUID conversion
///
/// This macro generates query functions that automatically handle ULID/UUID conversion
/// and provide proper error handling for database operations. It reduces boilerplate
/// when working with SQLX queries in the Sinex codebase.
///
/// # Usage
///
/// ```rust
/// db_query! {
///     async fn get_event_by_id(pool: &PgPool, id: Ulid) -> Option<RawEvent> {
///         "SELECT * FROM raw.events WHERE id = $1::uuid"
///     }
///     
///     async fn get_events_by_source(pool: &PgPool, source: &str, limit: i32) -> Vec<RawEvent> {
///         "SELECT * FROM raw.events WHERE source = $1 ORDER BY ts_ingest DESC LIMIT $2"
///     }
/// }
/// ```
///
/// This generates:
/// - Automatic ULID to UUID conversion for parameters
/// - Proper error handling and context
/// - SQLX query execution with proper type mapping
/// - Result type handling (Option<T>, Vec<T>, T)
pub fn db_query(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DbQueryInput);

    let mut generated = quote! {};

    for query in input.queries {
        generated.extend(generate_query_function(&query));
    }

    generated.into()
}

/// Macro for generating database transaction helpers
///
/// This macro generates transaction functions that automatically handle transaction
/// management, rollback on error, and proper error context. It reduces boilerplate
/// when working with database transactions.
///
/// # Usage
///
/// ```rust
/// db_transaction! {
///     async fn insert_multiple_events(pool: &PgPool, events: Vec<RawEvent>) -> Result<(), SinexError> {
///         for event in events {
///             EventQueries::insert_event(tx, &event.source, &event.event_type, &event.host, event.payload)
///                 .await?;
///         }
///     }
/// }
/// ```
///
/// This generates:
/// - Automatic transaction begin/commit/rollback
/// - Proper error handling and context
/// - Connection pool management
/// - Transaction parameter injection
pub fn db_transaction(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DbTransactionInput);

    let mut generated = quote! {};

    for transaction in input.transactions {
        generated.extend(generate_transaction_function(&transaction));
    }

    generated.into()
}

#[derive(Debug)]
struct DbQueryInput {
    queries: Vec<DbQuery>,
}

#[derive(Debug)]
struct DbQuery {
    signature: Signature,
    sql: LitStr,
}

#[derive(Debug)]
struct DbTransactionInput {
    transactions: Vec<DbTransaction>,
}

#[derive(Debug)]
struct DbTransaction {
    signature: Signature,
    body: Block,
}

impl syn::parse::Parse for DbQueryInput {
    fn parse(input: syn::parse::ParseStream) -> syn::Result<Self> {
        let mut queries = Vec::new();

        while !input.is_empty() {
            let signature: Signature = input.parse()?;

            let content;
            syn::braced!(content in input);
            let sql: LitStr = content.parse()?;

            queries.push(DbQuery { signature, sql });
        }

        Ok(DbQueryInput { queries })
    }
}

impl syn::parse::Parse for DbTransactionInput {
    fn parse(input: syn::parse::ParseStream) -> syn::Result<Self> {
        let mut transactions = Vec::new();

        while !input.is_empty() {
            let signature: Signature = input.parse()?;
            let body: Block = input.parse()?;

            transactions.push(DbTransaction { signature, body });
        }

        Ok(DbTransactionInput { transactions })
    }
}

fn generate_query_function(query: &DbQuery) -> proc_macro2::TokenStream {
    let signature = &query.signature;
    let sql = &query.sql;
    let fn_name = &signature.ident;

    // Extract parameters and their types
    let params = extract_query_parameters(&signature.inputs);
    let param_conversions = generate_parameter_conversions(&params);
    let param_names = params.iter().map(|p| &p.name).collect::<Vec<_>>();

    // Determine return type handling
    let return_type = &signature.output;
    let query_execution = match return_type {
        ReturnType::Type(_, ty) => {
            if is_option_type(ty) {
                quote! {
                    OperationQueries::query_optional(pool, #sql, &[#(#param_names),*])
                        .await
                        .map_err(|e| sinex_types::SinexError::database(e.to_string())
                            .wrap_err_with("operation", "query")
                            .wrap_err_with("function", stringify!(#fn_name))
                            .build())
                }
            } else if is_vec_type(ty) {
                quote! {
                    OperationQueries::query_all(pool, #sql, &[#(#param_names),*])
                        .await
                        .map_err(|e| sinex_types::SinexError::database(e.to_string())
                            .wrap_err_with("operation", "query")
                            .wrap_err_with("function", stringify!(#fn_name))
                            .build())
                }
            } else {
                quote! {
                    OperationQueries::query_one(pool, #sql, &[#(#param_names),*])
                        .await
                        .map_err(|e| sinex_types::SinexError::database(e.to_string())
                            .wrap_err_with("operation", "query")
                            .wrap_err_with("function", stringify!(#fn_name))
                            .build())
                }
            }
        }
        ReturnType::Default => {
            quote! {
                OperationQueries::execute(pool, #sql, &[#(#param_names),*])
                    .await
                    .map_err(|e| sinex_types::SinexError::database(e.to_string())
                        .wrap_err_with("operation", "query")
                        .wrap_err_with("function", stringify!(#fn_name))
                        .build())
                    .map(|_| ())
            }
        }
    };

    quote! {
        #[sinex_macros::with_context(operation = "database_query")]
        pub #signature {
            #param_conversions
            #query_execution
        }
    }
}

fn generate_transaction_function(transaction: &DbTransaction) -> proc_macro2::TokenStream {
    let signature = &transaction.signature;
    let body = &transaction.body;
    let fn_name = &signature.ident;

    // Extract pool parameter
    let pool_param = extract_pool_parameter(&signature.inputs);

    // Generate transaction wrapper
    quote! {
        #[sinex_macros::with_context(operation = "database_transaction")]
        pub #signature {
            let mut tx = #pool_param.begin().await
                .map_err(|e| sinex_types::SinexError::database(e.to_string())
                    .wrap_err_with("operation", "transaction_begin")
                    .wrap_err_with("function", stringify!(#fn_name))
                    .build())?;

            let result = async {
                #body
                Ok(())
            }.await;

            match result {
                Ok(_) => {
                    tx.commit().await
                        .map_err(|e| sinex_types::SinexError::database(e.to_string())
                            .wrap_err_with("operation", "transaction_commit")
                            .wrap_err_with("function", stringify!(#fn_name))
                            .build())
                }
                Err(e) => {
                    let _ = tx.rollback().await;
                    Err(e)
                }
            }
        }
    }
}

#[derive(Debug)]
struct QueryParameter {
    name: Ident,
    param_type: Type,
}

fn extract_query_parameters(inputs: &Punctuated<FnArg, Token![,]>) -> Vec<QueryParameter> {
    inputs
        .iter()
        .skip(1)
        .filter_map(|arg| {
            // Skip pool parameter
            match arg {
                FnArg::Typed(pat_type) => {
                    if let syn::Pat::Ident(pat_ident) = &*pat_type.pat {
                        Some(QueryParameter {
                            name: pat_ident.ident.clone(),
                            param_type: (*pat_type.ty).clone(),
                        })
                    } else {
                        None
                    }
                }
                _ => None,
            }
        })
        .collect()
}

fn extract_pool_parameter(inputs: &Punctuated<FnArg, Token![,]>) -> proc_macro2::TokenStream {
    if let Some(FnArg::Typed(pat_type)) = inputs.first() {
        if let syn::Pat::Ident(pat_ident) = &*pat_type.pat {
            let name = &pat_ident.ident;
            quote! { #name }
        } else {
            quote! { pool }
        }
    } else {
        quote! { pool }
    }
}

fn generate_parameter_conversions(params: &[QueryParameter]) -> proc_macro2::TokenStream {
    let conversions = params.iter().map(|param| {
        let name = &param.name;
        let param_type = &param.param_type;

        if is_ulid_type(param_type) {
            quote! {
                let #name = #name.to_uuid();
            }
        } else {
            quote! {
                let #name = #name;
            }
        }
    });

    quote! {
        #(#conversions)*
    }
}

fn is_option_type(ty: &Type) -> bool {
    match ty {
        Type::Path(type_path) => {
            if let Some(segment) = type_path.path.segments.last() {
                segment.ident == "Option"
            } else {
                false
            }
        }
        _ => false,
    }
}

fn is_vec_type(ty: &Type) -> bool {
    match ty {
        Type::Path(type_path) => {
            if let Some(segment) = type_path.path.segments.last() {
                segment.ident == "Vec"
            } else {
                false
            }
        }
        _ => false,
    }
}

fn is_ulid_type(ty: &Type) -> bool {
    match ty {
        Type::Path(type_path) => {
            if let Some(segment) = type_path.path.segments.last() {
                segment.ident == "Ulid"
            } else {
                false
            }
        }
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use syn::parse_quote;

    #[sinex_test]
    fn test_db_query_parsing() {
        let input = quote! {
            async fn get_event_by_id(pool: &PgPool, id: Ulid) -> Option<RawEvent> {
                "SELECT * FROM raw.events WHERE id = $1::uuid"
            }

            async fn get_events_by_source(pool: &PgPool, source: &str) -> Vec<RawEvent> {
                "SELECT * FROM raw.events WHERE source = $1"
            }
        };

        let parsed: DbQueryInput = syn::parse2(input).unwrap();
        assert_eq!(parsed.queries.len(), 2);
        assert_eq!(parsed.queries[0].signature.ident, "get_event_by_id");
        assert_eq!(parsed.queries[1].signature.ident, "get_events_by_source");
    }

    #[sinex_test]
    fn test_db_transaction_parsing() {
        let input = quote! {
            async fn insert_multiple_events(pool: &PgPool, events: Vec<RawEvent>) -> Result<(), SinexError> {
                for event in events {
                    EventQueries::insert_event(tx, &event.source, "event.type", &event.host, serde_json::json!({}))
                        .await?;
                }
            }
        };

        let parsed: DbTransactionInput = syn::parse2(input).unwrap();
        assert_eq!(parsed.transactions.len(), 1);
        assert_eq!(
            parsed.transactions[0].signature.ident,
            "insert_multiple_events"
        );
    }

    #[sinex_test]
    fn test_type_detection() {
        let option_type: Type = parse_quote!(Option<RawEvent>);
        assert!(is_option_type(&option_type));

        let vec_type: Type = parse_quote!(Vec<RawEvent>);
        assert!(is_vec_type(&vec_type));

        let ulid_type: Type = parse_quote!(Ulid);
        assert!(is_ulid_type(&ulid_type));

        let string_type: Type = parse_quote!(String);
        assert!(!is_option_type(&string_type));
        assert!(!is_vec_type(&string_type));
        assert!(!is_ulid_type(&string_type));
    }
}
