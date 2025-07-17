use proc_macro::TokenStream;
use quote::quote;
use syn::{parse_macro_input, punctuated::Punctuated, Expr, Ident, Token};

/// Macro for creating fluent validation chains
///
/// This macro simplifies the creation of validation chains by providing a more
/// concise syntax for common validation patterns. It reduces boilerplate when
/// validating configuration values and user inputs.
///
/// # Usage
///
/// ```rust
/// validation_chain! {
///     username: String => {
///         not_empty(),
///         min_length(3),
///         max_length(50),
///         matches_regex(r"^[a-zA-Z0-9_-]+$"),
///     },
///     email: String => {
///         not_empty(),
///         is_valid_email(),
///     },
///     port: u16 => {
///         in_range(1, 65535),
///     },
/// }
/// ```
///
/// This generates:
/// ```rust
/// let username = ValidationChain::validate(username, "username")
///     .not_empty()
///     .min_length(3)
///     .max_length(50)
///     .matches_regex(&regex::Regex::new(r"^[a-zA-Z0-9_-]+$").unwrap())
///     .into_result()?;
///
/// let email = ValidationChain::validate(email, "email")
///     .not_empty()
///     .is_valid_email()
///     .into_result()?;
///
/// let port = ValidationChain::validate(port, "port")
///     .in_range(1, 65535)
///     .into_result()?;
/// ```
pub fn validation_chain(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as ValidationChainInput);

    let mut generated = quote! {};

    for field in input.fields {
        let field_name = &field.name;
        let field_name_str = field_name.to_string();
        let _field_type = &field.field_type;

        let mut chain = quote! {
            ValidationChain::validate(#field_name, #field_name_str)
        };

        // Add each validation method
        for validation in &field.validations {
            chain = quote! {
                #chain.#validation
            };
        }

        // Complete the chain
        chain = quote! {
            let #field_name = #chain.into_result()?;
        };

        generated.extend(chain);
    }

    generated.into()
}

struct ValidationChainInput {
    fields: Vec<FieldValidation>,
}

struct FieldValidation {
    name: Ident,
    field_type: syn::Type,
    validations: Vec<ValidationMethod>,
}

struct ValidationMethod {
    name: Ident,
    args: Vec<Expr>,
}

impl syn::parse::Parse for ValidationChainInput {
    fn parse(input: syn::parse::ParseStream) -> syn::Result<Self> {
        let mut fields = Vec::new();

        while !input.is_empty() {
            let name: Ident = input.parse()?;
            input.parse::<Token![:]>()?;
            let field_type: syn::Type = input.parse()?;
            input.parse::<Token![=>]>()?;

            let content;
            syn::braced!(content in input);

            let mut validations = Vec::new();
            while !content.is_empty() {
                let method_name: Ident = content.parse()?;

                let args = if content.peek(syn::token::Paren) {
                    let args_content;
                    syn::parenthesized!(args_content in content);
                    Punctuated::<Expr, Token![,]>::parse_terminated(&args_content)?
                        .into_iter()
                        .collect()
                } else {
                    Vec::new()
                };

                validations.push(ValidationMethod {
                    name: method_name,
                    args,
                });

                if !content.is_empty() {
                    content.parse::<Token![,]>()?;
                }
            }

            fields.push(FieldValidation {
                name,
                field_type,
                validations,
            });

            if !input.is_empty() {
                input.parse::<Token![,]>()?;
            }
        }

        Ok(ValidationChainInput { fields })
    }
}

impl syn::parse::Parse for ValidationMethod {
    fn parse(input: syn::parse::ParseStream) -> syn::Result<Self> {
        let name: Ident = input.parse()?;

        let args = if input.peek(syn::token::Paren) {
            let content;
            syn::parenthesized!(content in input);
            Punctuated::<Expr, Token![,]>::parse_terminated(&content)?
                .into_iter()
                .collect()
        } else {
            Vec::new()
        };

        Ok(ValidationMethod { name, args })
    }
}

impl quote::ToTokens for ValidationMethod {
    fn to_tokens(&self, tokens: &mut proc_macro2::TokenStream) {
        let name = &self.name;
        let args = &self.args;

        if args.is_empty() {
            tokens.extend(quote! { #name() });
        } else {
            tokens.extend(quote! { #name(#(#args),*) });
        }
    }
}

/// Macro for creating custom validation functions
///
/// This macro helps create validation functions that can be used with ValidationChain.
/// It automatically handles the error creation and field name context.
///
/// # Usage
///
/// ```rust
/// validation_fn! {
///     fn is_valid_port(value: u16) -> bool {
///         value > 0 && value < 65536
///     }
///     
///     fn is_valid_email(value: &str) -> bool {
///         // Email validation logic
///         value.contains('@') && value.contains('.')
///     }
/// }
/// ```
pub fn validation_fn(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as ValidationFnInput);

    let mut generated = quote! {};

    for func in input.functions {
        let fn_name = &func.name;
        let fn_args = &func.args;
        let fn_body = &func.body;
        let return_type = &func.return_type;

        // Extract the value parameter (assumed to be first)
        let _value_param = fn_args
            .first()
            .expect("Validation function must have at least one parameter");

        let args_tokens: Vec<_> = fn_args.iter().collect();

        generated.extend(quote! {
            pub fn #fn_name(mut self, #(#args_tokens),*) -> Self {
                let validation_result: #return_type = {
                    #fn_body
                };

                if !validation_result {
                    self.errors.push(ValidationError::InvalidValue {
                        field: self.field_name.clone(),
                        message: format!("failed validation: {}", stringify!(#fn_name)),
                    });
                }
                self
            }
        });
    }

    generated.into()
}

struct ValidationFnInput {
    functions: Vec<ValidationFn>,
}

struct ValidationFn {
    name: Ident,
    args: Punctuated<syn::FnArg, Token![,]>,
    return_type: syn::Type,
    body: syn::Block,
}

impl syn::parse::Parse for ValidationFnInput {
    fn parse(input: syn::parse::ParseStream) -> syn::Result<Self> {
        let mut functions = Vec::new();

        while !input.is_empty() {
            input.parse::<Token![fn]>()?;
            let name: Ident = input.parse()?;

            let content;
            syn::parenthesized!(content in input);
            let args = Punctuated::<syn::FnArg, Token![,]>::parse_terminated(&content)?;

            input.parse::<Token![->]>()?;
            let return_type: syn::Type = input.parse()?;

            let body: syn::Block = input.parse()?;

            functions.push(ValidationFn {
                name,
                args,
                return_type,
                body,
            });
        }

        Ok(ValidationFnInput { functions })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_validation_chain_parsing() {
        let input = quote! {
            username: String => {
                not_empty(),
                min_length(3),
                max_length(50),
            },
            port: u16 => {
                in_range(1, 65535),
            },
        };

        let parsed: ValidationChainInput = syn::parse2(input).unwrap();
        assert_eq!(parsed.fields.len(), 2);
        assert_eq!(parsed.fields[0].validations.len(), 3);
        assert_eq!(parsed.fields[1].validations.len(), 1);
    }

    #[test]
    fn test_validation_method_parsing() {
        let input = quote! {
            min_length(5)
        };

        let parsed: ValidationMethod = syn::parse2(input).unwrap();
        assert_eq!(parsed.name, "min_length");
        assert_eq!(parsed.args.len(), 1);
    }
}
