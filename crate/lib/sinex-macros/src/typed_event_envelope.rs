use proc_macro::TokenStream;
use quote::{format_ident, quote};
use syn::{parse_macro_input, Attribute, DataEnum, DeriveInput, Fields, Ident, Type, Visibility};

/// Macro for generating typed event envelope implementations
///
/// This macro automatically generates helper methods for event envelopes,
/// including `to_json_event()` conversion and variant constructors.
/// It reduces boilerplate when working with strongly typed event envelopes.
///
/// # Usage
///
/// ```rust
/// #[typed_event_envelope]
/// pub enum EventEnvelope {
///     FileCreated(TypedRawEvent<FileCreatedPayload>),
///     FileModified(TypedRawEvent<FileModifiedPayload>),
///     FileDeleted(TypedRawEvent<FileDeletedPayload>),
///     Unknown(RawEvent),
/// }
/// ```
///
/// This generates:
/// - `to_json_event()` method that converts each variant to RawEvent
/// - Helper constructors for each variant
/// - Pattern matching utilities
/// - Serialization support
pub fn typed_event_envelope(attr: TokenStream, item: TokenStream) -> TokenStream {
    let _args = attr; // For now, we'll just store the raw attr tokens
    let input = parse_macro_input!(item as DeriveInput);

    let enum_name = &input.ident;
    let enum_vis = &input.vis;
    let enum_attrs = &input.attrs;

    let data_enum = match &input.data {
        syn::Data::Enum(data_enum) => data_enum,
        _ => panic!("typed_event_envelope can only be applied to enums"),
    };

    let mut generated = quote! {};

    // Generate the original enum with additional derives
    generated.extend(generate_enum_definition(
        enum_name, enum_vis, enum_attrs, data_enum,
    ));

    // Generate to_json_event implementation
    generated.extend(generate_to_json_event_impl(enum_name, data_enum));

    // Generate helper constructors
    generated.extend(generate_helper_constructors(enum_name, data_enum));

    // Generate pattern matching utilities
    generated.extend(generate_pattern_matching_utils(enum_name, data_enum));

    generated.into()
}

fn generate_enum_definition(
    enum_name: &Ident,
    enum_vis: &Visibility,
    enum_attrs: &[Attribute],
    data_enum: &DataEnum,
) -> proc_macro2::TokenStream {
    let variants = &data_enum.variants;

    quote! {
        #[derive(Debug, Clone)]
        #[derive(serde::Serialize, serde::Deserialize)]
        #(#enum_attrs)*
        #enum_vis enum #enum_name {
            #variants
        }
    }
}

fn generate_to_json_event_impl(
    enum_name: &Ident,
    data_enum: &DataEnum,
) -> proc_macro2::TokenStream {
    let match_arms = data_enum.variants.iter().map(|variant| {
        let variant_name = &variant.ident;

        match &variant.fields {
            Fields::Unnamed(fields) if fields.unnamed.len() == 1 => {
                // Handle TypedRawEvent<T> variants
                let field_type = &fields.unnamed[0].ty;
                if is_typed_raw_event_type(field_type) {
                    quote! {
                        #enum_name::#variant_name(event) => event.to_json_event(),
                    }
                } else if is_raw_event_type(field_type) {
                    quote! {
                        #enum_name::#variant_name(event) => event,
                    }
                } else {
                    quote! {
                        #enum_name::#variant_name(event) => event.into(),
                    }
                }
            }
            Fields::Unit => {
                // Handle unit variants
                quote! {
                    #enum_name::#variant_name => {
                        // Generate a placeholder event for unit variants
                        sinex_events::RawEvent::new(
                            "unknown".to_string(),
                            stringify!(#variant_name).to_string(),
                            serde_json::Value::Null,
                        )
                    },
                }
            }
            _ => {
                // Handle other field types
                quote! {
                    #enum_name::#variant_name(..) => {
                        // Default conversion for complex variants
                        sinex_events::RawEvent::new(
                            "unknown".to_string(),
                            stringify!(#variant_name).to_string(),
                            serde_json::Value::Null,
                        )
                    },
                }
            }
        }
    });

    // Collect variant names for the event_type_name method
    let variant_names: Vec<&Ident> = data_enum.variants.iter().map(|v| &v.ident).collect();
    let event_type_match_arms = variant_names.iter().map(|variant_name| {
        quote! {
            #enum_name::#variant_name(..) => stringify!(#variant_name),
        }
    });

    quote! {
        impl #enum_name {
            /// Convert this event envelope to a raw JSON event
            pub fn to_json_event(self) -> sinex_events::RawEvent {
                match self {
                    #(#match_arms)*
                }
            }

            /// Get the event type name as a string
            pub fn event_type_name(&self) -> &'static str {
                match self {
                    #(#event_type_match_arms)*
                }
            }

            /// Check if this is an unknown event
            pub fn is_unknown(&self) -> bool {
                matches!(self, #enum_name::Unknown(..))
            }
        }
    }
}

fn generate_helper_constructors(
    enum_name: &Ident,
    data_enum: &DataEnum,
) -> proc_macro2::TokenStream {
    let constructors = data_enum.variants.iter().filter_map(|variant| {
        let variant_name = &variant.ident;

        match &variant.fields {
            Fields::Unnamed(fields) if fields.unnamed.len() == 1 => {
                let field_type = &fields.unnamed[0].ty;
                let constructor_name =
                    format_ident!("new_{}", variant_name.to_string().to_lowercase());

                if is_typed_raw_event_type(field_type) {
                    Some(quote! {
                        /// Create a new #variant_name variant
                        pub fn #constructor_name(event: #field_type) -> Self {
                            #enum_name::#variant_name(event)
                        }
                    })
                } else {
                    Some(quote! {
                        /// Create a new #variant_name variant
                        pub fn #constructor_name(data: #field_type) -> Self {
                            #enum_name::#variant_name(data)
                        }
                    })
                }
            }
            Fields::Unit => {
                let constructor_name =
                    format_ident!("new_{}", variant_name.to_string().to_lowercase());
                Some(quote! {
                    /// Create a new #variant_name variant
                    pub fn #constructor_name() -> Self {
                        #enum_name::#variant_name
                    }
                })
            }
            _ => None,
        }
    });

    quote! {
        impl #enum_name {
            #(#constructors)*
        }
    }
}

fn generate_pattern_matching_utils(
    enum_name: &Ident,
    data_enum: &DataEnum,
) -> proc_macro2::TokenStream {
    let pattern_methods = data_enum.variants.iter().map(|variant| {
        let variant_name = &variant.ident;
        let method_name = format_ident!("is_{}", variant_name.to_string().to_lowercase());
        let as_method_name = format_ident!("as_{}", variant_name.to_string().to_lowercase());

        match &variant.fields {
            Fields::Unnamed(fields) if fields.unnamed.len() == 1 => {
                let field_type = &fields.unnamed[0].ty;
                quote! {
                    /// Check if this is a #variant_name variant
                    pub fn #method_name(&self) -> bool {
                        matches!(self, #enum_name::#variant_name(..))
                    }

                    /// Get the inner value if this is a #variant_name variant
                    pub fn #as_method_name(&self) -> Option<&#field_type> {
                        match self {
                            #enum_name::#variant_name(inner) => Some(inner),
                            _ => None,
                        }
                    }
                }
            }
            Fields::Unit => {
                quote! {
                    /// Check if this is a #variant_name variant
                    pub fn #method_name(&self) -> bool {
                        matches!(self, #enum_name::#variant_name)
                    }
                }
            }
            _ => quote! {
                /// Check if this is a #variant_name variant
                pub fn #method_name(&self) -> bool {
                    matches!(self, #enum_name::#variant_name(..))
                }
            },
        }
    });

    quote! {
        impl #enum_name {
            #(#pattern_methods)*
        }
    }
}

fn is_typed_raw_event_type(ty: &Type) -> bool {
    match ty {
        Type::Path(type_path) => {
            if let Some(segment) = type_path.path.segments.last() {
                segment.ident == "TypedRawEvent"
            } else {
                false
            }
        }
        _ => false,
    }
}

fn is_raw_event_type(ty: &Type) -> bool {
    match ty {
        Type::Path(type_path) => {
            if let Some(segment) = type_path.path.segments.last() {
                segment.ident == "RawEvent"
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
    use sinex_test_utils::sinex_test;
    use syn::parse_quote;

    #[sinex_test]
    fn test_typed_event_envelope_parsing() -> color_eyre::eyre::Result<()> {
        let input = quote! {
            pub enum EventEnvelope {
                FileCreated(TypedRawEvent<FileCreatedPayload>),
                FileModified(TypedRawEvent<FileModifiedPayload>),
                Unknown(RawEvent),
            }
        };

        let parsed: DeriveInput = syn::parse2(input).unwrap();
        assert_eq!(parsed.ident, "EventEnvelope");

        if let syn::Data::Enum(data_enum) = parsed.data {
            assert_eq!(data_enum.variants.len(), 3);
        } else {
            panic!("Expected enum");
        }
        Ok(())
    }

    #[sinex_test]
    fn test_typed_raw_event_detection() -> color_eyre::eyre::Result<()> {
        let typed_event: Type = parse_quote!(TypedRawEvent<FileCreatedPayload>);
        assert!(is_typed_raw_event_type(&typed_event));

        let raw_event: Type = parse_quote!(RawEvent);
        assert!(is_raw_event_type(&raw_event));
        assert!(!is_typed_raw_event_type(&raw_event));

        let other_type: Type = parse_quote!(String);
        assert!(!is_typed_raw_event_type(&other_type));
        assert!(!is_raw_event_type(&other_type));
        Ok(())
    }
}
