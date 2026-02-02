//! `EventPayload` derive macro implementation

use proc_macro2::TokenStream;
use quote::{format_ident, quote};
use syn::{parse_macro_input, Data, DeriveInput, Error, Fields, Ident, Type};

pub fn derive_event_payload_impl(input: proc_macro::TokenStream) -> proc_macro::TokenStream {
    let input = parse_macro_input!(input as DeriveInput);

    match derive_event_payload_inner(input) {
        Ok(tokens) => tokens.into(),
        Err(err) => err.to_compile_error().into(),
    }
}

fn derive_event_payload_inner(input: DeriveInput) -> syn::Result<TokenStream> {
    // Only works on structs
    match &input.data {
        Data::Struct(_) => {}
        _ => {
            return Err(Error::new_spanned(
                &input,
                "EventPayload can only be derived for structs",
            ))
        }
    }

    let name = &input.ident;

    // Parse attributes to get source and event_type
    let attrs = parse_event_payload_attrs(&input.attrs)?;

    let source = attrs.source;
    let event_type = attrs.event_type;
    let version = attrs.version;

    let builder_methods = generate_builder_methods(&input);
    let builder_impl = if builder_methods.is_empty() {
        quote! {}
    } else {
        quote! {
            impl #name {
                #(#builder_methods)*
            }
        }
    };

    // Generate the implementation
    // We use crate:: to work within sinex_types itself
    // The schema_registry code is conditionally compiled only when sqlx feature is enabled
    let expanded = quote! {
        const _: () = {
            use ::sinex_primitives::domain::{EventSource, EventType};
            use ::sinex_primitives::events::EventPayload;

            impl EventPayload for #name {
                const SOURCE: EventSource = EventSource::from_static(#source);
                const EVENT_TYPE: EventType = EventType::from_static(#event_type);
                const VERSION: &'static str = #version;
            }

            // Register this payload type with inventory
            const _: () = {
                use ::sinex_primitives::events::schema_registry;

                ::inventory::submit! {
                    schema_registry::PayloadInfo {
                        type_name: ::std::concat!(::std::module_path!(), "::", ::std::stringify!(#name)),
                        source: #source,
                        event_type: #event_type,
                        version: #version,
                        schema_fn: || {
                            let schema = ::schemars::schema_for!(#name);
                            ::serde_json::to_value(&schema).expect("Schema must serialize")
                        },
                    }
                }
            };
        };

        #builder_impl
    };

    Ok(expanded)
}

fn generate_builder_methods(input: &DeriveInput) -> Vec<TokenStream> {
    let mut methods = Vec::new();

    let Data::Struct(data_struct) = &input.data else {
        return methods;
    };

    let Fields::Named(fields_named) = &data_struct.fields else {
        return methods;
    };

    for field in &fields_named.named {
        if let Some(field_ident) = &field.ident {
            let method_ident = format_ident!("with_{}", field_ident);
            let doc = format!("Builder-style method for `{field_ident}'");
            let setter = build_setter(&method_ident, field_ident, &field.ty, &doc);
            methods.push(setter);
        }
    }

    methods
}

fn build_setter(method_ident: &Ident, field_ident: &Ident, ty: &Type, doc: &str) -> TokenStream {
    if let Some(inner) = option_inner_type(ty) {
        quote! {
            #[doc = #doc]
            pub fn #method_ident(mut self, value: impl Into<#inner>) -> Self {
                self.#field_ident = Some(value.into());
                self
            }
        }
    } else {
        quote! {
            #[doc = #doc]
            pub fn #method_ident(mut self, value: impl Into<#ty>) -> Self {
                self.#field_ident = value.into();
                self
            }
        }
    }
}

fn option_inner_type(ty: &Type) -> Option<&Type> {
    if let Type::Path(type_path) = ty {
        type_path.path.segments.last().and_then(|segment| {
            if segment.ident == "Option" {
                if let syn::PathArguments::AngleBracketed(args) = &segment.arguments {
                    args.args.first().and_then(|arg| {
                        if let syn::GenericArgument::Type(inner_ty) = arg {
                            Some(inner_ty)
                        } else {
                            None
                        }
                    })
                } else {
                    None
                }
            } else {
                None
            }
        })
    } else {
        None
    }
}

struct EventPayloadAttrs {
    source: String,
    event_type: String,
    version: String,
}

fn parse_event_payload_attrs(attrs: &[syn::Attribute]) -> syn::Result<EventPayloadAttrs> {
    let mut source = None;
    let mut event_type = None;
    let mut version = None;

    for attr in attrs {
        if attr.path().is_ident("event_payload") {
            // Parse the attribute using syn 2.0 style
            attr.parse_nested_meta(|meta| {
                if meta.path.is_ident("source") {
                    let value = meta.value()?;
                    let s: syn::LitStr = value.parse()?;
                    source = Some(s.value());
                    Ok(())
                } else if meta.path.is_ident("event_type") {
                    let value = meta.value()?;
                    let s: syn::LitStr = value.parse()?;
                    event_type = Some(s.value());
                    Ok(())
                } else if meta.path.is_ident("version") {
                    let value = meta.value()?;
                    let s: syn::LitStr = value.parse()?;
                    version = Some(s.value());
                    Ok(())
                } else {
                    Err(meta.error("unrecognized event_payload attribute"))
                }
            })?;
        }
    }

    let source =
        source.ok_or_else(|| Error::new_spanned(attrs.first(), "missing 'source' attribute"))?;
    let event_type = event_type
        .ok_or_else(|| Error::new_spanned(attrs.first(), "missing 'event_type' attribute"))?;
    let version = version.unwrap_or_else(|| "1.0.0".to_string()); // Default version

    Ok(EventPayloadAttrs {
        source,
        event_type,
        version,
    })
}
