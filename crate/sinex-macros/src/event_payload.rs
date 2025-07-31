//! EventPayload derive macro implementation

use proc_macro2::TokenStream;
use quote::quote;
use syn::{parse_macro_input, Data, DeriveInput, Error};

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

    // Generate the implementation
    // Use $crate when inside sinex-events, otherwise use full path
    let expanded = quote! {
        const _: () = {
            use ::sinex_types::domain::{EventSource, EventType};

            impl crate::EventPayload for #name {
                const SOURCE: EventSource = EventSource::from_static(#source);
                const EVENT_TYPE: EventType = EventType::from_static(#event_type);
                const VERSION: &'static str = #version;
            }

            // Register this payload type with inventory
            ::inventory::submit! {
                crate::schema_registry::PayloadInfo {
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

    Ok(expanded)
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
        if !attr.path().is_ident("event_payload") {
            continue;
        }

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
