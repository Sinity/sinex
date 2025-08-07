use proc_macro::TokenStream;
use quote::quote;
use syn::{parse_macro_input, Ident, LitStr, Token};

/// Macro for generating event type registries with automatic constant generation
///
/// This macro reduces boilerplate when defining event types by automatically generating:
/// - Event type constants
/// - Source constants
/// - EventEnvelope enum variants
/// - EventEnvelope to_json_event() match arms
///
/// # Usage
///
/// ```rust
/// event_registry! {
///     sources {
///         FILESYSTEM => "fs",
///         SHELL => "shell",
///         CLIPBOARD => "clipboard",
///     }
///     
///     events {
///         filesystem => FILESYSTEM {
///             FILE_CREATED => event_types::file::CREATED with FileCreatedPayload,
///             FILE_MODIFIED => event_types::file::MODIFIED with FileModifiedPayload,
///             FILE_DELETED => event_types::file::DELETED with FileDeletedPayload,
///         },
///         shell => SHELL {
///             COMMAND_EXECUTED => "command.executed" with CommandExecutedPayload,
///             COMMAND_COMPLETED => "command.completed" with CommandCompletedPayload,
///         },
///         clipboard => CLIPBOARD {
///             COPIED => "clipboard.copied" with ClipboardCopiedPayload,
///             SELECTED => "clipboard.selected" with ClipboardSelectedPayload,
///         },
///     }
/// }
/// ```
pub fn event_registry(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as EventRegistryInput);

    let mut generated = quote! {};

    // Generate source constants
    generated.extend(generate_source_constants(&input.sources));

    // Generate event type constants
    generated.extend(generate_event_type_constants(&input.events));

    // Generate EventEnvelope enum
    generated.extend(generate_event_envelope_enum(&input.events));

    // Generate EventEnvelope impl
    generated.extend(generate_event_envelope_impl(&input.events));

    generated.into()
}

struct EventRegistryInput {
    sources: Vec<SourceDef>,
    events: Vec<EventCategory>,
}

struct SourceDef {
    name: Ident,
    value: LitStr,
}

struct EventCategory {
    name: Ident,
    _source: Ident,
    events: Vec<EventDef>,
}

struct EventDef {
    name: Ident,
    event_type: LitStr,
    payload: Ident,
}

impl syn::parse::Parse for EventRegistryInput {
    fn parse(input: syn::parse::ParseStream) -> syn::Result<Self> {
        let mut sources = Vec::new();
        let mut events = Vec::new();

        while !input.is_empty() {
            let lookahead = input.lookahead1();
            if lookahead.peek(syn::Ident) {
                let ident: Ident = input.parse()?;
                match ident.to_string().as_str() {
                    "sources" => {
                        let content;
                        syn::braced!(content in input);

                        while !content.is_empty() {
                            let name: Ident = content.parse()?;
                            content.parse::<Token![=>]>()?;
                            let value: LitStr = content.parse()?;
                            sources.push(SourceDef { name, value });

                            if !content.is_empty() {
                                content.parse::<Token![,]>()?;
                            }
                        }
                    }
                    "events" => {
                        let content;
                        syn::braced!(content in input);

                        while !content.is_empty() {
                            let name: Ident = content.parse()?;
                            content.parse::<Token![=>]>()?;
                            let source: Ident = content.parse()?;

                            let event_content;
                            syn::braced!(event_content in content);

                            let mut category_events = Vec::new();
                            while !event_content.is_empty() {
                                let event_name: Ident = event_content.parse()?;
                                event_content.parse::<Token![=>]>()?;
                                let event_type: LitStr = event_content.parse()?;
                                event_content.parse::<syn::Ident>()?; // "with"
                                let payload: Ident = event_content.parse()?;

                                category_events.push(EventDef {
                                    name: event_name,
                                    event_type,
                                    payload,
                                });

                                if !event_content.is_empty() {
                                    event_content.parse::<Token![,]>()?;
                                }
                            }

                            events.push(EventCategory {
                                name,
                                _source: source,
                                events: category_events,
                            });

                            if !content.is_empty() {
                                content.parse::<Token![,]>()?;
                            }
                        }
                    }
                    _ => {
                        return Err(syn::Error::new(
                            ident.span(),
                            "expected 'sources' or 'events'",
                        ))
                    }
                }
            } else {
                return Err(lookahead.error());
            }
        }

        Ok(EventRegistryInput { sources, events })
    }
}

fn generate_source_constants(sources: &[SourceDef]) -> proc_macro2::TokenStream {
    let constants = sources.iter().map(|source| {
        let name = &source.name;
        let value = &source.value;
        quote! {
            pub const #name: &str = #value;
        }
    });

    quote! {
        pub mod sources {
            #(#constants)*
        }
    }
}

fn generate_event_type_constants(events: &[EventCategory]) -> proc_macro2::TokenStream {
    let categories = events.iter().map(|category| {
        let category_name = &category.name;
        let constants = category.events.iter().map(|event| {
            let name = &event.name;
            let event_type = &event.event_type;
            quote! {
                pub const #name: &str = #event_type;
            }
        });

        quote! {
            pub mod #category_name {
                #(#constants)*
            }
        }
    });

    quote! {
        pub mod event_types {
            #(#categories)*
        }
    }
}

fn generate_event_envelope_enum(events: &[EventCategory]) -> proc_macro2::TokenStream {
    let variants = events.iter().flat_map(|category| {
        category.events.iter().map(|event| {
            let variant_name = &event.name;
            let payload = &event.payload;
            quote! {
                #variant_name(TypedRawEvent<#payload>),
            }
        })
    });

    quote! {
        #[derive(Debug, Clone)]
        pub enum EventEnvelope {
            #(#variants)*
            Unknown(RawEvent),
        }
    }
}

fn generate_event_envelope_impl(events: &[EventCategory]) -> proc_macro2::TokenStream {
    let match_arms = events.iter().flat_map(|category| {
        category.events.iter().map(|event| {
            let variant_name = &event.name;
            quote! {
                EventEnvelope::#variant_name(event) => event.to_json_event(),
            }
        })
    });

    quote! {
        impl EventEnvelope {
            pub fn to_json_event(self) -> RawEvent {
                match self {
                    #(#match_arms)*
                    EventEnvelope::Unknown(event) => event,
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[sinex_test]
    fn test_event_registry_parsing() {
        let input = quote! {
            sources {
                FILESYSTEM => "fs",
                SHELL => "shell",
            }

            events {
                filesystem => FILESYSTEM {
                    FILE_CREATED => event_types::file::CREATED with FileCreatedPayload,
                    FILE_MODIFIED => event_types::file::MODIFIED with FileModifiedPayload,
                },
            }
        };

        let parsed: EventRegistryInput = syn::parse2(input).unwrap();
        assert_eq!(parsed.sources.len(), 2);
        assert_eq!(parsed.events.len(), 1);
        assert_eq!(parsed.events[0].events.len(), 2);
    }
}
