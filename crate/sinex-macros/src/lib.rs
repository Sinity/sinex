//! Derives for `sinex-model`.
//!
//! `#[derive(EventPayload)]` reads one `#[event(...)]` attribute:
//!
//! ```text
//! #[event(
//!     type = "command.executed",   // stored event type
//!     key = ["native_id"],         // or `key = location`
//!     replace = newer,             // or `replace = repeat`
//!     time = started_at,           // a `Timestamp` or `Option<Timestamp>` field; omit for unknown
//!     quality = intrinsic,         // TimeQuality variant, snake_case
//!     removed_when = deleted_at,   // optional `Option<_>` field: Some means removed at source
//!     keep_known = ["title"],      // optional fields a newer reading does not erase
//! )]
//! ```
//!
//! `#[derive(DbCheck)]` on a fieldless enum lists its variants in snake_case,
//! matching `#[serde(rename_all = "snake_case")]`.
use proc_macro::TokenStream;
use proc_macro2::Span;
use quote::quote;
use syn::{Data, DeriveInput, Fields, Ident, LitStr, Type, parse_macro_input};

#[proc_macro_derive(EventPayload, attributes(event))]
pub fn event_payload(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    match expand_event_payload(&input) {
        Ok(tokens) => tokens.into(),
        Err(e) => e.to_compile_error().into(),
    }
}

#[proc_macro_derive(DbCheck)]
pub fn db_check(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    match expand_db_check(&input) {
        Ok(tokens) => tokens.into(),
        Err(e) => e.to_compile_error().into(),
    }
}

enum Key {
    Location,
    Fields(Vec<LitStr>),
}

fn expand_event_payload(input: &DeriveInput) -> syn::Result<proc_macro2::TokenStream> {
    let name = &input.ident;
    let Data::Struct(data) = &input.data else {
        return Err(syn::Error::new_spanned(
            name,
            "EventPayload derives on structs",
        ));
    };
    let Fields::Named(fields) = &data.fields else {
        return Err(syn::Error::new_spanned(
            name,
            "EventPayload needs named fields",
        ));
    };
    let field_type = |ident: &Ident| -> syn::Result<&Type> {
        fields
            .named
            .iter()
            .find(|f| f.ident.as_ref() == Some(ident))
            .map(|f| &f.ty)
            .ok_or_else(|| syn::Error::new_spanned(ident, "no such field"))
    };

    let mut event_type: Option<LitStr> = None;
    let mut key: Option<Key> = None;
    let mut replace: Option<Ident> = None;
    let mut time: Option<Ident> = None;
    let mut quality: Option<Ident> = None;
    let mut removed_when: Option<Ident> = None;
    let mut keep_known: Vec<LitStr> = Vec::new();
    for attr in input.attrs.iter().filter(|a| a.path().is_ident("event")) {
        attr.parse_nested_meta(|meta| {
            if meta.path.is_ident("type") {
                event_type = Some(meta.value()?.parse()?);
            } else if meta.path.is_ident("key") {
                let value = meta.value()?;
                if value.peek(syn::token::Bracket) {
                    let content;
                    syn::bracketed!(content in value);
                    let list = content.parse_terminated(|p| p.parse::<LitStr>(), syn::Token![,])?;
                    key = Some(Key::Fields(list.into_iter().collect()));
                } else {
                    let word: Ident = value.parse()?;
                    if word != "location" {
                        return Err(syn::Error::new_spanned(
                            word,
                            "key is `location` or a list of field names",
                        ));
                    }
                    key = Some(Key::Location);
                }
            } else if meta.path.is_ident("replace") {
                replace = Some(meta.value()?.parse()?);
            } else if meta.path.is_ident("time") {
                time = Some(meta.value()?.parse()?);
            } else if meta.path.is_ident("quality") {
                quality = Some(meta.value()?.parse()?);
            } else if meta.path.is_ident("removed_when") {
                removed_when = Some(meta.value()?.parse()?);
            } else if meta.path.is_ident("keep_known") {
                let value = meta.value()?;
                let content;
                syn::bracketed!(content in value);
                let list = content.parse_terminated(|p| p.parse::<LitStr>(), syn::Token![,])?;
                keep_known = list.into_iter().collect();
            } else {
                return Err(meta.error("unknown event attribute"));
            }
            Ok(())
        })?;
    }
    let event_type = event_type.ok_or_else(|| syn::Error::new_spanned(name, "missing `type`"))?;
    let key = key.ok_or_else(|| syn::Error::new_spanned(name, "missing `key`"))?;
    let replace = replace.ok_or_else(|| syn::Error::new_spanned(name, "missing `replace`"))?;

    let key_tokens = match &key {
        Key::Location => quote!(::sinex_model::KeyKind::Location),
        Key::Fields(list) => {
            for field in list {
                field_type(&Ident::new(&field.value(), field.span()))?;
            }
            quote!(::sinex_model::KeyKind::Fields(&[#(#list),*]))
        }
    };
    let replace_tokens = match replace.to_string().as_str() {
        "newer" => quote!(::sinex_model::ReplaceRule::ReplaceWhenNewer),
        "repeat" => quote!(::sinex_model::ReplaceRule::SuppressRepeat),
        _ => {
            return Err(syn::Error::new_spanned(
                replace,
                "replace is `newer` or `repeat`",
            ));
        }
    };
    let time_tokens = match &time {
        None => quote!(::sinex_model::TsOrig::unknown()),
        Some(field) => {
            let quality = quality
                .as_ref()
                .ok_or_else(|| syn::Error::new_spanned(field, "`time` needs `quality`"))?;
            let variant = Ident::new(&camel(&quality.to_string()), quality.span());
            let optional = is_option(field_type(field)?);
            if optional {
                quote!(::sinex_model::TsOrig::maybe(self.#field, ::sinex_model::TimeQuality::#variant))
            } else {
                quote!(::sinex_model::TsOrig::known(self.#field, ::sinex_model::TimeQuality::#variant))
            }
        }
    };
    for field in &keep_known {
        let ident = Ident::new(&field.value(), field.span());
        if !is_option(field_type(&ident)?) {
            return Err(syn::Error::new_spanned(
                field,
                "keep_known fields are `Option<_>`",
            ));
        }
    }
    let removed_tokens = removed_when.map(|field| {
        quote! {
            fn record_state(&self) -> ::sinex_model::RecordState {
                if self.#field.is_some() {
                    ::sinex_model::RecordState::Removed
                } else {
                    ::sinex_model::RecordState::Present
                }
            }
        }
    });

    Ok(quote! {
        impl ::sinex_model::EventPayload for #name {
            const EVENT_TYPE: &'static str = #event_type;
            const KEY: ::sinex_model::KeyKind = #key_tokens;
            const REPLACE: ::sinex_model::ReplaceRule = #replace_tokens;
            const KEEP_KNOWN: &'static [&'static str] = &[#(#keep_known),*];

            fn ts_orig(&self) -> ::sinex_model::TsOrig {
                #time_tokens
            }

            #removed_tokens

            fn payload_schema() -> ::sinex_model::__private::serde_json::Value {
                ::sinex_model::__private::serde_json::to_value(
                    ::sinex_model::__private::schemars::schema_for!(#name),
                )
                .expect("schemas serialize")
            }
        }
    })
}

fn expand_db_check(input: &DeriveInput) -> syn::Result<proc_macro2::TokenStream> {
    let name = &input.ident;
    let Data::Enum(data) = &input.data else {
        return Err(syn::Error::new_spanned(name, "DbCheck derives on enums"));
    };
    let mut values = Vec::new();
    let mut arms = Vec::new();
    for variant in &data.variants {
        if !matches!(variant.fields, Fields::Unit) {
            return Err(syn::Error::new_spanned(
                variant,
                "DbCheck variants carry no data",
            ));
        }
        let ident = &variant.ident;
        let text = LitStr::new(&snake(&ident.to_string()), Span::call_site());
        arms.push(quote!(Self::#ident => #text));
        values.push(text);
    }
    Ok(quote! {
        impl ::sinex_model::DbCheck for #name {
            const VALUES: &'static [&'static str] = &[#(#values),*];
            fn as_db(&self) -> &'static str {
                match self { #(#arms),* }
            }
        }
    })
}

fn is_option(ty: &Type) -> bool {
    matches!(ty, Type::Path(p) if p.path.segments.last().is_some_and(|s| s.ident == "Option"))
}

fn snake(camel: &str) -> String {
    let mut out = String::new();
    for (i, c) in camel.chars().enumerate() {
        if c.is_uppercase() {
            if i > 0 {
                out.push('_');
            }
            out.extend(c.to_lowercase());
        } else {
            out.push(c);
        }
    }
    out
}

fn camel(snake: &str) -> String {
    snake
        .split('_')
        .map(|part| {
            let mut chars = part.chars();
            chars
                .next()
                .map(|c| c.to_uppercase().chain(chars).collect::<String>())
                .unwrap_or_default()
        })
        .collect()
}
