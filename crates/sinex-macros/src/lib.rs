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
//!     search = words,              // optional: none, words or words_and_trigram
//!     owns(field = content, size = size, when = stored), // optional: stored bytes the event owns
//! )]
//! ```
//!
//! `#[derive(DbCheck)]` on a fieldless enum lists its variants in snake_case,
//! matching `#[serde(rename_all = "snake_case")]`.
use proc_macro::TokenStream;
use proc_macro2::Span;
use quote::quote;
use syn::ext::IdentExt;
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

/// Return the serialized tag of an internally tagged enum.
#[proc_macro_derive(TagName, attributes(serde))]
pub fn tag_name(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    match expand_tag_name(&input) {
        Ok(tokens) => tokens.into(),
        Err(error) => error.to_compile_error().into(),
    }
}

fn serde_value(attrs: &[syn::Attribute], key: &str) -> syn::Result<Option<LitStr>> {
    let mut value = None;
    for attr in attrs.iter().filter(|attr| attr.path().is_ident("serde")) {
        let entries = attr.parse_args_with(
            syn::punctuated::Punctuated::<syn::Meta, syn::Token![,]>::parse_terminated,
        )?;
        for entry in entries {
            if entry.path().is_ident(key) {
                let syn::Meta::NameValue(entry) = entry else {
                    return Err(syn::Error::new_spanned(entry, "expected a string value"));
                };
                let syn::Expr::Lit(syn::ExprLit {
                    lit: syn::Lit::Str(text),
                    ..
                }) = &entry.value
                else {
                    return Err(syn::Error::new_spanned(entry, "expected a string value"));
                };
                value = Some(text.clone());
            }
        }
    }
    Ok(value)
}

fn expand_tag_name(input: &DeriveInput) -> syn::Result<proc_macro2::TokenStream> {
    let name = &input.ident;
    let Data::Enum(data) = &input.data else {
        return Err(syn::Error::new_spanned(name, "TagName derives on enums"));
    };
    let tag = serde_value(&input.attrs, "tag")?
        .ok_or_else(|| syn::Error::new_spanned(name, "TagName needs #[serde(tag = \"name\")]"))?;
    let method: Ident = syn::parse_str(&tag.value())
        .map_err(|_| syn::Error::new_spanned(&tag, "the tag must be a Rust method name"))?;
    let rename_all = serde_value(&input.attrs, "rename_all")?;
    if let Some(rename) = &rename_all {
        if !matches!(rename.value().as_str(), "snake_case" | "kebab-case") {
            return Err(syn::Error::new_spanned(
                rename,
                "TagName supports snake_case and kebab-case",
            ));
        }
    }
    let mut arms = Vec::new();
    for variant in &data.variants {
        let ident = &variant.ident;
        let text = match serde_value(&variant.attrs, "rename")? {
            Some(text) => text,
            None => {
                let name = ident.unraw().to_string();
                let renamed = match rename_all.as_ref().map(LitStr::value).as_deref() {
                    Some(rule @ ("snake_case" | "kebab-case")) => {
                        let mut text = String::new();
                        for (index, character) in name.chars().enumerate() {
                            if index > 0 && character.is_uppercase() {
                                text.push(if rule == "snake_case" { '_' } else { '-' });
                            }
                            // Serde lowercases only ASCII in variant names.
                            text.push(character.to_ascii_lowercase());
                        }
                        text
                    }
                    _ => name,
                };
                LitStr::new(&renamed, ident.span())
            }
        };
        let pattern = match &variant.fields {
            Fields::Unit => quote!(Self::#ident),
            Fields::Named(_) => quote!(Self::#ident { .. }),
            Fields::Unnamed(_) => quote!(Self::#ident(..)),
        };
        arms.push(quote!(#pattern => #text));
    }
    let (impl_generics, ty_generics, where_clause) = input.generics.split_for_impl();
    Ok(quote! {
        impl #impl_generics #name #ty_generics #where_clause {
            pub fn #method(&self) -> &'static str {
                match self { #(#arms),* }
            }
        }
    })
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
    let mut version: Option<syn::LitInt> = None;
    let mut search: Option<Ident> = None;
    let mut key: Option<Key> = None;
    let mut replace: Option<Ident> = None;
    let mut time: Option<Ident> = None;
    let mut quality: Option<Ident> = None;
    let mut removed_when: Option<Ident> = None;
    let mut keep_known: Vec<LitStr> = Vec::new();
    let mut owns: Option<(Ident, Ident, Option<Ident>)> = None;
    for attr in input.attrs.iter().filter(|a| a.path().is_ident("event")) {
        attr.parse_nested_meta(|meta| {
            if meta.path.is_ident("version") {
                let value: syn::LitInt = meta.value()?.parse()?;
                if value.base10_parse::<u64>()? == 0 {
                    return Err(meta.error("kind version must be positive"));
                }
                version = Some(value);
            } else if meta.path.is_ident("search") {
                search = Some(meta.value()?.parse()?);
            } else if meta.path.is_ident("type") {
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
            } else if meta.path.is_ident("owns") {
                let (mut field, mut size, mut when) = (None, None, None);
                meta.parse_nested_meta(|inner| {
                    if inner.path.is_ident("field") {
                        field = Some(inner.value()?.parse::<Ident>()?);
                    } else if inner.path.is_ident("size") {
                        size = Some(inner.value()?.parse::<Ident>()?);
                    } else if inner.path.is_ident("when") {
                        when = Some(inner.value()?.parse::<Ident>()?);
                    } else {
                        return Err(inner.error("owns takes `field`, `size` and optional `when`"));
                    }
                    Ok(())
                })?;
                let field = field.ok_or_else(|| meta.error("owns needs `field`"))?;
                let size = size.ok_or_else(|| meta.error("owns needs `size`"))?;
                owns = Some((field, size, when));
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
    let mut hints = Vec::new();
    for field in &fields.named {
        for attr in field.attrs.iter().filter(|a| a.path().is_ident("event")) {
            attr.parse_nested_meta(|meta| {
                if !meta.path.is_ident("class") {
                    return Err(meta.error("a field hint is `class = open`, `guarded` or `sealed`"));
                }
                let class: Ident = meta.value()?.parse()?;
                let variant = match class.to_string().as_str() {
                    "open" => quote!(Open),
                    "guarded" => quote!(Guarded),
                    "sealed" => quote!(Sealed),
                    _ => return Err(meta.error("class is `open`, `guarded` or `sealed`")),
                };
                let field_name = field.ident.as_ref().expect("named field").to_string();
                let pointer = format!("/{}", field_name.replace('~', "~0").replace('/', "~1"));
                hints.push(quote!((#pointer, ::sinex_model::Class::#variant)));
                Ok(())
            })?;
        }
    }
    let removed_tokens = removed_when.as_ref().map(|field| {
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

    let owns_tokens = match &owns {
        None => None,
        Some((field, size, when)) => {
            field_type(field)?;
            field_type(size)?;
            let guard = match when {
                Some(w) => {
                    field_type(w)?;
                    quote!(if !self.#w { return ::std::vec::Vec::new(); })
                }
                None => quote!(),
            };
            Some(quote! {
                fn owned_blobs(&self) -> ::std::vec::Vec<::sinex_model::OwnedBlob> {
                    #guard
                    ::std::vec![::sinex_model::OwnedBlob {
                        blob: self.#field.clone(),
                        size: self.#size,
                    }]
                }
            })
        }
    };

    let key_rule = match &key {
        Key::Location => quote!(::sinex_model::kinds::KeyRule::Location),
        Key::Fields(fields) => quote!(::sinex_model::kinds::KeyRule::Fields {
            fields: ::std::vec![#(#fields.to_string()),*]
        }),
    };
    let time_rule = match &time {
        None => quote!(::sinex_model::kinds::TimeRule::Unknown),
        Some(field) => {
            let field = field.to_string();
            let quality = quality.as_ref().expect("time quality checked");
            let variant = Ident::new(&camel(&quality.to_string()), quality.span());
            quote!(::sinex_model::kinds::TimeRule::FirstPresent {
                fields: ::std::vec![#field.to_string()], quality: ::sinex_model::TimeQuality::#variant,
            })
        }
    };
    let removed_rule = match &removed_when {
        None => quote!(::sinex_model::kinds::RemovedRule::Never),
        Some(field) => {
            let field = field.to_string();
            quote!(::sinex_model::kinds::RemovedRule::WhenPresent { field: #field.to_string() })
        }
    };
    let blob_rules = match &owns {
        None => quote!(::std::vec![]),
        Some((field, size, when)) => {
            let (field, size) = (field.to_string(), size.to_string());
            let when = match when {
                None => quote!(None),
                Some(field) => {
                    let field = field.to_string();
                    quote!(Some(#field.to_string()))
                }
            };
            quote!(
                ::std::vec![::sinex_model::kinds::OwnedBlobRule { field: #field.to_string(), size: #size.to_string(), when: #when }]
            )
        }
    };

    let version = version.map(|value| quote!(const VERSION: u64 = #value;));
    let search = match search {
        None => None,
        Some(value) => {
            let variant = match value.to_string().as_str() {
                "none" => quote!(None),
                "words" => quote!(Words),
                "words_and_trigram" => quote!(WordsAndTrigram),
                _ => {
                    return Err(syn::Error::new_spanned(
                        value,
                        "search is `none`, `words` or `words_and_trigram`",
                    ));
                }
            };
            Some(
                quote!(const SEARCH: ::sinex_model::kinds::Search = ::sinex_model::kinds::Search::#variant;),
            )
        }
    };
    Ok(quote! {
        impl ::sinex_model::EventPayload for #name {
            #version
            #search
            const EVENT_TYPE: &'static str = #event_type;
            const KEY: ::sinex_model::KeyKind = #key_tokens;
            const REPLACE: ::sinex_model::ReplaceRule = #replace_tokens;
            const KEEP_KNOWN: &'static [&'static str] = &[#(#keep_known),*];
            const FIELD_HINTS: &'static [(&'static str, ::sinex_model::Class)] = &[#(#hints),*];

            fn ts_orig(&self) -> ::sinex_model::TsOrig {
                #time_tokens
            }

            #removed_tokens

            #owns_tokens

            fn declaration() -> ::sinex_model::KindDeclaration {
                ::sinex_model::KindDeclaration {
                    kind: ::sinex_model::KindRef { name: Self::EVENT_TYPE.to_string(), version: Self::VERSION },
                    declaring_producer: "sinex".to_string(), payload_schema: Some(Self::payload_schema()),
                    normalization: ::sinex_model::kinds::Normalization::Rust,
                    key_rule: Some(#key_rule), replace_rule: Some(Self::REPLACE), time_rule: Some(#time_rule),
                    field_hints: Self::FIELD_HINTS.iter().map(|(field, class)| ::sinex_model::kinds::FieldHint { field: (*field).into(), class: *class }).collect(), removed_rule: Some(#removed_rule),
                    keep_known: ::std::vec![#(#keep_known.to_string()),*], owned_blobs: #blob_rules,
                    search: Self::SEARCH,
                }
            }

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
    let mut parse_arms = Vec::new();
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
        parse_arms.push(quote!(#text => Some(Self::#ident)));
        values.push(text);
    }
    Ok(quote! {
        impl ::sinex_model::DbCheck for #name {
            const VALUES: &'static [&'static str] = &[#(#values),*];
            fn as_db(&self) -> &'static str {
                match self { #(#arms),* }
            }
            fn from_db(text: &str) -> Option<Self> {
                match text { #(#parse_arms,)* _ => None }
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

#[cfg(test)]
mod tests;
