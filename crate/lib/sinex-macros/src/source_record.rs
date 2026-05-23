//! `#[derive(SourceRecord)]` proc-macro.
//!
//! Generates a `&'static DeclarativeParserSpec` constant + `impl MaterialParser`
//! for a struct, from the struct's `#[source_record(...)]` attribute and its
//! fields' `#[source]` / `#[privacy]` / `#[timestamp]` / `#[occurrence_key]` /
//! `#[suppress_if]` / `#[required]` / `#[skip]` / `#[default]` attributes.
//!
//! See `crate/lib/sinex-node-sdk/docs/declarative_parser.md` for the locked
//! design.

use proc_macro2::TokenStream;
use quote::{format_ident, quote};
use syn::{Data, DataStruct, DeriveInput, Error, Field, Fields, Type, parse_macro_input};

pub fn derive_source_record_impl(input: proc_macro::TokenStream) -> proc_macro::TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    match derive_source_record_inner(&input) {
        Ok(tokens) => tokens.into(),
        Err(err) => err.to_compile_error().into(),
    }
}

fn derive_source_record_inner(input: &DeriveInput) -> syn::Result<TokenStream> {
    let struct_name = &input.ident;

    // Parse the struct-level #[source_record(...)] attribute.
    let attrs = parse_source_record_attrs(&input.attrs)?;

    // Collect fields.
    let fields = match &input.data {
        Data::Struct(DataStruct {
            fields: Fields::Named(named),
            ..
        }) => &named.named,
        _ => {
            return Err(Error::new_spanned(
                input,
                "#[derive(SourceRecord)] only works on structs with named fields",
            ));
        }
    };

    // Parse each field's attributes into a FieldDecl.
    let mut field_decls = Vec::with_capacity(fields.len());
    for field in fields {
        field_decls.push(parse_field_decl(field)?);
    }

    // --- Extension A: validate discriminator consistency ---
    // Find the field(s) with event_dispatch mappings.
    let dispatch_fields: Vec<&FieldDecl> = field_decls
        .iter()
        .filter(|d| !d.event_dispatch.is_empty())
        .collect();

    // Validate: at most one field may carry event_dispatch.
    if dispatch_fields.len() > 1 {
        return Err(Error::new_spanned(
            input,
            format!(
                "at most one field may have #[event_dispatch(...)]; found on fields: {}",
                dispatch_fields
                    .iter()
                    .map(|d| d.name.as_str())
                    .collect::<Vec<_>>()
                    .join(", ")
            ),
        ));
    }

    // If discriminator_field is set but no dispatch field found, error.
    if let Some(ref disc_field) = attrs.discriminator_field {
        let found = field_decls.iter().any(|d| &d.name == disc_field);
        if !found {
            return Err(Error::new_spanned(
                input,
                format!(
                    "discriminator = \"{disc_field}\" but no field with that name exists on the struct"
                ),
            ));
        }
    }

    // Build the discriminator token.  Two ways to declare:
    // 1. `discriminator = "kind"` on the struct + `#[event_dispatch(...)]` on the field.
    // 2. Just `#[event_dispatch(...)]` on the field (field name becomes the discriminator).
    let discriminator_token = {
        // Prefer the struct-level `discriminator = "..."` key.  Fall back to the
        // field that carries event_dispatch.
        let disc_info: Option<(&str, &FieldDecl)> =
            if let Some(ref disc_name) = attrs.discriminator_field {
                dispatch_fields
                    .first()
                    .map(|fd| (disc_name.as_str(), *fd))
                    .or({
                        // discriminator declared but no dispatch field — still build discriminator
                        // with an empty case table (unusual, but valid).
                        None
                    })
            } else {
                dispatch_fields.first().map(|fd| (fd.name.as_str(), *fd))
            };

        if let Some((disc_field_name, fd)) = disc_info {
            let on_unknown_tok =
                on_unknown_token(attrs.on_unknown.as_deref().unwrap_or("default"))?;
            let cases: Vec<TokenStream> = fd
                .event_dispatch
                .iter()
                .map(|(val, et)| {
                    quote! {
                        _sdk_parser::DiscriminatorCase {
                            value: #val.to_string(),
                            event_type: _sdk_domain::EventType::from_static(#et),
                            event_source: None,
                        }
                    }
                })
                .collect();

            quote! {
                Some(_sdk_parser::Discriminator {
                    field: #disc_field_name.to_string(),
                    cases: vec![ #(#cases),* ],
                    on_unknown: #on_unknown_tok,
                })
            }
        } else {
            quote!(None)
        }
    };

    // --- Manifest declared_event_types ---
    // Collect all event types: base type + any dispatch cases.
    let all_event_type_pairs: Vec<TokenStream> = {
        let mut pairs = vec![]; // (event_source, event_type) tokens
        let base_event_source_lit = attrs.event_source.clone().unwrap_or_else(|| {
            attrs
                .source_unit_id
                .split('.')
                .next()
                .unwrap_or(&attrs.source_unit_id)
                .to_string()
        });
        let base_et = &attrs.event_type;
        let base_es = &base_event_source_lit;
        pairs.push(quote! {
            (_sdk_domain::EventSource::from_static(#base_es), _sdk_domain::EventType::from_static(#base_et))
        });
        // Add dispatch cases.
        for fd in &dispatch_fields {
            for (_, et) in &fd.event_dispatch {
                pairs.push(quote! {
                    (_sdk_domain::EventSource::from_static(#base_es), _sdk_domain::EventType::from_static(#et))
                });
            }
        }
        pairs
    };

    // Generate the spec constant.
    let spec_const_name = format_ident!(
        "_SOURCE_RECORD_SPEC_{}",
        struct_name.to_string().to_uppercase()
    );

    let parser_id_lit = &attrs.id;
    let source_unit_id_lit = &attrs.source_unit_id;
    let event_type_lit = &attrs.event_type;
    let event_source_lit = attrs.event_source.clone().unwrap_or_else(|| {
        // Default: first dot-segment of source_unit_id (e.g.
        // "terminal.atuin-history" → "terminal").
        attrs
            .source_unit_id
            .split('.')
            .next()
            .unwrap_or(&attrs.source_unit_id)
            .to_string()
    });
    let version_lit = attrs.version.clone().unwrap_or_else(|| "1.0.0".to_string());
    let default_privacy_context_token = privacy_context_token(
        attrs
            .default_privacy_context
            .as_deref()
            .unwrap_or("Metadata"),
    )?;
    let input_format_token = input_format_token(&attrs.input_shape)?;

    // Determine whether any field uses carry — if so, use StatefulDeclarativeParser.
    let has_carry_fields = field_decls.iter().any(|d| d.carry.is_some());

    let field_specs = field_decls
        .iter()
        .map(field_decl_to_token)
        .collect::<syn::Result<Vec<_>>>()?;

    // For stateful structs, we persist carry-state between parse_record calls via
    // a Mutex<StatefulDeclarativeParser> static inside the anonymous const block.
    // Each `const _: () = { ... }` expansion has its own private scope, so the
    // static name doesn't collide across struct types.
    let stateful_impl = if has_carry_fields {
        quote! {
            #[allow(non_upper_case_globals)]
            static __STATEFUL_PARSER: LazyLock<
                ::std::sync::Mutex<_sdk_parser::StatefulDeclarativeParser>
            > = LazyLock::new(|| {
                ::std::sync::Mutex::new(
                    _sdk_parser::StatefulDeclarativeParser::new(
                        ::std::clone::Clone::clone(&*#spec_const_name)
                    )
                )
            });
        }
    } else {
        quote! {}
    };

    let parse_record_impl = if has_carry_fields {
        quote! {
            async fn parse_record(
                &mut self,
                record: _sdk_parser_types::SourceRecord,
                ctx: &_sdk_parser_types::ParserContext,
            ) -> ::sinex_node_sdk::parser::ParserResult<Vec<_sdk_parser_types::ParsedEventIntent>> {
                let binding = _sdk_parser::BindingConfig::default();
                self.parse_record_with_binding(record, ctx, &binding).await
            }

            async fn parse_record_with_binding(
                &mut self,
                record: _sdk_parser_types::SourceRecord,
                ctx: &_sdk_parser_types::ParserContext,
                binding: &_sdk_parser::BindingConfig,
            ) -> ::sinex_node_sdk::parser::ParserResult<Vec<_sdk_parser_types::ParsedEventIntent>> {
                let mut guard = __STATEFUL_PARSER.lock().unwrap_or_else(|e| e.into_inner());
                guard.evaluate(record, ctx, binding)
                    .map_err(|e| ::sinex_node_sdk::parser::ParserError::Field(e.to_string()))
            }
        }
    } else {
        quote! {
            async fn parse_record(
                &mut self,
                record: _sdk_parser_types::SourceRecord,
                ctx: &_sdk_parser_types::ParserContext,
            ) -> ::sinex_node_sdk::parser::ParserResult<Vec<_sdk_parser_types::ParsedEventIntent>> {
                let binding = _sdk_parser::BindingConfig::default();
                self.parse_record_with_binding(record, ctx, &binding).await
            }

            async fn parse_record_with_binding(
                &mut self,
                record: _sdk_parser_types::SourceRecord,
                ctx: &_sdk_parser_types::ParserContext,
                binding: &_sdk_parser::BindingConfig,
            ) -> ::sinex_node_sdk::parser::ParserResult<Vec<_sdk_parser_types::ParsedEventIntent>> {
                _sdk_parser::DeclarativeParser::evaluate(
                    Self::parser_spec(),
                    record,
                    ctx,
                    binding,
                ).map_err(|e| ::sinex_node_sdk::parser::ParserError::Field(e.to_string()))
            }
        }
    };

    let generated = quote! {
        const _: () = {
            use ::sinex_node_sdk::parser as _sdk_parser;
            use ::sinex_primitives as _sdk_primitives;
            use ::sinex_primitives::domain as _sdk_domain;
            use ::sinex_primitives::parser as _sdk_parser_types;
            use ::sinex_primitives::privacy as _sdk_privacy;
            use std::sync::LazyLock;

            #[allow(non_upper_case_globals)]
            static #spec_const_name: LazyLock<_sdk_parser::DeclarativeParserSpec> =
                LazyLock::new(|| _sdk_parser::DeclarativeParserSpec {
                    parser_id: _sdk_parser_types::ParserId::from_static(#parser_id_lit),
                    parser_version: #version_lit.into(),
                    source_unit_id: _sdk_parser_types::SourceUnitId::from_static(#source_unit_id_lit),
                    event_source: _sdk_domain::EventSource::from_static(#event_source_lit),
                    event_type: _sdk_domain::EventType::from_static(#event_type_lit),
                    default_privacy_context: _sdk_privacy::ProcessingContext::#default_privacy_context_token,
                    input_format: _sdk_parser::InputFormat::#input_format_token,
                    fields: vec![ #(#field_specs),* ],
                    discriminator: #discriminator_token,
                });

            // Stateful parser static for carry-across-records support (Extension F).
            // Placed here (inside const _: ()) so it can reference #spec_const_name.
            // This is a module-level static scoped to this anonymous const.
            #stateful_impl

            impl #struct_name {
                /// Returns the static parser spec generated from the struct's
                /// `#[source_record(...)]` and field attributes.
                pub fn parser_spec() -> &'static _sdk_parser::DeclarativeParserSpec {
                    &*#spec_const_name
                }
            }

            #[::async_trait::async_trait]
            impl ::sinex_node_sdk::parser::MaterialParser for #struct_name {
                type Config = ();

                fn manifest(&self) -> _sdk_parser_types::ParserManifest {
                    let spec = Self::parser_spec();
                    _sdk_parser_types::ParserManifest {
                        parser_id: spec.parser_id.clone(),
                        parser_version: spec.parser_version.clone(),
                        accepted_input_shapes: vec![input_format_to_kind(spec.input_format)],
                        source_unit_id: spec.source_unit_id.clone(),
                        declared_event_types: vec![ #(#all_event_type_pairs),* ],
                        privacy_contexts: collect_privacy_contexts(spec),
                        proof_obligations: Vec::new(),
                        description: format!("Declarative parser for {}", stringify!(#struct_name)),
                    }
                }

                #parse_record_impl
            }

            // Helpers internal to the generated module.
            fn input_format_to_kind(
                fmt: _sdk_parser::InputFormat,
            ) -> _sdk_parser_types::InputShapeKind {
                use _sdk_parser::InputFormat;
                use _sdk_parser_types::InputShapeKind;
                match fmt {
                    InputFormat::Json => InputShapeKind::AppendOnlyFile, // JSON-per-line
                    InputFormat::TabSeparated | InputFormat::RawLine => InputShapeKind::AppendOnlyFile,
                    InputFormat::CsvRow => InputShapeKind::AppendOnlyFile,
                    InputFormat::SqliteRow => InputShapeKind::SqliteQuery,
                }
            }

            fn collect_privacy_contexts(
                spec: &_sdk_parser::DeclarativeParserSpec,
            ) -> Vec<_sdk_privacy::ProcessingContext> {
                let mut seen: Vec<_sdk_privacy::ProcessingContext> = Vec::new();
                seen.push(spec.default_privacy_context);
                for field in &spec.fields {
                    if let Some(c) = field.privacy_context {
                        if !seen.contains(&c) {
                            seen.push(c);
                        }
                    }
                }
                seen
            }
        };
    };

    Ok(generated)
}

// ---------------------------------------------------------------------------
// Struct-level attribute parsing
// ---------------------------------------------------------------------------

#[derive(Debug, Default)]
struct SourceRecordAttrs {
    id: String,
    source_unit_id: String,
    input_shape: String,
    event_type: String,
    event_source: Option<String>,
    default_privacy_context: Option<String>,
    version: Option<String>,
    // Extension A: discriminator support
    discriminator_field: Option<String>,
    on_unknown: Option<String>,
}

fn parse_source_record_attrs(attrs: &[syn::Attribute]) -> syn::Result<SourceRecordAttrs> {
    let mut id = None;
    let mut source_unit_id = None;
    let mut input_shape = None;
    let mut event_type = None;
    let mut event_source = None;
    let mut default_privacy_context = None;
    let mut version = None;
    let mut discriminator_field = None;
    let mut on_unknown = None;

    let mut found = false;
    for attr in attrs {
        if !attr.path().is_ident("source_record") {
            continue;
        }
        found = true;
        attr.parse_nested_meta(|meta| {
            let key = meta
                .path
                .get_ident()
                .map(std::string::ToString::to_string)
                .ok_or_else(|| meta.error("expected attribute key"))?;
            let value = meta.value()?;
            let s: syn::LitStr = value.parse()?;
            match key.as_str() {
                "id" => id = Some(s.value()),
                "source_unit_id" => source_unit_id = Some(s.value()),
                "input_shape" => input_shape = Some(s.value()),
                "event_type" => event_type = Some(s.value()),
                "event_source" => event_source = Some(s.value()),
                "default_privacy_context" => default_privacy_context = Some(s.value()),
                "version" => version = Some(s.value()),
                "discriminator" => discriminator_field = Some(s.value()),
                "on_unknown" => on_unknown = Some(s.value()),
                other => {
                    return Err(meta.error(format!(
                        "unknown source_record attribute '{other}'; expected one of: id, \
                         source_unit_id, input_shape, event_type, event_source, \
                         default_privacy_context, version, discriminator, on_unknown"
                    )));
                }
            }
            Ok(())
        })?;
    }

    if !found {
        return Err(Error::new_spanned(
            attrs.first(),
            "missing #[source_record(...)] attribute on the struct",
        ));
    }

    let id = id.ok_or_else(|| Error::new_spanned(attrs.first(), "source_record: missing 'id'"))?;
    let source_unit_id = source_unit_id.ok_or_else(|| {
        Error::new_spanned(attrs.first(), "source_record: missing 'source_unit_id'")
    })?;
    let input_shape = input_shape
        .ok_or_else(|| Error::new_spanned(attrs.first(), "source_record: missing 'input_shape'"))?;
    let event_type = event_type
        .ok_or_else(|| Error::new_spanned(attrs.first(), "source_record: missing 'event_type'"))?;

    Ok(SourceRecordAttrs {
        id,
        source_unit_id,
        input_shape,
        event_type,
        event_source,
        default_privacy_context,
        version,
        discriminator_field,
        on_unknown,
    })
}

// ---------------------------------------------------------------------------
// Field-level attribute parsing
// ---------------------------------------------------------------------------

#[derive(Debug)]
struct FieldDecl {
    name: String,
    field_type: FieldType,
    source: FieldSourceDecl,
    required: bool,
    default: Option<String>, // String literal, parsed as JSON value at evaluator time
    skip_payload: bool,
    privacy_context: Option<String>,
    occurrence_key: bool,
    timestamp: Option<TimestampDecl>,
    suppress_if: Option<SuppressDecl>,
    // Extension A: discriminator event_dispatch mapping (value => event_type).
    // Only one field per struct may have this set.
    event_dispatch: Vec<(String, String)>, // (discriminator_value, event_type)
    // Extension F: carry_across_records
    carry: Option<CarryDecl>,
}

/// Parsed `#[carry_across_records(...)]` attribute.
#[derive(Debug)]
struct CarryDecl {
    policy: String,
    from_carry: Option<String>,
    clear_on_use: bool,
}

#[derive(Debug)]
enum FieldType {
    String,
    Integer,
    Number,
    Boolean,
    Json,
}

#[derive(Debug)]
enum FieldSourceDecl {
    JsonPointer(String),
    ColumnIndex(usize),
    ColumnName(String),
    RawLine,
}

#[derive(Debug)]
struct TimestampDecl {
    format: String,
    fallback: Option<String>,
}

#[derive(Debug)]
struct SuppressDecl {
    binding_field: String,
    whole_event: bool,
}

fn parse_field_decl(field: &Field) -> syn::Result<FieldDecl> {
    let name = field
        .ident
        .as_ref()
        .ok_or_else(|| Error::new_spanned(field, "field must be named"))?
        .to_string();

    let field_type = infer_field_type(&field.ty);
    let mut source: Option<FieldSourceDecl> = None;
    let mut required = false;
    let mut default: Option<String> = None;
    let mut skip_payload = false;
    let mut privacy_context: Option<String> = None;
    let mut occurrence_key = false;
    let mut timestamp: Option<TimestampDecl> = None;
    let mut suppress_if: Option<SuppressDecl> = None;
    let mut event_dispatch: Vec<(String, String)> = Vec::new();
    let mut carry: Option<CarryDecl> = None;

    for attr in &field.attrs {
        let path = match attr.path().get_ident() {
            Some(i) => i.to_string(),
            None => continue,
        };
        match path.as_str() {
            "source" => {
                attr.parse_nested_meta(|meta| {
                    let key = meta
                        .path
                        .get_ident()
                        .map(std::string::ToString::to_string)
                        .ok_or_else(|| meta.error("expected source attribute key"))?;
                    match key.as_str() {
                        "json_pointer" => {
                            let v: syn::LitStr = meta.value()?.parse()?;
                            source = Some(FieldSourceDecl::JsonPointer(v.value()));
                            Ok(())
                        }
                        "column_index" => {
                            let v: syn::LitInt = meta.value()?.parse()?;
                            source = Some(FieldSourceDecl::ColumnIndex(v.base10_parse()?));
                            Ok(())
                        }
                        "column_name" => {
                            let v: syn::LitStr = meta.value()?.parse()?;
                            source = Some(FieldSourceDecl::ColumnName(v.value()));
                            Ok(())
                        }
                        "raw_line" => {
                            source = Some(FieldSourceDecl::RawLine);
                            Ok(())
                        }
                        other => Err(meta.error(format!(
                            "unknown source kind '{other}'; expected one of: \
                             json_pointer, column_index, column_name, raw_line"
                        ))),
                    }
                })?;
            }
            "required" => {
                required = true;
            }
            "skip" => {
                skip_payload = true;
            }
            "occurrence_key" => {
                occurrence_key = true;
            }
            "default" => {
                // Support both #[default = "0"] (MetaNameValue) and #[default("0")] (List).
                if let syn::Meta::NameValue(nv) = &attr.meta {
                    if let syn::Expr::Lit(syn::ExprLit {
                        lit: syn::Lit::Str(s),
                        ..
                    }) = &nv.value
                    {
                        default = Some(s.value());
                    } else {
                        return Err(Error::new_spanned(
                            &nv.value,
                            "expected string literal: #[default = \"...\"]",
                        ));
                    }
                } else {
                    let v: syn::LitStr = attr.parse_args()?;
                    default = Some(v.value());
                }
            }
            "privacy" => {
                attr.parse_nested_meta(|meta| {
                    if meta.path.is_ident("context") {
                        let v: syn::LitStr = meta.value()?.parse()?;
                        privacy_context = Some(v.value());
                        Ok(())
                    } else {
                        Err(meta.error("expected #[privacy(context = \"...\")]"))
                    }
                })?;
            }
            "timestamp" => {
                let mut format = None;
                let mut fallback = None;
                attr.parse_nested_meta(|meta| {
                    if meta.path.is_ident("format") {
                        let v: syn::LitStr = meta.value()?.parse()?;
                        format = Some(v.value());
                        Ok(())
                    } else if meta.path.is_ident("fallback") {
                        let v: syn::LitStr = meta.value()?.parse()?;
                        fallback = Some(v.value());
                        Ok(())
                    } else {
                        Err(meta.error(
                            "expected timestamp(format = \"...\") or timestamp(fallback = \"...\")",
                        ))
                    }
                })?;
                let format = format
                    .ok_or_else(|| Error::new_spanned(attr, "timestamp: missing 'format'"))?;
                timestamp = Some(TimestampDecl { format, fallback });
            }
            "suppress_if" => {
                let mut binding_field = None;
                let mut whole_event = false;
                attr.parse_nested_meta(|meta| {
                    if meta.path.is_ident("binding_field") {
                        let v: syn::LitStr = meta.value()?.parse()?;
                        binding_field = Some(v.value());
                        Ok(())
                    } else if meta.path.is_ident("whole_event") {
                        let v: syn::LitBool = meta.value()?.parse()?;
                        whole_event = v.value;
                        Ok(())
                    } else {
                        Err(meta.error(
                            "expected suppress_if(binding_field = \"...\") or suppress_if(whole_event = true)",
                        ))
                    }
                })?;
                let binding_field = binding_field.ok_or_else(|| {
                    Error::new_spanned(attr, "suppress_if: missing 'binding_field'")
                })?;
                suppress_if = Some(SuppressDecl {
                    binding_field,
                    whole_event,
                });
            }
            // --- Extension A: #[event_dispatch("val" => "event.type", ...)] ---
            "event_dispatch" => {
                event_dispatch = parse_event_dispatch_attr(attr)?;
            }
            // --- Extension F: #[carry_across_records(policy = "...", ...)] ---
            "carry_across_records" => {
                carry = Some(parse_carry_attr(attr)?);
            }
            _ => {} // Other attributes (serde, etc.) ignored.
        }
    }

    // Fields with `carry.policy = ConsumeCarried` don't need a #[source] —
    // they pull from carry-state, not from the record bytes.
    let needs_source = carry.as_ref().is_none_or(|c| c.policy != "consume_carried");
    let source = if needs_source {
        Some(source.ok_or_else(|| {
            Error::new_spanned(
                field,
                format!(
                    "field '{name}' is missing a #[source(...)] attribute (expected \
                     json_pointer, column_index, column_name, or raw_line)"
                ),
            )
        })?)
    } else {
        // For ConsumeCarried fields without a #[source], use RawLine as a
        // placeholder — the evaluator won't extract from the record for this field.
        source.or(Some(FieldSourceDecl::RawLine))
    }
    .ok_or_else(|| {
        Error::new_spanned(
            field,
            format!("field '{name}' is missing a #[source(...)] attribute"),
        )
    })?;

    Ok(FieldDecl {
        name,
        field_type,
        source,
        required,
        default,
        skip_payload,
        privacy_context,
        occurrence_key,
        timestamp,
        suppress_if,
        event_dispatch,
        carry,
    })
}

/// Parse `#[event_dispatch("Created" => "file.created", "Deleted" => "file.deleted", ...)]`.
///
/// Returns `Vec<(discriminator_value, event_type)>`.
fn parse_event_dispatch_attr(attr: &syn::Attribute) -> syn::Result<Vec<(String, String)>> {
    use proc_macro2::TokenTree;

    let mut cases: Vec<(String, String)> = Vec::new();

    // The attribute body is a token stream like:
    //   "Created" => "file.created", "Deleted" => "file.deleted"
    // We parse it using a custom token-stream walker.
    let tokens: proc_macro2::TokenStream = attr.parse_args()?;
    let mut iter = tokens.into_iter().peekable();

    loop {
        // Expect: LitStr ("Created")
        let key = match iter.next() {
            Some(TokenTree::Literal(lit)) => {
                let s = lit.to_string();
                // Strip surrounding quotes.
                if s.starts_with('"') && s.ends_with('"') {
                    s[1..s.len() - 1].to_string()
                } else {
                    return Err(syn::Error::new(lit.span(), "expected string literal"));
                }
            }
            Some(tok) => return Err(syn::Error::new(tok.span(), "expected string literal")),
            None => break, // clean end of token stream
        };

        // Expect: `=>`
        match iter.next() {
            Some(TokenTree::Punct(p)) if p.as_char() == '=' => {}
            Some(tok) => return Err(syn::Error::new(tok.span(), "expected `=>`")),
            None => {
                return Err(syn::Error::new(
                    proc_macro2::Span::call_site(),
                    "expected `=>`",
                ));
            }
        }
        match iter.next() {
            Some(TokenTree::Punct(p)) if p.as_char() == '>' => {}
            Some(tok) => return Err(syn::Error::new(tok.span(), "expected `>`")),
            None => {
                return Err(syn::Error::new(
                    proc_macro2::Span::call_site(),
                    "expected `>`",
                ));
            }
        }

        // Expect: LitStr ("file.created")
        let event_type = match iter.next() {
            Some(TokenTree::Literal(lit)) => {
                let s = lit.to_string();
                if s.starts_with('"') && s.ends_with('"') {
                    s[1..s.len() - 1].to_string()
                } else {
                    return Err(syn::Error::new(
                        lit.span(),
                        "expected string literal for event type",
                    ));
                }
            }
            Some(tok) => {
                return Err(syn::Error::new(
                    tok.span(),
                    "expected string literal for event type",
                ));
            }
            None => {
                return Err(syn::Error::new(
                    proc_macro2::Span::call_site(),
                    "expected event type string",
                ));
            }
        };

        cases.push((key, event_type));

        // Optional trailing comma.
        match iter.peek() {
            Some(TokenTree::Punct(p)) if p.as_char() == ',' => {
                iter.next();
            }
            _ => {}
        }
    }

    if cases.is_empty() {
        return Err(syn::Error::new_spanned(
            attr,
            "#[event_dispatch] must have at least one mapping",
        ));
    }
    Ok(cases)
}

/// Parse `#[carry_across_records(policy = "set_then_consume", from_carry = "ts", clear_on_use = true)]`.
fn parse_carry_attr(attr: &syn::Attribute) -> syn::Result<CarryDecl> {
    let mut policy = None;
    let mut from_carry = None;
    let mut clear_on_use = false;

    attr.parse_nested_meta(|meta| {
        let key = meta
            .path
            .get_ident()
            .map(std::string::ToString::to_string)
            .ok_or_else(|| meta.error("expected carry_across_records attribute key"))?;
        match key.as_str() {
            "policy" => {
                let v: syn::LitStr = meta.value()?.parse()?;
                policy = Some(v.value());
            }
            "from_carry" => {
                let v: syn::LitStr = meta.value()?.parse()?;
                from_carry = Some(v.value());
            }
            "clear_on_use" => {
                let v: syn::LitBool = meta.value()?.parse()?;
                clear_on_use = v.value;
            }
            other => {
                return Err(meta.error(format!(
                    "unknown carry_across_records attribute '{other}'; expected one of: \
                     policy, from_carry, clear_on_use"
                )));
            }
        }
        Ok(())
    })?;

    let policy =
        policy.ok_or_else(|| Error::new_spanned(attr, "carry_across_records: missing 'policy'"))?;

    // Validate policy value.
    match policy.as_str() {
        "set_then_consume" | "set_then_retain" | "consume_carried" => {}
        other => {
            return Err(Error::new_spanned(
                attr,
                format!(
                    "unknown carry policy '{other}'; expected one of: \
                     set_then_consume, set_then_retain, consume_carried"
                ),
            ));
        }
    }

    Ok(CarryDecl {
        policy,
        from_carry,
        clear_on_use,
    })
}

fn infer_field_type(ty: &Type) -> FieldType {
    let s = quote!(#ty).to_string().replace(' ', "");
    if s == "String" || s.contains("&str") || s.contains("Cow<") {
        FieldType::String
    } else if matches!(
        s.as_str(),
        "i8" | "i16" | "i32" | "i64" | "isize" | "u8" | "u16" | "u32" | "u64" | "usize"
    ) {
        FieldType::Integer
    } else if matches!(s.as_str(), "f32" | "f64") {
        FieldType::Number
    } else if s == "bool" {
        FieldType::Boolean
    } else {
        FieldType::Json
    }
}

// ---------------------------------------------------------------------------
// Spec construction
// ---------------------------------------------------------------------------

fn input_format_token(input_shape: &str) -> syn::Result<TokenStream> {
    Ok(match input_shape {
        "json" => quote!(Json),
        "tab_separated" => quote!(TabSeparated),
        "csv_row" => quote!(CsvRow),
        "sqlite_row" => quote!(SqliteRow),
        "raw_line" => quote!(RawLine),
        other => {
            return Err(Error::new_spanned(
                proc_macro2::Literal::string(other),
                format!(
                    "unknown input_shape '{other}'; expected one of: json, \
                     tab_separated, csv_row, sqlite_row, raw_line"
                ),
            ));
        }
    })
}

fn privacy_context_token(name: &str) -> syn::Result<TokenStream> {
    Ok(match name {
        "Command" => quote!(Command),
        "Clipboard" => quote!(Clipboard),
        "WindowTitle" => quote!(WindowTitle),
        "Journal" => quote!(Journal),
        "Dbus" => quote!(Dbus),
        "Notification" => quote!(Notification),
        "Document" => quote!(Document),
        "Metadata" => quote!(Metadata),
        "SourceCapture" => quote!(SourceCapture),
        other => {
            return Err(Error::new_spanned(
                proc_macro2::Literal::string(other),
                format!(
                    "unknown privacy context '{other}'; expected one of: \
                     Command, Clipboard, WindowTitle, Journal, Dbus, \
                     Notification, Document, Metadata, SourceCapture"
                ),
            ));
        }
    })
}

fn field_decl_to_token(d: &FieldDecl) -> syn::Result<TokenStream> {
    let name = &d.name;
    let source_token = match &d.source {
        FieldSourceDecl::JsonPointer(p) => {
            quote!(_sdk_parser::FieldSource::JsonPointer { pointer: #p.into() })
        }
        FieldSourceDecl::ColumnIndex(i) => {
            quote!(_sdk_parser::FieldSource::ColumnIndex { index: #i })
        }
        FieldSourceDecl::ColumnName(n) => {
            quote!(_sdk_parser::FieldSource::ColumnName { name: #n.into() })
        }
        FieldSourceDecl::RawLine => quote!(_sdk_parser::FieldSource::RawLine),
    };

    let field_type_token = match d.field_type {
        FieldType::String => quote!(_sdk_parser::FieldType::String),
        FieldType::Integer => quote!(_sdk_parser::FieldType::Integer),
        FieldType::Number => quote!(_sdk_parser::FieldType::Number),
        FieldType::Boolean => quote!(_sdk_parser::FieldType::Boolean),
        FieldType::Json => quote!(_sdk_parser::FieldType::Json),
    };

    let required = d.required;
    let skip_payload = d.skip_payload;
    let occurrence_key = d.occurrence_key;

    let default_token = if let Some(s) = &d.default {
        // Best-effort: try to parse as JSON, else wrap as string.
        quote!(Some(::serde_json::from_str::<::serde_json::Value>(#s)
            .unwrap_or_else(|_| ::serde_json::Value::String(#s.to_string()))))
    } else {
        quote!(None)
    };

    let privacy_token = if let Some(name) = &d.privacy_context {
        let tok = privacy_context_token(name)?;
        quote!(Some(_sdk_privacy::ProcessingContext::#tok))
    } else {
        quote!(None)
    };

    let timestamp_token = if let Some(ts) = &d.timestamp {
        let format_tok = timestamp_format_token(&ts.format)?;
        let fallback_tok = match ts.fallback.as_deref() {
            Some("error") => quote!(_sdk_parser::TimestampFallback::Error),
            Some("material_timing") | None => {
                quote!(_sdk_parser::TimestampFallback::MaterialTiming)
            }
            Some(other) => {
                return Err(Error::new_spanned(
                    proc_macro2::Literal::string(other),
                    format!(
                        "unknown timestamp fallback '{other}'; expected \
                         'material_timing' or 'error'"
                    ),
                ));
            }
        };
        quote!(Some(_sdk_parser::TimestampSpec {
            format: _sdk_parser::TimestampFormat::#format_tok,
            fallback: #fallback_tok,
        }))
    } else {
        quote!(None)
    };

    let suppress_token = if let Some(s) = &d.suppress_if {
        let bf = &s.binding_field;
        let we = s.whole_event;
        quote!(Some(_sdk_parser::SuppressPredicate {
            binding_field: #bf.into(),
            whole_event: #we,
        }))
    } else {
        quote!(None)
    };

    let carry_token = if let Some(c) = &d.carry {
        let policy_tok = carry_policy_token(&c.policy)?;
        let from_carry_tok = if let Some(s) = &c.from_carry {
            quote!(Some(#s.to_string()))
        } else {
            quote!(None)
        };
        let clear = c.clear_on_use;
        quote!(Some(_sdk_parser::CarrySpec {
            policy: #policy_tok,
            from_carry: #from_carry_tok,
            clear_on_use: #clear,
        }))
    } else {
        quote!(None)
    };

    Ok(quote!(_sdk_parser::FieldSpec {
        name: #name.into(),
        source: #source_token,
        field_type: #field_type_token,
        required: #required,
        default: #default_token,
        skip_payload: #skip_payload,
        privacy_context: #privacy_token,
        occurrence_key: #occurrence_key,
        timestamp: #timestamp_token,
        suppress_if: #suppress_token,
        carry: #carry_token,
    }))
}

fn carry_policy_token(name: &str) -> syn::Result<TokenStream> {
    Ok(match name {
        "set_then_consume" => quote!(_sdk_parser::StatefulCarryPolicy::SetThenConsume),
        "set_then_retain" => quote!(_sdk_parser::StatefulCarryPolicy::SetThenRetain),
        "consume_carried" => quote!(_sdk_parser::StatefulCarryPolicy::ConsumeCarried),
        other => {
            return Err(Error::new_spanned(
                proc_macro2::Literal::string(other),
                format!(
                    "unknown carry policy '{other}'; expected one of: \
                     set_then_consume, set_then_retain, consume_carried"
                ),
            ));
        }
    })
}

fn on_unknown_token(name: &str) -> syn::Result<TokenStream> {
    Ok(match name {
        "skip" | "skip_record" => quote!(_sdk_parser::DiscriminatorFallback::SkipRecord),
        "error" => quote!(_sdk_parser::DiscriminatorFallback::Error),
        "default" => quote!(_sdk_parser::DiscriminatorFallback::Default),
        other => {
            return Err(Error::new_spanned(
                proc_macro2::Literal::string(other),
                format!(
                    "unknown on_unknown value '{other}'; expected one of: \
                     skip, error, default"
                ),
            ));
        }
    })
}

fn timestamp_format_token(name: &str) -> syn::Result<TokenStream> {
    Ok(match name {
        "unix_seconds" => quote!(UnixSeconds),
        "unix_seconds_nanos" => quote!(UnixSecondsNanos),
        "unix_millis" => quote!(UnixMillis),
        "unix_micros" => quote!(UnixMicros),
        "rfc3339" => quote!(Rfc3339),
        "iso8601" => quote!(Iso8601),
        other => {
            return Err(Error::new_spanned(
                proc_macro2::Literal::string(other),
                format!(
                    "unknown timestamp format '{other}'; expected one of: \
                     unix_seconds, unix_seconds_nanos, unix_millis, \
                     unix_micros, rfc3339, iso8601"
                ),
            ));
        }
    })
}
