//! `#[derive(SourceDefinition)]` proc-macro.
//!
//! Collapses the four registration sites a source author would otherwise wire
//! by hand — each cross-referenced by hand-copied strings — into one struct:
//!
//!   1. `SourceContract` (semantic identity) registration
//!   2. `SourceRuntimeBinding` (deployment shape) registration
//!   3. `register_source!` adapter + parser factory wiring
//!   4. `impl MaterialParser` via `DeclarativeParser::evaluate`
//!
//! Site 4 reuses the exact declarative-parser code path that
//! `#[derive(SourceRecord)]` emits (see [`crate::source_record`]). The struct
//! carries the same `#[privacy(...)]` / `#[timestamp(...)]` /
//! `#[occurrence_key]` / `#[source(...)]` field attributes; the struct-level
//! `#[source_definition(...)]` attribute carries the union of what
//! `SourceContract` and `SourceRuntimeBinding` need.
//!
//! # Cross-crate note
//!
//! Sites 1, 2 and 4 are rooted in `::sinex_primitives` and are portable. Site 3
//! emits `crate::register_source!` and `crate::runtime::parser::<Adapter>`,
//! which resolve inside the `sinexd` crate (where every real source lives).
//! When the derive's struct-level attributes are invalid the macro short-circuits
//! to a `compile_error!` *before* emitting site 3, so compile-fail fixtures in
//! `sinex-macros` itself do not need `sinexd` in scope.

use proc_macro2::{Span, TokenStream};
use quote::quote;
use syn::{DeriveInput, Error, parse_macro_input};

use crate::source_record::{
    ParserSpecAttrs, collect_dispatch_event_types, generate_material_parser, parse_struct_fields,
};

pub fn derive_source_definition_impl(input: proc_macro::TokenStream) -> proc_macro::TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    match derive_source_definition_inner(&input) {
        Ok(tokens) => tokens.into(),
        Err(err) => err.to_compile_error().into(),
    }
}

fn derive_source_definition_inner(input: &DeriveInput) -> syn::Result<TokenStream> {
    let struct_name = &input.ident;

    let attrs = parse_source_definition_attrs(&input.attrs)?;
    let field_decls = parse_struct_fields(input, "SourceDefinition")?;

    // --- Compile-fail check: every #[event_dispatch] target must be a declared
    // event type of this source definition. ---
    let mut declared_types: Vec<String> = vec![attrs.event_type.clone()];
    declared_types.extend(attrs.additional_event_types.iter().cloned());
    for target in collect_dispatch_event_types(&field_decls) {
        if !declared_types.contains(&target) {
            return Err(Error::new(
                Span::call_site(),
                format!(
                    "#[event_dispatch] target event type \"{target}\" is not declared by this \
                     source definition (declared: {}); add it to \
                     #[source_definition(event_types = \"...\")]",
                    declared_types.join(", ")
                ),
            ));
        }
    }

    // --- Site 4: declarative MaterialParser (shared with SourceRecord). ---
    let parser_tokens = generate_material_parser(struct_name, &attrs.parser_spec_attrs(), &field_decls)?;

    // --- Site 1: SourceContract. ---
    let contract_tokens = generate_source_contract(&attrs, &declared_types)?;

    // --- Site 2: SourceRuntimeBinding. ---
    let binding_tokens = generate_source_runtime_binding(&attrs)?;

    // --- Site 3: register_source! adapter + parser factory wiring. ---
    let factory_tokens = generate_factory_registration(struct_name, &attrs)?;

    // The source struct is a pure marker: its fields declare the parser spec,
    // never hold runtime data. The factory constructs it via `Default`, so we
    // generate `Default` here rather than asking the author to add
    // `#[derive(Default)]` — which would conflict with field-level
    // `#[default = "..."]` attributes (std's Default derive rejects them).
    let default_tokens = generate_default_impl(input)?;

    Ok(quote! {
        #parser_tokens
        #default_tokens
        #contract_tokens
        #binding_tokens
        #factory_tokens
    })
}

/// Generate a `Default` impl that zero-initializes every field. The source
/// struct never carries runtime data (the parser reads the static spec), so a
/// blanket `Default::default()` per field is sufficient.
fn generate_default_impl(input: &DeriveInput) -> syn::Result<TokenStream> {
    let struct_name = &input.ident;
    let fields = match &input.data {
        syn::Data::Struct(syn::DataStruct {
            fields: syn::Fields::Named(named),
            ..
        }) => &named.named,
        _ => {
            return Err(Error::new(
                Span::call_site(),
                "#[derive(SourceDefinition)] only works on structs with named fields",
            ));
        }
    };
    let field_inits = fields.iter().map(|f| {
        let ident = &f.ident;
        quote!(#ident: ::core::default::Default::default())
    });
    Ok(quote! {
        impl ::core::default::Default for #struct_name {
            fn default() -> Self {
                Self { #(#field_inits),* }
            }
        }
    })
}

// ---------------------------------------------------------------------------
// Struct-level attribute parsing
// ---------------------------------------------------------------------------

#[derive(Debug, Default)]
struct SourceDefinitionAttrs {
    // Identity / parser
    id: String,
    namespace: String,
    event_type: String,
    event_source: String,
    input_shape: String,
    adapter: String,
    default_privacy_context: Option<String>,
    version: Option<String>,
    /// JSON literal for the parser's `baseline_adapter_config()` (e.g. atuin's
    /// `{"query":"history"}`). Preserves the adapter-mandatory config the
    /// imperative parser used to declare.
    baseline_adapter_config: Option<String>,
    /// Extra event types this definition is allowed to emit (besides
    /// `event_type`). Used by the dispatch-target compile-fail check and the
    /// contract's `event_types` list.
    additional_event_types: Vec<String>,

    // SourceContract semantic fields
    privacy_tier: Option<String>,
    horizons: Vec<String>,
    retention: Option<String>,
    /// REQUIRED — missing `occurrence_identity` is a compile error.
    occurrence_identity: Option<String>,
    access_policy: Option<String>,

    // SourceRuntimeBinding deployment fields
    implementation: Option<String>,
    privacy_context: Option<String>,
    material_policy: Option<String>,
    checkpoint_policy: Option<String>,
    resource_shape: Option<String>,
    runner_pack: Option<String>,
    checkpoint_family: Option<String>,
    runtime_shape: Option<String>,
    package_impact: Option<String>,
    implementation_mode: Option<String>,
    capabilities: Vec<String>,
}

impl SourceDefinitionAttrs {
    fn parser_spec_attrs(&self) -> ParserSpecAttrs {
        ParserSpecAttrs {
            parser_id: self.id.clone(),
            source_id: self.id.clone(),
            input_shape: self.input_shape.clone(),
            event_type: self.event_type.clone(),
            event_source: self.event_source.clone(),
            default_privacy_context: self
                .default_privacy_context
                .clone()
                .unwrap_or_else(|| "Metadata".to_string()),
            version: self.version.clone().unwrap_or_else(|| "1.0.0".to_string()),
            discriminator_field: None,
            on_unknown: None,
            baseline_adapter_config: self.baseline_adapter_config.clone(),
        }
    }
}

fn split_csv(value: &str) -> Vec<String> {
    value
        .split(',')
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(str::to_string)
        .collect()
}

fn parse_source_definition_attrs(attrs: &[syn::Attribute]) -> syn::Result<SourceDefinitionAttrs> {
    let mut out = SourceDefinitionAttrs::default();
    let mut found = false;

    for attr in attrs {
        if !attr.path().is_ident("source_definition") {
            continue;
        }
        found = true;
        attr.parse_nested_meta(|meta| {
            let key = meta
                .path
                .get_ident()
                .map(std::string::ToString::to_string)
                .ok_or_else(|| meta.error("expected attribute key"))?;
            let s: syn::LitStr = meta.value()?.parse()?;
            let v = s.value();
            match key.as_str() {
                "id" => out.id = v,
                "namespace" => out.namespace = v,
                "event_type" => out.event_type = v,
                "event_source" => out.event_source = v,
                "input_shape" => out.input_shape = v,
                "adapter" => out.adapter = v,
                "default_privacy_context" => out.default_privacy_context = Some(v),
                "version" => out.version = Some(v),
                "baseline_adapter_config" => out.baseline_adapter_config = Some(v),
                "event_types" => out.additional_event_types = split_csv(&v),
                "privacy_tier" => out.privacy_tier = Some(v),
                "horizons" => out.horizons = split_csv(&v),
                "retention" => out.retention = Some(v),
                "occurrence_identity" => out.occurrence_identity = Some(v),
                "access_policy" => out.access_policy = Some(v),
                "implementation" => out.implementation = Some(v),
                "privacy_context" => out.privacy_context = Some(v),
                "material_policy" => out.material_policy = Some(v),
                "checkpoint_policy" => out.checkpoint_policy = Some(v),
                "resource_shape" => out.resource_shape = Some(v),
                "runner_pack" => out.runner_pack = Some(v),
                "checkpoint_family" => out.checkpoint_family = Some(v),
                "runtime_shape" => out.runtime_shape = Some(v),
                "package_impact" => out.package_impact = Some(v),
                "implementation_mode" => out.implementation_mode = Some(v),
                "capabilities" => out.capabilities = split_csv(&v),
                other => {
                    return Err(meta.error(format!(
                        "unknown source_definition attribute '{other}'"
                    )));
                }
            }
            Ok(())
        })?;
    }

    if !found {
        return Err(Error::new(
            Span::call_site(),
            "missing #[source_definition(...)] attribute on the struct",
        ));
    }

    // Required keys.
    let require = |val: &str, name: &str| -> syn::Result<()> {
        if val.is_empty() {
            Err(Error::new(
                Span::call_site(),
                format!("source_definition: missing required '{name}'"),
            ))
        } else {
            Ok(())
        }
    };
    require(&out.id, "id")?;
    require(&out.namespace, "namespace")?;
    require(&out.event_type, "event_type")?;
    require(&out.event_source, "event_source")?;
    require(&out.input_shape, "input_shape")?;
    require(&out.adapter, "adapter")?;

    // Compile-fail check #1: occurrence_identity is mandatory.
    if out.occurrence_identity.is_none() {
        return Err(Error::new(
            Span::call_site(),
            "source_definition: missing required 'occurrence_identity' (one of: \
             natural, anchor, uuid5:<namespace>)",
        ));
    }

    Ok(out)
}

// ---------------------------------------------------------------------------
// Site 1: SourceContract
// ---------------------------------------------------------------------------

fn generate_source_contract(
    attrs: &SourceDefinitionAttrs,
    declared_types: &[String],
) -> syn::Result<TokenStream> {
    let id = &attrs.id;
    let namespace = &attrs.namespace;
    let access_policy = attrs.access_policy.as_deref().unwrap_or("internal");

    // event_types: (event_source, event_type) pairs. All emitted under the
    // definition's event_source for v1.
    let event_source = &attrs.event_source;
    let event_type_pairs = declared_types.iter().map(|et| {
        quote!((#event_source, #et))
    });

    let privacy_tier = privacy_tier_token(attrs.privacy_tier.as_deref().unwrap_or("Sensitive"))?;

    let horizons = if attrs.horizons.is_empty() {
        vec![quote!(::sinex_primitives::source_contracts::Horizon::Continuous)]
    } else {
        attrs
            .horizons
            .iter()
            .map(|h| horizon_token(h))
            .collect::<syn::Result<Vec<_>>>()?
    };

    let retention = retention_token(attrs.retention.as_deref().unwrap_or("forever"))?;
    let occurrence_identity = occurrence_identity_token(
        attrs
            .occurrence_identity
            .as_deref()
            .expect("occurrence_identity required (checked during attr parsing)"),
    )?;

    Ok(quote! {
        ::sinex_primitives::source_contracts::__register::inventory::submit! {
            ::sinex_primitives::source_contracts::SourceContract {
                id: #id,
                namespace: #namespace,
                event_types: &[ #(#event_type_pairs),* ],
                privacy_tier: #privacy_tier,
                horizons: &[ #(#horizons),* ],
                retention: #retention,
                occurrence_identity: #occurrence_identity,
                access_policy: #access_policy,
            }
        }
    })
}

// ---------------------------------------------------------------------------
// Site 2: SourceRuntimeBinding
// ---------------------------------------------------------------------------

fn generate_source_runtime_binding(attrs: &SourceDefinitionAttrs) -> syn::Result<TokenStream> {
    let id = &attrs.id;
    let namespace = &attrs.namespace;
    let subject = format!("source:{id}");

    let implementation = attrs.implementation.as_deref().unwrap_or("sinexd");
    let adapter = &attrs.adapter;
    let output_event_type = &attrs.event_type;
    let privacy_context = attrs.privacy_context.as_deref().unwrap_or("Metadata");
    let material_policy = attrs.material_policy.as_deref().unwrap_or("");
    let checkpoint_policy = attrs.checkpoint_policy.as_deref().unwrap_or("");
    let resource_shape = attrs.resource_shape.as_deref().unwrap_or("");
    let runner_pack = attrs.runner_pack.as_deref().unwrap_or("sinexd-source");
    let package_impact = attrs.package_impact.as_deref().unwrap_or("no_new_output");
    let implementation_mode = attrs.implementation_mode.as_deref().unwrap_or("sinexd:source");

    let checkpoint_family =
        checkpoint_family_token(attrs.checkpoint_family.as_deref().unwrap_or("append_stream"))?;
    let runtime_shape =
        runtime_shape_token(attrs.runtime_shape.as_deref().unwrap_or("continuous"))?;

    let capabilities_call = if attrs.capabilities.is_empty() {
        quote!()
    } else {
        let caps = attrs.capabilities.iter();
        quote!(.capabilities(&[ #(#caps),* ]))
    };

    Ok(quote! {
        ::sinex_primitives::source_contracts::__register::inventory::submit! {
            ::sinex_primitives::source_contracts::SourceRuntimeBinding::builder(
                ::sinex_primitives::source_contracts::SubjectRef::from_static(#subject),
                #id,
                #namespace,
            )
            .implementation(#implementation)
            .adapter(#adapter)
            .output_event_type(#output_event_type)
            .privacy_context(#privacy_context)
            .material_policy(#material_policy)
            .checkpoint_policy(#checkpoint_policy)
            .resource_shape(#resource_shape)
            .source_id(#id)
            .runner_pack(#runner_pack)
            #capabilities_call
            .checkpoint_family(#checkpoint_family)
            .runtime_shape(#runtime_shape)
            .package_impact(#package_impact)
            .implementation_mode(#implementation_mode)
            .build_impact(::sinex_primitives::source_contracts::SourceBuildImpact::ZERO)
            .build()
        }
    })
}

// ---------------------------------------------------------------------------
// Site 3: register_source! factory wiring
// ---------------------------------------------------------------------------

fn generate_factory_registration(
    struct_name: &syn::Ident,
    attrs: &SourceDefinitionAttrs,
) -> syn::Result<TokenStream> {
    let id = &attrs.id;
    let adapter_ident = adapter_type_ident(&attrs.adapter)?;

    Ok(quote! {
        crate::register_source!(
            source_id: #id,
            adapter: crate::runtime::parser::#adapter_ident,
            parser: #struct_name,
        );
    })
}

/// Resolve the adapter type identifier from the `adapter = "..."` attribute.
///
/// The attribute carries the adapter type name (also used verbatim as the
/// binding's `adapter` string field). The factory wiring references it under
/// `crate::runtime::parser::<Adapter>`.
fn adapter_type_ident(adapter: &str) -> syn::Result<syn::Ident> {
    // Slice 1 wires the adapters that have a 1:1 adapter type re-exported from
    // `crate::runtime::parser`. Bare-ident form only (no generics) for v1.
    match adapter {
        "SqliteRowAdapter" | "AppendOnlyFileAdapter" | "StaticFileAdapter"
        | "DirectoryWalkAdapter" => Ok(syn::Ident::new(adapter, Span::call_site())),
        other => Err(Error::new(
            Span::call_site(),
            format!(
                "source_definition: adapter \"{other}\" is not yet wired for factory \
                 registration (slice 1 supports: SqliteRowAdapter, AppendOnlyFileAdapter, \
                 StaticFileAdapter, DirectoryWalkAdapter)"
            ),
        )),
    }
}

// ---------------------------------------------------------------------------
// Enum token helpers
// ---------------------------------------------------------------------------

fn privacy_tier_token(name: &str) -> syn::Result<TokenStream> {
    let variant = match name {
        "Public" | "public" => quote!(Public),
        "Sensitive" | "sensitive" => quote!(Sensitive),
        "Secret" | "secret" => quote!(Secret),
        other => {
            return Err(Error::new(
                Span::call_site(),
                format!("unknown privacy_tier '{other}'; expected Public, Sensitive, or Secret"),
            ));
        }
    };
    Ok(quote!(::sinex_primitives::source_contracts::PrivacyTier::#variant))
}

fn horizon_token(name: &str) -> syn::Result<TokenStream> {
    let variant = match name {
        "continuous" | "Continuous" => quote!(Continuous),
        "historical" | "Historical" => quote!(Historical),
        other => {
            return Err(Error::new(
                Span::call_site(),
                format!("unknown horizon '{other}'; expected continuous or historical"),
            ));
        }
    };
    Ok(quote!(::sinex_primitives::source_contracts::Horizon::#variant))
}

fn retention_token(spec: &str) -> syn::Result<TokenStream> {
    let mut parts = spec.split(':');
    let kind = parts.next().unwrap_or("");
    match kind {
        "forever" => Ok(quote!(::sinex_primitives::source_contracts::RetentionPolicy::Forever)),
        "days" => {
            let days: u32 = parts
                .next()
                .and_then(|d| d.parse().ok())
                .ok_or_else(|| Error::new(Span::call_site(), "retention days expects 'days:N'"))?;
            Ok(quote!(::sinex_primitives::source_contracts::RetentionPolicy::Days { days: #days }))
        }
        "tiered" => {
            let hot: u32 = parts.next().and_then(|d| d.parse().ok()).ok_or_else(|| {
                Error::new(Span::call_site(), "retention tiered expects 'tiered:HOT:WARM'")
            })?;
            let warm: u32 = parts.next().and_then(|d| d.parse().ok()).ok_or_else(|| {
                Error::new(Span::call_site(), "retention tiered expects 'tiered:HOT:WARM'")
            })?;
            Ok(quote!(::sinex_primitives::source_contracts::RetentionPolicy::Tiered {
                hot_days: #hot,
                warm_days: #warm,
            }))
        }
        other => Err(Error::new(
            Span::call_site(),
            format!("unknown retention '{other}'; expected forever, days:N, or tiered:HOT:WARM"),
        )),
    }
}

fn occurrence_identity_token(spec: &str) -> syn::Result<TokenStream> {
    let mut parts = spec.splitn(2, ':');
    let kind = parts.next().unwrap_or("");
    match kind {
        "natural" => Ok(quote!(::sinex_primitives::source_contracts::OccurrenceIdentity::Natural)),
        "anchor" => Ok(quote!(::sinex_primitives::source_contracts::OccurrenceIdentity::Anchor)),
        "uuid5" => {
            let ns = parts.next().ok_or_else(|| {
                Error::new(Span::call_site(), "occurrence_identity uuid5 expects 'uuid5:<namespace>'")
            })?;
            Ok(quote!(::sinex_primitives::source_contracts::OccurrenceIdentity::Uuid5From(#ns)))
        }
        other => Err(Error::new(
            Span::call_site(),
            format!(
                "unknown occurrence_identity '{other}'; expected natural, anchor, or uuid5:<ns>"
            ),
        )),
    }
}

fn checkpoint_family_token(spec: &str) -> syn::Result<TokenStream> {
    let mut parts = spec.split(':');
    let kind = parts.next().unwrap_or("");
    match kind {
        "append_stream" => {
            Ok(quote!(::sinex_primitives::source_contracts::CheckpointFamily::AppendStream))
        }
        "journal" => Ok(quote!(::sinex_primitives::source_contracts::CheckpointFamily::Journal)),
        "polling" => Ok(quote!(::sinex_primitives::source_contracts::CheckpointFamily::Polling)),
        "live_observation" => {
            Ok(quote!(::sinex_primitives::source_contracts::CheckpointFamily::LiveObservation))
        }
        "mutable_snapshot" => {
            let backing = parts.next().ok_or_else(|| {
                Error::new(
                    Span::call_site(),
                    "checkpoint_family mutable_snapshot expects \
                     'mutable_snapshot:<backing_store>:<occurrence_anchor>'",
                )
            })?;
            let anchor = parts.next().ok_or_else(|| {
                Error::new(
                    Span::call_site(),
                    "checkpoint_family mutable_snapshot expects \
                     'mutable_snapshot:<backing_store>:<occurrence_anchor>'",
                )
            })?;
            Ok(quote!(::sinex_primitives::source_contracts::CheckpointFamily::MutableSnapshot {
                backing_store_kind: #backing,
                occurrence_anchor: #anchor,
            }))
        }
        other => Err(Error::new(
            Span::call_site(),
            format!(
                "unknown checkpoint_family '{other}'; expected append_stream, journal, polling, \
                 live_observation, or mutable_snapshot:<backing>:<anchor>"
            ),
        )),
    }
}

fn runtime_shape_token(name: &str) -> syn::Result<TokenStream> {
    let variant = match name {
        "continuous" | "Continuous" => quote!(Continuous),
        "on_demand" | "OnDemand" => quote!(OnDemand),
        "scheduled" | "Scheduled" => quote!(Scheduled),
        other => {
            return Err(Error::new(
                Span::call_site(),
                format!("unknown runtime_shape '{other}'; expected continuous, on_demand, scheduled"),
            ));
        }
    };
    Ok(quote!(::sinex_primitives::source_contracts::RuntimeShape::#variant))
}
