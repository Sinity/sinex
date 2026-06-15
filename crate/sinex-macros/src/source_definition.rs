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
//! Sites 1–3 are emitted by the shared [`crate::source_registration`] module
//! (also used by `#[derive(SourceMeta)]`). Site 4 reuses the exact
//! declarative-parser code path that `#[derive(SourceRecord)]` emits (see
//! [`crate::source_record`]). The struct carries the same `#[privacy(...)]` /
//! `#[timestamp(...)]` / `#[occurrence_key]` / `#[source(...)]` field
//! attributes; the struct-level `#[source_definition(...)]` attribute carries
//! the union of what `SourceContract` and `SourceRuntimeBinding` need.
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
use crate::source_registration::{
    RegistrationAttrs, generate_factory_registration, generate_source_contract,
    generate_source_runtime_binding, parse_enum_expr_attr, parse_enum_path_attr,
    parse_enum_path_list_attr, split_csv,
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
    let registration = attrs.registration_attrs();
    let field_decls = parse_struct_fields(input, "SourceDefinition")?;

    // --- Compile-fail check: every #[event_dispatch] target must be a declared
    // event type of this source definition. ---
    let declared_types = registration.declared_types();
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
    let parser_tokens =
        generate_material_parser(struct_name, &attrs.parser_spec_attrs(), &field_decls)?;

    // --- Sites 1–3: contract, binding, factory (shared with SourceMeta). ---
    let contract_tokens = generate_source_contract(&registration, &declared_types)?;
    let binding_tokens = generate_source_runtime_binding(&registration)?;
    let factory_tokens = generate_factory_registration(struct_name, &registration)?;

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
    /// Typed enum path token (e.g. `PrivacyTier::Public`), emitted verbatim.
    privacy_tier: Option<proc_macro2::TokenStream>,
    /// Typed enum path tokens (e.g. `Horizon::Continuous`), emitted verbatim.
    horizons: Vec<proc_macro2::TokenStream>,
    /// Typed enum-expression token (`RetentionPolicy::Forever`, ..), verbatim.
    retention: Option<proc_macro2::TokenStream>,
    /// Typed enum-expression token (`OccurrenceIdentity::Anchor`, ..), verbatim.
    /// REQUIRED — missing `occurrence_identity` is a compile error.
    occurrence_identity: Option<proc_macro2::TokenStream>,
    /// Typed enum-expression token (`AccessScope::TargetHome { .. }`), verbatim.
    access_scope: Option<proc_macro2::TokenStream>,

    // SourceRuntimeBinding deployment fields
    implementation: Option<String>,
    /// Typed enum path token (`ProcessingContext::Command`), emitted verbatim.
    privacy_context: Option<proc_macro2::TokenStream>,
    /// Typed enum path token (`ResourceProfile::BoundedFile`), emitted verbatim.
    resource_profile: Option<proc_macro2::TokenStream>,
    /// Typed enum path token (`RunnerPack::SinexdSource`), emitted verbatim.
    runner_pack: Option<proc_macro2::TokenStream>,
    /// Typed enum-expression token (unit-variant path or `MutableSnapshot { .. }`
    /// struct variant), emitted verbatim.
    checkpoint_family: Option<proc_macro2::TokenStream>,
    /// Typed enum path token (e.g. `RuntimeShape::OnDemand`), emitted verbatim.
    runtime_shape: Option<proc_macro2::TokenStream>,
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

    /// Project the registration-relevant subset for the shared contract /
    /// binding / factory emission (sites 1–3).
    fn registration_attrs(&self) -> RegistrationAttrs {
        RegistrationAttrs {
            id: self.id.clone(),
            namespace: self.namespace.clone(),
            event_type: self.event_type.clone(),
            event_source: self.event_source.clone(),
            adapter: self.adapter.clone(),
            additional_event_types: self.additional_event_types.clone(),
            privacy_tier: self.privacy_tier.clone(),
            horizons: self.horizons.clone(),
            retention: self.retention.clone(),
            occurrence_identity: self.occurrence_identity.clone(),
            access_scope: self.access_scope.clone(),
            implementation: self.implementation.clone(),
            privacy_context: self.privacy_context.clone(),
            resource_profile: self.resource_profile.clone(),
            runner_pack: self.runner_pack.clone(),
            checkpoint_family: self.checkpoint_family.clone(),
            runtime_shape: self.runtime_shape.clone(),
            capabilities: self.capabilities.clone(),
            proposed: false,
            // SourceDefinition is the declarative adapter+parser form; it never
            // uses the monitor-emit factory shape.
            monitor_emit_fn: None,
            monitor_phase: None,
            register_factory: true,
            factory_adapter: None,
            extra_bindings: Vec::new(),
        }
    }
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
            // The enum-valued attributes take typed path/expression values
            // (e.g. `PrivacyTier::Public`); `horizons` is a typed path list
            // (`horizons(Horizon::Continuous, ..)`); all other keys take a
            // string literal.
            match key.as_str() {
                "privacy_tier" => {
                    out.privacy_tier = Some(parse_enum_path_attr(&meta)?);
                    return Ok(());
                }
                "runtime_shape" => {
                    out.runtime_shape = Some(parse_enum_path_attr(&meta)?);
                    return Ok(());
                }
                "checkpoint_family" => {
                    out.checkpoint_family = Some(parse_enum_expr_attr(&meta)?);
                    return Ok(());
                }
                "retention" => {
                    out.retention = Some(parse_enum_expr_attr(&meta)?);
                    return Ok(());
                }
                "occurrence_identity" => {
                    out.occurrence_identity = Some(parse_enum_expr_attr(&meta)?);
                    return Ok(());
                }
                "horizons" => {
                    out.horizons = parse_enum_path_list_attr(&meta)?;
                    return Ok(());
                }
                "privacy_context" => {
                    out.privacy_context = Some(parse_enum_path_attr(&meta)?);
                    return Ok(());
                }
                "resource_profile" => {
                    out.resource_profile = Some(parse_enum_path_attr(&meta)?);
                    return Ok(());
                }
                "runner_pack" => {
                    out.runner_pack = Some(parse_enum_path_attr(&meta)?);
                    return Ok(());
                }
                "access_scope" => {
                    out.access_scope = Some(parse_enum_expr_attr(&meta)?);
                    return Ok(());
                }
                _ => {}
            }
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
                "implementation" => out.implementation = Some(v),
                "capabilities" => out.capabilities = split_csv(&v),
                other => {
                    return Err(
                        meta.error(format!("unknown source_definition attribute '{other}'"))
                    );
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
