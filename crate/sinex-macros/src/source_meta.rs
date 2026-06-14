//! `#[derive(SourceMeta)]` proc-macro.
//!
//! The imperative sibling of [`crate::source_definition`]. Where
//! `#[derive(SourceDefinition)]` is for fully *declarative* sources — it
//! generates the `MaterialParser` from field attributes — many real sources
//! need a hand-written parser: stateful dedup across rotation, multi-line state
//! machines, multi-event fan-out, custom timestamp parsing. For those,
//! `#[derive(SourceMeta)]` collapses only the *registration* boilerplate:
//!
//!   1. `SourceContract` (semantic identity) registration,
//!   2. `SourceRuntimeBinding` (deployment shape) registration,
//!   3. `register_source!` adapter + parser factory wiring,
//!
//! and stops there. It does **not** emit an `impl MaterialParser` — the author
//! keeps theirs. This removes the two error-prone, string-cross-referenced
//! `register_source_contract!` / `register_source_runtime_binding!` calls per
//! imperative source while preserving the custom parsing logic verbatim.
//!
//! Sites 1–3 are emitted by the shared [`crate::source_registration`] module,
//! so a `SourceMeta` registration and a `SourceDefinition` registration of the
//! same shape produce byte-for-byte identical contract/binding/factory tokens.
//!
//! # Application site
//!
//! The derive is applied **directly to the hand-written parser struct** (the
//! `MaterialParser` implementor). The factory wiring references that struct as
//! its parser type, so no separate marker struct is needed. The struct must
//! already provide `Default` (every imperative parser here derives it) — the
//! factory constructs the parser via `Default::default()`.
//!
//! # Cross-crate note
//!
//! Like `SourceDefinition`, site 3 emits `crate::register_source!` and
//! `crate::runtime::parser::<Adapter>`, which resolve inside the `sinexd`
//! crate. Attribute validation short-circuits to `compile_error!` before
//! emitting site 3, so compile-fail fixtures in `sinex-macros` itself do not
//! need `sinexd` in scope.

use proc_macro2::{Span, TokenStream};
use quote::quote;
use syn::{DeriveInput, Error, parse_macro_input};

use crate::source_registration::{
    RegistrationAttrs, generate_factory_registration, generate_source_contract,
    generate_source_runtime_binding, split_csv,
};

pub fn derive_source_meta_impl(input: proc_macro::TokenStream) -> proc_macro::TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    match derive_source_meta_inner(&input) {
        Ok(tokens) => tokens.into(),
        Err(err) => err.to_compile_error().into(),
    }
}

fn derive_source_meta_inner(input: &DeriveInput) -> syn::Result<TokenStream> {
    let struct_name = &input.ident;

    let registration = parse_source_meta_attrs(&input.attrs)?;
    let declared_types = registration.declared_types();

    // Sites 1–3 only — the parser is hand-written and kept by the author.
    let contract_tokens = generate_source_contract(&registration, &declared_types)?;
    let binding_tokens = generate_source_runtime_binding(&registration)?;
    let factory_tokens = generate_factory_registration(struct_name, &registration)?;

    Ok(quote! {
        #contract_tokens
        #binding_tokens
        #factory_tokens
    })
}

/// Parse the struct-level `#[source_meta(...)]` attribute into the shared
/// [`RegistrationAttrs`]. `SourceMeta` carries no parser-spec keys
/// (`input_shape`, `default_privacy_context`, `version`,
/// `baseline_adapter_config`) — those belong to the declarative parser, which
/// `SourceMeta` does not generate.
fn parse_source_meta_attrs(attrs: &[syn::Attribute]) -> syn::Result<RegistrationAttrs> {
    let mut out = RegistrationAttrs::default();
    let mut found = false;

    for attr in attrs {
        if !attr.path().is_ident("source_meta") {
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
                "adapter" => out.adapter = v,
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
                "monitor_emit_fn" => out.monitor_emit_fn = Some(v),
                "monitor_phase" => out.monitor_phase = Some(v),
                other => {
                    return Err(meta.error(format!("unknown source_meta attribute '{other}'")));
                }
            }
            Ok(())
        })?;
    }

    if !found {
        return Err(Error::new(
            Span::call_site(),
            "missing #[source_meta(...)] attribute on the struct",
        ));
    }

    // Required keys.
    let require = |val: &str, name: &str| -> syn::Result<()> {
        if val.is_empty() {
            Err(Error::new(
                Span::call_site(),
                format!("source_meta: missing required '{name}'"),
            ))
        } else {
            Ok(())
        }
    };
    require(&out.id, "id")?;
    require(&out.namespace, "namespace")?;
    require(&out.event_type, "event_type")?;
    require(&out.event_source, "event_source")?;
    require(&out.adapter, "adapter")?;

    // Compile-fail invariant: occurrence_identity is mandatory (mirrors
    // SourceDefinition; full matrix is slice 4).
    if out.occurrence_identity.is_none() {
        return Err(Error::new(
            Span::call_site(),
            "source_meta: missing required 'occurrence_identity' (one of: \
             natural, anchor, uuid5:<namespace>)",
        ));
    }

    Ok(out)
}
