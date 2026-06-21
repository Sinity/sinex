//! Shared registration-site code generation for source derives.
//!
//! Both `#[derive(SourceDefinition)]` (fully declarative — slice 1) and
//! `#[derive(SourceMeta)]` (imperative parser kept by hand — slice 3) emit the
//! same three registration sites:
//!
//!   1. `SourceContract` (semantic identity),
//!   2. `SourceRuntimeBinding` (deployment shape),
//!   3. `register_source!` adapter + parser factory wiring.
//!
//! `SourceDefinition` additionally emits a declarative `MaterialParser` (site
//! 4) from field attributes; `SourceMeta` does not — the author writes the
//! parser themselves. The shared code below is parameterised on
//! [`RegistrationAttrs`], the union of fields the three sites consume, so the
//! two derives stay byte-for-byte consistent in everything they have in common.

use proc_macro2::{Span, TokenStream};
use quote::{ToTokens, quote};
use syn::{Error, Ident};

/// The struct-level attribute fields consumed by the three shared registration
/// sites (contract, binding, factory). Both source derives populate this from
/// their own attribute parser and hand it to the emission functions here.
#[derive(Debug, Default)]
pub(crate) struct RegistrationAttrs {
    // Identity
    pub id: String,
    pub namespace: String,
    pub event_type: String,
    pub event_source: String,
    pub adapter: String,
    /// Optional primary runtime-binding subject. Absent => `source:{id}`.
    ///
    /// Package contracts with several modes sometimes need to render or author
    /// one mode as the primary binding while keeping the SourceContract id at
    /// the package id. Extra `binding(...)` entries already support this field;
    /// the primary binding uses the same semantics.
    pub subject: Option<String>,
    /// Extra event types this source may emit besides `event_type`.
    pub additional_event_types: Vec<String>,

    // SourceContract semantic fields.
    //
    // `privacy_tier` is a typed enum path token (e.g. `PrivacyTier::Public`),
    // captured verbatim by the attribute parser and emitted directly. `None`
    // means the attribute was absent; the generator supplies the default path.
    pub privacy_tier: Option<TokenStream>,
    /// Typed enum path tokens (e.g. `Horizon::Continuous`), one per list entry,
    /// emitted verbatim. Empty => generator supplies the default.
    pub horizons: Vec<TokenStream>,
    /// Typed enum-expression token (`RetentionPolicy::Forever`,
    /// `RetentionPolicy::Days { .. }`), emitted verbatim. `None` => default.
    pub retention: Option<TokenStream>,
    /// Typed enum-expression token (`OccurrenceIdentity::Anchor`,
    /// `OccurrenceIdentity::Uuid5From("..")`), emitted verbatim.
    /// REQUIRED — checked during attribute parsing, not here.
    pub occurrence_identity: Option<TokenStream>,
    /// Typed enum-expression token (`AccessScope::TargetHome { .. }` or a unit
    /// variant path), emitted verbatim. `None` => generator supplies the default.
    pub access_scope: Option<TokenStream>,

    // SourceRuntimeBinding deployment fields
    pub implementation: Option<String>,
    /// Typed enum path token (`ProcessingContext::Command`), emitted verbatim.
    /// `None` => generator supplies the default path.
    pub privacy_context: Option<TokenStream>,
    /// Typed enum path token (`ResourceProfile::BoundedFile`), emitted verbatim.
    /// `None` => generator supplies the default path.
    pub resource_profile: Option<TokenStream>,
    /// Typed enum path token (`RunnerPack::SinexdSource`), emitted verbatim.
    /// `None` => generator supplies the default path.
    pub runner_pack: Option<TokenStream>,
    /// Typed enum-expression token. A path for the unit variants
    /// (`CheckpointFamily::AppendStream`) or a struct-variant expression
    /// (`CheckpointFamily::MutableSnapshot { .. }`) — captured verbatim and
    /// emitted directly. `None` => generator supplies the default path.
    pub checkpoint_family: Option<TokenStream>,
    /// Typed enum path token (e.g. `RuntimeShape::OnDemand`), emitted verbatim.
    /// `None` => generator supplies the default path.
    pub runtime_shape: Option<TokenStream>,
    /// Typed enum-expression token (`MaterialLifecyclePolicy::RetainRaw`),
    /// emitted verbatim. `None` => generator supplies the default path.
    pub material_lifecycle: Option<TokenStream>,
    /// Typed enum-expression token (`TransportSemantics::JETSTREAM_DURABLE`),
    /// emitted verbatim. `None` => generator supplies the default path.
    pub transport_semantics: Option<TokenStream>,
    pub capabilities: Vec<String>,
    /// Mark the runtime binding as future-state metadata rather than a live
    /// deployment binding.
    pub proposed: bool,

    // Monitor-emit factory form. A monitor source's adapter (e.g. MonitorDriver)
    // fires an emit fn at a lifecycle phase instead of running a `MaterialParser`
    // over records — it has an adapter but no parser. When `monitor_emit_fn` is
    // set the factory wiring uses the `register_source!(emit_at:, emit:)` form
    // instead of the adapter+parser form, bypassing the `adapter_type_ident`
    // allowlist (the adapter string is still carried on the binding).
    pub monitor_emit_fn: Option<String>,
    pub monitor_phase: Option<String>,
    /// Emit `register_source!` factory wiring.
    ///
    /// External producers publish `EventIntent` envelopes themselves and need
    /// only contract + binding metadata; no parser or source factory should be
    /// registered for them.
    pub register_factory: bool,
    /// Emit parser dispatch only instead of adapter-backed source factory
    /// wiring.
    pub parser_only_factory: bool,
    /// Optional Rust adapter type path used only for `register_source!`
    /// factory wiring. The binding keeps `adapter` as deployment metadata.
    pub factory_adapter: Option<TokenStream>,
    /// Optional Rust parser type path used only for `register_source!` factory
    /// wiring. Most `SourceMeta` derives are placed on the parser type itself;
    /// this override covers sources whose metadata marker lives separately
    /// from the production parser implementation.
    pub factory_parser: Option<TokenStream>,
    /// Optional SourceDriver type path for sources whose parser dispatch and
    /// runtime lifecycle are separate registrations.
    pub driver_factory: Option<TokenStream>,
    /// Additional runtime bindings for one source contract.
    pub extra_bindings: Vec<RuntimeBindingAttrs>,
}

/// Optional overrides for additional runtime bindings emitted by
/// `#[derive(SourceMeta)]`.
#[derive(Debug, Default)]
pub(crate) struct RuntimeBindingAttrs {
    pub subject: Option<String>,
    pub event_type: Option<String>,
    pub implementation: Option<String>,
    pub adapter: Option<String>,
    pub privacy_context: Option<TokenStream>,
    pub resource_profile: Option<TokenStream>,
    pub runner_pack: Option<TokenStream>,
    pub checkpoint_family: Option<TokenStream>,
    pub runtime_shape: Option<TokenStream>,
    pub material_lifecycle: Option<TokenStream>,
    pub transport_semantics: Option<TokenStream>,
    pub capabilities: Vec<String>,
    pub proposed: Option<bool>,
}

impl RegistrationAttrs {
    /// All event types this source declares: `event_type` followed by any
    /// `additional_event_types`. Used by the contract's `event_types` list and
    /// by the dispatch-target compile-fail check in `SourceDefinition`.
    pub(crate) fn declared_types(&self) -> Vec<String> {
        let mut declared = vec![self.event_type.clone()];
        declared.extend(self.additional_event_types.iter().cloned());
        declared
    }
}

pub(crate) fn split_csv(value: &str) -> Vec<String> {
    value
        .split(',')
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(str::to_string)
        .collect()
}

// ---------------------------------------------------------------------------
// Site 1: SourceContract
// ---------------------------------------------------------------------------

pub(crate) fn generate_source_contract(
    attrs: &RegistrationAttrs,
    declared_types: &[String],
) -> syn::Result<TokenStream> {
    let id = &attrs.id;
    let namespace = &attrs.namespace;
    let access_scope = attrs
        .access_scope
        .clone()
        .unwrap_or_else(|| quote!(::sinex_primitives::source_contracts::AccessScope::Internal));

    // event_types: (event_source, event_type) pairs. All emitted under the
    // definition's event_source for v1.
    let event_source = &attrs.event_source;
    let event_type_pairs = declared_types.iter().map(|et| quote!((#event_source, #et)));

    // The author writes a typed path (`PrivacyTier::Public`); it is emitted
    // verbatim. When the attribute is absent the default path is fully
    // qualified so it resolves without requiring the source to import the enum.
    let privacy_tier = attrs
        .privacy_tier
        .clone()
        .unwrap_or_else(|| quote!(::sinex_primitives::source_contracts::PrivacyTier::Sensitive));

    // Typed enum tokens emitted verbatim; absent attrs fall back to a
    // fully-qualified default path (resolves without an import in the source).
    let horizons = if attrs.horizons.is_empty() {
        vec![quote!(
            ::sinex_primitives::source_contracts::Horizon::Continuous
        )]
    } else {
        attrs.horizons.clone()
    };

    let retention = attrs
        .retention
        .clone()
        .unwrap_or_else(|| quote!(::sinex_primitives::source_contracts::RetentionPolicy::Forever));
    let occurrence_identity = attrs.occurrence_identity.clone().ok_or_else(|| {
        Error::new(
            Span::call_site(),
            "occurrence_identity required (checked during attr parsing)",
        )
    })?;

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
                access_scope: #access_scope,
            }
        }
    })
}

// ---------------------------------------------------------------------------
// Site 2: SourceRuntimeBinding
// ---------------------------------------------------------------------------

pub(crate) fn generate_source_runtime_binding(
    attrs: &RegistrationAttrs,
) -> syn::Result<TokenStream> {
    let primary = generate_one_source_runtime_binding(attrs, None)?;
    let mut extra = Vec::with_capacity(attrs.extra_bindings.len());
    for binding in &attrs.extra_bindings {
        extra.push(generate_one_source_runtime_binding(attrs, Some(binding))?);
    }

    Ok(quote! {
        #primary
        #(#extra)*
    })
}

fn generate_one_source_runtime_binding(
    attrs: &RegistrationAttrs,
    binding: Option<&RuntimeBindingAttrs>,
) -> syn::Result<TokenStream> {
    let id = &attrs.id;
    let namespace = &attrs.namespace;
    let subject = binding
        .and_then(|binding| binding.subject.as_deref())
        .or(attrs.subject.as_deref())
        .map(str::to_owned)
        .unwrap_or_else(|| format!("source:{id}"));

    let implementation = binding
        .and_then(|binding| binding.implementation.as_deref())
        .or(attrs.implementation.as_deref())
        .unwrap_or("sinexd");
    let adapter = binding
        .and_then(|binding| binding.adapter.as_deref())
        .unwrap_or(&attrs.adapter);
    let output_event_type = binding
        .and_then(|binding| binding.event_type.as_deref())
        .unwrap_or(&attrs.event_type);

    // Typed enum tokens emitted verbatim; absent attributes fall back to a
    // fully-qualified default path (resolves without an import in the source).
    let privacy_context = binding
        .and_then(|binding| binding.privacy_context.clone())
        .or_else(|| attrs.privacy_context.clone())
        .unwrap_or_else(|| quote!(::sinex_primitives::privacy::ProcessingContext::Metadata));
    let resource_profile = binding
        .and_then(|binding| binding.resource_profile.clone())
        .or_else(|| attrs.resource_profile.clone())
        .unwrap_or_else(|| {
            quote!(::sinex_primitives::source_contracts::ResourceProfile::BoundedFile)
        });
    let runner_pack = binding
        .and_then(|binding| binding.runner_pack.clone())
        .or_else(|| attrs.runner_pack.clone())
        .unwrap_or_else(|| quote!(::sinex_primitives::source_contracts::RunnerPack::SinexdSource));
    let checkpoint_family = binding
        .and_then(|binding| binding.checkpoint_family.clone())
        .or_else(|| attrs.checkpoint_family.clone())
        .unwrap_or_else(|| {
            quote!(::sinex_primitives::source_contracts::CheckpointFamily::AppendStream)
        });
    let runtime_shape = binding
        .and_then(|binding| binding.runtime_shape.clone())
        .or_else(|| attrs.runtime_shape.clone())
        .unwrap_or_else(|| quote!(::sinex_primitives::source_contracts::RuntimeShape::Continuous));
    let material_lifecycle = binding
        .and_then(|binding| binding.material_lifecycle.clone())
        .or_else(|| attrs.material_lifecycle.clone())
        .unwrap_or_else(|| {
            quote!(::sinex_primitives::source_contracts::MaterialLifecyclePolicy::default_for(#resource_profile))
        });
    let transport_semantics = binding
        .and_then(|binding| binding.transport_semantics.clone())
        .or_else(|| attrs.transport_semantics.clone())
        .unwrap_or_else(|| {
            quote!(::sinex_primitives::source_contracts::TransportSemantics::default_for(
                #runner_pack,
                #checkpoint_family,
                #runtime_shape,
            ))
        });

    let capabilities = binding
        .filter(|binding| !binding.capabilities.is_empty())
        .map(|binding| &binding.capabilities)
        .unwrap_or(&attrs.capabilities);
    let capabilities_call = if capabilities.is_empty() {
        quote!()
    } else {
        let caps = capabilities.iter();
        quote!(.capabilities(&[ #(#caps),* ]))
    };
    let proposed = binding
        .and_then(|binding| binding.proposed)
        .unwrap_or(attrs.proposed);

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
            .resource_profile(#resource_profile)
            .source_id(#id)
            .runner_pack(#runner_pack)
            .material_lifecycle(#material_lifecycle)
            .transport_semantics(#transport_semantics)
            #capabilities_call
            .checkpoint_family(#checkpoint_family)
            .runtime_shape(#runtime_shape)
            .build_impact(::sinex_primitives::source_contracts::SourceBuildImpact::ZERO)
            .proposed(#proposed)
            .build()
        }
    })
}

// ---------------------------------------------------------------------------
// Site 3: register_source! factory wiring
// ---------------------------------------------------------------------------

/// Emit the `register_source!` adapter + parser factory wiring. `parser_name`
/// is the struct the macro is applied to: for `SourceDefinition` it is the
/// declarative parser marker; for `SourceMeta` it is the author's hand-written
/// `MaterialParser` implementor.
pub(crate) fn generate_factory_registration(
    parser_name: &Ident,
    attrs: &RegistrationAttrs,
) -> syn::Result<TokenStream> {
    let id = &attrs.id;
    let parser_path = attrs
        .factory_parser
        .clone()
        .unwrap_or_else(|| quote!(#parser_name));

    // Monitor-emit form: the adapter fires an emit fn at a lifecycle phase
    // rather than running a parser, so there is no parser to wire. Emits
    // `register_source!(emit_at:, emit:)` verbatim, bypassing the
    // `adapter_type_ident` allowlist (the adapter string is still carried on the
    // binding for deployment metadata).
    let factory_tokens = if !attrs.register_factory {
        quote!()
    } else if let Some(emit_fn) = &attrs.monitor_emit_fn {
        let phase = attrs.monitor_phase.as_deref().ok_or_else(|| {
            Error::new(
                Span::call_site(),
                "source_meta: 'monitor_emit_fn' requires 'monitor_phase'",
            )
        })?;
        let emit_ident = Ident::new(emit_fn, Span::call_site());
        let phase_ident = Ident::new(phase, Span::call_site());
        quote! {
            crate::register_source!(
                source_id: #id,
                emit_at: crate::sources::monitor_driver::MonitorPhase::#phase_ident,
                emit: #emit_ident,
            );
        }
    } else if attrs.parser_only_factory {
        quote! {
            crate::register_source!(
                source_id: #id,
                parser: #parser_path,
            );
        }
    } else {
        let adapter_path = if let Some(adapter) = &attrs.factory_adapter {
            quote!(#adapter)
        } else {
            let adapter_ident = adapter_type_ident(&attrs.adapter)?;
            quote!(crate::runtime::parser::#adapter_ident)
        };
        quote! {
            crate::register_source!(
                source_id: #id,
                adapter: #adapter_path,
                parser: #parser_path,
            );
        }
    };

    let driver_tokens = if let Some(driver) = &attrs.driver_factory {
        quote! {
            crate::register_source!(
                source_id: #id,
                driver: #driver,
            );
        }
    } else {
        quote!()
    };

    Ok(quote! {
        #factory_tokens
        #driver_tokens
    })
}

/// Resolve the adapter type identifier from the `adapter = "..."` attribute.
///
/// The attribute carries the adapter type name (also used verbatim as the
/// binding's `adapter` string field). The factory wiring references it under
/// `crate::runtime::parser::<Adapter>`.
fn adapter_type_ident(adapter: &str) -> syn::Result<Ident> {
    // Bare-ident form only (no generics) for the adapters re-exported from
    // `crate::runtime::parser`. Each entry must be a real adapter type that
    // compiles with `run_adapter_source::<Adapter, Parser>` — verified by the
    // sources that already wire it via the explicit `register_source!`
    // adapter+parser form.
    match adapter {
        "SqliteRowAdapter"
        | "AppendOnlyFileAdapter"
        | "StaticFileAdapter"
        | "DirectoryWalkAdapter"
        | "DbusStreamAdapter"
        | "JournalctlStreamAdapter"
        | "FileDropAdapter"
        | "FileContentDropAdapter"
        | "ClipboardPollingAdapter"
        | "UnixSocketStreamAdapter" => Ok(Ident::new(adapter, Span::call_site())),
        other => Err(Error::new(
            Span::call_site(),
            format!(
                "adapter \"{other}\" is not yet wired for factory registration \
                 (supported: SqliteRowAdapter, AppendOnlyFileAdapter, StaticFileAdapter, \
                 DirectoryWalkAdapter, DbusStreamAdapter, JournalctlStreamAdapter, \
                 FileDropAdapter, FileContentDropAdapter, ClipboardPollingAdapter, \
                 UnixSocketStreamAdapter); a generic or locally-aliased adapter \
                 type is not supported — keep the explicit register_source! wiring"
            ),
        )),
    }
}

// ---------------------------------------------------------------------------
// Typed enum-attribute parsing
// ---------------------------------------------------------------------------

/// Parse the value of a scalar enum attribute (`privacy_tier`, `runtime_shape`)
/// as a typed path such as `PrivacyTier::Public`. Captured verbatim and emitted
/// at the registration site, so an invalid variant is a type error at use, not
/// a stringly match in the macro. Rejects non-path values (e.g. string
/// literals) with a syn parse error pointing at the offending token.
pub(crate) fn parse_enum_path_attr(meta: &syn::meta::ParseNestedMeta) -> syn::Result<TokenStream> {
    let path: syn::Path = meta.value()?.parse()?;
    Ok(path.into_token_stream())
}

/// Parse the value of an enum attribute that may carry data, as a typed enum
/// *expression*. Used for `checkpoint_family` (`MutableSnapshot { .. }`),
/// `retention` (`Days { .. }` / `Tiered { .. }`), and `occurrence_identity`
/// (`Uuid5From("..")`). Accepts both the unit-variant path form and the
/// struct/tuple-variant forms. Emitted verbatim at the registration site.
pub(crate) fn parse_enum_expr_attr(meta: &syn::meta::ParseNestedMeta) -> syn::Result<TokenStream> {
    let expr: syn::Expr = meta.value()?.parse()?;
    Ok(expr.into_token_stream())
}

/// Parse a list-valued enum attribute written as a nested meta list, e.g.
/// `horizons(Horizon::Continuous, Horizon::Historical)`. Each entry is a typed
/// path captured verbatim; the generator emits them into the contract's list.
pub(crate) fn parse_enum_path_list_attr(
    meta: &syn::meta::ParseNestedMeta,
) -> syn::Result<Vec<TokenStream>> {
    let mut paths = Vec::new();
    meta.parse_nested_meta(|inner| {
        paths.push(inner.path.into_token_stream());
        Ok(())
    })?;
    Ok(paths)
}
