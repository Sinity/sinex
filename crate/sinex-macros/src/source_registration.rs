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
use quote::quote;
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
    /// Extra event types this source may emit besides `event_type`.
    pub additional_event_types: Vec<String>,

    // SourceContract semantic fields
    pub privacy_tier: Option<String>,
    pub horizons: Vec<String>,
    pub retention: Option<String>,
    /// REQUIRED — checked during attribute parsing, not here.
    pub occurrence_identity: Option<String>,
    pub access_policy: Option<String>,

    // SourceRuntimeBinding deployment fields
    pub implementation: Option<String>,
    pub privacy_context: Option<String>,
    pub material_policy: Option<String>,
    pub checkpoint_policy: Option<String>,
    pub resource_shape: Option<String>,
    pub runner_pack: Option<String>,
    pub checkpoint_family: Option<String>,
    pub runtime_shape: Option<String>,
    pub package_impact: Option<String>,
    pub implementation_mode: Option<String>,
    pub capabilities: Vec<String>,
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

pub(crate) fn generate_source_runtime_binding(
    attrs: &RegistrationAttrs,
) -> syn::Result<TokenStream> {
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

/// Emit the `register_source!` adapter + parser factory wiring. `parser_name`
/// is the struct the macro is applied to: for `SourceDefinition` it is the
/// declarative parser marker; for `SourceMeta` it is the author's hand-written
/// `MaterialParser` implementor.
pub(crate) fn generate_factory_registration(
    parser_name: &Ident,
    attrs: &RegistrationAttrs,
) -> syn::Result<TokenStream> {
    let id = &attrs.id;
    let adapter_ident = adapter_type_ident(&attrs.adapter)?;

    Ok(quote! {
        crate::register_source!(
            source_id: #id,
            adapter: crate::runtime::parser::#adapter_ident,
            parser: #parser_name,
        );
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
        "SqliteRowAdapter" | "AppendOnlyFileAdapter" | "StaticFileAdapter"
        | "DirectoryWalkAdapter" | "DbusStreamAdapter" | "JournalctlStreamAdapter"
        | "FileDropAdapter" | "FileContentDropAdapter" | "ClipboardPollingAdapter"
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
