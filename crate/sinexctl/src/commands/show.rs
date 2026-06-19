use std::str::FromStr as _;

use clap::Args;
use serde_json::{Value, json};
use sinex_primitives::public_ref::{PublicSinexRef, ResolvedObjectView};
use sinex_primitives::rpc::method_catalog;
use sinex_primitives::views::{SinexObjectKind, ViewEnvelope};

use crate::Result;
use crate::client::GatewayClient;
use crate::commands::ops::operation_to_view;
use crate::fmt::print_finite_envelope;
use crate::model::OutputFormat;
use crate::model::format_registry::command_catalog;

#[derive(Debug, Args)]
#[command(after_help = "\
EXAMPLES:
    sinexctl show source-material:01912345-6789-7abc-def0-123456789abc
    sinexctl show operation:01HQ2KM...
    sinexctl show policy:privacy/default --format json
")]
pub struct ShowCommand {
    /// Public Sinex ref in '<kind>:<id>' form.
    #[arg(value_name = "REF")]
    ref_: String,
}

impl ShowCommand {
    pub fn execute_local_if_supported(&self, format: OutputFormat) -> Result<bool> {
        let public_ref = PublicSinexRef::from_str(&self.ref_)?;
        let public_ref_text = public_ref.to_string();
        let object_ref = public_ref.into_object_ref();
        let Some(view) = resolve_local_catalog_ref(&object_ref)? else {
            return Ok(false);
        };

        let envelope = ViewEnvelope::new("sinexctl.show", view).with_query_echo(json!({
            "ref": public_ref_text
        }));
        if print_finite_envelope(&envelope, format)? {
            return Ok(true);
        }

        println!("{}", format_resolved_object_table(&envelope.payload));
        Ok(true)
    }

    pub async fn execute(&self, client: &GatewayClient, format: OutputFormat) -> Result<()> {
        let public_ref = PublicSinexRef::from_str(&self.ref_)?;
        let envelope = resolve_ref(client, public_ref).await?;

        if print_finite_envelope(&envelope, format)? {
            return Ok(());
        }

        println!("{}", format_resolved_object_table(&envelope.payload));
        Ok(())
    }
}

async fn resolve_ref(
    client: &GatewayClient,
    public_ref: PublicSinexRef,
) -> Result<ViewEnvelope<ResolvedObjectView>> {
    let public_ref_text = public_ref.to_string();
    let object_ref = public_ref.into_object_ref();

    if let Some(view) = resolve_local_catalog_ref(&object_ref)? {
        return Ok(
            ViewEnvelope::new("sinexctl.show", view).with_query_echo(json!({
                "ref": public_ref_text
            })),
        );
    }

    let view = match object_ref.kind {
        SinexObjectKind::SourceMaterial => {
            let response = client
                .sources_show(sinex_primitives::rpc::sources::SourcesShowRequest {
                    material_id: object_ref.id.clone(),
                })
                .await?;
            ResolvedObjectView::resolved(
                object_ref,
                "sinexctl.sources.show",
                serde_json::to_value(response)?,
            )
        }
        SinexObjectKind::SourceDriver => {
            let envelope = client.sources_status_view().await?;
            let Some(source) = envelope
                .payload
                .sources
                .iter()
                .find(|source| source.source_id == object_ref.id)
            else {
                return Ok(ViewEnvelope::new(
                    "sinexctl.show",
                    ResolvedObjectView::not_found(object_ref, "sinexctl.sources.status"),
                )
                .with_query_echo(json!({
                    "ref": public_ref_text
                })));
            };
            ResolvedObjectView::resolved(
                object_ref,
                "sinexctl.sources.status",
                serde_json::to_value(source)?,
            )
        }
        SinexObjectKind::Operation => {
            let operation = client.ops_get(&object_ref.id).await?;
            let view = operation_to_view(&operation);
            ResolvedObjectView::resolved(
                object_ref,
                "sinexctl.ops.get",
                serde_json::to_value(view)?,
            )
        }
        _ => ResolvedObjectView::unsupported(object_ref),
    };

    Ok(
        ViewEnvelope::new("sinexctl.show", view).with_query_echo(json!({
            "ref": public_ref_text
        })),
    )
}

fn resolve_local_catalog_ref(
    object_ref: &sinex_primitives::views::SinexObjectRef,
) -> Result<Option<ResolvedObjectView>> {
    match object_ref.kind {
        SinexObjectKind::Command => {
            let Some(entry) = command_catalog()
                .into_iter()
                .find(|entry| entry.path == object_ref.id)
            else {
                return Ok(Some(ResolvedObjectView::not_found(
                    object_ref.clone(),
                    "sinexctl.command_catalog",
                )));
            };
            Ok(Some(ResolvedObjectView::resolved(
                object_ref.clone(),
                "sinexctl.command_catalog",
                serde_json::to_value(entry)?,
            )))
        }
        SinexObjectKind::RpcMethod => {
            let Some(method) = method_catalog()
                .into_iter()
                .find(|method| method.name == object_ref.id)
            else {
                return Ok(Some(ResolvedObjectView::not_found(
                    object_ref.clone(),
                    "sinex.rpc.method_catalog",
                )));
            };
            Ok(Some(ResolvedObjectView::resolved(
                object_ref.clone(),
                "sinex.rpc.method_catalog",
                serde_json::to_value(method)?,
            )))
        }
        _ => Ok(None),
    }
}

fn format_resolved_object_table(view: &ResolvedObjectView) -> String {
    let mut lines = vec![
        format!("Ref: {}", view.public_ref),
        format!("Kind: {:?}", view.ref_.kind),
        format!("Status: {:?}", view.status),
    ];
    if let Some(surface) = &view.source_surface {
        lines.push(format!("Source surface: {surface}"));
    }
    if let Some(message) = &view.message {
        lines.push(format!("Message: {message}"));
    }
    if view.payload != Value::Null {
        lines.push("Payload: use --format json for full object details".to_string());
    }
    lines.join("\n")
}

#[cfg(test)]
mod tests {
    use super::*;
    use sinex_primitives::public_ref::ResolvedObjectStatus;
    use sinex_primitives::views::SinexObjectRef;
    use xtask::sandbox::prelude::sinex_test;

    #[sinex_test]
    async fn local_catalog_resolver_resolves_command_refs() -> xtask::TestResult<()> {
        let ref_ = SinexObjectRef::new(SinexObjectKind::Command, "show");
        let view = resolve_local_catalog_ref(&ref_)?
            .expect("command refs are handled by the local command catalog");

        assert_eq!(view.status, ResolvedObjectStatus::Resolved);
        assert_eq!(
            view.source_surface.as_deref(),
            Some("sinexctl.command_catalog")
        );
        assert_eq!(view.payload["path"], "show");
        assert_eq!(view.payload["family"], "query");
        Ok(())
    }

    #[sinex_test]
    async fn local_catalog_resolver_resolves_rpc_method_refs() -> xtask::TestResult<()> {
        let ref_ = SinexObjectRef::new(SinexObjectKind::RpcMethod, "sources.show");
        let view = resolve_local_catalog_ref(&ref_)?
            .expect("rpc-method refs are handled by the typed RPC catalog");

        assert_eq!(view.status, ResolvedObjectStatus::Resolved);
        assert_eq!(
            view.source_surface.as_deref(),
            Some("sinex.rpc.method_catalog")
        );
        assert_eq!(view.payload["name"], "sources.show");
        assert_eq!(view.payload["role"], "read_only");
        Ok(())
    }

    #[sinex_test]
    async fn local_catalog_resolver_reports_not_found_for_catalog_misses() -> xtask::TestResult<()>
    {
        let ref_ = SinexObjectRef::new(SinexObjectKind::Command, "missing command");
        let view = resolve_local_catalog_ref(&ref_)?
            .expect("command refs are handled even when the command is absent");

        assert_eq!(view.status, ResolvedObjectStatus::NotFound);
        assert_eq!(
            view.source_surface.as_deref(),
            Some("sinexctl.command_catalog")
        );
        Ok(())
    }

    #[sinex_test]
    async fn local_catalog_resolver_leaves_gateway_refs_to_gateway_paths() -> xtask::TestResult<()>
    {
        let ref_ = SinexObjectRef::new(SinexObjectKind::SourceMaterial, "material-id");

        assert!(resolve_local_catalog_ref(&ref_)?.is_none());
        Ok(())
    }

    #[sinex_test]
    async fn show_command_identifies_catalog_refs_as_local() -> xtask::TestResult<()> {
        let command = ShowCommand {
            ref_: "command:show".to_string(),
        };
        let public_ref = PublicSinexRef::from_str(&command.ref_)?;
        let object_ref = public_ref.into_object_ref();

        assert!(resolve_local_catalog_ref(&object_ref)?.is_some());
        Ok(())
    }

    #[sinex_test]
    async fn show_command_leaves_material_refs_for_gateway_client() -> xtask::TestResult<()> {
        let command = ShowCommand {
            ref_: "source-material:material-id".to_string(),
        };
        let public_ref = PublicSinexRef::from_str(&command.ref_)?;
        let object_ref = public_ref.into_object_ref();

        assert!(resolve_local_catalog_ref(&object_ref)?.is_none());
        Ok(())
    }

    #[sinex_test]
    async fn table_renderer_shows_resolution_status_and_surface() -> xtask::TestResult<()> {
        let ref_ = SinexObjectRef::new(SinexObjectKind::Command, "show");
        let table = format_resolved_object_table(&ResolvedObjectView::resolved(
            ref_,
            "sinexctl.command_catalog",
            json!({"path": "show"}),
        ));

        assert!(table.contains("Ref: command:show"));
        assert!(table.contains("Status: Resolved"));
        assert!(table.contains("Source surface: sinexctl.command_catalog"));
        assert!(table.contains("Payload: use --format json for full object details"));
        Ok(())
    }
}
