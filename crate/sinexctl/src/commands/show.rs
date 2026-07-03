use std::str::FromStr as _;

use clap::Args;
use serde_json::{Value, json};
use sinex_primitives::public_ref::{PublicSinexRef, ResolvedObjectView};
use sinex_primitives::query::{LineageDirection, LineageQuery, QueryResultEvent};
use sinex_primitives::rpc::method_catalog;
use sinex_primitives::views::{EventCardView, SinexObjectKind, ViewEnvelope};
use sinex_primitives::{Event, Id, JsonValue};

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

pub(crate) async fn resolve_ref(
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
        SinexObjectKind::Event => {
            let event_id = Id::<Event<JsonValue>>::from_str(&object_ref.id)?;
            let lineage = client
                .trace_lineage(LineageQuery {
                    event_id,
                    direction: LineageDirection::Ancestors,
                    max_depth: 1,
                })
                .await?;
            resolved_event_card_view(object_ref, lineage.root)?
        }
        _ => ResolvedObjectView::unsupported(object_ref),
    };

    Ok(
        ViewEnvelope::new("sinexctl.show", view).with_query_echo(json!({
            "ref": public_ref_text
        })),
    )
}

fn resolved_event_card_view(
    object_ref: sinex_primitives::views::SinexObjectRef,
    event: Event<JsonValue>,
) -> Result<ResolvedObjectView> {
    let card = EventCardView::from_query_event(&QueryResultEvent {
        event,
        relevance_score: None,
        snippet: None,
    });
    Ok(ResolvedObjectView::resolved(
        object_ref,
        "sinex.events.lineage",
        serde_json::to_value(card)?,
    ))
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
#[path = "show_test.rs"]
mod tests;
