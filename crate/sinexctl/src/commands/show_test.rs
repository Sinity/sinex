use super::*;
use sinex_primitives::public_ref::ResolvedObjectStatus;
use sinex_primitives::testing::event_fixture;
use sinex_primitives::views::SinexObjectRef;
use sinex_primitives::{Event, EventSource, EventType, Id, JsonValue, Uuid};
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
async fn local_catalog_resolver_reports_not_found_for_catalog_misses() -> xtask::TestResult<()> {
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
async fn local_catalog_resolver_leaves_gateway_refs_to_gateway_paths() -> xtask::TestResult<()> {
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
async fn event_resolver_wraps_events_as_cards() -> xtask::TestResult<()> {
    let mut event = event_fixture(
        EventSource::from_static("git"),
        EventType::from_static("commit.created"),
        json!({"summary": "commit fixture"}),
    );
    event.id = Some(Id::<Event<JsonValue>>::from_uuid(Uuid::now_v7()));
    let event_id = event.id.expect("fixture event has id").to_string();
    let view =
        resolved_event_card_view(SinexObjectRef::new(SinexObjectKind::Event, event_id), event)?;

    assert_eq!(view.status, ResolvedObjectStatus::Resolved);
    assert_eq!(view.source_surface.as_deref(), Some("sinex.events.lineage"));
    assert_eq!(view.payload["source"]["raw"], "git");
    assert_eq!(view.payload["event_type"], "commit.created");
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
