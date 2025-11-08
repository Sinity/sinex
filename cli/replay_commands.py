#!/usr/bin/env python3
"""Replay commands for Sinex CLI (RPC-backed control plane)."""

import json
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

import click
from rich import box
from rich.console import Console
from rich.panel import Panel
from rich.table import Table

try:
    from .rpc_client import create_client, SinexRPCError
except ImportError:  # pragma: no cover - direct execution fallback
    from rpc_client import create_client, SinexRPCError  # type: ignore


console = Console()
STATE_CHOICES = [
    "planning",
    "previewed",
    "approved",
    "executing",
    "committing",
    "completed",
    "failed",
    "cancelled",
]


def parse_time_option(label: str, value: Optional[str]) -> Optional[str]:
    """Validate and normalise ISO 8601 timestamps."""
    if value is None:
        return None
    normalized = value.strip()
    if not normalized:
        return None
    if normalized.endswith("Z"):
        normalized = normalized[:-1] + "+00:00"
    try:
        parsed = datetime.fromisoformat(normalized)
    except ValueError as exc:  # pragma: no cover - exercised via CLI
        raise click.ClickException(
            f"Invalid ISO 8601 timestamp for {label}: {value}"
        ) from exc
    return parsed.isoformat()


def build_scope(
    processor_id: str,
    since: Optional[str],
    until: Optional[str],
    material_ids: Tuple[str, ...],
    event_types: Tuple[str, ...],
) -> Dict[str, Any]:
    scope: Dict[str, Any] = {
        "processor_id": processor_id,
        "time_window": None,
        "material_filter": list(material_ids) or None,
        "filters": {},
    }

    if since or until:
        scope["time_window"] = [since, until]

    if event_types:
        scope["filters"]["event_types"] = list(event_types)

    if not scope["filters"]:
        scope["filters"] = {}

    return scope


def call_rpc(fn, *args, **kwargs):
    try:
        return fn(*args, **kwargs)
    except SinexRPCError as err:  # pragma: no cover - network/runtime failure
        raise click.ClickException(str(err)) from err


def render_operation(operation: Dict[str, Any]) -> None:
    table = Table(title="Replay Operation", box=box.SIMPLE)
    table.add_column("Field", style="cyan")
    table.add_column("Value", overflow="fold")

    table.add_row("Operation ID", str(operation.get("operation_id", "")))
    table.add_row("State", str(operation.get("state", "")))
    table.add_row("Actor", str(operation.get("actor", "")))
    table.add_row("Processor", operation.get("scope", {}).get("processor_id", "-"))
    table.add_row("Created", str(operation.get("created_at", "-")))
    table.add_row("Approved By", str(operation.get("approved_by", "-")))
    table.add_row("Executor", str(operation.get("executor_node", "-")))
    table.add_row("Started", str(operation.get("started_at", "-")))
    table.add_row("Finished", str(operation.get("finished_at", "-")))

    checkpoint = operation.get("checkpoint", {})
    checkpoint_text = (
        f"{checkpoint.get('processed_events', 0)} / {checkpoint.get('total_events', 0)}"
    )
    table.add_row("Checkpoint", checkpoint_text)

    console.print(table)

    scope_json = json.dumps(operation.get("scope", {}), indent=2)
    console.print(Panel(scope_json, title="Scope", border_style="cyan"))

    preview = operation.get("preview_summary")
    if preview:
        render_preview(preview)


def render_preview(preview: Dict[str, Any]) -> None:
    table = Table(title="Preview Summary", box=box.SIMPLE)
    table.add_column("Metric", style="cyan")
    table.add_column("Value", overflow="fold")

    table.add_row("Total Events", str(preview.get("total_events", "-")))

    window = preview.get("time_window") or {}
    table.add_row(
        "Time Window",
        f"{window.get('start', '-') } → {window.get('end', '-')}",
    )

    material = preview.get("material_filter")
    if material:
        table.add_row("Materials", json.dumps(material))

    top_types = preview.get("top_event_types") or []
    if top_types:
        summary = ", ".join(
            f"{entry.get('event_type')}: {entry.get('count')}" for entry in top_types
        )
        table.add_row("Top Event Types", summary)

    console.print(table)


def render_operation_list(operations: List[Dict[str, Any]]) -> None:
    if not operations:
        console.print("[yellow]No replay operations found[/yellow]")
        return

    table = Table(title="Replay Operations", box=box.MINIMAL_DOUBLE_HEAD)
    table.add_column("Operation ID", style="cyan")
    table.add_column("State", style="magenta")
    table.add_column("Processor", style="white")
    table.add_column("Actor", style="green")
    table.add_column("Created", style="yellow")

    for op in operations:
        scope = op.get("scope", {})
        table.add_row(
            str(op.get("operation_id", "")),
            str(op.get("state", "")),
            scope.get("processor_id", "-"),
            str(op.get("actor", "")),
            str(op.get("created_at", "")),
        )

    console.print(table)


@click.group()
def replay() -> None:
    """Event replay planning, approval, and execution commands."""


@replay.command("plan")
@click.option("--processor", "-p", "processor_id", help="Processor / event source ID")
@click.option("--source", "-s", "source_id", help="Alias for --processor")
@click.option(
    "--event-type",
    "-t",
    "event_types",
    multiple=True,
    help="Event type filter (repeatable)",
)
@click.option(
    "--material-id",
    "-m",
    "material_ids",
    multiple=True,
    help="Material ULID filter (repeatable)",
)
@click.option("--since", help="Earliest event timestamp (ISO 8601)")
@click.option("--until", help="Latest event timestamp (ISO 8601)")
@click.option(
    "--actor",
    default="sinex-cli",
    show_default=True,
    help="Actor recorded with the operation",
)
def plan_command(
    processor_id: Optional[str],
    source_id: Optional[str],
    event_types: Tuple[str, ...],
    material_ids: Tuple[str, ...],
    since: Optional[str],
    until: Optional[str],
    actor: str,
) -> None:
    """Create a replay operation for a processor/source."""

    target = processor_id or source_id
    if not target:
        raise click.UsageError("Please provide --processor (or --source).")

    scope = build_scope(
        target,
        parse_time_option("since", since),
        parse_time_option("until", until),
        material_ids,
        event_types,
    )

    client = create_client()
    operation = call_rpc(client.replay_create_operation, actor, scope)

    console.print(
        f"[bold green]Replay operation planned[/bold green] (state: {operation.get('state')})"
    )
    render_operation(operation)
    console.print("Next: run 'exo replay preview <operation_id>' to inspect the scope.")


@replay.command("preview")
@click.argument("operation_id")
def preview_command(operation_id: str) -> None:
    """Generate and display the preview summary for an operation."""

    client = create_client()
    operation, preview = call_rpc(client.replay_preview_operation, operation_id)

    console.print("[bold cyan]Preview generated[/bold cyan]")
    render_operation(operation)
    render_preview(preview)


@replay.command("approve")
@click.argument("operation_id")
@click.option(
    "--approver",
    default="sinex-cli",
    show_default=True,
    help="Identifier to record as the approver",
)
def approve_command(operation_id: str, approver: str) -> None:
    """Approve a previewed replay operation."""

    client = create_client()
    operation = call_rpc(client.replay_approve_operation, operation_id, approver)
    console.print("[bold green]Operation approved[/bold green]")
    render_operation(operation)


@replay.command("execute")
@click.argument("operation_id")
@click.option(
    "--executor",
    default="sinex-cli",
    show_default=True,
    help="Identifier of the executor",
)
def execute_command(operation_id: str, executor: str) -> None:
    """Execute an approved replay operation."""

    client = create_client()
    operation = call_rpc(client.replay_execute_operation, operation_id, executor)
    console.print("[bold green]Replay execution completed[/bold green]")
    render_operation(operation)


@replay.command("cancel")
@click.argument("operation_id")
@click.option("--reason", help="Cancellation reason")
def cancel_command(operation_id: str, reason: Optional[str]) -> None:
    """Cancel a pending or running replay operation."""

    client = create_client()
    operation = call_rpc(client.replay_cancel_operation, operation_id, reason)
    console.print("[bold yellow]Operation cancelled[/bold yellow]")
    render_operation(operation)


@replay.command("status")
@click.argument("operation_id")
def status_command(operation_id: str) -> None:
    """Show the current status of a replay operation."""

    client = create_client()
    operation = call_rpc(client.replay_operation_status, operation_id)
    render_operation(operation)


@replay.command("list")
@click.option(
    "--state",
    type=click.Choice(STATE_CHOICES, case_sensitive=False),
    help="Filter by replay state",
)
def list_command(state: Optional[str]) -> None:
    """List replay operations, optionally filtering by state."""

    client = create_client()
    normalized = state.lower() if state else None
    operations = call_rpc(client.replay_list_operations, normalized)
    render_operation_list(operations)
