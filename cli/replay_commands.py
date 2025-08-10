#!/usr/bin/env python3
"""
Replay commands for Sinex CLI
"""

import click
import json
from datetime import datetime
from typing import Optional

from rich.console import Console
from rich.prompt import Prompt, Confirm

try:
    from .replay_planner import (
        ReplayPlanner, ReplayPlan, ReplayGate, GateType,
        display_replay_plan, display_execution_results
    )
except ImportError:
    from replay_planner import (
        ReplayPlanner, ReplayPlan, ReplayGate, GateType,
        display_replay_plan, display_execution_results
    )

console = Console()


@click.group()
def replay():
    """Event replay planning and execution commands."""
    pass


@replay.command('plan')
@click.option('--name', '-n', required=True, help='Name for the replay plan')
@click.option('--description', '-d', help='Description of the replay')
@click.option('--source', '-s', help='Filter by event source')
@click.option('--event-type', '-t', help='Filter by event type')
@click.option('--since', help='Replay events since datetime')
@click.option('--until', help='Replay events until datetime')
@click.option('--limit', type=int, default=1000, help='Maximum events to replay')
@click.option('--target', type=click.Choice(['preview', 'nats', 'database']), 
              default='preview', help='Target system for replay')
@click.option('--dry-run/--live', default=True, help='Dry run mode (default: true)')
@click.option('--interactive', '-i', is_flag=True, help='Interactive gate configuration')
def create_plan(name: str, description: Optional[str], source: Optional[str],
                event_type: Optional[str], since: Optional[str], until: Optional[str],
                limit: int, target: str, dry_run: bool, interactive: bool):
    """Create a new replay plan."""
    
    planner = ReplayPlanner()
    
    # Build source query
    source_query = {}
    if source:
        source_query['source'] = source
    if event_type:
        source_query['event_type'] = event_type
    if since:
        source_query['since'] = since
    if until:
        source_query['until'] = until
    source_query['limit'] = limit
    
    # Create plan
    plan = planner.create_plan(
        name=name,
        description=description or f"Replay plan created at {datetime.utcnow()}",
        source_query=source_query,
        target_system=target,
        dry_run=dry_run
    )
    
    # Interactive gate configuration
    if interactive:
        console.print("\n[cyan]Configure Control Gates[/cyan]")
        console.print("Gates pause replay execution for inspection or control.\n")
        
        while Confirm.ask("Add a gate?", default=False):
            gate_type = Prompt.ask(
                "Gate type",
                choices=["time", "count", "event", "manual", "condition"],
                default="count"
            )
            
            gate_name = Prompt.ask("Gate name")
            gate_desc = Prompt.ask("Gate description")
            
            # Configure condition based on type
            if gate_type == "count":
                condition = int(Prompt.ask("Pause after how many events?", default="10"))
            elif gate_type == "event":
                condition = Prompt.ask("Event type to pause on")
            elif gate_type == "time":
                condition = Prompt.ask("Timestamp to pause at (ISO format)")
            elif gate_type == "manual":
                condition = None
            else:  # condition
                console.print("Enter JSON condition (e.g., {'key': 'value'})")
                condition_str = Prompt.ask("Condition")
                condition = json.loads(condition_str)
            
            action = Prompt.ask(
                "Gate action",
                choices=["pause", "skip", "transform"],
                default="pause"
            )
            
            gate = ReplayGate(
                gate_type=GateType(gate_type),
                name=gate_name,
                description=gate_desc,
                condition=condition,
                action=action
            )
            
            planner.add_gate(plan, gate)
            console.print(f"[green]Added gate: {gate_name}[/green]")
    
    # Save plan
    planner.save_plan(plan)
    
    console.print(f"\n[bold green]Replay plan created![/bold green]")
    console.print(f"Operation ID: [yellow]{plan.operation_id}[/yellow]")
    
    # Display plan
    display_replay_plan(plan)
    
    # Preview events
    if Confirm.ask("\nPreview events that will be replayed?", default=True):
        events = planner.preview_events(**source_query)
        console.print(f"\nFound [cyan]{len(events)}[/cyan] events to replay")
        
        if events and Confirm.ask("Show first 5 events?", default=True):
            for i, event in enumerate(events[:5]):
                console.print(f"\n[yellow]Event {i+1}:[/yellow]")
                console.print(f"  Type: {event['event_type']}")
                console.print(f"  Source: {event['source']}")
                console.print(f"  Time: {event.get('ts_orig', event['ts_ingest'])}")
                if event.get('payload'):
                    console.print(f"  Payload: {json.dumps(event['payload'], indent=2)[:200]}...")


@replay.command('list')
@click.option('--status', type=click.Choice(['draft', 'executing', 'completed', 'failed']),
              help='Filter by status')
@click.option('--limit', type=int, default=20, help='Maximum plans to show')
def list_plans(status: Optional[str], limit: int):
    """List replay plans."""
    
    planner = ReplayPlanner()
    
    with planner.get_connection() as conn:
        with conn.cursor() as cur:
            query = """
                SELECT operation_id, status, metadata, created_at, executed_at
                FROM core.replay_operations
                WHERE operation_type = 'replay'
            """
            params = []
            
            if status:
                query += " AND status = %s"
                params.append(status)
            
            query += " ORDER BY created_at DESC LIMIT %s"
            params.append(limit)
            
            cur.execute(query, params)
            plans = cur.fetchall()
    
    if not plans:
        console.print("[yellow]No replay plans found[/yellow]")
        return
    
    from rich.table import Table
    table = Table(title="Replay Plans")
    
    table.add_column("Operation ID", style="cyan")
    table.add_column("Name", style="white")
    table.add_column("Status", style="yellow")
    table.add_column("Target", style="green")
    table.add_column("Created", style="blue")
    table.add_column("Executed", style="magenta")
    
    for plan in plans:
        metadata = plan['metadata']
        table.add_row(
            plan['operation_id'],
            metadata.get('name', 'Unnamed'),
            plan['status'],
            metadata.get('target_system', 'unknown'),
            plan['created_at'].strftime('%Y-%m-%d %H:%M'),
            plan['executed_at'].strftime('%Y-%m-%d %H:%M') if plan['executed_at'] else 'Never'
        )
    
    console.print(table)


@replay.command('show')
@click.argument('operation_id')
def show_plan(operation_id: str):
    """Show details of a replay plan."""
    
    planner = ReplayPlanner()
    plan = planner.load_plan(operation_id)
    
    if not plan:
        console.print(f"[red]Plan not found: {operation_id}[/red]")
        return
    
    display_replay_plan(plan)
    
    # Show source query details
    console.print("\n[cyan]Source Query:[/cyan]")
    for key, value in plan.source_query.items():
        console.print(f"  {key}: {value}")
    
    # Show transformation details if any
    if plan.transformations:
        console.print("\n[cyan]Transformations:[/cyan]")
        for transform in plan.transformations:
            console.print(f"  • {transform}")


@replay.command('execute')
@click.argument('operation_id')
@click.option('--force', is_flag=True, help='Skip confirmation')
@click.option('--no-interactive', is_flag=True, help='Disable interactive gates')
def execute_plan(operation_id: str, force: bool, no_interactive: bool):
    """Execute a replay plan."""
    
    planner = ReplayPlanner()
    plan = planner.load_plan(operation_id)
    
    if not plan:
        console.print(f"[red]Plan not found: {operation_id}[/red]")
        return
    
    if plan.status == 'completed':
        console.print("[yellow]Warning: This plan has already been executed[/yellow]")
        if not Confirm.ask("Execute again?", default=False):
            return
    
    # Display plan details
    display_replay_plan(plan)
    
    # Confirmation
    if not force:
        mode = "DRY RUN" if plan.dry_run else "LIVE"
        console.print(f"\n[yellow]Mode: {mode}[/yellow]")
        
        if not plan.dry_run:
            console.print("[red]WARNING: This is a LIVE execution![/red]")
        
        if not Confirm.ask("\nProceed with execution?", default=True):
            console.print("[yellow]Execution cancelled[/yellow]")
            return
    
    # Execute plan
    console.print("\n[cyan]Starting replay execution...[/cyan]\n")
    
    try:
        results = planner.execute_plan(plan, interactive=not no_interactive)
        display_execution_results(results)
        
    except Exception as e:
        console.print(f"\n[red]Execution failed: {e}[/red]")
        return


@replay.command('delete')
@click.argument('operation_id')
@click.option('--force', is_flag=True, help='Skip confirmation')
def delete_plan(operation_id: str, force: bool):
    """Delete a replay plan."""
    
    planner = ReplayPlanner()
    plan = planner.load_plan(operation_id)
    
    if not plan:
        console.print(f"[red]Plan not found: {operation_id}[/red]")
        return
    
    if not force:
        console.print(f"Plan: [yellow]{plan.name}[/yellow]")
        console.print(f"Status: {plan.status}")
        
        if not Confirm.ask("\nDelete this plan?", default=False):
            console.print("[yellow]Deletion cancelled[/yellow]")
            return
    
    with planner.get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                DELETE FROM core.replay_operations
                WHERE operation_id = %s AND operation_type = 'replay'
            """, (operation_id,))
            conn.commit()
    
    console.print(f"[green]Plan deleted: {operation_id}[/green]")


@replay.command('add-gate')
@click.argument('operation_id')
@click.option('--type', '-t', 'gate_type', 
              type=click.Choice(['time', 'count', 'event', 'manual', 'condition']),
              required=True, help='Gate type')
@click.option('--name', '-n', required=True, help='Gate name')
@click.option('--description', '-d', help='Gate description')
@click.option('--condition', '-c', help='Gate condition (varies by type)')
@click.option('--action', '-a', type=click.Choice(['pause', 'skip', 'transform']),
              default='pause', help='Gate action')
def add_gate_to_plan(operation_id: str, gate_type: str, name: str,
                     description: Optional[str], condition: Optional[str],
                     action: str):
    """Add a gate to an existing replay plan."""
    
    planner = ReplayPlanner()
    plan = planner.load_plan(operation_id)
    
    if not plan:
        console.print(f"[red]Plan not found: {operation_id}[/red]")
        return
    
    if plan.status != 'draft':
        console.print(f"[red]Cannot modify plan with status: {plan.status}[/red]")
        return
    
    # Parse condition based on type
    parsed_condition = None
    if gate_type == 'count':
        parsed_condition = int(condition) if condition else 10
    elif gate_type == 'event':
        parsed_condition = condition
    elif gate_type == 'time':
        parsed_condition = condition
    elif gate_type == 'condition':
        parsed_condition = json.loads(condition) if condition else {}
    
    gate = ReplayGate(
        gate_type=GateType(gate_type),
        name=name,
        description=description or f"{gate_type} gate",
        condition=parsed_condition,
        action=action
    )
    
    planner.add_gate(plan, gate)
    planner.save_plan(plan)
    
    console.print(f"[green]Gate added to plan: {name}[/green]")
    display_replay_plan(plan)