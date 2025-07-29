#!/usr/bin/env python3
"""
Sinex CLI - Query your digital memory (Phase 2 Enhanced)
"""

import os
import sys
import json
import subprocess
import shutil
from datetime import datetime, timedelta
from typing import Optional, List, Dict, Any
from pathlib import Path

import click
import psycopg2
from psycopg2.extras import RealDictCursor
from rich.console import Console
from rich.table import Table
from rich.json import JSON
from rich.text import Text
from rich.panel import Panel
from rich import box

# Import RPC client
try:
    from .rpc_client import SinexRPCClient, SinexRPCError, create_client
except ImportError:
    # Handle case where running directly
    from rpc_client import SinexRPCClient, SinexRPCError, create_client

console = Console()


def get_db_connection():
    """Get database connection using environment variable or default."""
    db_url = os.environ.get('DATABASE_URL', 'postgresql://localhost/sinex')
    return psycopg2.connect(db_url, cursor_factory=RealDictCursor)


def get_rpc_client(rpc_url: Optional[str] = None) -> SinexRPCClient:
    """Get RPC client using environment variable or default."""
    if rpc_url is None:
        rpc_url = os.environ.get('SINEX_RPC_URL', 'http://127.0.0.1:9999')
    
    return SinexRPCClient(rpc_url)


def parse_time_delta(time_str: str) -> timedelta:
    """Parse time string like '1h', '30m', '2d' into timedelta."""
    units = {
        's': 'seconds',
        'm': 'minutes', 
        'h': 'hours',
        'd': 'days',
        'w': 'weeks'
    }
    
    unit = time_str[-1]
    if unit not in units:
        raise ValueError(f"Invalid time unit: {unit}")
    
    value = int(time_str[:-1])
    return timedelta(**{units[unit]: value})


def _query_with_rpc(rpc_url: Optional[str], source: Optional[str], event_type: Optional[str],
                   since: Optional[str], until: Optional[str], last: Optional[str], 
                   limit: int, host: Optional[str]) -> List[Dict]:
    """Query events using RPC client."""
    try:
        client = get_rpc_client(rpc_url)
        events = client.query_events_compatible(
            source=source,
            event_type=event_type,
            since=since,
            until=until,
            last=last,
            limit=limit,
            host=host
        )
        return events
    except SinexRPCError:
        # Re-raise RPC errors as-is
        raise
    except Exception as e:
        raise SinexRPCError(-32603, f"RPC query failed: {e}") from e


def _query_with_database(source: Optional[str], event_type: Optional[str],
                        since: Optional[str], until: Optional[str], last: Optional[str],
                        limit: int, host: Optional[str]) -> List[Dict]:
    """Query events using direct database connection."""
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            # Build query
            query_parts = [
                "SELECT event_id, source, event_type, ts_ingest, ts_orig, host, "
                "ingestor_version, payload_schema_id, payload FROM core.events"
            ]
            conditions = []
            params = []
            
            if source:
                conditions.append("source = %s")
                params.append(source)
                
            if event_type:
                conditions.append("event_type = %s")
                params.append(event_type)
                
            if host:
                conditions.append("host = %s")
                params.append(host)
            
            if since:
                since_dt = parse_datetime(since)
                conditions.append("ts_ingest >= %s")
                params.append(since_dt)
                
            if until:
                until_dt = parse_datetime(until)
                conditions.append("ts_ingest <= %s")
                params.append(until_dt)
            
            if last:
                time_delta = parse_time_delta(last)
                conditions.append("ts_ingest > %s")
                params.append(datetime.now(datetime.timezone.utc) - time_delta)
            
            if conditions:
                query_parts.append("WHERE " + " AND ".join(conditions))
            
            query_parts.append("ORDER BY ts_ingest DESC")
            query_parts.append(f"LIMIT {limit}")
            
            query_sql = " ".join(query_parts)
            
            cur.execute(query_sql, params)
            events = cur.fetchall()
            
    return events


def _sources_with_rpc(rpc_url: Optional[str]) -> List[Dict]:
    """Get sources statistics using RPC client."""
    try:
        client = get_rpc_client(rpc_url)
        sources = client.get_sources_statistics()
        return sources
    except SinexRPCError:
        # Re-raise RPC errors as-is
        raise
    except Exception as e:
        raise SinexRPCError(-32603, f"RPC sources query failed: {e}") from e


def _sources_with_database() -> List[Dict]:
    """Get sources statistics using direct database connection (legacy mode)."""
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT 
                    source, 
                    COUNT(*) as event_count,
                    COUNT(DISTINCT event_type) as event_type_count,
                    COUNT(DISTINCT host) as host_count,
                    MIN(ts_ingest) as first_event,
                    MAX(ts_ingest) as last_event,
                    AVG(CASE WHEN ts_orig IS NOT NULL THEN 
                        EXTRACT(EPOCH FROM (ts_ingest - ts_orig)) ELSE NULL END) as avg_ingest_delay
                FROM core.events
                GROUP BY source
                ORDER BY event_count DESC
            """)
            sources = cur.fetchall()
    return sources


def _stats_with_rpc(rpc_url: Optional[str]) -> None:
    """Show stats using RPC client - limited functionality."""
    try:
        client = get_rpc_client(rpc_url)
        
        # Get basic event counts by source
        counts = client.get_event_count_by_source(days_back=7)
        total_events = sum(counts.values())
        
        console.print(f"\n[bold]📊 Total Events (last 7 days):[/bold] {total_events:,}")
        console.print(f"[dim]Note: Using RPC mode - limited statistics available[/dim]")
        console.print(f"[dim]Use --use-db flag for full statistics[/dim]")
        
        # Show event counts by source
        if counts:
            console.print("\n[bold]📋 Events by Source (last 7 days):[/bold]")
            source_table = Table()
            source_table.add_column("Source", style="cyan")
            source_table.add_column("Count", justify="right", style="white")
            
            for source, count in sorted(counts.items(), key=lambda x: x[1], reverse=True):
                source_table.add_row(source, f"{count:,}")
            
            console.print(source_table)
        
        # Try to get heatmap data
        try:
            heatmap = client.get_activity_heatmap(bucket_size_minutes=60, limit=24)
            if heatmap:
                console.print(f"\n[bold]📅 Recent Activity:[/bold]")
                for bucket in heatmap[:10]:  # Show last 10 time buckets
                    count = bucket.get('event_count', 0)
                    time_bucket = bucket.get('time_bucket', 'Unknown')
                    bar_length = min(30, count // max(1, total_events // 100))
                    bar = "█" * bar_length
                    console.print(f"{time_bucket}: {bar} {count:,} events")
        except Exception:
            console.print("\n[yellow]Activity heatmap not available via RPC[/yellow]")
        
    except SinexRPCError:
        # Re-raise RPC errors as-is
        raise
    except Exception as e:
        raise SinexRPCError(-32603, f"RPC stats query failed: {e}") from e


def _stats_with_database() -> None:
    """Show stats using direct database connection (legacy mode)."""
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            # Total events
            cur.execute("SELECT COUNT(*) as total FROM core.events")
            total = cur.fetchone()['total']
            
            # DLQ statistics
            cur.execute("""
                SELECT 
                    COUNT(*) as total_dlq,
                    COUNT(*) FILTER (WHERE resolved_at IS NULL) as pending_dlq,
                    COUNT(*) FILTER (WHERE resolved_at IS NOT NULL) as resolved_dlq
                FROM sinex_schemas.dlq_events
            """)
            dlq_stats = cur.fetchone()
            
            # Events by day (last 7 days)
            cur.execute("""
                SELECT DATE(ts_ingest) as day, COUNT(*) as count,
                       COUNT(DISTINCT source) as sources
                FROM core.events
                WHERE ts_ingest > NOW() - INTERVAL '7 days'
                GROUP BY day
                ORDER BY day DESC
            """)
            daily_counts = cur.fetchall()
            
            # Schema usage
            cur.execute("""
                SELECT s.event_source, s.event_type, s.schema_version,
                       COUNT(e.id) as usage_count
                FROM sinex_schemas.event_payload_schemas s
                LEFT JOIN core.events e ON e.payload_schema_id = s.id
                WHERE s.is_active = true
                GROUP BY s.event_source, s.event_type, s.schema_version
                ORDER BY usage_count DESC
                LIMIT 10
            """)
            schema_usage = cur.fetchall()
            
            # Automaton health
            cur.execute("""
                SELECT 
                    payload->>'automaton_name' as automaton_name,
                    payload->>'status' as status,
                    MAX(ts_ingest) as last_heartbeat
                FROM core.events
                WHERE source = 'sinex' AND event_type = 'automaton.heartbeat'
                GROUP BY payload->>'automaton_name', payload->>'status'
                ORDER BY last_heartbeat DESC
            """)
            automaton_health = cur.fetchall()
    
    console.print(f"\n[bold]📊 Total Events:[/bold] {total:,}")
    
    # DLQ statistics
    console.print(f"\n[bold]🚨 DLQ Statistics:[/bold]")
    console.print(f"Total DLQ entries: {dlq_stats['total_dlq']:,}")
    console.print(f"Pending: {dlq_stats['pending_dlq']:,}")
    console.print(f"Resolved: {dlq_stats['resolved_dlq']:,}")
    
    # Daily activity
    console.print("\n[bold]📅 Daily Activity (last 7 days):[/bold]")
    for day in daily_counts:
        bar_length = min(50, day['count'] // max(1, total // 1000))
        bar = "█" * bar_length
        console.print(f"{day['day']}: {bar} {day['count']:,} events ({day['sources']} sources)")
    
    # Schema usage
    if schema_usage:
        console.print("\n[bold]📋 Most Used Schemas:[/bold]")
        schema_table = Table()
        schema_table.add_column("Source", style="cyan")
        schema_table.add_column("Event Type", style="green")
        schema_table.add_column("Version", style="yellow")
        schema_table.add_column("Usage", justify="right", style="white")
        
        for schema in schema_usage:
            schema_table.add_row(
                schema['event_source'],
                schema['event_type'],
                schema['schema_version'],
                f"{schema['usage_count']:,}"
            )
        
        console.print(schema_table)
    
    # Agent health
    if agent_health:
        console.print("\n[bold]🤖 Agent Health:[/bold]")
        for agent in agent_health:
            last_hb = agent['last_heartbeat']
            if last_hb:
                age = datetime.now(datetime.timezone.utc) - last_hb.replace(tzinfo=None)
                if age.total_seconds() < 300:  # 5 minutes
                    status_icon = "🟢"
                elif age.total_seconds() < 3600:  # 1 hour
                    status_icon = "🟡"
                else:
                    status_icon = "🔴"
            else:
                status_icon = "⚫"
            
            console.print(f"  {status_icon} {agent['agent_name']}: {agent['status']} "
                         f"(last: {last_hb.strftime('%H:%M:%S') if last_hb else 'never'})")


def parse_datetime(date_str: str) -> datetime:
    """Parse datetime string in various formats."""
    formats = [
        '%Y-%m-%d %H:%M:%S',
        '%Y-%m-%d %H:%M',
        '%Y-%m-%d',
        '%H:%M:%S',
        '%H:%M'
    ]
    
    for fmt in formats:
        try:
            if 'Y' not in fmt:  # Time only, use today's date
                today = datetime.now().date()
                time_obj = datetime.strptime(date_str, fmt).time()
                return datetime.combine(today, time_obj)
            return datetime.strptime(date_str, fmt)
        except ValueError:
            continue
    
    raise ValueError(f"Unable to parse datetime: {date_str}")


@click.group()
@click.option('--interactive', '-i', is_flag=True, help='Launch interactive query builder')
@click.option('--rpc-url', help='RPC server URL (default: http://127.0.0.1:9999)', envvar='SINEX_RPC_URL')
@click.option('--use-db', is_flag=True, help='Use direct database connection instead of RPC')
@click.pass_context
def cli(ctx, interactive, rpc_url, use_db):
    """Sinex CLI - Query your digital memory."""
    # Store config in context for subcommands
    ctx.ensure_object(dict)
    ctx.obj['rpc_url'] = rpc_url
    ctx.obj['use_db'] = use_db
    
    if interactive:
        try:
            from .interactive import run_interactive_mode
            run_interactive_mode()
        except ImportError:
            console.print("[red]Interactive mode not available[/red]")
        return


@cli.command()
@click.option('--source', '-s', help='Filter by event source (e.g., hyprland)')
@click.option('--event-type', '-t', help='Filter by event type (e.g., window_focused)')
@click.option('--since', help='Show events since datetime (YYYY-MM-DD HH:MM:SS)')
@click.option('--until', help='Show events until datetime (YYYY-MM-DD HH:MM:SS)')
@click.option('--last', '-l', help='Show events from last N time (e.g., 1h, 30m, 2d)')
@click.option('--limit', '-n', default=50, help='Maximum number of events to show')
@click.option('--host', help='Filter by host')
@click.option('--payload-jq', help='JQ filter for payload (requires jq command)')
@click.option('--output-format', type=click.Choice(['table', 'json', 'csv', 'yaml']), 
              default='table', help='Output format')
@click.pass_context
def query(ctx, source: Optional[str], event_type: Optional[str], since: Optional[str], 
          until: Optional[str], last: Optional[str], limit: int, host: Optional[str],
          payload_jq: Optional[str], output_format: str):
    """Enhanced query for events from the sinex database."""
    
    use_db = ctx.obj.get('use_db', False)
    rpc_url = ctx.obj.get('rpc_url')
    
    try:
        if use_db:
            # Use direct database connection (legacy mode)
            events = _query_with_database(source, event_type, since, until, last, limit, host)
        else:
            # Use RPC (default mode)
            events = _query_with_rpc(rpc_url, source, event_type, since, until, last, limit, host)
        
        # Apply JQ filter if specified
        if payload_jq and events:
            events = apply_jq_filter(events, payload_jq)
        
        # Output in specified format
        if output_format == 'json':
            output_json(events)
        elif output_format == 'csv':
            output_csv(events)
        elif output_format == 'yaml':
            output_yaml(events)
        else:
            display_events_enhanced(events)
            
    except SinexRPCError as e:
        console.print(f"[red]RPC Error: {e}[/red]")
        console.print(f"[yellow]Try using --use-db flag for direct database access[/yellow]")
        sys.exit(1)
    except Exception as e:
        console.print(f"[red]Error: {e}[/red]")
        sys.exit(1)


@cli.group()
def schema():
    """Schema and type introspection commands."""
    pass


@schema.command('list')
@click.option('--source', '-s', help='Filter by source')
@click.option('--event-type', '-t', help='Filter by event type')
@click.option('--active-only', is_flag=True, help='Show only active schemas')
def schema_list(source: Optional[str], event_type: Optional[str], active_only: bool):
    """List event payload schemas."""
    
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            query_parts = [
                "SELECT id, event_source, event_type, schema_version, "
                "description, created_at, is_active FROM sinex_schemas.event_payload_schemas"
            ]
            conditions = []
            params = []
            
            if source:
                conditions.append("event_source = %s")
                params.append(source)
                
            if event_type:
                conditions.append("event_type = %s")
                params.append(event_type)
                
            if active_only:
                conditions.append("is_active = true")
            
            if conditions:
                query_parts.append("WHERE " + " AND ".join(conditions))
            
            query_parts.append("ORDER BY event_source, event_type, schema_version")
            
            query_sql = " ".join(query_parts)
            cur.execute(query_sql, params)
            schemas = cur.fetchall()
    
    if not schemas:
        console.print("[yellow]No schemas found.[/yellow]")
        return
    
    table = Table(title="Event Payload Schemas")
    table.add_column("Source", style="cyan")
    table.add_column("Event Type", style="green")
    table.add_column("Version", style="yellow")
    table.add_column("Active", style="red")
    table.add_column("Description", style="white")
    table.add_column("Created", style="dim")
    
    for schema in schemas:
        table.add_row(
            schema['event_source'],
            schema['event_type'],
            schema['schema_version'],
            "✓" if schema['is_active'] else "✗",
            schema['description'] or "",
            schema['created_at'].strftime('%Y-%m-%d') if schema['created_at'] else ""
        )
    
    console.print(table)


@schema.command('get')
@click.argument('schema_identifier')
def schema_get(schema_identifier: str):
    """Get a specific schema by ID, or source/type/version."""
    
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            # Try to parse as ID first, then as source/type/version
            if '/' in schema_identifier:
                parts = schema_identifier.split('/')
                if len(parts) == 3:
                    source, event_type, version = parts
                    cur.execute("""
                        SELECT * FROM sinex_schemas.event_payload_schemas
                        WHERE event_source = %s AND event_type = %s AND schema_version = %s
                    """, (source, event_type, version))
                elif len(parts) == 2:
                    source, event_type = parts
                    cur.execute("""
                        SELECT * FROM sinex_schemas.event_payload_schemas
                        WHERE event_source = %s AND event_type = %s AND is_active = true
                    """, (source, event_type))
                else:
                    click.echo("Invalid format. Use source/type/version or source/type")
                    return
            else:
                # Assume it's a UUID/ULID
                cur.execute("""
                    SELECT * FROM sinex_schemas.event_payload_schemas
                    WHERE encode(id, 'hex') = %s
                """, (schema_identifier,))
            
            schema = cur.fetchone()
    
    if not schema:
        console.print(f"[red]Schema not found: {schema_identifier}[/red]")
        return
    
    # Display schema information
    panel_content = []
    panel_content.append(f"[bold]Source:[/bold] {schema['event_source']}")
    panel_content.append(f"[bold]Event Type:[/bold] {schema['event_type']}")
    panel_content.append(f"[bold]Version:[/bold] {schema['schema_version']}")
    panel_content.append(f"[bold]Active:[/bold] {'Yes' if schema['is_active'] else 'No'}")
    panel_content.append(f"[bold]Created:[/bold] {schema['created_at']}")
    if schema['description']:
        panel_content.append(f"[bold]Description:[/bold] {schema['description']}")
    
    console.print(Panel("\n".join(panel_content), title="Schema Information"))
    
    # Display JSON schema
    json_schema = schema['json_schema_definition']
    console.print("\n[bold]JSON Schema:[/bold]")
    console.print(JSON.from_data(json_schema, indent=2))


@cli.group()
def automaton():
    """Automaton introspection commands."""
    pass


@cli.group()
def processor():
    """Processor introspection commands (unified view of ingestors and automata)."""
    pass


@processor.command('list')
@click.option('--type', '-t', type=click.Choice(['ingestor', 'automaton']), help='Filter by processor type')
@click.option('--status', '-s', help='Filter by status (development, stable, deprecated)')
def processor_list(type: Optional[str], status: Optional[str]):
    """List all registered processors (ingestors and automata)."""
    
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            query_parts = [
                "SELECT processor_name, processor_type, description, version, status, "
                "produces_event_types, last_heartbeat_ts, registered_at "
                "FROM sinex_schemas.processor_manifests"
            ]
            params = []
            where_conditions = []
            
            if type:
                where_conditions.append("processor_type = %s")
                params.append(type)
            
            if status:
                where_conditions.append("status = %s")
                params.append(status)
            
            if where_conditions:
                query_parts.append("WHERE " + " AND ".join(where_conditions))
            
            query_parts.append("ORDER BY processor_type, processor_name")
            
            query_sql = " ".join(query_parts)
            cur.execute(query_sql, params)
            processors = cur.fetchall()
    
    if not processors:
        console.print("[yellow]No processors found.[/yellow]")
        return
    
    table = Table(title="Registered Processors")
    table.add_column("Processor", style="cyan")
    table.add_column("Type", style="blue")
    table.add_column("Version", style="green")
    table.add_column("Status", style="yellow")
    table.add_column("Last Heartbeat", style="red")
    table.add_column("Description", style="white")
    
    for processor in processors:
        last_heartbeat = processor['last_heartbeat_ts']
        if last_heartbeat:
            # Check if heartbeat is recent (within 5 minutes)
            age = datetime.now(datetime.timezone.utc) - last_heartbeat.replace(tzinfo=None)
            if age.total_seconds() < 300:
                heartbeat_style = "green"
                heartbeat_text = "🟢 " + last_heartbeat.strftime('%H:%M:%S')
            else:
                heartbeat_style = "red"
                heartbeat_text = "🔴 " + last_heartbeat.strftime('%H:%M:%S')
        else:
            heartbeat_style = "dim"
            heartbeat_text = "Never"
        
        # Add type-specific emoji
        type_emoji = "🔄" if processor['processor_type'] == 'automaton' else "📥"
        
        table.add_row(
            processor['processor_name'],
            f"{type_emoji} {processor['processor_type']}",
            processor['version'],
            processor['status'],
            Text(heartbeat_text, style=heartbeat_style),
            processor['description'] or ""
        )
    
    console.print(table)


@processor.command('status')
@click.argument('processor_name')
def processor_status(processor_name: str):
    """Show detailed status for a specific processor."""
    
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            # Get processor manifest
            cur.execute("""
                SELECT * FROM sinex_schemas.processor_manifests
                WHERE processor_name = %s
            """, (processor_name,))
            processor = cur.fetchone()
            
            if not processor:
                console.print(f"[red]Processor not found: {processor_name}[/red]")
                return
            
            # Get recent checkpoints if this is an automaton
            checkpoints = []
            if processor['processor_type'] == 'automaton':
                cur.execute("""
                    SELECT automaton_name, last_processed_id, processed_count, 
                           last_activity, state_data
                    FROM core.automaton_checkpoints
                    WHERE automaton_name = %s
                    ORDER BY last_activity DESC
                """, (processor_name,))
                checkpoints = cur.fetchall()
            
            # Display processor information
            console.print(f"\n[bold]Processor: {processor_name}[/bold]")
            console.print(f"Type: {processor['processor_type']}")
            console.print(f"Version: {processor['version']}")
            console.print(f"Status: {processor['status']}")
            console.print(f"Description: {processor['description'] or 'N/A'}")
            
            if processor['produces_event_types']:
                console.print(f"Produces: {', '.join(processor['produces_event_types'])}")
            
            if processor['consumes_event_types']:
                console.print(f"Consumes: {', '.join(processor['consumes_event_types'])}")
            
            # Show checkpoints for automata
            if checkpoints:
                console.print("\n[bold]Checkpoints:[/bold]")
                for checkpoint in checkpoints:
                    console.print(f"  Last processed: {checkpoint['last_processed_id'] or 'None'}")
                    console.print(f"  Processed count: {checkpoint['processed_count']}")
                    console.print(f"  Last activity: {checkpoint['last_activity']}")
                    if checkpoint['state_data']:
                        console.print(f"  State: {checkpoint['state_data']}")
            
            console.print(f"\nRegistered: {processor['registered_at']}")
            console.print(f"Last seen: {processor['last_seen']}")
            console.print(f"Last heartbeat: {processor['last_heartbeat_ts'] or 'Never'}")


@automaton.command('list')
@click.option('--status', '-s', help='Filter by status (development, stable, deprecated)')
def automaton_list(status: Optional[str]):
    """List all registered automata."""
    
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            query_parts = [
                "SELECT processor_name, description, version, status, "
                "produces_event_types, last_heartbeat_ts, registered_at "
                "FROM sinex_schemas.processor_manifests "
                "WHERE processor_type = 'automaton'"
            ]
            params = []
            
            if status:
                query_parts.append("AND status = %s")
                params.append(status)
            
            query_parts.append("ORDER BY processor_name")
            
            query_sql = " ".join(query_parts)
            cur.execute(query_sql, params)
            automata = cur.fetchall()
    
    if not automata:
        console.print("[yellow]No automata found.[/yellow]")
        return
    
    table = Table(title="Registered Automata")
    table.add_column("Automaton", style="cyan")
    table.add_column("Version", style="green")
    table.add_column("Status", style="yellow")
    table.add_column("Last Heartbeat", style="red")
    table.add_column("Description", style="white")
    
    for automaton in automata:
        last_heartbeat = automaton['last_heartbeat_ts']
        if last_heartbeat:
            # Check if heartbeat is recent (within 5 minutes)
            age = datetime.now(datetime.timezone.utc) - last_heartbeat.replace(tzinfo=None)
            if age.total_seconds() < 300:
                heartbeat_style = "green"
                heartbeat_text = "🟢 " + last_heartbeat.strftime('%H:%M:%S')
            else:
                heartbeat_style = "red"
                heartbeat_text = "🔴 " + last_heartbeat.strftime('%H:%M:%S')
        else:
            heartbeat_style = "dim"
            heartbeat_text = "Never"
        
        table.add_row(
            automaton['processor_name'],
            automaton['version'],
            automaton['status'],
            Text(heartbeat_text, style=heartbeat_style),
            automaton['description'] or ""
        )
    
    console.print(table)


@automaton.command('status')
@click.argument('automaton_name')
def automaton_status(automaton_name: str):
    """Show detailed status for a specific automaton."""
    
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            # Get automaton manifest
            cur.execute("""
                SELECT * FROM sinex_schemas.processor_manifests
                WHERE processor_name = %s AND processor_type = 'automaton'
            """, (automaton_name,))
            automaton = cur.fetchone()
            
            if not automaton:
                console.print(f"[red]Automaton not found: {automaton_name}[/red]")
                return
            
            # Get recent heartbeats
            cur.execute("""
                SELECT payload, ts_ingest FROM core.events
                WHERE source = 'sinex' AND event_type = 'automaton.heartbeat'
                AND payload->>'automaton_name' = %s
                ORDER BY ts_ingest DESC
                LIMIT 5
            """, (automaton_name,))
            heartbeats = cur.fetchall()
            
            # Get recent errors
            cur.execute("""
                SELECT payload, ts_ingest FROM core.events
                WHERE source = 'sinex' AND event_type = 'automaton.error'
                AND payload->>'automaton_name' = %s
                ORDER BY ts_ingest DESC
                LIMIT 10
            """, (automaton_name,))
            errors = cur.fetchall()
            
            # Get DLQ count from actual DLQ table
            cur.execute("""
                SELECT 
                    COUNT(*) as total_dlq,
                    COUNT(*) FILTER (WHERE resolved_at IS NULL) as pending_dlq,
                    COUNT(*) FILTER (WHERE resolved_at IS NOT NULL) as resolved_dlq
                FROM sinex_schemas.dlq_events
                WHERE automaton_name = %s
            """, (automaton_name,))
            dlq_counts = cur.fetchone()
    
    # Display automaton information
    panel_content = []
    panel_content.append(f"[bold]Automaton:[/bold] {automaton['processor_name']}")
    panel_content.append(f"[bold]Version:[/bold] {automaton['version']}")
    panel_content.append(f"[bold]Status:[/bold] {automaton['status']}")
    panel_content.append(f"[bold]Description:[/bold] {automaton['description'] or 'N/A'}")
    panel_content.append(f"[bold]Registered:[/bold] {automaton['registered_at']}")
    panel_content.append(f"[bold]DLQ Total:[/bold] {dlq_counts['total_dlq']}")
    panel_content.append(f"[bold]DLQ Pending:[/bold] {dlq_counts['pending_dlq']}")
    panel_content.append(f"[bold]DLQ Resolved:[/bold] {dlq_counts['resolved_dlq']}")
    
    console.print(Panel("\n".join(panel_content), title=f"Automaton Status: {automaton_name}"))
    
    # Display event types produced
    if automaton['produces_event_types']:
        console.print("\n[bold]Produces Event Types:[/bold]")
        produces = automaton['produces_event_types']
        console.print(f"  {', '.join(produces)}")
    
    # Display recent heartbeats
    if heartbeats:
        console.print(f"\n[bold]Recent Heartbeats ({len(heartbeats)}):[/bold]")
        hb_table = Table()
        hb_table.add_column("Time", style="cyan")
        hb_table.add_column("Status", style="green")
        hb_table.add_column("Uptime", style="yellow")
        hb_table.add_column("Events", style="white")
        hb_table.add_column("DLQ", style="red")
        
        for hb in heartbeats:
            payload = hb['payload']
            hb_table.add_row(
                hb['ts_ingest'].strftime('%H:%M:%S'),
                payload.get('status', 'unknown'),
                f"{payload.get('uptime_seconds', 0)}s",
                str(payload.get('events_processed_session', 0)),
                str(payload.get('dlq_size', 0))
            )
        
        console.print(hb_table)
    
    # Display recent errors
    if errors:
        console.print(f"\n[bold]Recent Errors ({len(errors)}):[/bold]")
        for error in errors:
            payload = error['payload']
            severity = payload.get('severity', 'unknown')
            color = {'critical': 'red', 'error': 'yellow', 'warning': 'blue'}.get(severity, 'white')
            
            console.print(f"[{color}]●[/{color}] [{severity.upper()}] {payload.get('error_message', 'Unknown error')}")
            console.print(f"   Context: {payload.get('error_context', 'N/A')}")
            console.print(f"   Time: {error['ts_ingest'].strftime('%Y-%m-%d %H:%M:%S')}")
            console.print()


def apply_jq_filter(events: List[Dict], jq_filter: str) -> List[Dict]:
    """Apply JQ filter to event payloads."""
    try:
        import tempfile
        import subprocess
        
        # Create temporary file with event data
        with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
            json.dump([event['payload'] for event in events], f)
            temp_file = f.name
        
        # Run jq filter
        result = subprocess.run(
            ['jq', jq_filter, temp_file],
            capture_output=True,
            text=True,
            check=True
        )
        
        # Parse filtered results
        filtered_payloads = json.loads(result.stdout)
        
        # Update events with filtered payloads
        for i, event in enumerate(events):
            if i < len(filtered_payloads):
                event['payload'] = filtered_payloads[i]
        
        # Clean up
        os.unlink(temp_file)
        
        return [e for e in events if e['payload']]  # Remove empty results
        
    except Exception as e:
        console.print(f"[red]JQ filter error: {e}[/red]")
        return events


def output_json(events: List[Dict]):
    """Output events as JSON."""
    # Convert datetime and bytes objects for JSON serialization
    for event in events:
        if event.get('ts_ingest'):
            event['ts_ingest'] = event['ts_ingest'].isoformat()
        if event.get('ts_orig'):
            event['ts_orig'] = event['ts_orig'].isoformat()
        if event.get('id'):
            event['id'] = event['id'].hex() if hasattr(event['id'], 'hex') else str(event['id'])
        if event.get('payload_schema_id'):
            event['payload_schema_id'] = event['payload_schema_id'].hex() if hasattr(event['payload_schema_id'], 'hex') else str(event['payload_schema_id'])
    
    click.echo(json.dumps(events, indent=2))


def output_csv(events: List[Dict]):
    """Output events as CSV."""
    import csv
    import io
    
    if not events:
        return
    
    output = io.StringIO()
    writer = csv.DictWriter(output, fieldnames=events[0].keys())
    writer.writeheader()
    
    for event in events:
        # Convert complex objects to strings
        row = {}
        for key, value in event.items():
            if isinstance(value, (dict, list)):
                row[key] = json.dumps(value)
            elif hasattr(value, 'isoformat'):
                row[key] = value.isoformat()
            else:
                row[key] = str(value)
        writer.writerow(row)
    
    click.echo(output.getvalue())


def output_yaml(events: List[Dict]):
    """Output events as YAML."""
    try:
        import yaml
        
        # Convert datetime objects
        for event in events:
            if event.get('ts_ingest'):
                event['ts_ingest'] = event['ts_ingest'].isoformat()
            if event.get('ts_orig'):
                event['ts_orig'] = event['ts_orig'].isoformat()
        
        click.echo(yaml.dump(events, default_flow_style=False))
    except ImportError:
        console.print("[red]YAML output requires 'pyyaml' package[/red]")


def display_events_enhanced(events: List[Dict[str, Any]]):
    """Display events in an enhanced formatted table."""
    if not events:
        console.print("[yellow]No events found.[/yellow]")
        return
    
    table = Table(title=f"Recent Events ({len(events)} shown)")
    table.add_column("Time", style="cyan")
    table.add_column("Source", style="green")
    table.add_column("Event Type", style="yellow")
    table.add_column("Host", style="blue")
    table.add_column("Summary", style="white")
    
    for event in events:
        payload = event['payload']
        source = event['source']
        event_type = event['event_type']
        
        # Enhanced summary extraction based on new event types
        summary = extract_event_summary(source, event_type, payload)
        
        # Use original timestamp if available, fallback to ingest
        timestamp = event.get('ts_orig') or event['ts_ingest']
        
        table.add_row(
            timestamp.strftime('%H:%M:%S'),
            source,
            event_type,
            event.get('host', 'unknown'),
            summary
        )
    
    console.print(table)


def extract_event_summary(source: str, event_type: str, payload: Dict) -> str:
    """Extract a human-readable summary from event payload."""
    
    if source == "hyprland":
        if event_type == "window_focused":
            app = payload.get('app_class', 'unknown')
            title = payload.get('window_title', '')[:40]
            return f"{app}: {title}"
        elif event_type == "workspace_changed":
            return f"Workspace {payload.get('workspace_id', 'unknown')}"
        elif event_type == "state_snapshot":
            clients = len(payload.get('clients', []))
            workspaces = len(payload.get('workspaces', []))
            return f"{clients} windows, {workspaces} workspaces"
    
    elif source == "terminal.kitty":
        if event_type == "command_executed":
            cmd = payload.get('command_string', '')[:50]
            exit_code = payload.get('exit_code', 0)
            return f"[{exit_code}] {cmd}"
    
    elif source == "filesystem":
        if event_type in ["file_created", "file_modified", "file_deleted"]:
            path = payload.get('path', '')
            filename = Path(path).name if path else 'unknown'
            return f"{filename}"
        elif event_type == "file_renamed":
            old_name = Path(payload.get('path', '')).name
            new_name = Path(payload.get('new_path', '')).name
            return f"{old_name} → {new_name}"
    
    elif source == "sinex":
        if event_type == "automaton.heartbeat":
            automaton = payload.get('automaton_name', 'unknown')
            status = payload.get('status', 'unknown')
            return f"{agent}: {status}"
        elif event_type == "automaton.error":
            automaton = payload.get('automaton_name', 'unknown')
            severity = payload.get('severity', 'unknown')
            return f"{agent} [{severity}]"
    
    # Fallback: try to extract meaningful info from payload
    if isinstance(payload, dict):
        # Look for common meaningful fields
        for key in ['message', 'description', 'title', 'name', 'command', 'path']:
            if key in payload:
                value = str(payload[key])[:50]
                return value
    
    return str(payload)[:50]


# Add enhanced sources command
@cli.command()
@click.pass_context
def sources(ctx):
    """List all event sources with enhanced statistics."""
    
    use_db = ctx.obj.get('use_db', False)
    rpc_url = ctx.obj.get('rpc_url')
    
    try:
        if use_db:
            # Use direct database connection (legacy mode)
            sources = _sources_with_database()
        else:
            # Use RPC (default mode)
            sources = _sources_with_rpc(rpc_url)
        
        table = Table(title="Event Sources")
        table.add_column("Source", style="cyan")
        table.add_column("Events", justify="right", style="green")
        table.add_column("Types", justify="right", style="yellow")
        table.add_column("Hosts", justify="right", style="blue")
        table.add_column("First Event", style="dim")
        table.add_column("Last Event", style="dim")
        table.add_column("Avg Delay", justify="right", style="magenta")
        
        for source in sources:
            delay = source.get('avg_ingest_delay')
            delay_str = f"{delay:.2f}s" if delay else "N/A"
            
            first_event = source.get('first_event')
            last_event = source.get('last_event')
            
            table.add_row(
                source['source'],
                f"{source['event_count']:,}",
                str(source.get('event_type_count', 1)),
                str(source.get('host_count', 1)),
                first_event.strftime('%Y-%m-%d') if first_event else "N/A",
                last_event.strftime('%Y-%m-%d') if last_event else "N/A",
                delay_str
            )
        
        console.print(table)
        
    except SinexRPCError as e:
        console.print(f"[red]RPC Error: {e}[/red]")
        console.print(f"[yellow]Try using --use-db flag for direct database access[/yellow]")
        sys.exit(1)
    except Exception as e:
        console.print(f"[red]Error: {e}[/red]")
        sys.exit(1)


@cli.group()
def blob():
    """Blob management commands (git-annex integration)."""
    pass


@blob.command('ingest')
@click.argument('file_path', type=click.Path(exists=True))
@click.option('--description', '-d', help='Description for the blob')
@click.option('--annex-repo', '-r', help='Git-annex repository path', 
              default=lambda: os.environ.get('SINEX_ANNEX_PATH', '/realm/data/sinex-annex/sinex-blobs'))
def blob_ingest(file_path: str, description: Optional[str], annex_repo: str):
    """Ingest a file into the blob storage system."""
    import subprocess
    import hashlib
    from pathlib import Path
    
    file_path = Path(file_path)
    annex_repo = Path(annex_repo)
    
    if not annex_repo.exists():
        console.print(f"[red]Git-annex repository not found: {annex_repo}[/red]")
        console.print("Run: ./script/init_git_annex.sh to initialize")
        return
    
    try:
        # Compute BLAKE3 hash (or SHA256 as fallback)
        console.print(f"Computing hash for: {file_path.name}")
        hash_cmd = subprocess.run(
            ['blake3sum', str(file_path)], 
            capture_output=True, text=True, check=True
        )
        blake3_hash = hash_cmd.stdout.split()[0]
        
        # Check if blob already exists
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT id, annex_key, original_filename FROM core.blobs WHERE checksum_md5 = %s",
                    (blake3_hash,)
                )
                existing = cur.fetchone()
                
                if existing:
                    console.print(f"[yellow]File already exists in blob store![/yellow]")
                    console.print(f"Blob ID: {existing['id']}")
                    console.print(f"Annex Key: {existing['annex_key']}")
                    console.print(f"Original Name: {existing['original_filename']}")
                    return
        
        # Copy file to annex repository and add it
        working_copy = annex_repo / file_path.name
        subprocess.run(['cp', str(file_path), str(working_copy)], check=True)
        
        # Add to git-annex
        console.print("Adding to git-annex...")
        add_result = subprocess.run(
            ['git-annex', 'add', file_path.name],
            cwd=annex_repo, capture_output=True, text=True, check=True
        )
        
        # Get annex key
        key_result = subprocess.run(
            ['git-annex', 'lookupkey', file_path.name],
            cwd=annex_repo, capture_output=True, text=True, check=True
        )
        annex_key = key_result.stdout.strip()
        
        # Get file metadata
        file_stat = file_path.stat()
        size_bytes = file_stat.st_size
        
        # Simple MIME type detection
        mime_map = {
            '.txt': 'text/plain',
            '.md': 'text/markdown',
            '.json': 'application/json',
            '.pdf': 'application/pdf',
            '.jpg': 'image/jpeg',
            '.jpeg': 'image/jpeg',
            '.png': 'image/png',
            '.mp4': 'video/mp4',
            '.mp3': 'audio/mpeg',
        }
        mime_type = mime_map.get(file_path.suffix.lower(), 'application/octet-stream')
        
        # Insert into database
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                # Generate ULID for blob ID (simplified - using a timestamp-based approach)
                import time
                import uuid
                blob_id = str(uuid.uuid4()).replace('-', '')  # Simplified ULID substitute
                
                cur.execute("""
                    INSERT INTO core.blobs 
                    (id, annex_key, original_filename, size_bytes, mime_type, 
                     checksum_sha256, checksum_md5, storage_backend, verification_status)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                """, (
                    blob_id,
                    annex_key,
                    file_path.name,
                    size_bytes,
                    mime_type,
                    blake3_hash,  # Using blake3 as sha256 field for now
                    blake3_hash,  # Store blake3 in md5 field
                    'git-annex',
                    'verified'
                ))
                conn.commit()
        
        # Commit to git
        subprocess.run(['git', 'add', file_path.name], cwd=annex_repo, check=True)
        commit_msg = f"Add blob: {file_path.name}"
        if description:
            commit_msg += f" - {description}"
        subprocess.run(['git', 'commit', '-m', commit_msg], cwd=annex_repo, check=True)
        
        console.print(f"[green]✅ Successfully ingested blob![/green]")
        console.print(f"Blob ID: {blob_id}")
        console.print(f"Annex Key: {annex_key}")
        console.print(f"Size: {size_bytes:,} bytes")
        console.print(f"Hash: {blake3_hash}")
        
    except subprocess.CalledProcessError as e:
        console.print(f"[red]Command failed: {e.cmd}[/red]")
        console.print(f"[red]Error: {e.stderr}[/red]")
    except Exception as e:
        console.print(f"[red]Error: {e}[/red]")


@blob.command('list')
@click.option('--limit', '-n', default=20, help='Number of blobs to show')
@click.option('--mime-type', '-m', help='Filter by MIME type')
def blob_list(limit: int, mime_type: Optional[str]):
    """List blobs in storage."""
    
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            query = """
                SELECT id, annex_key, original_filename, size_bytes, mime_type,
                       verification_status, created_at
                FROM core.blobs
            """
            params = []
            
            if mime_type:
                query += " WHERE mime_type = %s"
                params.append(mime_type)
            
            query += " ORDER BY created_at DESC LIMIT %s"
            params.append(limit)
            
            cur.execute(query, params)
            blobs = cur.fetchall()
    
    if not blobs:
        console.print("[yellow]No blobs found.[/yellow]")
        return
    
    table = Table(title=f"Blob Storage ({len(blobs)} shown)")
    table.add_column("ID", style="cyan")
    table.add_column("Filename", style="green")
    table.add_column("Size", justify="right", style="yellow")
    table.add_column("MIME Type", style="blue")
    table.add_column("Status", style="white")
    table.add_column("Created", style="dim")
    
    for blob in blobs:
        size_mb = blob['size_bytes'] / (1024 * 1024)
        size_str = f"{size_mb:.1f}MB" if size_mb >= 1 else f"{blob['size_bytes']:,}B"
        
        status_color = {
            'verified': 'green',
            'pending': 'yellow',
            'corrupted': 'red',
            'missing': 'red'
        }.get(blob['verification_status'], 'white')
        
        table.add_row(
            blob['id'][:8],  # Truncated ID
            blob['original_filename'],
            size_str,
            blob['mime_type'] or 'unknown',
            f"[{status_color}]{blob['verification_status']}[/{status_color}]",
            blob['created_at'].strftime('%Y-%m-%d') if blob['created_at'] else 'unknown'
        )
    
    console.print(table)


@blob.command('get')
@click.argument('blob_id')
@click.option('--output', '-o', help='Output file path')
@click.option('--annex-repo', '-r', help='Git-annex repository path',
              default=lambda: os.environ.get('SINEX_ANNEX_PATH', '/realm/data/sinex-annex/sinex-blobs'))
def blob_get(blob_id: str, output: Optional[str], annex_repo: str):
    """Retrieve a blob from storage."""
    import subprocess
    from pathlib import Path
    
    annex_repo = Path(annex_repo)
    
    # Get blob metadata
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT id, annex_key, original_filename, size_bytes, mime_type
                FROM core.blobs 
                WHERE event_id LIKE %s OR event_id = %s
            """, (f"{blob_id}%", blob_id))
            blob = cur.fetchone()
    
    if not blob:
        console.print(f"[red]Blob not found: {blob_id}[/red]")
        return
    
    console.print(f"Found blob: {blob['original_filename']}")
    console.print(f"Annex key: {blob['annex_key']}")
    
    try:
        # Ensure content is available in annex
        console.print("Ensuring content is available...")
        subprocess.run(
            ['git-annex', 'get', blob['annex_key']],
            cwd=annex_repo, check=True
        )
        
        # Find the symlink path
        # This is simplified - in practice you'd search for the symlink
        symlink_path = annex_repo / blob['original_filename']
        
        if not symlink_path.exists():
            console.print(f"[red]Symlink not found: {symlink_path}[/red]")
            return
        
        # Copy to output location
        output_path = Path(output) if output else Path.cwd() / blob['original_filename']
        
        if symlink_path.is_symlink():
            # Resolve symlink and copy actual content
            actual_path = symlink_path.resolve()
            subprocess.run(['cp', str(actual_path), str(output_path)], check=True)
        else:
            subprocess.run(['cp', str(symlink_path), str(output_path)], check=True)
        
        console.print(f"[green]✅ Retrieved blob to: {output_path}[/green]")
        
    except subprocess.CalledProcessError as e:
        console.print(f"[red]Git-annex command failed: {e}[/red]")
    except Exception as e:
        console.print(f"[red]Error: {e}[/red]")


@blob.command('verify')
@click.option('--annex-repo', '-r', help='Git-annex repository path',
              default=lambda: os.environ.get('SINEX_ANNEX_PATH', '/realm/data/sinex-annex/sinex-blobs'))
@click.option('--fast', is_flag=True, help='Fast verification (no content check)')
def blob_verify(annex_repo: str, fast: bool):
    """Verify blob integrity using git-annex fsck."""
    import subprocess
    from pathlib import Path
    
    annex_repo = Path(annex_repo)
    
    if not annex_repo.exists():
        console.print(f"[red]Git-annex repository not found: {annex_repo}[/red]")
        return
    
    try:
        console.print("Running git-annex fsck...")
        
        cmd = ['git-annex', 'fsck']
        if fast:
            cmd.append('--fast')
        
        result = subprocess.run(
            cmd, cwd=annex_repo, 
            capture_output=True, text=True, check=True
        )
        
        console.print("[green]✅ Verification completed successfully[/green]")
        if result.stdout:
            console.print("Output:")
            console.print(result.stdout)
        
    except subprocess.CalledProcessError as e:
        console.print(f"[red]Verification failed: {e}[/red]")
        if e.stderr:
            console.print("Error output:")
            console.print(e.stderr)


@blob.command('stage')
@click.argument('file_path', type=click.Path(exists=True))
@click.option('--source-id', '-s', required=True, help='Source identifier (e.g., "old-laptop-bash", "live-kitty-stream")')
@click.option('--comment', '-c', help='User comment describing the significance of this source material')
@click.option('--tags', '-t', help='Comma-separated tags for grouping and filtering')
@click.option('--annex-repo', '-r', help='Git-annex repository path', 
              default=lambda: os.environ.get('SINEX_ANNEX_PATH', '/realm/data/sinex-annex/sinex-blobs'))
def blob_stage(file_path: str, source_id: str, comment: Optional[str], tags: Optional[str], annex_repo: str):
    """Stage external source material into the unified architecture.
    
    This command is the critical acquisition workflow that creates entries in 
    raw.source_material_registry and provides the foundation for the unified 
    data architecture.
    
    Examples:
        exo blob stage /path/to/data.json --source-id live-kitty-stream
        exo blob stage history.txt --source-id old-laptop-bash --comment "Historical command data from old laptop" --tags backup,historical,shell
    """
    import subprocess
    import socket
    import uuid
    import os
    import getpass
    from pathlib import Path
    import shlex
    
    file_path = Path(file_path)
    annex_repo = Path(annex_repo)
    
    # Validate inputs
    source_id = source_id.strip()
    if not source_id:
        console.print(f"[red]Error: source-id cannot be empty[/red]")
        return
    
    if not annex_repo.exists():
        console.print(f"[red]Git-annex repository not found: {annex_repo}[/red]")
        console.print("Run: ./script/init_git_annex.sh to initialize")
        return
    
    # Parse tags
    parsed_tags = []
    if tags:
        parsed_tags = [tag.strip() for tag in tags.split(',') if tag.strip()]
    
    # Generate stage batch ID to group files staged together
    stage_batch_id = str(uuid.uuid4())
    operation_id = None
    
    try:
        # Step 1: Compute BLAKE3 checksum (fallback to SHA256 for testing)
        console.print(f"📁 Computing checksum for: {file_path.name}")
        try:
            hash_cmd = subprocess.run(
                ['blake3sum', str(file_path)], 
                capture_output=True, text=True, check=True
            )
            blake3_hash = hash_cmd.stdout.split()[0]
        except FileNotFoundError:
            # Fallback to SHA256 if blake3sum not available
            hash_cmd = subprocess.run(
                ['sha256sum', str(file_path)], 
                capture_output=True, text=True, check=True
            )
            blake3_hash = hash_cmd.stdout.split()[0]
            console.print("[yellow]Note: Using SHA256 fallback (blake3sum not available)[/yellow]")
        
        # Step 2: Check for deduplication in source_material_registry
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT blob_id, source_identifier, staged_at FROM raw.source_material_registry WHERE checksum = %s",
                    (blake3_hash,)
                )
                existing = cur.fetchone()
                
                if existing:
                    console.print(f"[yellow]⚠️  File already staged in source material registry![/yellow]")
                    console.print(f"Blob ID: {existing['blob_id']}")
                    console.print(f"Source ID: {existing['source_identifier']}")
                    console.print(f"Staged: {existing['staged_at']}")
                    console.print(f"Checksum: {blake3_hash}")
                    return
        
        # Step 3: Start operation logging
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                # Get the full command for logging
                full_command = f"exo blob stage {shlex.quote(str(file_path))} --source-id {shlex.quote(source_id)}"
                if comment:
                    full_command += f" --comment {shlex.quote(comment)}"
                if tags:
                    full_command += f" --tags {shlex.quote(tags)}"
                
                # Start operation
                import json
                cur.execute(
                    "SELECT core.start_operation(%s, %s, %s::jsonb) AS operation_id",
                    ('stage', getpass.getuser(), json.dumps({
                        'command': full_command,
                        'file_path': str(file_path),
                        'source_identifier': source_id,
                        'checksum': blake3_hash,
                        'stage_batch_id': stage_batch_id
                    }))
                )
                operation_id = cur.fetchone()['operation_id']
                conn.commit()
        
        # Step 4: Get file metadata
        file_stat = file_path.stat()
        source_size = file_stat.st_size
        source_mtime = file_stat.st_mtime
        
        # Step 5: Add file to git-annex
        console.print("📦 Adding to git-annex...")
        working_copy = annex_repo / file_path.name
        
        # Copy file to annex repository
        subprocess.run(['cp', str(file_path), str(working_copy)], check=True)
        
        # Add to git-annex
        add_result = subprocess.run(
            ['git-annex', 'add', file_path.name],
            cwd=annex_repo, capture_output=True, text=True, check=True
        )
        
        # Get annex key
        key_result = subprocess.run(
            ['git-annex', 'lookupkey', file_path.name],
            cwd=annex_repo, capture_output=True, text=True, check=True
        )
        annex_key = key_result.stdout.strip()
        
        # Step 6: Create source_material_registry record
        console.print("💾 Creating source material registry record...")
        
        # Get system context
        hostname = socket.gethostname()
        staged_by_user = getpass.getuser()
        
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                # Generate ULID for blob_id using database function
                cur.execute("SELECT gen_ulid() AS blob_id")
                blob_id = cur.fetchone()['blob_id']
                
                # Insert into source_material_registry
                cur.execute("""
                    INSERT INTO raw.source_material_registry (
                        blob_id, checksum, stage_batch_id,
                        source_identifier, user_comment, user_tags,
                        staged_by_user, staged_on_host, staged_via_command,
                        source_path, source_mtime, source_size,
                        timing_info_type, source_material_format, processing_status
                    ) VALUES (
                        %s, %s, %s,
                        %s, %s, %s,
                        %s, %s, %s,
                        %s, to_timestamp(%s), %s,
                        %s, %s, %s
                    )
                """, (
                    blob_id, blake3_hash, stage_batch_id,
                    source_id, comment, parsed_tags,
                    staged_by_user, hostname, full_command,
                    str(file_path.absolute()), source_mtime, source_size,
                    'none',  # timing_info_type - could be enhanced with content analysis
                    'raw',   # source_material_format - could be inferred from file extension
                    'staged' # processing_status
                ))
                conn.commit()
        
        # Step 7: Commit to git
        subprocess.run(['git', 'add', file_path.name], cwd=annex_repo, check=True)
        commit_msg = f"Stage source material: {source_id}"
        if comment:
            commit_msg += f" - {comment}"
        subprocess.run(['git', 'commit', '-m', commit_msg], cwd=annex_repo, check=True)
        
        # Step 8: Complete operation logging
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT core.complete_operation(%s, %s::jsonb) AS result",
                    (operation_id, json.dumps({
                        'blob_id': str(blob_id),
                        'checksum': blake3_hash,
                        'annex_key': annex_key,
                        'source_size': source_size,
                        'stage_batch_id': stage_batch_id
                    }))
                )
                conn.commit()
        
        # Step 9: Success feedback
        console.print(f"[green]✅ Successfully staged source material![/green]")
        console.print(f"🆔 Blob ID: {blob_id}")
        console.print(f"🏷️  Source ID: {source_id}")
        console.print(f"📊 Size: {source_size:,} bytes")
        console.print(f"🔒 Checksum: {blake3_hash}")
        console.print(f"🗝️  Annex Key: {annex_key}")
        console.print(f"📦 Batch ID: {stage_batch_id}")
        if parsed_tags:
            console.print(f"🏷️  Tags: {', '.join(parsed_tags)}")
        
    except subprocess.CalledProcessError as e:
        # Fail the operation if we started logging
        if operation_id:
            try:
                with get_db_connection() as conn:
                    with conn.cursor() as cur:
                        cur.execute(
                            "SELECT core.fail_operation(%s, %s::jsonb) AS result",
                            (operation_id, json.dumps({
                                'error': str(e),
                                'stderr': e.stderr if e.stderr else None,
                                'command': e.cmd if hasattr(e, 'cmd') else None
                            }))
                        )
                        conn.commit()
            except Exception:
                pass  # Don't fail the failure logging
        
        console.print(f"[red]Command failed: {' '.join(e.cmd) if hasattr(e, 'cmd') else str(e)}[/red]")
        if hasattr(e, 'stderr') and e.stderr:
            console.print(f"[red]Error: {e.stderr}[/red]")
        return
        
    except Exception as e:
        # Fail the operation if we started logging
        if operation_id:
            try:
                with get_db_connection() as conn:
                    with conn.cursor() as cur:
                        cur.execute(
                            "SELECT core.fail_operation(%s, %s::jsonb) AS result",
                            (operation_id, json.dumps({'error': str(e)}))
                        )
                        conn.commit()
            except Exception:
                pass  # Don't fail the failure logging
        
        console.print(f"[red]Error: {e}[/red]")
        return


@blob.command('archive')
@click.argument('blob_id')
@click.option('--reason', '-r', required=True, help='Reason for archiving this blob')
@click.option('--dry-run', is_flag=True, help='Show what would be archived without actually doing it')
@click.option('--force', is_flag=True, help='Archive without confirmation prompt')
def blob_archive(blob_id: str, reason: str, dry_run: bool, force: bool):
    """Archive a blob and all events derived from it (The Sledgehammer).
    
    This command performs cascading archival of all events that originated from 
    the specified blob, effectively retracting all data derived from this source.
    
    This is the 'sledgehammer' approach - use with caution as it affects all
    events that trace back to this blob.
    
    Examples:
        exo blob archive 01ARZ3NDEKTSV4RRFFQ69G5FAV --reason "Duplicate data source"
        exo blob archive 01ARZ3NDEKTSV4RRFFQ69G5FAV --reason "Privacy request" --dry-run
    """
    import getpass
    import json
    
    operation_id = None
    
    try:
        # Validate blob_id format (basic ULID check)
        if len(blob_id) != 26:
            console.print(f"[red]Error: Invalid blob_id format. Expected 26-character ULID.[/red]")
            return
        
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                # Step 1: Verify blob exists
                cur.execute(
                    "SELECT blob_id, source_identifier, user_comment, staged_at, staged_by_user "
                    "FROM raw.source_material_registry WHERE blob_id = %s::uuid",
                    (blob_id,)
                )
                blob_info = cur.fetchone()
                
                if not blob_info:
                    console.print(f"[red]Error: Blob {blob_id} not found in source material registry.[/red]")
                    return
                
                # Step 2: Find all events with this source_material_id
                cur.execute("""
                    SELECT COUNT(*) as direct_events
                    FROM core.events 
                    WHERE source_material_id = %s::uuid
                """, (blob_id,))
                direct_count = cur.fetchone()['direct_events']
                
                # Step 3: Find dependent events (events that depend on events from this blob)
                cur.execute("""
                    WITH blob_events AS (
                        SELECT event_id FROM core.events WHERE source_material_id = %s::uuid
                    ),
                    all_dependent_events AS (
                        SELECT DISTINCT d.event_id, d.dependency_depth
                        FROM blob_events be
                        CROSS JOIN LATERAL core.find_dependent_events(be.event_id) d
                    )
                    SELECT COUNT(*) as dependent_events
                    FROM all_dependent_events
                """, (blob_id,))
                dependent_count = cur.fetchone()['dependent_events']
                
                total_events = direct_count + dependent_count
                
                # Step 4: Display impact summary
                console.print(f"\n[bold blue]Blob Archive Impact Analysis[/bold blue]")
                console.print(f"Blob ID: [yellow]{blob_id}[/yellow]")
                console.print(f"Source: [cyan]{blob_info['source_identifier']}[/cyan]")
                console.print(f"Comment: {blob_info['user_comment'] or 'None'}")
                console.print(f"Staged: {blob_info['staged_at']} by {blob_info['staged_by_user']}")
                console.print(f"\n[bold]Events to be archived:[/bold]")
                console.print(f"  Direct events: [yellow]{direct_count}[/yellow]")
                console.print(f"  Dependent events: [yellow]{dependent_count}[/yellow]")
                console.print(f"  Total events: [red]{total_events}[/red]")
                console.print(f"Reason: [cyan]{reason}[/cyan]")
                
                if dry_run:
                    console.print(f"\n[green]Dry run mode - no changes made.[/green]")
                    return
                
                if total_events == 0:
                    console.print(f"\n[yellow]No events found for this blob. Nothing to archive.[/yellow]")
                    return
                
                # Step 5: Confirmation
                if not force:
                    response = click.confirm(
                        f"\nAre you sure you want to archive {total_events} events? This action cannot be easily undone.",
                        default=False
                    )
                    if not response:
                        console.print("[yellow]Archive cancelled.[/yellow]")
                        return
                
                # Step 6: Start operation logging
                cur.execute(
                    "SELECT core.start_operation(%s, %s, %s::jsonb) AS operation_id",
                    ('archive', getpass.getuser(), json.dumps({
                        'operation_type': 'blob_archive',
                        'blob_id': blob_id,
                        'reason': reason,
                        'expected_direct_events': direct_count,
                        'expected_dependent_events': dependent_count,
                        'total_expected_events': total_events
                    }))
                )
                operation_id = cur.fetchone()['operation_id']
                conn.commit()
                
                # Step 7: Set archive metadata for the trigger
                cur.execute(
                    "SELECT core.set_archive_metadata(%s, %s, %s)",
                    (getpass.getuser(), f"blob_archive: {reason}", None)
                )
                
                # Step 8: Delete events (dependent events first, then direct events)
                # This ensures cascading works correctly
                
                # First delete dependent events
                if dependent_count > 0:
                    cur.execute("""
                        WITH blob_events AS (
                            SELECT event_id FROM core.events WHERE source_material_id = %s::uuid
                        ),
                        all_dependent_events AS (
                            SELECT DISTINCT d.event_id
                            FROM blob_events be
                            CROSS JOIN LATERAL core.find_dependent_events(be.event_id) d
                        )
                        DELETE FROM core.events 
                        WHERE event_id IN (SELECT event_id FROM all_dependent_events)
                    """, (blob_id,))
                    dependent_deleted = cur.rowcount
                else:
                    dependent_deleted = 0
                
                # Then delete direct events from the blob
                if direct_count > 0:
                    cur.execute(
                        "DELETE FROM core.events WHERE source_material_id = %s::uuid",
                        (blob_id,)
                    )
                    direct_deleted = cur.rowcount
                else:
                    direct_deleted = 0
                
                # Step 9: Mark blob as archived in source material registry
                cur.execute(
                    "UPDATE raw.source_material_registry SET processing_status = 'archived' WHERE blob_id = %s::uuid",
                    (blob_id,)
                )
                
                # Step 10: Complete operation logging
                actual_total = direct_deleted + dependent_deleted
                cur.execute(
                    "SELECT core.complete_operation(%s, %s::jsonb) AS result",
                    (operation_id, json.dumps({
                        'direct_events_archived': direct_deleted,
                        'dependent_events_archived': dependent_deleted,
                        'total_events_archived': actual_total,
                        'blob_status_updated': True
                    }))
                )
                conn.commit()
                
                # Step 11: Success message
                console.print(f"\n[green]Successfully archived blob {blob_id}[/green]")
                console.print(f"Events archived: {actual_total} (direct: {direct_deleted}, dependent: {dependent_deleted})")
                console.print(f"Operation ID: {operation_id}")
                
    except Exception as e:
        # Fail the operation if we started logging
        if operation_id:
            try:
                with get_db_connection() as conn:
                    with conn.cursor() as cur:
                        cur.execute(
                            "SELECT core.fail_operation(%s, %s::jsonb) AS result",
                            (operation_id, json.dumps({
                                'error': str(e),
                                'error_type': type(e).__name__
                            }))
                        )
                        conn.commit()
            except Exception:
                pass  # Don't fail the failure logging
        
        console.print(f"[red]Error during blob archive: {e}[/red]")
        return


@cli.group()
def dlq():
    """Dead Letter Queue management commands."""
    pass


@dlq.command('list')
@click.option('--agent', '-a', help='Filter by agent name')
@click.option('--source', '-s', help='Filter by event source')
@click.option('--event-type', '-t', help='Filter by event type')
@click.option('--category', '-c', 
              type=click.Choice(['retryable', 'permanent', 'system', 'user']),
              help='Filter by error category')
@click.option('--limit', '-n', default=50, help='Maximum number of DLQ entries to show')
@click.option('--include-resolved', is_flag=True, help='Include resolved entries')
@click.option('--output-format', type=click.Choice(['table', 'json', 'csv']), 
              default='table', help='Output format')
def dlq_list(agent: Optional[str], source: Optional[str], event_type: Optional[str], 
             category: Optional[str], limit: int, include_resolved: bool, output_format: str):
    """List DLQ entries with filtering options."""
    
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            # Build dynamic query
            query_parts = [
                "SELECT dlq_id, automaton_name, source, event_type, failure_reason, "
                "error_category, retry_count, failed_at, last_retry_at, next_retry_at, "
                "resolved_at, resolved_by, "
                "EXTRACT(EPOCH FROM (now() - failed_at)) AS age_seconds "
                "FROM sinex_schemas.dlq_events"
            ]
            conditions = []
            params = []
            
            if not include_resolved:
                conditions.append("resolved_at IS NULL")
            
            if agent:
                conditions.append("automaton_name = %s")
                params.append(agent)
            
            if source:
                conditions.append("source = %s")
                params.append(source)
            
            if event_type:
                conditions.append("event_type = %s")
                params.append(event_type)
            
            if category:
                conditions.append("error_category = %s")
                params.append(category)
            
            if conditions:
                query_parts.append("WHERE " + " AND ".join(conditions))
            
            query_parts.append("ORDER BY failed_at DESC")
            query_parts.append(f"LIMIT {limit}")
            
            query_sql = " ".join(query_parts)
            cur.execute(query_sql, params)
            dlq_entries = cur.fetchall()
    
    if not dlq_entries:
        console.print("[yellow]No DLQ entries found.[/yellow]")
        return
    
    if output_format == 'json':
        # Convert datetime and decimal objects for JSON serialization
        import decimal
        for entry in dlq_entries:
            for key, value in entry.items():
                if hasattr(value, 'isoformat'):
                    entry[key] = value.isoformat()
                elif isinstance(value, decimal.Decimal):
                    entry[key] = float(value)
        click.echo(json.dumps(list(dlq_entries), indent=2))
    elif output_format == 'csv':
        import csv
        import io
        output = io.StringIO()
        writer = csv.DictWriter(output, fieldnames=dlq_entries[0].keys())
        writer.writeheader()
        for entry in dlq_entries:
            row = {}
            for key, value in entry.items():
                if hasattr(value, 'isoformat'):
                    row[key] = value.isoformat()
                else:
                    row[key] = str(value)
            writer.writerow(row)
        click.echo(output.getvalue())
    else:
        # Table format
        table = Table(title=f"DLQ Entries ({len(dlq_entries)} shown)")
        table.add_column("DLQ ID", style="cyan")
        table.add_column("Automaton", style="green")
        table.add_column("Source", style="yellow")
        table.add_column("Event Type", style="blue")
        table.add_column("Category", style="magenta")
        table.add_column("Retries", justify="right", style="white")
        table.add_column("Age", justify="right", style="dim")
        table.add_column("Status", style="red")
        table.add_column("Failure Reason", style="white")
        
        for entry in dlq_entries:
            age_seconds = int(entry['age_seconds']) if entry['age_seconds'] else 0
            age_str = format_duration(age_seconds)
            
            status = "Resolved" if entry['resolved_at'] else "Pending"
            if entry['resolved_by']:
                status += f" ({entry['resolved_by']})"
            
            # Truncate failure reason for display
            reason = entry['failure_reason'][:60] + "..." if len(entry['failure_reason']) > 60 else entry['failure_reason']
            
            table.add_row(
                str(entry['dlq_id'])[:8],  # Truncated DLQ ID
                entry['automaton_name'],
                entry['source'],
                entry['event_type'],
                entry['error_category'],
                str(entry['retry_count']),
                age_str,
                status,
                reason
            )
        
        console.print(table)


@dlq.command('show')
@click.argument('dlq_id')
def dlq_show(dlq_id: str):
    """Show detailed information about a specific DLQ entry."""
    
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            # Handle partial DLQ ID matching
            if len(dlq_id) < 26:  # Partial ULID
                cur.execute("""
                    SELECT * FROM sinex_schemas.dlq_events 
                    WHERE dlq_id::text LIKE %s
                """, (f"{dlq_id}%",))
            else:
                cur.execute("""
                    SELECT * FROM sinex_schemas.dlq_events 
                    WHERE dlq_id = %s
                """, (dlq_id,))
            
            entry = cur.fetchone()
    
    if not entry:
        console.print(f"[red]DLQ entry not found: {dlq_id}[/red]")
        return
    
    # Display DLQ entry details
    panel_content = []
    panel_content.append(f"[bold]DLQ ID:[/bold] {entry['dlq_id']}")
    panel_content.append(f"[bold]Failed Event ID:[/bold] {entry['failed_event_id']}")
    panel_content.append(f"[bold]Automaton:[/bold] {entry['automaton_name']}")
    panel_content.append(f"[bold]Source:[/bold] {entry['source']}")
    panel_content.append(f"[bold]Event Type:[/bold] {entry['event_type']}")
    panel_content.append(f"[bold]Error Category:[/bold] {entry['error_category']}")
    panel_content.append(f"[bold]Retry Count:[/bold] {entry['retry_count']}")
    panel_content.append(f"[bold]Failed At:[/bold] {entry['failed_at']}")
    
    if entry['last_retry_at']:
        panel_content.append(f"[bold]Last Retry:[/bold] {entry['last_retry_at']}")
    if entry['next_retry_at']:
        panel_content.append(f"[bold]Next Retry:[/bold] {entry['next_retry_at']}")
    
    age_seconds = int((datetime.now(datetime.timezone.utc) - entry['failed_at'].replace(tzinfo=None)).total_seconds())
    panel_content.append(f"[bold]Age:[/bold] {format_duration(age_seconds)}")
    
    if entry['resolved_at']:
        panel_content.append(f"[bold]Resolved At:[/bold] {entry['resolved_at']}")
        panel_content.append(f"[bold]Resolved By:[/bold] {entry['resolved_by']}")
    
    console.print(Panel("\n".join(panel_content), title="DLQ Entry Details"))
    
    # Display failure reason
    console.print(f"\n[bold]Failure Reason:[/bold]")
    console.print(Panel(entry['failure_reason'], title="Error Details"))
    
    # Display original event payload
    console.print(f"\n[bold]Original Event Payload:[/bold]")
    console.print(JSON.from_data(entry['original_event_payload'], indent=2))
    
    # Display additional metadata if present
    if entry['additional_metadata']:
        console.print(f"\n[bold]Additional Metadata:[/bold]")
        console.print(JSON.from_data(entry['additional_metadata'], indent=2))


@dlq.command('retry')
@click.argument('dlq_id')
@click.option('--dry-run', is_flag=True, help='Show what would be retried without actually doing it')
def dlq_retry(dlq_id: str, dry_run: bool):
    """Retry a specific DLQ entry."""
    
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            # Find the DLQ entry
            if len(dlq_id) < 26:  # Partial ULID
                cur.execute("""
                    SELECT * FROM sinex_schemas.dlq_events 
                    WHERE dlq_id::text LIKE %s AND resolved_at IS NULL
                """, (f"{dlq_id}%",))
            else:
                cur.execute("""
                    SELECT * FROM sinex_schemas.dlq_events 
                    WHERE dlq_id = %s AND resolved_at IS NULL
                """, (dlq_id,))
            
            entry = cur.fetchone()
    
    if not entry:
        console.print(f"[red]Unresolved DLQ entry not found: {dlq_id}[/red]")
        return
    
    console.print(f"[bold]Retrying DLQ entry:[/bold] {entry['dlq_id']}")
    console.print(f"Automaton: {entry['automaton_name']}")
    console.print(f"Source: {entry['source']}")
    console.print(f"Event Type: {entry['event_type']}")
    console.print(f"Current Retry Count: {entry['retry_count']}")
    
    if dry_run:
        console.print("[yellow]DRY RUN: Would retry this entry[/yellow]")
        return
    
    # Update retry information
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            # Calculate next retry time with exponential backoff
            next_retry_seconds = min(300 * (2 ** entry['retry_count']), 3600)  # Cap at 1 hour
            next_retry_at = datetime.now(datetime.timezone.utc) + timedelta(seconds=next_retry_seconds)
            
            cur.execute("""
                UPDATE sinex_schemas.dlq_events 
                SET retry_count = retry_count + 1,
                    last_retry_at = now(),
                    next_retry_at = %s
                WHERE dlq_id = %s
            """, (next_retry_at, entry['dlq_id']))
            
            conn.commit()
    
    console.print(f"[green]✅ DLQ entry marked for retry[/green]")
    console.print(f"Next retry scheduled for: {next_retry_at}")
    console.print(f"New retry count: {entry['retry_count'] + 1}")


@dlq.command('resolve')
@click.argument('dlq_id')
@click.option('--resolution', type=click.Choice(['manual', 'purged']), 
              default='manual', help='How the entry was resolved')
@click.option('--dry-run', is_flag=True, help='Show what would be resolved without actually doing it')
def dlq_resolve(dlq_id: str, resolution: str, dry_run: bool):
    """Manually resolve a DLQ entry."""
    
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            # Find the DLQ entry
            if len(dlq_id) < 26:  # Partial ULID
                cur.execute("""
                    SELECT * FROM sinex_schemas.dlq_events 
                    WHERE dlq_id::text LIKE %s AND resolved_at IS NULL
                """, (f"{dlq_id}%",))
            else:
                cur.execute("""
                    SELECT * FROM sinex_schemas.dlq_events 
                    WHERE dlq_id = %s AND resolved_at IS NULL
                """, (dlq_id,))
            
            entry = cur.fetchone()
    
    if not entry:
        console.print(f"[red]Unresolved DLQ entry not found: {dlq_id}[/red]")
        return
    
    console.print(f"[bold]Resolving DLQ entry:[/bold] {entry['dlq_id']}")
    console.print(f"Automaton: {entry['automaton_name']}")
    console.print(f"Source: {entry['source']}")
    console.print(f"Event Type: {entry['event_type']}")
    console.print(f"Resolution: {resolution}")
    
    if dry_run:
        console.print("[yellow]DRY RUN: Would resolve this entry[/yellow]")
        return
    
    # Mark as resolved
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                UPDATE sinex_schemas.dlq_events 
                SET resolved_at = now(),
                    resolved_by = %s
                WHERE dlq_id = %s
            """, (resolution, entry['dlq_id']))
            
            conn.commit()
    
    console.print(f"[green]✅ DLQ entry resolved as: {resolution}[/green]")


@dlq.command('stats')
@click.option('--agent', '-a', help='Filter stats by agent name')
@click.option('--days', '-d', default=7, help='Number of days to analyze')
def dlq_stats(agent: Optional[str], days: int):
    """Show DLQ statistics and trends."""
    
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            # Build base query conditions
            where_clause = "WHERE failed_at > now() - interval '%s days'"
            params = [days]
            
            if agent:
                where_clause += " AND automaton_name = %s"
                params.append(agent)
            
            # Total DLQ entries
            cur.execute(f"""
                SELECT 
                    COUNT(*) as total_dlq,
                    COUNT(*) FILTER (WHERE resolved_at IS NULL) as pending,
                    COUNT(*) FILTER (WHERE resolved_at IS NOT NULL) as resolved,
                    COUNT(*) FILTER (WHERE error_category = 'retryable') as retryable,
                    COUNT(*) FILTER (WHERE error_category = 'permanent') as permanent,
                    COUNT(*) FILTER (WHERE error_category = 'system') as system,
                    COUNT(*) FILTER (WHERE error_category = 'user') as user
                FROM sinex_schemas.dlq_events
                {where_clause}
            """, params)
            totals = cur.fetchone()
            
            # DLQ by agent
            cur.execute(f"""
                SELECT 
                    automaton_name,
                    COUNT(*) as total,
                    COUNT(*) FILTER (WHERE resolved_at IS NULL) as pending,
                    AVG(retry_count) as avg_retries
                FROM sinex_schemas.dlq_events
                {where_clause}
                GROUP BY automaton_name
                ORDER BY total DESC
            """, params)
            by_agent = cur.fetchall()
            
            # DLQ by error category
            cur.execute(f"""
                SELECT 
                    error_category,
                    COUNT(*) as total,
                    COUNT(*) FILTER (WHERE resolved_at IS NULL) as pending,
                    AVG(retry_count) as avg_retries
                FROM sinex_schemas.dlq_events
                {where_clause}
                GROUP BY error_category
                ORDER BY total DESC
            """, params)
            by_category = cur.fetchall()
            
            # DLQ trends by day
            cur.execute(f"""
                SELECT 
                    DATE(failed_at) as day,
                    COUNT(*) as count,
                    COUNT(*) FILTER (WHERE resolved_at IS NOT NULL) as resolved_count
                FROM sinex_schemas.dlq_events
                {where_clause}
                GROUP BY DATE(failed_at)
                ORDER BY day DESC
            """, params)
            daily_trends = cur.fetchall()
    
    # Display overall statistics
    console.print(f"\n[bold]📊 DLQ Statistics (Last {days} days)[/bold]")
    if agent:
        console.print(f"[dim]Filtered by agent: {agent}[/dim]")
    
    console.print(f"\n[bold]Overall Summary:[/bold]")
    console.print(f"Total DLQ Entries: {totals['total_dlq']:,}")
    console.print(f"Pending: {totals['pending']:,}")
    console.print(f"Resolved: {totals['resolved']:,}")
    
    console.print(f"\n[bold]By Error Category:[/bold]")
    console.print(f"Retryable: {totals['retryable']:,}")
    console.print(f"Permanent: {totals['permanent']:,}")
    console.print(f"System: {totals['system']:,}")
    console.print(f"User: {totals['user']:,}")
    
    # DLQ by agent
    if by_agent:
        console.print(f"\n[bold]By Automaton:[/bold]")
        automaton_table = Table()
        automaton_table.add_column("Automaton", style="cyan")
        automaton_table.add_column("Total", justify="right", style="white")
        automaton_table.add_column("Pending", justify="right", style="red")
        automaton_table.add_column("Avg Retries", justify="right", style="yellow")
        
        for row in by_agent:
            automaton_table.add_row(
                row['automaton_name'],
                f"{row['total']:,}",
                f"{row['pending']:,}",
                f"{row['avg_retries']:.1f}" if row['avg_retries'] else "0"
            )
        
        console.print(automaton_table)
    
    # DLQ by category
    if by_category:
        console.print(f"\n[bold]By Category:[/bold]")
        category_table = Table()
        category_table.add_column("Category", style="cyan")
        category_table.add_column("Total", justify="right", style="white")
        category_table.add_column("Pending", justify="right", style="red")
        category_table.add_column("Avg Retries", justify="right", style="yellow")
        
        for row in by_category:
            category_table.add_row(
                row['error_category'],
                f"{row['total']:,}",
                f"{row['pending']:,}",
                f"{row['avg_retries']:.1f}" if row['avg_retries'] else "0"
            )
        
        console.print(category_table)
    
    # Daily trends
    if daily_trends:
        console.print(f"\n[bold]Daily Trends:[/bold]")
        for day in daily_trends:
            resolved_pct = (day['resolved_count'] / day['count'] * 100) if day['count'] > 0 else 0
            console.print(f"{day['day']}: {day['count']:,} failures, {day['resolved_count']:,} resolved ({resolved_pct:.1f}%)")


@dlq.command('purge')
@click.option('--agent', '-a', help='Purge entries for specific agent')
@click.option('--category', '-c', 
              type=click.Choice(['retryable', 'permanent', 'system', 'user']),
              help='Purge entries by category')
@click.option('--older-than', help='Purge entries older than N days (e.g., 30d)')
@click.option('--resolved-only', is_flag=True, help='Only purge resolved entries')
@click.option('--dry-run', is_flag=True, help='Show what would be purged without actually doing it')
@click.option('--force', is_flag=True, help='Skip confirmation prompt')
def dlq_purge(agent: Optional[str], category: Optional[str], older_than: Optional[str], 
              resolved_only: bool, dry_run: bool, force: bool):
    """Purge DLQ entries based on criteria."""
    
    # Build query conditions
    conditions = []
    params = []
    
    if agent:
        conditions.append("agent_name = %s")
        params.append(agent)
    
    if category:
        conditions.append("error_category = %s")
        params.append(category)
    
    if older_than:
        try:
            days = int(older_than.rstrip('d'))
            conditions.append("failed_at < now() - interval '%s days'")
            params.append(days)
        except ValueError:
            console.print(f"[red]Invalid format for --older-than: {older_than}. Use format like '30d'[/red]")
            return
    
    if resolved_only:
        conditions.append("resolved_at IS NOT NULL")
    
    if not conditions:
        console.print("[red]ERROR: You must specify at least one purge criterion[/red]")
        console.print("Use --agent, --category, --older-than, or --resolved-only")
        return
    
    # Check what would be purged
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            where_clause = "WHERE " + " AND ".join(conditions)
            
            cur.execute(f"""
                SELECT 
                    COUNT(*) as total,
                    COUNT(*) FILTER (WHERE resolved_at IS NULL) as pending,
                    COUNT(*) FILTER (WHERE resolved_at IS NOT NULL) as resolved
                FROM sinex_schemas.dlq_events
                {where_clause}
            """, params)
            
            counts = cur.fetchone()
    
    if counts['total'] == 0:
        console.print("[yellow]No DLQ entries match the purge criteria.[/yellow]")
        return
    
    # Show what would be purged
    console.print(f"[bold]Purge Preview:[/bold]")
    console.print(f"Total entries to purge: {counts['total']:,}")
    console.print(f"Pending entries: {counts['pending']:,}")
    console.print(f"Resolved entries: {counts['resolved']:,}")
    
    if dry_run:
        console.print("[yellow]DRY RUN: Would purge these entries[/yellow]")
        return
    
    # Confirm purge
    if not force:
        if not click.confirm(f"Are you sure you want to purge {counts['total']} DLQ entries?"):
            console.print("[yellow]Purge cancelled.[/yellow]")
            return
    
    # Perform the purge
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(f"""
                DELETE FROM sinex_schemas.dlq_events
                {where_clause}
            """, params)
            
            deleted_count = cur.rowcount
            conn.commit()
    
    console.print(f"[green]✅ Purged {deleted_count:,} DLQ entries[/green]")


def format_duration(seconds: int) -> str:
    """Format duration in seconds to human-readable format."""
    if seconds < 60:
        return f"{seconds}s"
    elif seconds < 3600:
        minutes = seconds // 60
        return f"{minutes}m"
    elif seconds < 86400:
        hours = seconds // 3600
        return f"{hours}h"
    else:
        days = seconds // 86400
        return f"{days}d"


@cli.command()
@click.pass_context
def stats(ctx):
    """Show enhanced database statistics."""
    
    use_db = ctx.obj.get('use_db', False)
    rpc_url = ctx.obj.get('rpc_url')
    
    try:
        if use_db:
            # Use direct database connection (legacy mode)
            _stats_with_database()
        else:
            # Use RPC (default mode) - limited functionality for now
            _stats_with_rpc(rpc_url)
            
    except SinexRPCError as e:
        console.print(f"[red]RPC Error: {e}[/red]")
        console.print(f"[yellow]Try using --use-db flag for direct database access[/yellow]")
        sys.exit(1)
    except Exception as e:
        console.print(f"[red]Error: {e}[/red]")
        sys.exit(1)


@cli.command()
@click.option('--automaton', '-a', required=True, help='Automaton name to replay')
@click.option('--since', '-s', help='Start time for replay (ISO format or relative like "1d")')
@click.option('--until', '-u', help='End time for replay (ISO format or relative like "1h")')
@click.option('--dry-run', is_flag=True, help='Show what would be replayed without executing')
@click.option('--force', is_flag=True, help='Skip confirmation prompt')
@click.pass_context
def replay(ctx, automaton: str, since: Optional[str], until: Optional[str], dry_run: bool, force: bool):
    """Replay automaton processing with dependency cascade.
    
    This command implements the replay coordinator from the comprehensive plan.
    It traces dependency chains, archives old events, and triggers re-computation.
    """
    use_db = ctx.obj.get('use_db', False)
    
    if not use_db:
        console.print("[red]Error: Replay command requires direct database access. Use --use-db flag.[/red]")
        sys.exit(1)
    
    try:
        console.print(f"[bold blue]🔄 Sinex Replay Coordinator[/bold blue]")
        console.print(f"Target automaton: [cyan]{automaton}[/cyan]")
        
        # Parse time range
        since_dt = parse_time_argument(since) if since else None
        until_dt = parse_time_argument(until) if until else datetime.utcnow()
        
        if since_dt:
            console.print(f"Time range: [yellow]{since_dt.isoformat()}[/yellow] to [yellow]{until_dt.isoformat()}[/yellow]")
        else:
            console.print(f"Time range: [yellow]All time[/yellow] to [yellow]{until_dt.isoformat()}[/yellow]")
        
        # Step 1: Find events to be replayed
        console.print("\n[bold]Step 1: Identifying events for replay[/bold]")
        target_events = find_target_events(automaton, since_dt, until_dt)
        
        if not target_events:
            console.print("[yellow]No events found for the specified criteria.[/yellow]")
            return
        
        console.print(f"Found [cyan]{len(target_events)}[/cyan] events from automaton [cyan]{automaton}[/cyan]")
        
        # Step 2: Build dependency graph
        console.print("\n[bold]Step 2: Building dependency cascade graph[/bold]")
        dependency_graph = build_dependency_graph(target_events)
        
        total_affected = sum(len(events) for events in dependency_graph.values())
        console.print(f"Total events in cascade: [red]{total_affected}[/red]")
        
        # Step 3: Display impact analysis
        display_impact_analysis(dependency_graph)
        
        if dry_run:
            console.print("\n[yellow]🚫 Dry run mode - no changes will be made[/yellow]")
            return
        
        # Step 4: User confirmation
        if not force:
            if not click.confirm(f"\n⚠️  This will archive {total_affected} events and trigger re-processing. Continue?"):
                console.print("[yellow]Replay cancelled.[/yellow]")
                return
        
        # Step 5: Execute replay
        console.print("\n[bold]Step 3: Executing replay operation[/bold]")
        execute_replay(automaton, dependency_graph, since_dt, until_dt)
        
        console.print("\n[green]✅ Replay completed successfully![/green]")
        console.print(f"[dim]Automaton [cyan]{automaton}[/cyan] will now re-process events from the specified time range.[/dim]")
        
    except Exception as e:
        console.print(f"[red]❌ Replay failed: {e}[/red]")
        sys.exit(1)


def parse_time_argument(time_str: str) -> datetime:
    """Parse time argument (ISO format or relative like '1d', '2h')."""
    if not time_str:
        return None
        
    # Try ISO format first
    try:
        return datetime.fromisoformat(time_str.replace('Z', '+00:00'))
    except ValueError:
        pass
    
    # Try relative format
    if time_str.endswith('d'):
        days = int(time_str[:-1])
        return datetime.utcnow() - timedelta(days=days)
    elif time_str.endswith('h'):
        hours = int(time_str[:-1])
        return datetime.utcnow() - timedelta(hours=hours)
    elif time_str.endswith('m'):
        minutes = int(time_str[:-1])
        return datetime.utcnow() - timedelta(minutes=minutes)
    else:
        raise ValueError(f"Invalid time format: {time_str}. Use ISO format or relative like '1d', '2h'")


def find_target_events(automaton: str, since_dt: Optional[datetime], until_dt: datetime) -> List[Dict]:
    """Find events created by the target automaton in the specified time range."""
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            query = """
                SELECT event_id, source, event_type, ts_orig, ts_ingest, payload
                FROM core.events 
                WHERE source = %s AND ts_orig <= %s
            """
            params = [automaton, until_dt]
            
            if since_dt:
                query += " AND ts_orig >= %s"
                params.append(since_dt)
            
            query += " ORDER BY ts_orig ASC"
            
            cur.execute(query, params)
            return [dict(row) for row in cur.fetchall()]


def build_dependency_graph(target_events: List[Dict]) -> Dict[str, List[Dict]]:
    """Build the full dependency cascade graph starting from target events."""
    target_ids = [event['id'] for event in target_events]
    all_affected = {}
    
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            # Start with target events
            all_affected['target'] = target_events
            
            # Find all dependent events recursively
            dependent_events = []
            for target_id in target_ids:
                cur.execute("""
                    SELECT event_id, dependency_depth 
                    FROM raw.find_dependent_events(%s)
                    ORDER BY dependency_depth, event_id
                """, [target_id])
                
                for row in cur.fetchall():
                    # Get the full event details
                    cur.execute("""
                        SELECT id, source, event_type, ts_orig, ts_ingest, payload
                        FROM core.events WHERE event_id = %s
                    """, [row['event_id']])
                    
                    event = cur.fetchone()
                    if event:
                        dependent_events.append({
                            **dict(event),
                            'dependency_depth': row['dependency_depth']
                        })
            
            # Group by dependency depth
            for event in dependent_events:
                depth = event['dependency_depth']
                depth_key = f"depth_{depth}"
                if depth_key not in all_affected:
                    all_affected[depth_key] = []
                all_affected[depth_key].append(event)
    
    return all_affected


def display_impact_analysis(dependency_graph: Dict[str, List[Dict]]):
    """Display a detailed impact analysis of the replay operation."""
    console.print("\n[bold]Impact Analysis:[/bold]")
    
    table = Table(box=box.ROUNDED)
    table.add_column("Level", style="cyan")
    table.add_column("Count", justify="right", style="red")
    table.add_column("Sources", style="yellow")
    table.add_column("Event Types", style="green")
    
    for level, events in dependency_graph.items():
        if not events:
            continue
            
        sources = set(event['source'] for event in events)
        event_types = set(event['event_type'] for event in events)
        
        table.add_row(
            level.replace('_', ' ').title(),
            str(len(events)),
            ", ".join(sorted(sources)[:3]) + ("..." if len(sources) > 3 else ""),
            ", ".join(sorted(event_types)[:3]) + ("..." if len(event_types) > 3 else "")
        )
    
    console.print(table)


def execute_replay(automaton: str, dependency_graph: Dict[str, List[Dict]], 
                  since_dt: Optional[datetime], until_dt: datetime):
    """Execute the actual replay operation."""
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            # Begin transaction
            cur.execute("BEGIN")
            
            try:
                # Set archive metadata
                cur.execute("""
                    SELECT raw.set_archive_metadata(
                        %s, %s, NULL
                    )
                """, [
                    'exo-replay-coordinator',
                    f'Replay of automaton {automaton} from {since_dt or "beginning"} to {until_dt}'
                ])
                
                # Archive all affected events in reverse dependency order
                total_archived = 0
                for level in sorted(dependency_graph.keys(), reverse=True):
                    events = dependency_graph[level]
                    if not events:
                        continue
                    
                    event_ids = [event['id'] for event in events]
                    console.print(f"  Archiving {len(event_ids)} events from {level}...")
                    
                    # Archive events in batches
                    for i in range(0, len(event_ids), 100):
                        batch = event_ids[i:i+100]
                        cur.execute("""
                            DELETE FROM core.events 
                            WHERE event_id = ANY(%s)
                        """, [batch])
                        total_archived += cur.rowcount
                
                console.print(f"  Total archived: [red]{total_archived}[/red] events")
                
                # Commit the transaction
                cur.execute("COMMIT")
                console.print("  ✅ Archive transaction committed")
                
                # The automaton will automatically pick up and re-process the raw events
                # when it runs its next scan cycle
                console.print(f"  🔄 Automaton [cyan]{automaton}[/cyan] will re-process on next scan")
                
            except Exception as e:
                cur.execute("ROLLBACK")
                raise Exception(f"Replay transaction failed: {e}")


@cli.command('event-archive')
@click.argument('event_id')
@click.option('--reason', '-r', required=True, help='Reason for archiving this event')
@click.option('--dry-run', is_flag=True, help='Show what would be archived without actually doing it')
@click.option('--force', is_flag=True, help='Archive without confirmation prompt')
@click.pass_context
def event_archive(ctx, event_id: str, reason: str, dry_run: bool, force: bool):
    """Archive a specific event and its dependents (The Surgical Tool).
    
    This command performs surgical archival of a single event and all events
    that depend on it. This is more precise than blob archive and allows for
    fine-grained data curation.
    
    Use this command when you want to remove specific events while preserving
    the rest of the data from the same source.
    
    Examples:
        exo event-archive 01ARZ3NDEKTSV4RRFFQ69G5FAV --reason "Duplicate event"
        exo event-archive 01ARZ3NDEKTSV4RRFFQ69G5FAV --reason "Privacy concern" --dry-run
    """
    import getpass
    import json
    
    use_db = ctx.obj.get('use_db', False)
    
    if not use_db:
        console.print("[red]Error: Event archive command requires direct database access. Use --use-db flag.[/red]")
        sys.exit(1)
    
    operation_id = None
    
    try:
        # Validate event_id format (basic ULID check)
        if len(event_id) != 26:
            console.print(f"[red]Error: Invalid event_id format. Expected 26-character ULID.[/red]")
            return
        
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                # Step 1: Verify event exists
                cur.execute("""
                    SELECT event_id, source, event_type, ts_orig, host, 
                           source_material_id, source_event_ids
                    FROM core.events 
                    WHERE event_id = %s::uuid
                """, (event_id,))
                event_info = cur.fetchone()
                
                if not event_info:
                    console.print(f"[red]Error: Event {event_id} not found.[/red]")
                    return
                
                # Step 2: Find dependent events
                cur.execute("""
                    SELECT event_id, dependency_depth
                    FROM core.find_dependent_events(%s::uuid)
                    ORDER BY dependency_depth DESC, event_id
                """, (event_id,))
                dependent_events = cur.fetchall()
                dependent_count = len(dependent_events)
                total_events = 1 + dependent_count  # +1 for the target event itself
                
                # Step 3: Display impact summary
                console.print(f"\n[bold blue]Event Archive Impact Analysis[/bold blue]")
                console.print(f"Event ID: [yellow]{event_id}[/yellow]")
                console.print(f"Source: [cyan]{event_info['source']}[/cyan]")
                console.print(f"Type: [cyan]{event_info['event_type']}[/cyan]")
                console.print(f"Original Time: {event_info['ts_orig']}")
                console.print(f"Host: {event_info['host']}")
                
                # Show provenance info
                if event_info['source_material_id']:
                    console.print(f"Source Material: [yellow]{event_info['source_material_id']}[/yellow]")
                if event_info['source_event_ids']:
                    console.print(f"Source Events: {len(event_info['source_event_ids'])} events")
                
                console.print(f"\n[bold]Events to be archived:[/bold]")
                console.print(f"  Target event: [yellow]1[/yellow]")
                console.print(f"  Dependent events: [yellow]{dependent_count}[/yellow]")
                console.print(f"  Total events: [red]{total_events}[/red]")
                
                # Show dependency tree if there are dependents
                if dependent_count > 0:
                    console.print(f"\n[bold]Dependency Tree:[/bold]")
                    current_depth = -1
                    for dep in dependent_events[:10]:  # Show first 10 for readability
                        if dep['dependency_depth'] != current_depth:
                            current_depth = dep['dependency_depth']
                            console.print(f"  Depth {current_depth}:")
                        console.print(f"    {dep['event_id']}")
                    if dependent_count > 10:
                        console.print(f"    ... and {dependent_count - 10} more")
                
                console.print(f"Reason: [cyan]{reason}[/cyan]")
                
                if dry_run:
                    console.print(f"\n[green]Dry run mode - no changes made.[/green]")
                    return
                
                # Step 4: Confirmation
                if not force:
                    response = click.confirm(
                        f"\nAre you sure you want to archive {total_events} events? This action cannot be easily undone.",
                        default=False
                    )
                    if not response:
                        console.print("[yellow]Archive cancelled.[/yellow]")
                        return
                
                # Step 5: Start operation logging
                cur.execute(
                    "SELECT core.start_operation(%s, %s, %s::jsonb) AS operation_id",
                    ('archive', getpass.getuser(), json.dumps({
                        'operation_type': 'event_archive',
                        'target_event_id': event_id,
                        'reason': reason,
                        'expected_dependent_events': dependent_count,
                        'total_expected_events': total_events
                    }))
                )
                operation_id = cur.fetchone()['operation_id']
                conn.commit()
                
                # Step 6: Set archive metadata for the trigger
                cur.execute(
                    "SELECT core.set_archive_metadata(%s, %s, %s)",
                    (getpass.getuser(), f"event_archive: {reason}", None)
                )
                
                # Step 7: Delete events (dependent events first, then target event)
                # This ensures cascading works correctly
                events_deleted = 0
                
                # First delete dependent events (deepest first)
                for dep in dependent_events:
                    cur.execute(
                        "DELETE FROM core.events WHERE event_id = %s::uuid",
                        (dep['event_id'],)
                    )
                    if cur.rowcount > 0:
                        events_deleted += 1
                
                # Then delete the target event
                cur.execute(
                    "DELETE FROM core.events WHERE event_id = %s::uuid",
                    (event_id,)
                )
                if cur.rowcount > 0:
                    events_deleted += 1
                
                # Step 8: Complete operation logging
                cur.execute(
                    "SELECT core.complete_operation(%s, %s::jsonb) AS result",
                    (operation_id, json.dumps({
                        'events_archived': events_deleted,
                        'expected_events': total_events,
                        'target_event_archived': True
                    }))
                )
                conn.commit()
                
                # Step 9: Success message
                console.print(f"\n[green]Successfully archived event {event_id}[/green]")
                console.print(f"Events archived: {events_deleted}")
                console.print(f"Operation ID: {operation_id}")
                
    except Exception as e:
        # Fail the operation if we started logging
        if operation_id:
            try:
                with get_db_connection() as conn:
                    with conn.cursor() as cur:
                        cur.execute(
                            "SELECT core.fail_operation(%s, %s::jsonb) AS result",
                            (operation_id, json.dumps({
                                'error': str(e),
                                'error_type': type(e).__name__
                            }))
                        )
                        conn.commit()
            except Exception:
                pass  # Don't fail the failure logging
        
        console.print(f"[red]Error during event archive: {e}[/red]")
        return


@cli.command()
@click.option('--event-id', '-e', required=True, help='ULID of the archived event to restore')
@click.option('--cascade', is_flag=True, help='Restore entire dependency subtree (default: true)')
@click.option('--dry-run', is_flag=True, help='Show what would be restored without executing')
@click.option('--force', is_flag=True, help='Skip confirmation prompt')
@click.pass_context
def restore(ctx, event_id: str, cascade: bool, dry_run: bool, force: bool):
    """Restore an archived event (rollback a replay operation).
    
    This command is the symmetric opposite of replay. It finds the archived event,
    identifies the replacement subtree, and swaps them atomically.
    """
    use_db = ctx.obj.get('use_db', False)
    
    if not use_db:
        console.print("[red]Error: Restore command requires direct database access. Use --use-db flag.[/red]")
        sys.exit(1)
    
    try:
        console.print(f"[bold blue]🔄 Sinex Restore Coordinator[/bold blue]")
        console.print(f"Target event: [cyan]{event_id}[/cyan]")
        
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                # Step 1: Find the archived event
                cur.execute("""
                    SELECT id, source, event_type, ts_orig, superseded_by_event_id, archive_reason
                    FROM audit.archived_events 
                    WHERE event_id = %s
                """, [event_id])
                
                archived_event = cur.fetchone()
                if not archived_event:
                    console.print(f"[red]❌ Archived event {event_id} not found[/red]")
                    sys.exit(1)
                
                console.print(f"Found archived event: [yellow]{archived_event['source']}[/yellow].[yellow]{archived_event['event_type']}[/yellow]")
                console.print(f"Archived reason: [dim]{archived_event['archive_reason']}[/dim]")
                
                # Step 2: Find the replacement event if it exists
                replacement_id = archived_event['superseded_by_event_id']
                if replacement_id:
                    console.print(f"Replacement event: [cyan]{replacement_id}[/cyan]")
                    
                    # Build dependency graph for replacement subtree
                    cur.execute("""
                        SELECT id, source, event_type, ts_orig, ts_ingest, payload
                        FROM core.events WHERE event_id = %s
                    """, [replacement_id])
                    
                    replacement_event = cur.fetchone()
                    if replacement_event:
                        replacement_graph = build_dependency_graph([dict(replacement_event)])
                        replacement_count = sum(len(events) for events in replacement_graph.values())
                    else:
                        replacement_graph = {}
                        replacement_count = 0
                    console.print(f"Replacement subtree contains [red]{replacement_count}[/red] events")
                else:
                    console.print("[yellow]No replacement event found - this was a deletion[/yellow]")
                    replacement_graph = {}
                    replacement_count = 0
                
                # Step 3: Build dependency graph for archived subtree if cascade is enabled
                if cascade:
                    # Find all related archived events that should be restored together
                    archived_graph = build_archived_dependency_graph(cur, event_id)
                    archived_count = sum(len(events) for events in archived_graph.values())
                    console.print(f"Archived subtree contains [green]{archived_count}[/green] events")
                else:
                    archived_graph = {0: [archived_event]}
                    archived_count = 1
                
                # Step 4: Show impact analysis
                console.print(f"\n[bold]Restore Impact Analysis:[/bold]")
                console.print(f"  Events to restore: [green]{archived_count}[/green]")
                console.print(f"  Events to archive: [red]{replacement_count}[/red]")
                console.print(f"  Net change: [{'green' if archived_count >= replacement_count else 'red'}]{archived_count - replacement_count:+d}[/{'green' if archived_count >= replacement_count else 'red'}] events")
                
                if dry_run:
                    console.print(f"\n[yellow]🔍 Dry run completed - no changes made[/yellow]")
                    return
                
                # Step 5: Confirmation
                if not force:
                    console.print(f"\n[bold red]⚠️  This will permanently modify the event database![/bold red]")
                    if not click.confirm("Continue with restore?"):
                        console.print("[yellow]Restore cancelled[/yellow]")
                        return
                
                # Step 6: Execute restore transaction
                console.print(f"\n[bold]Executing restore transaction...[/bold]")
                execute_restore(cur, archived_graph, replacement_graph, event_id)
                
                console.print(f"[green]✅ Restore completed successfully![/green]")
                console.print(f"[green]✅ Restored {archived_count} events, archived {replacement_count} events[/green]")
                
    except Exception as e:
        console.print(f"[red]❌ Restore failed: {e}[/red]")
        sys.exit(1)


def build_archived_dependency_graph(cur, root_event_id: str) -> Dict[int, List[Dict]]:
    """Build dependency graph for archived events starting from root event."""
    # For now, implement simple single-event restore
    # Can be enhanced later for full dependency tracking
    cur.execute("""
        SELECT id, source, event_type, ts_orig, superseded_by_event_id, archive_reason
        FROM audit.archived_events 
        WHERE event_id = %s
    """, [root_event_id])
    
    event = cur.fetchone()
    if event:
        return {0: [dict(event)]}
    return {}


def execute_restore(cur, archived_graph: Dict[int, List[Dict]], replacement_graph: Dict[int, List[Dict]], event_id: str):
    """Execute the atomic restore operation."""
    try:
        # Begin transaction
        cur.execute("BEGIN")
        
        # Step 1: Archive the replacement subtree (if it exists)
        if replacement_graph:
            console.print("  Archiving replacement subtree...")
            for level in sorted(replacement_graph.keys(), reverse=True):
                events = replacement_graph[level]
                if not events:
                    continue
                
                event_ids = [event['id'] for event in events]
                
                # Set archive metadata
                cur.execute("""
                    SELECT raw.set_archive_metadata(
                        'restore_coordinator', 
                        'Archived during restore operation',
                        %s
                    )
                """, [event_id])
                
                # Archive events
                for event_id_to_archive in event_ids:
                    cur.execute("DELETE FROM core.events WHERE event_id = %s", [event_id_to_archive])
        
        # Step 2: Restore the archived subtree
        console.print("  Restoring archived subtree...")
        for level in sorted(archived_graph.keys()):
            events = archived_graph[level]
            if not events:
                continue
                
            for event in events:
                # Use the restore function from the database
                cur.execute("""
                    SELECT audit.restore_archived_event(%s)
                """, [event['id']])
        
        # Commit transaction
        cur.execute("COMMIT")
        console.print("  ✅ Restore transaction committed")
        
    except Exception as e:
        cur.execute("ROLLBACK")
        raise Exception(f"Restore transaction failed: {e}")


@cli.group()
def explore():
    """Satellite exploration and diagnostic commands."""
    pass


@explore.command('coverage')
@click.option('--satellite', '-s', help='Satellite name (e.g., fs-watcher, terminal-satellite)')
@click.option('--time-range', '-t', default='1d', help='Time range to analyze (e.g., 1d, 12h, 2w)')
@click.option('--source', help='Filter by specific source within satellite')
@click.pass_context
def explore_coverage(ctx, satellite: Optional[str], time_range: str, source: Optional[str]):
    """Analyze coverage and identify ingestion gaps."""
    use_db = ctx.obj.get('use_db', False)
    
    if not use_db:
        console.print("[red]Error: Explore commands require direct database access. Use --use-db flag.[/red]")
        sys.exit(1)
    
    try:
        since_dt = datetime.utcnow() - parse_time_delta(time_range)
        
        console.print(f"[bold blue]🔍 Sinex Coverage Analysis[/bold blue]")
        console.print(f"Time range: [yellow]{since_dt.isoformat()}[/yellow] to [yellow]{datetime.utcnow().isoformat()}[/yellow]")
        
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                # Build query conditions
                conditions = ["ts_orig >= %s"]
                params = [since_dt]
                
                if satellite:
                    # Map satellite name to source pattern
                    satellite_patterns = {
                        'fs-watcher': 'fs-%',
                        'terminal-satellite': 'terminal-%', 
                        'desktop-satellite': 'desktop-%',
                        'system-satellite': 'system-%',
                    }
                    
                    pattern = satellite_patterns.get(satellite, f"{satellite}-%")
                    conditions.append("source LIKE %s")
                    params.append(pattern)
                
                if source:
                    conditions.append("source = %s")
                    params.append(source)
                
                # Event count analysis
                cur.execute(f"""
                    SELECT 
                        source,
                        COUNT(*) as event_count,
                        MIN(ts_orig) as first_event,
                        MAX(ts_orig) as last_event,
                        COUNT(DISTINCT event_type) as event_types
                    FROM core.events
                    WHERE {' AND '.join(conditions)}
                    GROUP BY source
                    ORDER BY event_count DESC
                """, params)
                
                results = cur.fetchall()
                
                if not results:
                    console.print("[yellow]No events found matching criteria[/yellow]")
                    return
                
                # Display results
                table = Table(title="Coverage Analysis")
                table.add_column("Source", style="cyan")
                table.add_column("Events", justify="right", style="green")
                table.add_column("Types", justify="right", style="blue")
                table.add_column("First Event", style="dim")
                table.add_column("Last Event", style="dim")
                table.add_column("Duration", style="yellow")
                
                for row in results:
                    duration = row['last_event'] - row['first_event']
                    table.add_row(
                        row['source'],
                        str(row['event_count']),
                        str(row['event_types']),
                        row['first_event'].strftime('%Y-%m-%d %H:%M'),
                        row['last_event'].strftime('%Y-%m-%d %H:%M'),
                        str(duration)
                    )
                
                console.print(table)
                
                # Gap analysis
                console.print(f"\n[bold]Gap Analysis:[/bold]")
                total_duration = datetime.utcnow() - since_dt
                
                for row in results:
                    source_duration = row['last_event'] - row['first_event']
                    coverage_pct = (source_duration.total_seconds() / total_duration.total_seconds()) * 100
                    
                    if coverage_pct < 80:
                        console.print(f"  [yellow]⚠️  {row['source']}: {coverage_pct:.1f}% coverage[/yellow]")
                    else:
                        console.print(f"  [green]✅ {row['source']}: {coverage_pct:.1f}% coverage[/green]")
                
    except Exception as e:
        console.print(f"[red]❌ Coverage analysis failed: {e}[/red]")
        sys.exit(1)


@explore.command('source-state')
@click.option('--satellite', '-s', required=True, help='Satellite name (e.g., fs-watcher, terminal-satellite)')
@click.option('--verbose', '-v', is_flag=True, help='Show detailed source state information')
@click.pass_context
def explore_source_state(ctx, satellite: str, verbose: bool):
    """Inspect current source state for a satellite."""
    use_db = ctx.obj.get('use_db', False)
    
    if not use_db:
        console.print("[red]Error: Explore commands require direct database access. Use --use-db flag.[/red]")
        sys.exit(1)
    
    try:
        console.print(f"[bold blue]🔍 Sinex Source State Analysis[/bold blue]")
        console.print(f"Satellite: [cyan]{satellite}[/cyan]")
        
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                # Get automaton checkpoint for this satellite
                cur.execute("""
                    SELECT 
                        automaton_name,
                        last_processed_id,
                        processed_count,
                        last_activity,
                        state_data
                    FROM core.automaton_checkpoints
                    WHERE automaton_name LIKE %s
                    ORDER BY last_activity DESC
                """, [f"%{satellite}%"])
                
                checkpoints = cur.fetchall()
                
                if not checkpoints:
                    console.print(f"[yellow]No checkpoint found for satellite '{satellite}'[/yellow]")
                    return
                
                console.print(f"\n[bold]Checkpoint Status:[/bold]")
                
                for checkpoint in checkpoints:
                    console.print(f"  [cyan]{checkpoint['automaton_name']}[/cyan]")
                    console.print(f"    Last processed: {checkpoint['last_processed_id'] or 'None'}")
                    console.print(f"    Processed count: {checkpoint['processed_count']}")
                    console.print(f"    Last activity: {checkpoint['last_activity']}")
                    
                    if verbose and checkpoint['state_data']:
                        console.print(f"    State data: {JSON(checkpoint['state_data'])}")
                    
                    console.print()
                
                # Get recent events from this satellite
                satellite_pattern = f"%{satellite}%"
                cur.execute("""
                    SELECT 
                        source,
                        event_type,
                        ts_orig,
                        payload
                    FROM core.events
                    WHERE source LIKE %s
                    ORDER BY ts_orig DESC
                    LIMIT 10
                """, [satellite_pattern])
                
                recent_events = cur.fetchall()
                
                if recent_events:
                    console.print(f"[bold]Recent Events (last 10):[/bold]")
                    table = Table()
                    table.add_column("Source", style="cyan")
                    table.add_column("Event Type", style="blue")
                    table.add_column("Timestamp", style="dim")
                    table.add_column("Payload Preview", style="dim")
                    
                    for event in recent_events:
                        payload_preview = str(event['payload'])[:50] + "..." if len(str(event['payload'])) > 50 else str(event['payload'])
                        table.add_row(
                            event['source'],
                            event['event_type'],
                            event['ts_orig'].strftime('%Y-%m-%d %H:%M:%S'),
                            payload_preview
                        )
                    
                    console.print(table)
                
    except Exception as e:
        console.print(f"[red]❌ Source state analysis failed: {e}[/red]")
        sys.exit(1)


@explore.command('missing-events')
@click.option('--satellite', '-s', help='Satellite name to analyze')
@click.option('--time-range', '-t', default='1h', help='Time range to check for missing events')
@click.pass_context
def explore_missing_events(ctx, satellite: Optional[str], time_range: str):
    """Detect potential missing events by analyzing patterns."""
    use_db = ctx.obj.get('use_db', False)
    
    if not use_db:
        console.print("[red]Error: Explore commands require direct database access. Use --use-db flag.[/red]")
        sys.exit(1)
    
    try:
        since_dt = datetime.utcnow() - parse_time_delta(time_range)
        
        console.print(f"[bold blue]🔍 Missing Events Analysis[/bold blue]")
        console.print(f"Time range: [yellow]{since_dt.isoformat()}[/yellow] to [yellow]{datetime.utcnow().isoformat()}[/yellow]")
        
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                # Look for gaps in time series data
                conditions = ["ts_orig >= %s"]
                params = [since_dt]
                
                if satellite:
                    satellite_patterns = {
                        'fs-watcher': 'fs-%',
                        'terminal-satellite': 'terminal-%', 
                        'desktop-satellite': 'desktop-%',
                        'system-satellite': 'system-%',
                    }
                    
                    pattern = satellite_patterns.get(satellite, f"{satellite}-%")
                    conditions.append("source LIKE %s")
                    params.append(pattern)
                
                # Find time gaps larger than expected
                cur.execute(f"""
                    WITH time_gaps AS (
                        SELECT 
                            source,
                            ts_orig,
                            LAG(ts_orig) OVER (PARTITION BY source ORDER BY ts_orig) as prev_ts,
                            ts_orig - LAG(ts_orig) OVER (PARTITION BY source ORDER BY ts_orig) as time_gap
                        FROM core.events
                        WHERE {' AND '.join(conditions)}
                    )
                    SELECT 
                        source,
                        prev_ts,
                        ts_orig,
                        time_gap
                    FROM time_gaps
                    WHERE time_gap > interval '10 minutes'
                    ORDER BY time_gap DESC
                    LIMIT 20
                """, params)
                
                gaps = cur.fetchall()
                
                if gaps:
                    console.print(f"\n[bold]Significant Time Gaps (>10 minutes):[/bold]")
                    table = Table()
                    table.add_column("Source", style="cyan")
                    table.add_column("Gap Start", style="dim")
                    table.add_column("Gap End", style="dim")
                    table.add_column("Duration", style="red")
                    
                    for gap in gaps:
                        table.add_row(
                            gap['source'],
                            gap['prev_ts'].strftime('%Y-%m-%d %H:%M:%S'),
                            gap['ts_orig'].strftime('%Y-%m-%d %H:%M:%S'),
                            str(gap['time_gap'])
                        )
                    
                    console.print(table)
                else:
                    console.print("[green]✅ No significant time gaps detected[/green]")
                
    except Exception as e:
        console.print(f"[red]❌ Missing events analysis failed: {e}[/red]")
        sys.exit(1)


@explore.command('curate')
@click.option('--time-range', '-t', default='1d', help='Time range to analyze for duplicates (e.g., 1d, 12h, 2w)')
@click.option('--source', '-s', help='Filter by specific source')
@click.option('--event-type', '-e', help='Filter by specific event type')
@click.option('--limit', '-n', default=50, help='Maximum number of duplicate groups to show')
@click.option('--auto-resolve', is_flag=True, help='Automatically resolve obvious duplicates without prompting')
@click.pass_context
def explore_curate(ctx, time_range: str, source: Optional[str], event_type: Optional[str], 
                  limit: int, auto_resolve: bool):
    """Interactive curation mode for resolving data ambiguities and duplicates.
    
    This command analyzes your data for logical duplicates, timing inconsistencies,
    and other anomalies, then provides an interactive menu to resolve them using
    the surgical event archive command.
    
    Examples:
        exo explore curate --time-range 1d
        exo explore curate --source terminal-satellite --auto-resolve
        exo explore curate --event-type command.executed --limit 20
    """
    import getpass
    import json
    from collections import defaultdict
    
    use_db = ctx.obj.get('use_db', False)
    
    if not use_db:
        console.print("[red]Error: Explore curate command requires direct database access. Use --use-db flag.[/red]")
        sys.exit(1)
    
    try:
        since_dt = datetime.utcnow() - parse_time_delta(time_range)
        
        console.print(f"[bold blue]🔍 Sinex Data Curation Analysis[/bold blue]")
        console.print(f"Time range: [yellow]{since_dt.isoformat()}[/yellow] to [yellow]{datetime.utcnow().isoformat()}[/yellow]")
        
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                # Build query conditions
                conditions = ["ts_orig >= %s"]
                params = [since_dt]
                
                if source:
                    conditions.append("source = %s")
                    params.append(source)
                
                if event_type:
                    conditions.append("event_type = %s")
                    params.append(event_type)
                
                where_clause = " AND ".join(conditions)
                
                # Find potential duplicates based on several criteria
                console.print("\n[bold]Searching for potential duplicates...[/bold]")
                
                # 1. Exact payload duplicates within short time windows
                cur.execute(f"""
                    WITH duplicate_groups AS (
                        SELECT 
                            source, event_type, payload::text as payload_text,
                            array_agg(event_id ORDER BY ts_orig) as event_ids,
                            array_agg(ts_orig ORDER BY ts_orig) as timestamps,
                            COUNT(*) as dup_count,
                            MAX(ts_orig) - MIN(ts_orig) as time_spread
                        FROM core.events 
                        WHERE {where_clause}
                        GROUP BY source, event_type, payload::text
                        HAVING COUNT(*) > 1 
                           AND MAX(ts_orig) - MIN(ts_orig) < INTERVAL '5 minutes'
                    )
                    SELECT * FROM duplicate_groups 
                    ORDER BY dup_count DESC, time_spread ASC
                    LIMIT %s
                """, params + [limit])
                
                duplicate_groups = cur.fetchall()
                
                if not duplicate_groups:
                    console.print("[green]✅ No exact payload duplicates found in the specified time range.[/green]")
                    
                    # Look for near-duplicates (same type, source, similar timestamps)
                    console.print("\n[bold]Searching for near-duplicates...[/bold]")
                    cur.execute(f"""
                        WITH time_clusters AS (
                            SELECT 
                                source, event_type,
                                event_id, ts_orig, payload,
                                LAG(ts_orig) OVER (PARTITION BY source, event_type ORDER BY ts_orig) as prev_ts
                            FROM core.events 
                            WHERE {where_clause}
                        ),
                        near_duplicates AS (
                            SELECT 
                                source, event_type, event_id, ts_orig, payload,
                                ts_orig - prev_ts as time_diff
                            FROM time_clusters
                            WHERE prev_ts IS NOT NULL 
                              AND ts_orig - prev_ts < INTERVAL '30 seconds'
                        )
                        SELECT * FROM near_duplicates 
                        ORDER BY time_diff ASC
                        LIMIT %s
                    """, params + [limit])
                    
                    near_duplicates = cur.fetchall()
                    
                    if not near_duplicates:
                        console.print("[green]✅ No near-duplicates found either. Your data looks clean![/green]")
                        return
                    else:
                        console.print(f"[yellow]Found {len(near_duplicates)} potential near-duplicates[/yellow]")
                        
                        # Show a few examples
                        for i, dup in enumerate(near_duplicates[:5]):
                            console.print(f"\n[dim]Near-duplicate {i+1}:[/dim]")
                            console.print(f"  Source: [cyan]{dup['source']}[/cyan]")
                            console.print(f"  Type: [cyan]{dup['event_type']}[/cyan]")
                            console.print(f"  Time diff: [yellow]{dup['time_diff']}[/yellow]")
                            console.print(f"  Event ID: {dup['event_id']}")
                        
                        if len(near_duplicates) > 5:
                            console.print(f"  ... and {len(near_duplicates) - 5} more")
                        
                        console.print("\n[yellow]Use specific event IDs with 'exo event-archive' to remove individual duplicates.[/yellow]")
                    return
                
                console.print(f"[yellow]Found {len(duplicate_groups)} duplicate groups[/yellow]")
                
                # Interactive resolution for each duplicate group
                resolved_count = 0
                for i, group in enumerate(duplicate_groups):
                    console.print(f"\n[bold]Duplicate Group {i+1}/{len(duplicate_groups)}[/bold]")
                    console.print(f"Source: [cyan]{group['source']}[/cyan]")
                    console.print(f"Type: [cyan]{group['event_type']}[/cyan]")
                    console.print(f"Count: [yellow]{group['dup_count']} events[/yellow]")
                    console.print(f"Time spread: [yellow]{group['time_spread']}[/yellow]")
                    
                    # Show the events in this group
                    event_ids = group['event_ids']
                    timestamps = group['timestamps']
                    
                    console.print(f"\nEvents in this group:")
                    for j, (event_id, ts) in enumerate(zip(event_ids, timestamps)):
                        console.print(f"  {j+1}. {event_id} at {ts}")
                    
                    if auto_resolve and group['dup_count'] <= 5 and group['time_spread'].total_seconds() < 60:
                        # Auto-resolve: keep the first event, archive the rest
                        events_to_archive = event_ids[1:]  # Keep first, archive rest
                        console.print(f"\n[green]Auto-resolving: keeping first event, archiving {len(events_to_archive)} duplicates[/green]")
                        
                        for event_id in events_to_archive:
                            # Use the archive metadata function
                            cur.execute(
                                "SELECT core.set_archive_metadata(%s, %s, %s)",
                                (getpass.getuser(), f"auto_curate: exact duplicate resolved", None)
                            )
                            cur.execute(
                                "DELETE FROM core.events WHERE event_id = %s",
                                (event_id,)
                            )
                        
                        resolved_count += len(events_to_archive)
                        continue
                    
                    # Interactive resolution
                    console.print(f"\nResolution options:")
                    console.print(f"  [P]refer event (choose which one to keep)")
                    console.print(f"  [A]rchive all but first")
                    console.print(f"  [S]kip this group")
                    console.print(f"  [Q]uit curation")
                    
                    choice = click.prompt("Choose action", type=click.Choice(['P', 'A', 'S', 'Q'], case_sensitive=False))
                    
                    if choice.upper() == 'Q':
                        break
                    elif choice.upper() == 'S':
                        continue
                    elif choice.upper() == 'A':
                        # Archive all but first
                        events_to_archive = event_ids[1:]
                        console.print(f"Archiving {len(events_to_archive)} duplicate events...")
                        
                        for event_id in events_to_archive:
                            cur.execute(
                                "SELECT core.set_archive_metadata(%s, %s, %s)",
                                (getpass.getuser(), f"manual_curate: duplicate resolved", None)
                            )
                            cur.execute(
                                "DELETE FROM core.events WHERE event_id = %s",
                                (event_id,)
                            )
                        
                        resolved_count += len(events_to_archive)
                        console.print(f"[green]✅ Archived {len(events_to_archive)} events[/green]")
                        
                    elif choice.upper() == 'P':
                        # Let user choose which event to prefer
                        event_choice = click.prompt(
                            f"Which event to keep? (1-{len(event_ids)})", 
                            type=click.IntRange(1, len(event_ids))
                        )
                        
                        preferred_event = event_ids[event_choice - 1]
                        events_to_archive = [eid for j, eid in enumerate(event_ids) if j != (event_choice - 1)]
                        
                        console.print(f"Keeping {preferred_event}, archiving {len(events_to_archive)} others...")
                        
                        for event_id in events_to_archive:
                            cur.execute(
                                "SELECT core.set_archive_metadata(%s, %s, %s)",
                                (getpass.getuser(), f"manual_curate: preferred {preferred_event}", None)
                            )
                            cur.execute(
                                "DELETE FROM core.events WHERE event_id = %s",
                                (event_id,)
                            )
                        
                        resolved_count += len(events_to_archive)
                        console.print(f"[green]✅ Archived {len(events_to_archive)} events[/green]")
                
                # Summary
                console.print(f"\n[bold green]Curation completed![/bold green]")
                console.print(f"Resolved events: {resolved_count}")
                
                if resolved_count > 0:
                    console.print(f"[dim]Use 'exo query --source audit.archived_events' to see archived events[/dim]")
                
    except Exception as e:
        console.print(f"[red]❌ Curation analysis failed: {e}[/red]")
        sys.exit(1)


@cli.command()
@click.option('--satellite', '-s', help='Satellite name to scan with (e.g., terminal, desktop, system, fs-watcher)')
@click.option('--all-satellites', is_flag=True, help='Scan with all available satellites')
@click.option('--since', help='Start time for historical scan (ISO format or relative like "1d")')
@click.option('--until', help='End time for historical scan (ISO format or relative like "1h")')
@click.option('--targets', multiple=True, help='Targets to scan (paths, filters, etc.)')
@click.option('--dry-run', is_flag=True, help='Show what would be scanned without actually doing it')
@click.option('--estimate', is_flag=True, help='Show scan estimation before execution')
@click.option('--interactive', is_flag=True, help='Enable interactive mode for decision making')
@click.option('--max-events', type=int, default=0, help='Maximum events to process (0 = unlimited)')
@click.option('--timeout', type=int, default=300, help='Timeout in seconds for each satellite scan')
@click.option('--parallel', is_flag=True, help='Run satellite scans in parallel')
@click.pass_context
def scan(ctx, satellite: Optional[str], all_satellites: bool, since: Optional[str], until: Optional[str], 
         targets: List[str], dry_run: bool, estimate: bool, interactive: bool, max_events: int, 
         timeout: int, parallel: bool):
    """Coordinate satellite scan operations for historical data processing.
    
    This command acts as a high-level coordinator that invokes satellite binaries
    directly with scan subcommands, supporting both single satellite and multi-satellite
    operations with progress tracking and error handling.
    
    Examples:
        exo scan --satellite terminal --since 2024-01-01 --until 2024-01-02
        exo scan --all-satellites --since 1d --dry-run
        exo scan --satellite fs-watcher --targets /path/to/logs --estimate
    """
    from datetime import datetime, timedelta
    import concurrent.futures
    import time
    import uuid
    
    if not satellite and not all_satellites:
        console.print("[red]Error: Must specify either --satellite or --all-satellites[/red]")
        sys.exit(1)
    
    if satellite and all_satellites:
        console.print("[red]Error: Cannot specify both --satellite and --all-satellites[/red]")
        sys.exit(1)
    
    # Available satellites with their binary names
    # Try to find satellites in the nix store first, then fall back to PATH
    def find_satellite_binary(name):
        # First try PATH
        path_binary = shutil.which(name)
        if path_binary:
            return path_binary
        
        # Then try target directory (for development)
        target_binary = f"./target/debug/{name}"
        if os.path.exists(target_binary):
            return target_binary
            
        # Finally try nix build output
        nix_binary = f"result/bin/{name}"
        if os.path.exists(nix_binary):
            return nix_binary
            
        return name  # Fallback to original name
    
    AVAILABLE_SATELLITES = {
        'terminal': find_satellite_binary('sinex-terminal-satellite'),
        'desktop': find_satellite_binary('sinex-desktop-satellite'), 
        'system': find_satellite_binary('sinex-system-satellite'),
        'fs-watcher': find_satellite_binary('sinex-fs-watcher')
    }
    
    # Determine which satellites to scan
    if all_satellites:
        satellites_to_scan = list(AVAILABLE_SATELLITES.keys())
    else:
        if satellite not in AVAILABLE_SATELLITES:
            console.print(f"[red]Error: Unknown satellite '{satellite}'. Available: {', '.join(AVAILABLE_SATELLITES.keys())}[/red]")
            sys.exit(1)
        satellites_to_scan = [satellite]
    
    # Log the operation (disabled for now - satellites don't have unified CLI yet)
    operation_id = None
    console.print(f"[blue]Note: Operations logging temporarily disabled pending satellite CLI unification[/blue]")
    
    # TODO: Re-enable when satellites implement StatefulStreamProcessor with scan subcommand
    # try:
    #     with get_db_connection() as conn:
    #         with conn.cursor() as cur:
    #             # Use ULID() PostgreSQL function to generate a proper ULID
    #             cur.execute("""
    #                 INSERT INTO core.operations_log (
    #                     operation_id, operation_type, status, invoked_by_user, parameters
    #                 ) VALUES (ulid(), %s, %s, %s, %s)
    #                 RETURNING operation_id
    #             """, (
    #                 'scan',
    #                 'started',
    #                 os.getenv('USER', 'unknown'),
    #                 json.dumps({
    #                     'satellites': satellites_to_scan,
    #                     'since': since,
    #                     'until': until,
    #                     'targets': list(targets),
    #                     'dry_run': dry_run,
    #                     'estimate': estimate,
    #                     'interactive': interactive,
    #                     'max_events': max_events,
    #                     'timeout': timeout,
    #                     'parallel': parallel
    #                 })
    #             ))
    #             
    #             # Get the generated operation_id
    #             row = cur.fetchone()
    #             operation_id = str(row[0]) if row else None
    #             conn.commit()
    # except Exception as e:
    #     console.print(f"[yellow]Warning: Could not log operation: {e}[/yellow]")
    
    start_time = time.time()
    
    def run_satellite_scan(satellite_name: str) -> Dict[str, Any]:
        """Run scan for a single satellite."""
        binary_name = AVAILABLE_SATELLITES[satellite_name]
        cmd = [binary_name, 'scan']
        
        # Add time range parameters
        if since:
            cmd.extend(['--from', f'timestamp:{since}'])
        if until:
            cmd.extend(['--until', until])
        
        # Add targets
        for target in targets:
            cmd.extend(['--targets', target])
        
        # Add flags
        if dry_run:
            cmd.append('--dry-run')
        if interactive:
            cmd.append('--interactive')
        if max_events > 0:
            cmd.extend(['--max-events', str(max_events)])
        if estimate:
            cmd.append('--estimate')
        
        console.print(f"[blue]Running: {' '.join(cmd)}[/blue]")
        
        try:
            result = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                timeout=timeout,
                cwd=os.getcwd()
            )
            
            return {
                'satellite': satellite_name,
                'success': result.returncode == 0,
                'returncode': result.returncode,
                'stdout': result.stdout,
                'stderr': result.stderr,
                'duration': time.time() - start_time
            }
        except subprocess.TimeoutExpired:
            return {
                'satellite': satellite_name,
                'success': False,
                'returncode': -1,
                'stdout': '',
                'stderr': f'Timeout after {timeout} seconds',
                'duration': timeout
            }
        except FileNotFoundError:
            return {
                'satellite': satellite_name,
                'success': False,
                'returncode': -1,
                'stdout': '',
                'stderr': f'Satellite binary {binary_name} not found',
                'duration': 0
            }
        except Exception as e:
            return {
                'satellite': satellite_name,
                'success': False,
                'returncode': -1,
                'stdout': '',
                'stderr': f'Error running scan: {e}',
                'duration': 0
            }
    
    # Execute scans
    console.print(f"[green]Starting scan coordination for {len(satellites_to_scan)} satellite(s)[/green]")
    
    results = []
    
    if parallel and len(satellites_to_scan) > 1:
        # Run satellites in parallel
        with concurrent.futures.ThreadPoolExecutor(max_workers=len(satellites_to_scan)) as executor:
            future_to_satellite = {
                executor.submit(run_satellite_scan, sat): sat 
                for sat in satellites_to_scan
            }
            
            for future in concurrent.futures.as_completed(future_to_satellite):
                satellite_name = future_to_satellite[future]
                try:
                    result = future.result()
                    results.append(result)
                except Exception as e:
                    results.append({
                        'satellite': satellite_name,
                        'success': False,
                        'returncode': -1,
                        'stdout': '',
                        'stderr': f'Exception in parallel execution: {e}',
                        'duration': 0
                    })
    else:
        # Run satellites sequentially
        for satellite_name in satellites_to_scan:
            with console.status(f"[bold green]Scanning with {satellite_name}..."):
                result = run_satellite_scan(satellite_name)
                results.append(result)
    
    # Display results
    console.print("\n" + "="*60)
    console.print("[bold]Scan Coordination Results[/bold]")
    console.print("="*60)
    
    total_duration = time.time() - start_time
    successful_scans = sum(1 for r in results if r['success'])
    failed_scans = len(results) - successful_scans
    
    console.print(f"[green]✅ Successful scans: {successful_scans}[/green]")
    console.print(f"[red]❌ Failed scans: {failed_scans}[/red]")
    console.print(f"[blue]⏱️  Total duration: {total_duration:.2f} seconds[/blue]")
    
    # Show detailed results
    for result in results:
        satellite = result['satellite']
        if result['success']:
            console.print(f"\n[bold green]✅ {satellite}[/bold green] (completed in {result['duration']:.2f}s)")
            if result['stdout'].strip():
                console.print("[dim]Output:[/dim]")
                console.print(result['stdout'])
        else:
            console.print(f"\n[bold red]❌ {satellite}[/bold red] (failed with code {result['returncode']})")
            if result['stderr'].strip():
                console.print("[dim]Error:[/dim]")
                console.print(f"[red]{result['stderr']}[/red]")
            if result['stdout'].strip():
                console.print("[dim]Output:[/dim]")
                console.print(result['stdout'])
    
    # Update operation log (disabled for now)
    # TODO: Re-enable when satellites implement unified CLI
    # if operation_id:
    #     try:
    #         with get_db_connection() as conn:
    #             with conn.cursor() as cur:
    #                 cur.execute("""
    #                     UPDATE core.operations_log 
    #                     SET status = %s, completed_at = NOW(), 
    #                         duration_ms = %s, summary = %s
    #                     WHERE operation_id = %s
    #                 """, (
    #                     'completed' if failed_scans == 0 else 'failed',
    #                     int(total_duration * 1000),
    #                     json.dumps({
    #                         'satellites_scanned': len(satellites_to_scan),
    #                         'successful_scans': successful_scans,
    #                         'failed_scans': failed_scans,
    #                         'results': results
    #                     }),
    #                     operation_id
    #                 ))
    #                 conn.commit()
    #     except Exception as e:
    #         console.print(f"[yellow]Warning: Could not update operation log: {e}[/yellow]")
    
    # Exit with error if any scans failed
    if failed_scans > 0:
        sys.exit(1)


@cli.group()
def telemetry():
    """Telemetry and metrics inspection commands."""
    pass


@telemetry.command('prometheus')
@click.option('--rpc-url', help='RPC server URL', envvar='SINEX_RPC_URL')
@click.option('--format', '-f', type=click.Choice(['text', 'json']), default='text', 
              help='Output format (text for Prometheus format, json for structured data)')
@click.pass_context
def telemetry_prometheus(ctx, rpc_url: Optional[str], format: str):
    """Export current Prometheus metrics."""
    try:
        client = get_rpc_client(rpc_url)
        
        # Call gateway RPC to get metrics
        response = client.call('telemetry.export_prometheus', {'format': format})
        
        if format == 'text':
            console.print(response['metrics'])
        else:
            # JSON format
            console.print(JSON.from_data(response['metrics'], indent=2))
            
    except SinexRPCError as e:
        console.print(f"[red]RPC Error: {e}[/red]")
        sys.exit(1)
    except Exception as e:
        console.print(f"[red]Error: {e}[/red]")
        sys.exit(1)


@telemetry.command('events')
@click.option('--component', '-c', help='Filter by component name')
@click.option('--event-type', '-t', help='Filter by telemetry event type')
@click.option('--since', '-s', help='Start time (ISO format or relative like "1h")')
@click.option('--limit', '-l', type=int, default=100, help='Maximum number of events to show')
@click.option('--format', '-f', type=click.Choice(['table', 'json', 'raw']), default='table',
              help='Output format')
@click.pass_context
def telemetry_events(ctx, component: Optional[str], event_type: Optional[str], 
                    since: Optional[str], limit: int, format: str):
    """Query telemetry events from the database."""
    use_db = ctx.obj.get('use_db', False)
    
    # Build query conditions
    conditions = ["source = 'sinex.telemetry'"]
    params = []
    
    if component:
        conditions.append("payload->>'component' = %s")
        params.append(component)
    
    if event_type:
        conditions.append("event_type = %s")
        params.append(event_type)
    
    if since:
        since_dt = parse_time_argument(since)
        conditions.append("ts_ingest >= %s")
        params.append(since_dt)
    
    # Query telemetry events
    if use_db:
        events = _query_telemetry_db(conditions, params, limit)
    else:
        # Use RPC
        try:
            client = get_rpc_client(ctx.obj.get('rpc_url'))
            response = client.call('telemetry.query_events', {
                'component': component,
                'event_type': event_type,
                'since': since,
                'limit': limit
            })
            events = response['events']
        except SinexRPCError as e:
            console.print(f"[red]RPC Error: {e}[/red]")
            sys.exit(1)
    
    if not events:
        console.print("[yellow]No telemetry events found.[/yellow]")
        return
    
    # Display results
    if format == 'json':
        console.print(JSON.from_data(events, indent=2))
    elif format == 'raw':
        for event in events:
            console.print(event)
    else:
        # Table format
        display_telemetry_table(events)


@telemetry.command('summary')
@click.option('--component', '-c', help='Filter by component name')
@click.option('--period', '-p', type=click.Choice(['1h', '24h', '7d', '30d']), default='24h',
              help='Time period for summary')
@click.option('--rpc-url', help='RPC server URL', envvar='SINEX_RPC_URL')
@click.pass_context
def telemetry_summary(ctx, component: Optional[str], period: str, rpc_url: Optional[str]):
    """Show telemetry summary statistics."""
    try:
        client = get_rpc_client(rpc_url)
        
        response = client.call('telemetry.summary', {
            'component': component,
            'period': period
        })
        
        summary = response['summary']
        
        # Display summary
        console.print(f"\n[bold]Telemetry Summary - Last {period}[/bold]")
        
        if component:
            console.print(f"Component: [cyan]{component}[/cyan]\n")
        
        # Event throughput
        if 'event_throughput' in summary:
            console.print("[bold]Event Throughput:[/bold]")
            table = Table(box=box.SIMPLE)
            table.add_column("Event Type", style="cyan")
            table.add_column("Count", justify="right")
            table.add_column("Rate/min", justify="right")
            
            for event_type, stats in summary['event_throughput'].items():
                table.add_row(
                    event_type,
                    f"{stats['count']:,}",
                    f"{stats['rate_per_minute']:.2f}"
                )
            console.print(table)
            console.print()
        
        # Performance metrics
        if 'performance' in summary:
            console.print("[bold]Performance Metrics:[/bold]")
            table = Table(box=box.SIMPLE)
            table.add_column("Operation", style="cyan")
            table.add_column("Count", justify="right")
            table.add_column("P50 (ms)", justify="right")
            table.add_column("P95 (ms)", justify="right")
            table.add_column("P99 (ms)", justify="right")
            
            for op, stats in summary['performance'].items():
                table.add_row(
                    op,
                    f"{stats['count']:,}",
                    f"{stats['p50']:.1f}",
                    f"{stats['p95']:.1f}",
                    f"{stats['p99']:.1f}"
                )
            console.print(table)
            console.print()
        
        # Resource usage
        if 'resources' in summary:
            console.print("[bold]Resource Usage:[/bold]")
            table = Table(box=box.SIMPLE)
            table.add_column("Component", style="cyan")
            table.add_column("Avg Memory (MB)", justify="right")
            table.add_column("Peak Memory (MB)", justify="right")
            table.add_column("Avg CPU %", justify="right")
            table.add_column("Peak CPU %", justify="right")
            
            for comp, stats in summary['resources'].items():
                table.add_row(
                    comp,
                    f"{stats['avg_memory_mb']:.1f}",
                    f"{stats['peak_memory_mb']:.1f}",
                    f"{stats['avg_cpu_percent']:.1f}",
                    f"{stats['peak_cpu_percent']:.1f}"
                )
            console.print(table)
            console.print()
        
        # Error summary
        if 'errors' in summary and summary['errors']:
            console.print("[bold]Error Summary:[/bold]")
            table = Table(box=box.SIMPLE)
            table.add_column("Error Type", style="red")
            table.add_column("Count", justify="right")
            table.add_column("Component", style="cyan")
            
            for error in summary['errors']:
                table.add_row(
                    error['type'],
                    f"{error['count']:,}",
                    error.get('component', 'unknown')
                )
            console.print(table)
            
    except SinexRPCError as e:
        console.print(f"[red]RPC Error: {e}[/red]")
        sys.exit(1)
    except Exception as e:
        console.print(f"[red]Error: {e}[/red]")
        sys.exit(1)


def _query_telemetry_db(conditions: List[str], params: List[Any], limit: int) -> List[Dict]:
    """Query telemetry events from database."""
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            where_clause = " AND ".join(conditions)
            query = f"""
                SELECT event_id, event_type, ts_ingest, payload
                FROM core.events
                WHERE {where_clause}
                ORDER BY ts_ingest DESC
                LIMIT %s
            """
            params.append(limit)
            
            cur.execute(query, params)
            rows = cur.fetchall()
            
            return [dict(row) for row in rows]


def display_telemetry_table(events: List[Dict]):
    """Display telemetry events in a table format."""
    table = Table(title="Telemetry Events", box=box.ROUNDED)
    table.add_column("Time", style="green")
    table.add_column("Component", style="cyan")
    table.add_column("Type", style="magenta")
    table.add_column("Details", style="white")
    
    for event in events:
        payload = event['payload']
        component = payload.get('component', 'unknown')
        event_type = event['event_type']
        
        # Format details based on event type
        details = ""
        if event_type == 'events.processed':
            count = payload.get('count', 0)
            by_type = payload.get('by_type', {})
            details = f"Count: {count}, Types: {len(by_type)}"
        elif event_type == 'operation.performance':
            operation = payload.get('operation', 'unknown')
            p50 = payload.get('duration_ms', {}).get('p50', 0)
            details = f"Op: {operation}, P50: {p50:.1f}ms"
        elif event_type == 'resource.usage':
            mem = payload.get('memory_mb', {}).get('current', 0)
            cpu = payload.get('cpu_percent', {}).get('avg', 0)
            details = f"Mem: {mem:.1f}MB, CPU: {cpu:.1f}%"
        elif event_type == 'errors.summary':
            total = payload.get('total_errors', 0)
            details = f"Total errors: {total}"
        
        table.add_row(
            event['ts_ingest'].strftime('%Y-%m-%d %H:%M:%S'),
            component,
            event_type,
            details
        )
    
    console.print(table)


@cli.group()
def completion():
    """Shell completion management."""
    pass


@completion.command('install')
@click.argument('shell', type=click.Choice(['bash', 'zsh', 'fish']))
@click.option('--completion-dir', help='Custom completion directory')
def completion_install(shell: str, completion_dir: Optional[str]):
    """Install shell completion for the specified shell."""
    try:
        from .completion import install_completion
        if install_completion(shell, completion_dir):
            console.print(f"[green]✅ {shell.title()} completion installed successfully![/green]")
        else:
            console.print(f"[red]❌ Failed to install {shell} completion[/red]")
            sys.exit(1)
    except ImportError:
        console.print("[red]Completion module not available[/red]")
        sys.exit(1)


@completion.command('generate')
@click.argument('shell', type=click.Choice(['bash', 'zsh', 'fish']))
def completion_generate(shell: str):
    """Generate completion script for the specified shell."""
    try:
        from .completion import generate_bash_completion, generate_zsh_completion, generate_fish_completion
        
        if shell == 'bash':
            content = generate_bash_completion()
        elif shell == 'zsh':
            content = generate_zsh_completion()
        elif shell == 'fish':
            content = generate_fish_completion()
        
        click.echo(content)
    except ImportError:
        console.print("[red]Completion module not available[/red]")
        sys.exit(1)


if __name__ == '__main__':
    try:
        cli()
    except Exception as e:
        console.print(f"[red]Error: {e}[/red]")
        sys.exit(1)