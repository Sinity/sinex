#!/usr/bin/env python3
"""
Sinex CLI - Query your digital memory (Phase 2 Enhanced)
"""

import os
import sys
import json
import subprocess
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
    # Debug: print the URL being used
    # print(f"Using DB URL: {db_url}")
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
    """Query events using direct database connection (legacy mode)."""
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            # Build query using new schema
            query_parts = [
                "SELECT id, source, event_type, ts_ingest, ts_orig, host, "
                "ingestor_version, payload_schema_id, payload FROM raw.events"
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
                FROM raw.events
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
            cur.execute("SELECT COUNT(*) as total FROM raw.events")
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
                FROM raw.events
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
                LEFT JOIN raw.events e ON e.payload_schema_id = s.id
                WHERE s.is_active = true
                GROUP BY s.event_source, s.event_type, s.schema_version
                ORDER BY usage_count DESC
                LIMIT 10
            """)
            schema_usage = cur.fetchall()
            
            # Agent health
            cur.execute("""
                SELECT 
                    payload->>'agent_name' as agent_name,
                    payload->>'status' as status,
                    MAX(ts_ingest) as last_heartbeat
                FROM raw.events
                WHERE source = 'sinex' AND event_type = 'agent.heartbeat'
                GROUP BY payload->>'agent_name', payload->>'status'
                ORDER BY last_heartbeat DESC
            """)
            agent_health = cur.fetchall()
    
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
def agent():
    """Agent introspection commands."""
    pass


@agent.command('list')
@click.option('--status', '-s', help='Filter by status (development, stable, deprecated)')
def agent_list(status: Optional[str]):
    """List all registered agents."""
    
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            query_parts = [
                "SELECT agent_name, description, version, status, "
                "produces_event_types, last_heartbeat_ts, registered_at "
                "FROM sinex_schemas.agent_manifests"
            ]
            params = []
            
            if status:
                query_parts.append("WHERE status = %s")
                params.append(status)
            
            query_parts.append("ORDER BY agent_name")
            
            query_sql = " ".join(query_parts)
            cur.execute(query_sql, params)
            agents = cur.fetchall()
    
    if not agents:
        console.print("[yellow]No agents found.[/yellow]")
        return
    
    table = Table(title="Registered Agents")
    table.add_column("Agent", style="cyan")
    table.add_column("Version", style="green")
    table.add_column("Status", style="yellow")
    table.add_column("Last Heartbeat", style="red")
    table.add_column("Description", style="white")
    
    for agent in agents:
        last_heartbeat = agent['last_heartbeat_ts']
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
            agent['agent_name'],
            agent['version'],
            agent['status'],
            Text(heartbeat_text, style=heartbeat_style),
            agent['description'] or ""
        )
    
    console.print(table)


@agent.command('status')
@click.argument('agent_name')
def agent_status(agent_name: str):
    """Show detailed status for a specific agent."""
    
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            # Get agent manifest
            cur.execute("""
                SELECT * FROM sinex_schemas.agent_manifests
                WHERE agent_name = %s
            """, (agent_name,))
            agent = cur.fetchone()
            
            if not agent:
                console.print(f"[red]Agent not found: {agent_name}[/red]")
                return
            
            # Get recent heartbeats
            cur.execute("""
                SELECT payload, ts_ingest FROM raw.events
                WHERE source = 'sinex' AND event_type = 'agent.heartbeat'
                AND payload->>'agent_name' = %s
                ORDER BY ts_ingest DESC
                LIMIT 5
            """, (agent_name,))
            heartbeats = cur.fetchall()
            
            # Get recent errors
            cur.execute("""
                SELECT payload, ts_ingest FROM raw.events
                WHERE source = 'sinex' AND event_type = 'agent.error'
                AND payload->>'agent_name' = %s
                ORDER BY ts_ingest DESC
                LIMIT 10
            """, (agent_name,))
            errors = cur.fetchall()
            
            # Get DLQ count from actual DLQ table
            cur.execute("""
                SELECT 
                    COUNT(*) as total_dlq,
                    COUNT(*) FILTER (WHERE resolved_at IS NULL) as pending_dlq,
                    COUNT(*) FILTER (WHERE resolved_at IS NOT NULL) as resolved_dlq
                FROM sinex_schemas.dlq_events
                WHERE agent_name = %s
            """, (agent_name,))
            dlq_counts = cur.fetchone()
    
    # Display agent information
    panel_content = []
    panel_content.append(f"[bold]Agent:[/bold] {agent['agent_name']}")
    panel_content.append(f"[bold]Version:[/bold] {agent['version']}")
    panel_content.append(f"[bold]Status:[/bold] {agent['status']}")
    panel_content.append(f"[bold]Description:[/bold] {agent['description'] or 'N/A'}")
    panel_content.append(f"[bold]Registered:[/bold] {agent['registered_at']}")
    panel_content.append(f"[bold]DLQ Total:[/bold] {dlq_counts['total_dlq']}")
    panel_content.append(f"[bold]DLQ Pending:[/bold] {dlq_counts['pending_dlq']}")
    panel_content.append(f"[bold]DLQ Resolved:[/bold] {dlq_counts['resolved_dlq']}")
    
    console.print(Panel("\n".join(panel_content), title=f"Agent Status: {agent_name}"))
    
    # Display event types produced
    if agent['produces_event_types']:
        console.print("\n[bold]Produces Event Types:[/bold]")
        produces = agent['produces_event_types']
        for source, types in produces.items():
            console.print(f"  [cyan]{source}:[/cyan] {', '.join(types)}")
    
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
        if event_type == "agent.heartbeat":
            agent = payload.get('agent_name', 'unknown')
            status = payload.get('status', 'unknown')
            return f"{agent}: {status}"
        elif event_type == "agent.error":
            agent = payload.get('agent_name', 'unknown')
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
                WHERE id LIKE %s OR id = %s
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
                "SELECT dlq_id, agent_name, source, event_type, failure_reason, "
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
                conditions.append("agent_name = %s")
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
        table.add_column("Agent", style="green")
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
                entry['agent_name'],
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
    panel_content.append(f"[bold]Agent:[/bold] {entry['agent_name']}")
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
    console.print(f"Agent: {entry['agent_name']}")
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
    console.print(f"Agent: {entry['agent_name']}")
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
                where_clause += " AND agent_name = %s"
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
                    agent_name,
                    COUNT(*) as total,
                    COUNT(*) FILTER (WHERE resolved_at IS NULL) as pending,
                    AVG(retry_count) as avg_retries
                FROM sinex_schemas.dlq_events
                {where_clause}
                GROUP BY agent_name
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
        console.print(f"\n[bold]By Agent:[/bold]")
        agent_table = Table()
        agent_table.add_column("Agent", style="cyan")
        agent_table.add_column("Total", justify="right", style="white")
        agent_table.add_column("Pending", justify="right", style="red")
        agent_table.add_column("Avg Retries", justify="right", style="yellow")
        
        for row in by_agent:
            agent_table.add_row(
                row['agent_name'],
                f"{row['total']:,}",
                f"{row['pending']:,}",
                f"{row['avg_retries']:.1f}" if row['avg_retries'] else "0"
            )
        
        console.print(agent_table)
    
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