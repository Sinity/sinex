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

console = Console()


def get_db_connection():
    """Get database connection using environment variable or default."""
    db_url = os.environ.get('DATABASE_URL', 'postgresql://localhost/sinex')
    return psycopg2.connect(db_url, cursor_factory=RealDictCursor)


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
def cli():
    """Sinex CLI - Query your digital memory."""
    pass


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
def query(source: Optional[str], event_type: Optional[str], since: Optional[str], 
          until: Optional[str], last: Optional[str], limit: int, host: Optional[str],
          payload_jq: Optional[str], output_format: str):
    """Enhanced query for events from the sinex database."""
    
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
                params.append(datetime.utcnow() - time_delta)
            
            if conditions:
                query_parts.append("WHERE " + " AND ".join(conditions))
            
            query_parts.append("ORDER BY ts_ingest DESC")
            query_parts.append(f"LIMIT {limit}")
            
            query_sql = " ".join(query_parts)
            
            cur.execute(query_sql, params)
            events = cur.fetchall()
    
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
                "produces_event_types, last_seen_heartbeat, registered_at "
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
        last_heartbeat = agent['last_seen_heartbeat']
        if last_heartbeat:
            # Check if heartbeat is recent (within 5 minutes)
            age = datetime.utcnow() - last_heartbeat.replace(tzinfo=None)
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
            
            # Get DLQ count
            cur.execute("""
                SELECT COUNT(*) as dlq_count FROM raw.events
                WHERE source = 'sinex' AND event_type = 'agent.dlq_event_written'
                AND payload->>'agent_name' = %s
            """, (agent_name,))
            dlq_count = cur.fetchone()['dlq_count']
    
    # Display agent information
    panel_content = []
    panel_content.append(f"[bold]Agent:[/bold] {agent['agent_name']}")
    panel_content.append(f"[bold]Version:[/bold] {agent['version']}")
    panel_content.append(f"[bold]Status:[/bold] {agent['status']}")
    panel_content.append(f"[bold]Description:[/bold] {agent['description'] or 'N/A'}")
    panel_content.append(f"[bold]Registered:[/bold] {agent['registered_at']}")
    panel_content.append(f"[bold]DLQ Events:[/bold] {dlq_count}")
    
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
def sources():
    """List all event sources with enhanced statistics."""
    
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
    
    table = Table(title="Event Sources")
    table.add_column("Source", style="cyan")
    table.add_column("Events", justify="right", style="green")
    table.add_column("Types", justify="right", style="yellow")
    table.add_column("Hosts", justify="right", style="blue")
    table.add_column("First Event", style="dim")
    table.add_column("Last Event", style="dim")
    table.add_column("Avg Delay", justify="right", style="magenta")
    
    for source in sources:
        delay = source['avg_ingest_delay']
        delay_str = f"{delay:.2f}s" if delay else "N/A"
        
        table.add_row(
            source['source'],
            f"{source['event_count']:,}",
            str(source['event_type_count']),
            str(source['host_count']),
            source['first_event'].strftime('%Y-%m-%d'),
            source['last_event'].strftime('%Y-%m-%d'),
            delay_str
        )
    
    console.print(table)


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


@cli.command()
def stats():
    """Show enhanced database statistics."""
    
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            # Total events
            cur.execute("SELECT COUNT(*) as total FROM raw.events")
            total = cur.fetchone()['total']
            
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
                age = datetime.utcnow() - last_hb.replace(tzinfo=None)
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


if __name__ == '__main__':
    try:
        cli()
    except Exception as e:
        console.print(f"[red]Error: {e}[/red]")
        sys.exit(1)