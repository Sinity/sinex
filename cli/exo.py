#!/usr/bin/env python3
"""
Sinnix Exocortex CLI - Query your digital memory
"""

import os
import sys
import json
from datetime import datetime, timedelta
from typing import Optional, List, Dict, Any

import click
import psycopg2
from psycopg2.extras import RealDictCursor
from rich.console import Console
from rich.table import Table
from rich.json import JSON

console = Console()


def get_db_connection():
    """Get database connection using environment variable or default."""
    db_url = os.environ.get('DATABASE_URL', 'postgresql://localhost/exocortex')
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


@click.group()
def cli():
    """Sinnix Exocortex CLI - Query your digital memory."""
    pass


@cli.command()
@click.option('--source', '-s', help='Filter by event source (e.g., hyprland)')
@click.option('--last', '-l', help='Show events from last N time (e.g., 1h, 30m, 2d)')
@click.option('--limit', '-n', default=50, help='Maximum number of events to show')
@click.option('--json', 'output_json', is_flag=True, help='Output as JSON')
def query(source: Optional[str], last: Optional[str], limit: int, output_json: bool):
    """Query recent events from the exocortex."""
    
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            # Build query
            query_parts = ["SELECT * FROM raw.events"]
            conditions = []
            params = []
            
            if source:
                conditions.append("source = %s")
                params.append(source)
            
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
    
    if output_json:
        # Convert datetime objects to ISO format for JSON serialization
        for event in events:
            event['ts_ingest'] = event['ts_ingest'].isoformat()
            event['id'] = str(event['id'])
        click.echo(json.dumps(events, indent=2))
    else:
        display_events(events)


@cli.command()
def sources():
    """List all event sources in the database."""
    
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT source, COUNT(*) as event_count,
                       MIN(ts_ingest) as first_event,
                       MAX(ts_ingest) as last_event
                FROM raw.events
                GROUP BY source
                ORDER BY event_count DESC
            """)
            sources = cur.fetchall()
    
    table = Table(title="Event Sources")
    table.add_column("Source", style="cyan")
    table.add_column("Event Count", justify="right", style="green")
    table.add_column("First Event", style="yellow")
    table.add_column("Last Event", style="yellow")
    
    for source in sources:
        table.add_row(
            source['source'],
            str(source['event_count']),
            source['first_event'].strftime('%Y-%m-%d %H:%M:%S'),
            source['last_event'].strftime('%Y-%m-%d %H:%M:%S')
        )
    
    console.print(table)


@cli.command()
def stats():
    """Show database statistics."""
    
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            # Total events
            cur.execute("SELECT COUNT(*) as total FROM raw.events")
            total = cur.fetchone()['total']
            
            # Events by day
            cur.execute("""
                SELECT DATE(ts_ingest) as day, COUNT(*) as count
                FROM raw.events
                WHERE ts_ingest > NOW() - INTERVAL '7 days'
                GROUP BY day
                ORDER BY day DESC
            """)
            daily_counts = cur.fetchall()
    
    console.print(f"\n[bold]Total Events:[/bold] {total:,}")
    
    console.print("\n[bold]Events per day (last 7 days):[/bold]")
    for day in daily_counts:
        bar = "█" * min(50, day['count'] // 100)
        console.print(f"{day['day']}: {bar} {day['count']:,}")


def display_events(events: List[Dict[str, Any]]):
    """Display events in a formatted table."""
    if not events:
        console.print("[yellow]No events found.[/yellow]")
        return
    
    table = Table(title=f"Recent Events ({len(events)} shown)")
    table.add_column("Time", style="cyan")
    table.add_column("Source", style="green")
    table.add_column("Type", style="yellow")
    table.add_column("Summary", style="white")
    
    for event in events:
        payload = event['payload']
        event_type = payload.get('type', 'unknown')
        
        # Extract summary based on event type
        summary = ""
        if event_type == "workspace_change":
            summary = f"Workspace {payload.get('data', {}).get('id', 'unknown')}"
        elif event_type == "window_change":
            data = payload.get('data', {})
            summary = f"{data.get('class', 'unknown')} - {data.get('title', 'unknown')[:50]}"
        else:
            summary = str(payload.get('data', ''))[:50]
        
        table.add_row(
            event['ts_ingest'].strftime('%H:%M:%S'),
            event['source'],
            event_type,
            summary
        )
    
    console.print(table)


if __name__ == '__main__':
    try:
        cli()
    except Exception as e:
        console.print(f"[red]Error: {e}[/red]")
        sys.exit(1)