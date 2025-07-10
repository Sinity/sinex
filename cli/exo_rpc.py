#!/usr/bin/env python3
"""
Sinex CLI - Query your digital memory (JSON-RPC version)
"""

import os
import sys
import json
import requests
from datetime import datetime, timedelta
from typing import Optional, List, Dict, Any
from pathlib import Path

import click
from rich.console import Console
from rich.table import Table
from rich.json import JSON
from rich.text import Text
from rich.panel import Panel
from rich import box

console = Console()


class SinexRPCClient:
    """JSON-RPC client for communicating with sinex-host"""
    
    def __init__(self, url: str = "http://127.0.0.1:9999/rpc"):
        self.url = url
        self.request_id = 0
    
    def call(self, method: str, params: Dict[str, Any] = None) -> Any:
        """Make a JSON-RPC call"""
        self.request_id += 1
        
        payload = {
            "jsonrpc": "2.0",
            "method": method,
            "params": params or {},
            "id": self.request_id
        }
        
        try:
            response = requests.post(self.url, json=payload)
            response.raise_for_status()
            
            result = response.json()
            if "error" in result:
                console.print(f"[red]RPC Error: {result['error']['message']}[/red]")
                sys.exit(1)
            
            return result.get("result")
        except requests.exceptions.ConnectionError:
            console.print("[red]Error: Cannot connect to sinex-host. Is it running?[/red]")
            console.print("Start it with: sinex-host rpc-server")
            sys.exit(1)
        except Exception as e:
            console.print(f"[red]Error: {e}[/red]")
            sys.exit(1)


# Global RPC client
rpc = SinexRPCClient()


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


def format_event(event: dict, show_content: bool = True) -> Panel:
    """Format an event for display."""
    title = f"[bold cyan]{event.get('source', 'unknown')}[/bold cyan] :: [yellow]{event.get('event_type', 'unknown')}[/yellow]"
    
    content_parts = []
    
    # Timestamp and ID
    timestamp = event.get('timestamp', 'unknown')
    if isinstance(timestamp, str):
        try:
            dt = datetime.fromisoformat(timestamp.replace('Z', '+00:00'))
            timestamp = dt.strftime('%Y-%m-%d %H:%M:%S')
        except:
            pass
    
    content_parts.append(f"[dim]Time:[/dim] {timestamp}")
    content_parts.append(f"[dim]ID:[/dim] {event.get('event_id', 'unknown')}")
    
    # Score if present
    if 'score' in event:
        content_parts.append(f"[dim]Score:[/dim] {event['score']:.2f}")
    
    # Payload/snippet
    if show_content:
        if 'snippet' in event:
            content_parts.append("")
            content_parts.append("[dim]Content:[/dim]")
            content_parts.append(event['snippet'])
        elif 'payload' in event:
            content_parts.append("")
            content_parts.append("[dim]Payload:[/dim]")
            content_parts.append(str(JSON(json.dumps(event['payload'], indent=2))))
    
    content = "\n".join(content_parts)
    
    return Panel(
        content,
        title=title,
        title_align="left",
        border_style="blue",
        box=box.ROUNDED
    )


@click.group()
def cli():
    """Sinex CLI - Query your digital memory"""
    pass


@cli.command()
@click.option('--source', '-s', help='Filter by event source')
@click.option('--type', '-t', 'event_type', help='Filter by event type')
@click.option('--since', help='Time range (e.g., 1h, 30m, 2d)')
@click.option('--limit', '-n', default=10, help='Number of results')
@click.option('--text', '-q', help='Search text in payloads')
@click.option('--json', 'output_json', is_flag=True, help='Output as JSON')
def query(source, event_type, since, limit, text, output_json):
    """Query recent events."""
    params = {
        "sources": [source] if source else [],
        "event_types": [event_type] if event_type else [],
        "limit": limit,
        "offset": 0
    }
    
    if text:
        params["text"] = text
    
    if since:
        try:
            delta = parse_time_delta(since)
            start_time = datetime.utcnow() - delta
            params["start_time"] = start_time.isoformat() + "Z"
        except ValueError as e:
            console.print(f"[red]Error parsing time: {e}[/red]")
            return
    
    results = rpc.call("search.search_events", params)
    
    if output_json:
        print(json.dumps(results, indent=2))
    else:
        if not results:
            console.print("[yellow]No events found[/yellow]")
            return
        
        for event in results:
            console.print(format_event(event))
            console.print()


@cli.command()
@click.argument('event_id')
@click.argument('content')
@click.option('--tags', '-t', multiple=True, help='Tags for the note')
def note(event_id, content, tags):
    """Add a note to an event."""
    params = {
        "event_id": event_id,
        "content": content,
        "tags": list(tags),
        "created_by": os.environ.get('USER', 'unknown')
    }
    
    result = rpc.call("pkm.create_note", params)
    console.print(f"[green]Note created:[/green] {result['annotation_id']}")


@cli.command()
@click.option('--days-back', '-d', default=7, help='Days to look back')
@click.option('--top', '-n', default=10, help='Number of top sources')
def activity(days_back, top):
    """Show activity summary."""
    # Get event counts by source
    counts = rpc.call("analytics.event_count_by_source", {"days_back": days_back})
    
    # Create table
    table = Table(title=f"Activity Summary (Last {days_back} Days)")
    table.add_column("Source", style="cyan")
    table.add_column("Events", justify="right", style="green")
    
    # Sort by count and limit
    sorted_counts = sorted(counts.items(), key=lambda x: x[1], reverse=True)[:top]
    
    for source, count in sorted_counts:
        table.add_row(source, str(count))
    
    console.print(table)


@cli.command()
@click.option('--bucket-minutes', '-b', default=60, help='Bucket size in minutes')
@click.option('--limit', '-n', default=24, help='Number of buckets')
def heatmap(bucket_minutes, limit):
    """Show activity heatmap."""
    results = rpc.call("analytics.activity_heatmap", {
        "bucket_size_minutes": bucket_minutes,
        "limit": limit
    })
    
    if not results:
        console.print("[yellow]No activity data[/yellow]")
        return
    
    # Create table
    table = Table(title=f"Activity Heatmap ({bucket_minutes}min buckets)")
    table.add_column("Time", style="cyan")
    table.add_column("Events", justify="right", style="green")
    table.add_column("Graph", style="blue")
    
    # Find max count for scaling
    max_count = max(r[1] for r in results) if results else 1
    
    for timestamp, count in results:
        # Parse timestamp
        dt = datetime.fromisoformat(timestamp.replace('Z', '+00:00'))
        time_str = dt.strftime('%Y-%m-%d %H:%M')
        
        # Create bar graph
        bar_width = int((count / max_count) * 40)
        bar = "█" * bar_width
        
        table.add_row(time_str, str(count), bar)
    
    console.print(table)


@cli.command()
@click.argument('content')
@click.option('--filename', '-f', default='content.txt', help='Filename for the blob')
@click.option('--type', '-t', 'content_type', default='text/plain', help='MIME type')
@click.option('--source', '-s', default='cli', help='Source identifier')
def store(content, filename, content_type, source):
    """Store content as a blob."""
    params = {
        "content": content,
        "filename": filename,
        "content_type": content_type,
        "source": source
    }
    
    result = rpc.call("content.store_blob", params)
    console.print(f"[green]Content stored:[/green] {result['annex_key']}")


@cli.command()
@click.argument('annex_key')
def retrieve(annex_key):
    """Retrieve content by annex key."""
    result = rpc.call("content.retrieve_blob", {"annex_key": annex_key})
    print(result['content'])


@cli.group()
def pkm():
    """Personal Knowledge Management commands."""
    pass


@pkm.command()
@click.argument('event_id')
@click.option('--entity', '-e', multiple=True, nargs=2, metavar='NAME TYPE', 
              help='Entity to create (name and type)')
def entities(event_id, entity):
    """Create entities from an event."""
    if not entity:
        console.print("[red]At least one entity must be specified[/red]")
        return
    
    entities_list = [(name, etype) for name, etype in entity]
    
    params = {
        "event_id": event_id,
        "entities": [{"name": name, "type": etype} for name, etype in entities_list]
    }
    
    result = rpc.call("pkm.create_entities_from_list", params)
    
    console.print(f"[green]Created {len(result['entity_ids'])} entities:[/green]")
    for entity_id in result['entity_ids']:
        console.print(f"  - {entity_id}")


@pkm.command()
@click.argument('from_entity')
@click.argument('to_entity')
@click.argument('relationship')
@click.option('--property', '-p', 'properties', multiple=True, nargs=2,
              metavar='KEY VALUE', help='Relationship properties')
def link(from_entity, to_entity, relationship, properties):
    """Link two entities."""
    props_dict = {k: v for k, v in properties}
    
    params = {
        "from_entity_id": from_entity,
        "to_entity_id": to_entity,
        "relationship_type": relationship,
        "properties": props_dict
    }
    
    result = rpc.call("pkm.link_entities", params)
    console.print(f"[green]Relation created:[/green] {result['relation_id']}")


@cli.command()
def status():
    """Check connection to sinex-host."""
    try:
        # Try a simple query
        rpc.call("analytics.event_count_by_source", {"days_back": 1})
        console.print("[green]✓ Connected to sinex-host[/green]")
    except SystemExit:
        # Already handled by the RPC client
        pass


if __name__ == '__main__':
    cli()