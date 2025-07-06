#!/usr/bin/env python3
"""
Interactive Query Builder for Sinex CLI
Uses fzf for fuzzy finding and interactive selection
"""

import os
import sys
import json
import subprocess
import tempfile
from pathlib import Path
from typing import List, Optional, Dict, Any, Tuple

import psycopg2
from psycopg2.extras import RealDictCursor
from rich.console import Console

console = Console()


def get_db_connection():
    """Get database connection using environment variable or default."""
    db_url = os.environ.get('DATABASE_URL', 'postgresql://localhost/sinex')
    return psycopg2.connect(db_url, cursor_factory=RealDictCursor)


def check_fzf_available() -> bool:
    """Check if fzf is available on the system."""
    try:
        subprocess.run(['fzf', '--version'], capture_output=True, check=True)
        return True
    except (subprocess.CalledProcessError, FileNotFoundError):
        return False


def fzf_select(items: List[str], prompt: str = "Select: ", multi: bool = False, 
               preview_command: Optional[str] = None) -> Optional[List[str]]:
    """Use fzf to select from a list of items."""
    if not items:
        return None
    
    cmd = ['fzf', '--prompt', prompt, '--height', '40%', '--border']
    
    if multi:
        cmd.append('--multi')
    
    if preview_command:
        cmd.extend(['--preview', preview_command])
    
    try:
        # Write items to stdin
        process = subprocess.Popen(
            cmd,
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True
        )
        
        stdout, stderr = process.communicate('\n'.join(items))
        
        if process.returncode == 0:
            selected = stdout.strip().split('\n') if stdout.strip() else []
            return [item for item in selected if item]  # Filter empty strings
        else:
            return None  # User cancelled or error
            
    except Exception as e:
        console.print(f"[red]Error running fzf: {e}[/red]")
        return None


def get_sources_with_counts() -> List[Tuple[str, int]]:
    """Get event sources with event counts."""
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT source, COUNT(*) as count
                    FROM raw.events
                    GROUP BY source
                    ORDER BY count DESC
                """)
                return [(row['source'], row['count']) for row in cur.fetchall()]
    except Exception:
        return []


def get_event_types_with_counts(source: Optional[str] = None) -> List[Tuple[str, int]]:
    """Get event types with counts, optionally filtered by source."""
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                if source:
                    cur.execute("""
                        SELECT event_type, COUNT(*) as count
                        FROM raw.events
                        WHERE source = %s
                        GROUP BY event_type
                        ORDER BY count DESC
                    """, (source,))
                else:
                    cur.execute("""
                        SELECT event_type, COUNT(*) as count
                        FROM raw.events
                        GROUP BY event_type
                        ORDER BY count DESC
                    """)
                return [(row['event_type'], row['count']) for row in cur.fetchall()]
    except Exception:
        return []


def get_hosts_with_counts() -> List[Tuple[str, int]]:
    """Get hosts with event counts."""
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT host, COUNT(*) as count
                    FROM raw.events
                    WHERE host IS NOT NULL
                    GROUP BY host
                    ORDER BY count DESC
                """)
                return [(row['host'], row['count']) for row in cur.fetchall()]
    except Exception:
        return []


def get_recent_time_ranges() -> List[str]:
    """Get common time range options."""
    return [
        '5m - Last 5 minutes',
        '15m - Last 15 minutes',
        '30m - Last 30 minutes',
        '1h - Last hour',
        '2h - Last 2 hours',
        '6h - Last 6 hours',
        '12h - Last 12 hours',
        '1d - Last day',
        '2d - Last 2 days',
        '1w - Last week',
        '2w - Last 2 weeks',
        '1m - Last month'
    ]


def get_sample_events(source: Optional[str] = None, event_type: Optional[str] = None, 
                     limit: int = 5) -> List[Dict]:
    """Get sample events for preview."""
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                conditions = []
                params = []
                
                if source:
                    conditions.append("source = %s")
                    params.append(source)
                
                if event_type:
                    conditions.append("event_type = %s")
                    params.append(event_type)
                
                where_clause = "WHERE " + " AND ".join(conditions) if conditions else ""
                
                cur.execute(f"""
                    SELECT source, event_type, ts_ingest, payload
                    FROM raw.events
                    {where_clause}
                    ORDER BY ts_ingest DESC
                    LIMIT %s
                """, params + [limit])
                
                return [dict(row) for row in cur.fetchall()]
    except Exception:
        return []


def format_event_preview(events: List[Dict]) -> str:
    """Format events for preview display."""
    if not events:
        return "No events found"
    
    lines = []
    lines.append(f"Sample Events ({len(events)} shown):")
    lines.append("-" * 50)
    
    for event in events:
        timestamp = event['ts_ingest'].strftime('%H:%M:%S')
        source = event['source']
        event_type = event['event_type']
        
        # Extract a summary from the payload
        payload = event['payload']
        if isinstance(payload, dict):
            # Try to find meaningful fields
            summary_fields = ['message', 'command', 'path', 'title', 'description']
            summary = None
            for field in summary_fields:
                if field in payload:
                    summary = str(payload[field])[:40]
                    break
            if not summary:
                summary = str(payload)[:40]
        else:
            summary = str(payload)[:40]
        
        lines.append(f"{timestamp} {source}.{event_type}: {summary}")
    
    return '\n'.join(lines)


def create_preview_script() -> str:
    """Create a temporary script for fzf preview."""
    script_content = '''#!/bin/bash
source_and_type="$1"
source=$(echo "$source_and_type" | cut -d' ' -f1)
event_type=$(echo "$source_and_type" | cut -d' ' -f2)

python3 -c "
import sys
sys.path.append('$(pwd)')
from cli.interactive import get_sample_events, format_event_preview
events = get_sample_events('$source', '$event_type')
print(format_event_preview(events))
" 2>/dev/null
'''
    
    with tempfile.NamedTemporaryFile(mode='w', suffix='.sh', delete=False) as f:
        f.write(script_content)
        f.flush()
        os.chmod(f.name, 0o755)
        return f.name


def interactive_query_builder() -> Dict[str, Any]:
    """Build a query interactively using fzf."""
    if not check_fzf_available():
        console.print("[red]Error: fzf is not available. Please install fzf for interactive mode.[/red]")
        console.print("Install with: brew install fzf (macOS) or your package manager")
        return {}
    
    query = {}
    
    console.print("[bold]Interactive Query Builder[/bold]")
    console.print("Use fzf to build your query step by step. Press Ctrl+C to skip any step.\n")
    
    # Step 1: Select source
    console.print("Step 1: Select event source (optional)")
    sources_with_counts = get_sources_with_counts()
    if sources_with_counts:
        source_items = [f"{source} ({count:,} events)" for source, count in sources_with_counts]
        source_items.append("(skip - all sources)")
        
        selected_sources = fzf_select(source_items, prompt="Source: ")
        if selected_sources and selected_sources[0] != "(skip - all sources)":
            # Extract source name from "source (count events)" format
            source = selected_sources[0].split(' (')[0]
            query['source'] = source
            console.print(f"Selected source: [cyan]{source}[/cyan]")
        else:
            console.print("Skipped source selection")
    
    # Step 2: Select event type
    console.print("\nStep 2: Select event type (optional)")
    selected_source = query.get('source')
    event_types_with_counts = get_event_types_with_counts(selected_source)
    
    if event_types_with_counts:
        event_type_items = [f"{event_type} ({count:,} events)" for event_type, count in event_types_with_counts]
        event_type_items.append("(skip - all event types)")
        
        selected_event_types = fzf_select(event_type_items, prompt="Event Type: ")
        if selected_event_types and selected_event_types[0] != "(skip - all event types)":
            # Extract event type from "event_type (count events)" format
            event_type = selected_event_types[0].split(' (')[0]
            query['event_type'] = event_type
            console.print(f"Selected event type: [cyan]{event_type}[/cyan]")
        else:
            console.print("Skipped event type selection")
    
    # Step 3: Select time range
    console.print("\nStep 3: Select time range (optional)")
    time_ranges = get_recent_time_ranges()
    time_ranges.append("(skip - all time)")
    time_ranges.append("(custom - enter manually)")
    
    selected_time = fzf_select(time_ranges, prompt="Time Range: ")
    if selected_time and selected_time[0] != "(skip - all time)":
        if selected_time[0] == "(custom - enter manually)":
            console.print("Enter custom time range:")
            console.print("Examples: 2024-01-01, 2024-01-01 10:00, 10:00")
            since = input("Since (YYYY-MM-DD HH:MM:SS): ").strip()
            until = input("Until (YYYY-MM-DD HH:MM:SS): ").strip()
            if since:
                query['since'] = since
            if until:
                query['until'] = until
        else:
            # Extract time value from "5m - Last 5 minutes" format
            time_value = selected_time[0].split(' - ')[0]
            query['last'] = time_value
            console.print(f"Selected time range: [cyan]{selected_time[0]}[/cyan]")
    else:
        console.print("Skipped time range selection")
    
    # Step 4: Select host
    console.print("\nStep 4: Select host (optional)")
    hosts_with_counts = get_hosts_with_counts()
    if hosts_with_counts:
        host_items = [f"{host} ({count:,} events)" for host, count in hosts_with_counts]
        host_items.append("(skip - all hosts)")
        
        selected_hosts = fzf_select(host_items, prompt="Host: ")
        if selected_hosts and selected_hosts[0] != "(skip - all hosts)":
            # Extract host from "host (count events)" format
            host = selected_hosts[0].split(' (')[0]
            query['host'] = host
            console.print(f"Selected host: [cyan]{host}[/cyan]")
        else:
            console.print("Skipped host selection")
    
    # Step 5: Set limit
    console.print("\nStep 5: Set result limit")
    limit_options = [
        "10 - Show 10 results",
        "25 - Show 25 results",
        "50 - Show 50 results (default)",
        "100 - Show 100 results",
        "250 - Show 250 results",
        "500 - Show 500 results",
        "(custom - enter manually)"
    ]
    
    selected_limit = fzf_select(limit_options, prompt="Limit: ")
    if selected_limit:
        if selected_limit[0] == "(custom - enter manually)":
            try:
                limit = int(input("Enter limit: ").strip())
                query['limit'] = limit
            except ValueError:
                query['limit'] = 50  # Default
        else:
            # Extract limit value from "50 - Show 50 results" format
            limit = int(selected_limit[0].split(' - ')[0])
            query['limit'] = limit
            console.print(f"Selected limit: [cyan]{limit}[/cyan]")
    else:
        query['limit'] = 50  # Default
    
    # Step 6: Select output format
    console.print("\nStep 6: Select output format")
    format_options = [
        "table - Rich table format (default)",
        "json - JSON output",
        "csv - CSV output",
        "yaml - YAML output"
    ]
    
    selected_format = fzf_select(format_options, prompt="Output Format: ")
    if selected_format:
        # Extract format from "table - Rich table format" format
        output_format = selected_format[0].split(' - ')[0]
        query['output_format'] = output_format
        console.print(f"Selected format: [cyan]{output_format}[/cyan]")
    else:
        query['output_format'] = 'table'  # Default
    
    return query


def build_exo_command(query: Dict[str, Any]) -> str:
    """Build the exo command from the interactive query."""
    cmd_parts = ['exo', 'query']
    
    # Add query parameters
    if 'source' in query:
        cmd_parts.extend(['--source', query['source']])
    
    if 'event_type' in query:
        cmd_parts.extend(['--event-type', query['event_type']])
    
    if 'since' in query:
        cmd_parts.extend(['--since', f'"{query["since"]}"'])
    
    if 'until' in query:
        cmd_parts.extend(['--until', f'"{query["until"]}"'])
    
    if 'last' in query:
        cmd_parts.extend(['--last', query['last']])
    
    if 'host' in query:
        cmd_parts.extend(['--host', query['host']])
    
    if 'limit' in query:
        cmd_parts.extend(['--limit', str(query['limit'])])
    
    if 'output_format' in query and query['output_format'] != 'table':
        cmd_parts.extend(['--output-format', query['output_format']])
    
    return ' '.join(cmd_parts)


def run_interactive_mode():
    """Run the interactive query builder."""
    try:
        query = interactive_query_builder()
        
        if not query:
            console.print("[yellow]No query built. Exiting.[/yellow]")
            return
        
        # Display the built query
        console.print("\n[bold]Built Query:[/bold]")
        for key, value in query.items():
            console.print(f"  {key}: [cyan]{value}[/cyan]")
        
        # Generate the command
        command = build_exo_command(query)
        console.print(f"\n[bold]Generated Command:[/bold]")
        console.print(f"[green]{command}[/green]")
        
        # Ask if user wants to run it
        console.print("\nOptions:")
        console.print("1. Run the query now")
        console.print("2. Copy command to clipboard")
        console.print("3. Exit")
        
        choice = input("Choose (1-3): ").strip()
        
        if choice == '1':
            # Import exo and run the query
            console.print("\n[bold]Running query...[/bold]")
            try:
                # Execute the query using the existing CLI
                import sys
                from pathlib import Path
                sys.path.insert(0, str(Path(__file__).parent))
                from exo import cli
                
                # Build arguments for click
                args = ['query']
                if 'source' in query:
                    args.extend(['--source', query['source']])
                if 'event_type' in query:
                    args.extend(['--event-type', query['event_type']])
                if 'since' in query:
                    args.extend(['--since', query['since']])
                if 'until' in query:
                    args.extend(['--until', query['until']])
                if 'last' in query:
                    args.extend(['--last', query['last']])
                if 'host' in query:
                    args.extend(['--host', query['host']])
                if 'limit' in query:
                    args.extend(['--limit', str(query['limit'])])
                if 'output_format' in query and query['output_format'] != 'table':
                    args.extend(['--output-format', query['output_format']])
                
                # Run the CLI command
                cli(args, standalone_mode=False)
                
            except Exception as e:
                console.print(f"[red]Error running query: {e}[/red]")
                console.print(f"You can run manually: [green]{command}[/green]")
        
        elif choice == '2':
            try:
                # Try to copy to clipboard
                subprocess.run(['pbcopy'], input=command, text=True, check=True)
                console.print("[green]Command copied to clipboard![/green]")
            except (subprocess.CalledProcessError, FileNotFoundError):
                try:
                    subprocess.run(['xclip', '-selection', 'clipboard'], input=command, text=True, check=True)
                    console.print("[green]Command copied to clipboard![/green]")
                except (subprocess.CalledProcessError, FileNotFoundError):
                    console.print("[yellow]Could not copy to clipboard. Here's the command:[/yellow]")
                    console.print(f"[green]{command}[/green]")
        
        else:
            console.print("Exiting.")
    
    except KeyboardInterrupt:
        console.print("\n[yellow]Interactive mode cancelled.[/yellow]")
    except Exception as e:
        console.print(f"[red]Error in interactive mode: {e}[/red]")


if __name__ == '__main__':
    run_interactive_mode()