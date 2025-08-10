#!/usr/bin/env python3
"""
Replay Planner for Sinex - Plan and execute event replays with gates and operation tracking
"""

import os
import sys
import json
import uuid
from datetime import datetime, timedelta
from typing import Optional, List, Dict, Any, Tuple
from dataclasses import dataclass
from enum import Enum

import click
import psycopg2
from psycopg2.extras import RealDictCursor
from rich.console import Console
from rich.table import Table
from rich.panel import Panel
from rich.prompt import Prompt, Confirm
from rich import box
from rich.text import Text
from rich.progress import track

console = Console()


class GateType(Enum):
    """Types of gates that can pause replay execution"""
    TIME = "time"          # Pause at specific timestamp
    COUNT = "count"        # Pause after N events
    EVENT = "event"        # Pause on specific event type
    MANUAL = "manual"      # Manual confirmation required
    CONDITION = "condition"  # Pause on payload condition


@dataclass
class ReplayGate:
    """Gate definition for replay control"""
    gate_type: GateType
    name: str
    description: str
    condition: Any  # Varies by gate type
    action: str = "pause"  # pause, skip, transform
    
    def should_trigger(self, event: Dict, event_count: int) -> bool:
        """Check if gate should trigger for given event"""
        if self.gate_type == GateType.COUNT:
            return event_count >= self.condition
        elif self.gate_type == GateType.EVENT:
            return event['event_type'] == self.condition
        elif self.gate_type == GateType.TIME:
            event_time = datetime.fromisoformat(event['ts_orig'] or event['ts_ingest'])
            gate_time = datetime.fromisoformat(self.condition)
            return event_time >= gate_time
        elif self.gate_type == GateType.CONDITION:
            # Evaluate JSON path condition
            return self._evaluate_condition(event)
        return False
    
    def _evaluate_condition(self, event: Dict) -> bool:
        """Evaluate complex conditions on event payload"""
        # Simple implementation - can be enhanced with JSONPath
        try:
            payload = event.get('payload', {})
            if isinstance(self.condition, dict):
                for key, expected_value in self.condition.items():
                    if payload.get(key) != expected_value:
                        return False
                return True
        except:
            return False
        return False


@dataclass 
class ReplayPlan:
    """Complete replay execution plan"""
    operation_id: str
    name: str
    description: str
    source_query: Dict  # Query parameters for source events
    target_system: str  # Where to replay (nats, database, etc.)
    gates: List[ReplayGate]
    transformations: List[Dict]  # Optional event transformations
    dry_run: bool = True
    created_at: datetime = None
    executed_at: Optional[datetime] = None
    status: str = "draft"
    
    def __post_init__(self):
        if self.created_at is None:
            self.created_at = datetime.utcnow()
        if not self.operation_id:
            self.operation_id = f"replay_{uuid.uuid4().hex[:8]}"


class ReplayPlanner:
    """Plan and execute event replays with control gates"""
    
    def __init__(self, db_url: Optional[str] = None):
        self.db_url = db_url or os.environ.get('DATABASE_URL', 'postgresql://localhost/sinex')
        
    def get_connection(self):
        """Get database connection"""
        return psycopg2.connect(self.db_url, cursor_factory=RealDictCursor)
    
    def preview_events(self, source: Optional[str] = None, 
                      event_type: Optional[str] = None,
                      since: Optional[str] = None,
                      until: Optional[str] = None,
                      limit: int = 100) -> List[Dict]:
        """Preview events that would be replayed"""
        with self.get_connection() as conn:
            with conn.cursor() as cur:
                query_parts = [
                    "SELECT event_id, source, event_type, ts_ingest, ts_orig, "
                    "host, payload, source_event_ids, material_id, anchor_byte "
                    "FROM core.events"
                ]
                
                conditions = []
                params = []
                
                if source:
                    conditions.append("source = %s")
                    params.append(source)
                
                if event_type:
                    conditions.append("event_type = %s")
                    params.append(event_type)
                    
                if since:
                    conditions.append("COALESCE(ts_orig, ts_ingest) >= %s")
                    params.append(since)
                    
                if until:
                    conditions.append("COALESCE(ts_orig, ts_ingest) <= %s")
                    params.append(until)
                
                if conditions:
                    query_parts.append("WHERE " + " AND ".join(conditions))
                
                query_parts.append("ORDER BY COALESCE(ts_orig, ts_ingest) ASC")
                query_parts.append(f"LIMIT {limit}")
                
                query = " ".join(query_parts)
                cur.execute(query, params)
                
                return cur.fetchall()
    
    def create_plan(self, name: str, description: str,
                   source_query: Dict, 
                   target_system: str = "preview",
                   dry_run: bool = True) -> ReplayPlan:
        """Create a new replay plan"""
        plan = ReplayPlan(
            operation_id=f"replay_{uuid.uuid4().hex[:8]}",
            name=name,
            description=description,
            source_query=source_query,
            target_system=target_system,
            gates=[],
            transformations=[],
            dry_run=dry_run
        )
        return plan
    
    def add_gate(self, plan: ReplayPlan, gate: ReplayGate) -> None:
        """Add a control gate to the replay plan"""
        plan.gates.append(gate)
    
    def save_plan(self, plan: ReplayPlan) -> None:
        """Save replay plan to database"""
        with self.get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO core.replay_operations (
                        operation_id, operation_type, status, 
                        metadata, created_at
                    ) VALUES (%s, %s, %s, %s, %s)
                    ON CONFLICT (operation_id) DO UPDATE SET
                        status = EXCLUDED.status,
                        metadata = EXCLUDED.metadata
                """, (
                    plan.operation_id,
                    'replay',
                    plan.status,
                    json.dumps({
                        'name': plan.name,
                        'description': plan.description,
                        'source_query': plan.source_query,
                        'target_system': plan.target_system,
                        'gates': [self._gate_to_dict(g) for g in plan.gates],
                        'transformations': plan.transformations,
                        'dry_run': plan.dry_run
                    }),
                    plan.created_at
                ))
                conn.commit()
    
    def _gate_to_dict(self, gate: ReplayGate) -> Dict:
        """Convert gate to dictionary for storage"""
        return {
            'type': gate.gate_type.value,
            'name': gate.name,
            'description': gate.description,
            'condition': gate.condition,
            'action': gate.action
        }
    
    def load_plan(self, operation_id: str) -> Optional[ReplayPlan]:
        """Load replay plan from database"""
        with self.get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT operation_id, status, metadata, created_at, executed_at
                    FROM core.replay_operations
                    WHERE operation_id = %s AND operation_type = 'replay'
                """, (operation_id,))
                
                row = cur.fetchone()
                if not row:
                    return None
                
                metadata = row['metadata']
                gates = [self._dict_to_gate(g) for g in metadata.get('gates', [])]
                
                return ReplayPlan(
                    operation_id=row['operation_id'],
                    name=metadata['name'],
                    description=metadata['description'],
                    source_query=metadata['source_query'],
                    target_system=metadata['target_system'],
                    gates=gates,
                    transformations=metadata.get('transformations', []),
                    dry_run=metadata.get('dry_run', True),
                    created_at=row['created_at'],
                    executed_at=row['executed_at'],
                    status=row['status']
                )
    
    def _dict_to_gate(self, gate_dict: Dict) -> ReplayGate:
        """Convert dictionary to gate object"""
        return ReplayGate(
            gate_type=GateType(gate_dict['type']),
            name=gate_dict['name'],
            description=gate_dict['description'],
            condition=gate_dict['condition'],
            action=gate_dict.get('action', 'pause')
        )
    
    def execute_plan(self, plan: ReplayPlan, interactive: bool = True) -> Dict:
        """Execute a replay plan with gate controls"""
        results = {
            'operation_id': plan.operation_id,
            'events_processed': 0,
            'events_skipped': 0,
            'gates_triggered': [],
            'errors': [],
            'start_time': datetime.utcnow()
        }
        
        # Mark plan as executing
        plan.status = 'executing'
        self.save_plan(plan)
        
        # Set operation_id in session for tracking
        with self.get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT set_operation_id(%s)", (plan.operation_id,))
                conn.commit()
        
        try:
            # Get events to replay
            events = self.preview_events(**plan.source_query)
            
            console.print(Panel(
                f"[bold green]Replay Plan: {plan.name}[/bold green]\n"
                f"Operation ID: {plan.operation_id}\n"
                f"Events to process: {len(events)}\n"
                f"Target: {plan.target_system}\n"
                f"Mode: {'DRY RUN' if plan.dry_run else 'LIVE'}",
                title="Replay Execution",
                box=box.ROUNDED
            ))
            
            # Process events with gate checks
            for idx, event in enumerate(track(events, description="Processing events...")):
                # Check gates
                for gate in plan.gates:
                    if gate.should_trigger(event, idx + 1):
                        results['gates_triggered'].append({
                            'gate': gate.name,
                            'event_idx': idx,
                            'event_id': event['event_id']
                        })
                        
                        if interactive and gate.action == 'pause':
                            self._handle_gate_pause(gate, event, idx)
                        elif gate.action == 'skip':
                            results['events_skipped'] += 1
                            continue
                
                # Apply transformations
                transformed_event = self._apply_transformations(event, plan.transformations)
                
                # Execute replay (or simulate in dry run)
                if not plan.dry_run:
                    self._replay_event(transformed_event, plan.target_system)
                
                results['events_processed'] += 1
            
            # Mark plan as completed
            plan.status = 'completed'
            plan.executed_at = datetime.utcnow()
            self.save_plan(plan)
            
        except Exception as e:
            results['errors'].append(str(e))
            plan.status = 'failed'
            self.save_plan(plan)
            raise
        finally:
            # Clear operation_id from session
            with self.get_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute("SELECT set_operation_id(NULL)")
                    conn.commit()
        
        results['end_time'] = datetime.utcnow()
        results['duration'] = (results['end_time'] - results['start_time']).total_seconds()
        
        return results
    
    def _handle_gate_pause(self, gate: ReplayGate, event: Dict, event_idx: int):
        """Handle interactive gate pause"""
        console.print(Panel(
            f"[yellow]Gate Triggered: {gate.name}[/yellow]\n"
            f"{gate.description}\n\n"
            f"Event #{event_idx + 1}: {event['event_type']}\n"
            f"Time: {event.get('ts_orig', event['ts_ingest'])}",
            title="Gate Pause",
            box=box.DOUBLE
        ))
        
        action = Prompt.ask(
            "Action",
            choices=["continue", "skip", "abort", "inspect"],
            default="continue"
        )
        
        if action == "abort":
            raise Exception("Replay aborted by user")
        elif action == "inspect":
            console.print(json.dumps(event, indent=2, default=str))
            # Recursive call to ask again
            self._handle_gate_pause(gate, event, event_idx)
        elif action == "skip":
            return "skip"
    
    def _apply_transformations(self, event: Dict, transformations: List[Dict]) -> Dict:
        """Apply transformations to event before replay"""
        transformed = event.copy()
        
        for transform in transformations:
            if transform['type'] == 'rename_field':
                old_key = transform['old_key']
                new_key = transform['new_key']
                if old_key in transformed['payload']:
                    transformed['payload'][new_key] = transformed['payload'].pop(old_key)
            elif transform['type'] == 'add_field':
                transformed['payload'][transform['key']] = transform['value']
            elif transform['type'] == 'remove_field':
                transformed['payload'].pop(transform['key'], None)
            elif transform['type'] == 'timestamp_shift':
                # Shift timestamps by specified duration
                shift = timedelta(seconds=transform['seconds'])
                if transformed.get('ts_orig'):
                    orig_time = datetime.fromisoformat(transformed['ts_orig'])
                    transformed['ts_orig'] = (orig_time + shift).isoformat()
        
        return transformed
    
    def _replay_event(self, event: Dict, target_system: str):
        """Actually replay the event to target system"""
        if target_system == "preview":
            # Just log it
            console.print(f"Would replay: {event['event_type']} - {event['event_id']}")
        elif target_system == "nats":
            # TODO: Publish to NATS with operation_id in metadata
            # The NATS publisher would add operation_id to event metadata
            pass
        elif target_system == "database":
            # Insert into staging table with operation_id
            with self.get_connection() as conn:
                with conn.cursor() as cur:
                    # The trigger will automatically add operation_id to metadata
                    cur.execute("""
                        INSERT INTO core.events (
                            source, event_type, host, payload, ts_orig, ts_ingest
                        ) VALUES (%s, %s, %s, %s, %s, CURRENT_TIMESTAMP)
                    """, (
                        f"replay_{event['source']}",
                        event['event_type'],
                        event['host'],
                        event['payload'],
                        event.get('ts_orig')
                    ))
                    conn.commit()
        else:
            raise ValueError(f"Unknown target system: {target_system}")


def display_replay_plan(plan: ReplayPlan):
    """Display replay plan details"""
    table = Table(title=f"Replay Plan: {plan.name}", box=box.ROUNDED)
    
    table.add_column("Property", style="cyan")
    table.add_column("Value", style="white")
    
    table.add_row("Operation ID", plan.operation_id)
    table.add_row("Description", plan.description)
    table.add_row("Status", plan.status)
    table.add_row("Target System", plan.target_system)
    table.add_row("Dry Run", "Yes" if plan.dry_run else "No")
    table.add_row("Gates", str(len(plan.gates)))
    table.add_row("Created", plan.created_at.isoformat() if plan.created_at else "N/A")
    table.add_row("Executed", plan.executed_at.isoformat() if plan.executed_at else "Never")
    
    console.print(table)
    
    if plan.gates:
        gates_table = Table(title="Control Gates", box=box.SIMPLE)
        gates_table.add_column("Name", style="yellow")
        gates_table.add_column("Type", style="cyan")
        gates_table.add_column("Condition", style="white")
        gates_table.add_column("Action", style="green")
        
        for gate in plan.gates:
            gates_table.add_row(
                gate.name,
                gate.gate_type.value,
                str(gate.condition)[:50],
                gate.action
            )
        
        console.print(gates_table)


def display_execution_results(results: Dict):
    """Display replay execution results"""
    duration = results.get('duration', 0)
    
    console.print(Panel(
        f"[bold green]Replay Complete[/bold green]\n\n"
        f"Operation ID: {results['operation_id']}\n"
        f"Events Processed: {results['events_processed']}\n"
        f"Events Skipped: {results['events_skipped']}\n"
        f"Gates Triggered: {len(results['gates_triggered'])}\n"
        f"Duration: {duration:.2f} seconds\n"
        f"Errors: {len(results.get('errors', []))}",
        title="Execution Results",
        box=box.DOUBLE
    ))
    
    if results.get('errors'):
        console.print("\n[red]Errors:[/red]")
        for error in results['errors']:
            console.print(f"  • {error}")