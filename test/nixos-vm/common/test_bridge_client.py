#!/usr/bin/env python3
"""
Test Bridge Client for VM Tests

This client allows VM tests to use the same test infrastructure as Rust tests
by communicating with the sinex-test-bridge service.
"""

import asyncio
import json
import time
from typing import Dict, List, Optional, Any, Union
from dataclasses import dataclass, asdict
from datetime import datetime
import urllib.request
import urllib.parse
import urllib.error


@dataclass
class Event:
    """Represents a Sinex event"""
    source: str
    event_type: str
    payload: Dict[str, Any]
    timestamp: Optional[str] = None
    id: Optional[str] = None


@dataclass
class WaitResult:
    """Result of a wait operation"""
    success: bool
    actual_count: int
    elapsed_ms: int


class TestBridgeClient:
    """Client for interacting with the Sinex test bridge service"""
    
    def __init__(self, base_url: str = "http://localhost:8899"):
        self.base_url = base_url.rstrip('/')
    
    def _request(self, method: str, path: str, data: Optional[Dict] = None, 
                 params: Optional[Dict] = None) -> Dict:
        """Make an HTTP request to the bridge service"""
        url = f"{self.base_url}{path}"
        
        # Add query parameters if provided
        if params:
            query_string = urllib.parse.urlencode(params)
            url = f"{url}?{query_string}"
        
        # Prepare request
        headers = {'Content-Type': 'application/json'}
        body = json.dumps(data).encode('utf-8') if data else None
        
        request = urllib.request.Request(
            url, 
            data=body, 
            headers=headers,
            method=method
        )
        
        try:
            with urllib.request.urlopen(request) as response:
                return json.loads(response.read().decode('utf-8'))
        except urllib.error.HTTPError as e:
            error_body = e.read().decode('utf-8')
            try:
                error_data = json.loads(error_body)
                raise Exception(f"Bridge error: {error_data.get('error', 'Unknown error')}")
            except json.JSONDecodeError:
                raise Exception(f"Bridge HTTP error {e.code}: {error_body}")
    
    # === Event Operations ===
    
    def create_event(self, source: str, event_type: str, payload: Dict[str, Any],
                     timestamp: Optional[datetime] = None) -> str:
        """Create a new event and return its ID"""
        data = {
            'source': source,
            'event_type': event_type,
            'payload': payload
        }
        if timestamp:
            data['timestamp'] = timestamp.isoformat() + 'Z'
        
        response = self._request('POST', '/events', data)
        return response['id']
    
    def get_event(self, event_id: str) -> Optional[Event]:
        """Get an event by ID"""
        response = self._request('GET', f'/events/{event_id}')
        if response:
            return Event(**response)
        return None
    
    def count_events(self, source: Optional[str] = None, 
                     event_type: Optional[str] = None) -> int:
        """Count events with optional filtering"""
        params = {}
        if source:
            params['source'] = source
        if event_type:
            params['event_type'] = event_type
            
        response = self._request('GET', '/events/count', params=params)
        return response['count']
    
    def query_events(self, source: Optional[str] = None,
                     event_type: Optional[str] = None,
                     limit: int = 100) -> List[Event]:
        """Query events with filtering"""
        data = {'limit': limit}
        if source:
            data['source'] = source
        if event_type:
            data['event_type'] = event_type
            
        response = self._request('POST', '/events/query', data)
        return [Event(**e) for e in response]
    
    def wait_for_events(self, expected_count: int, source: Optional[str] = None,
                        event_type: Optional[str] = None, 
                        timeout_seconds: int = 5) -> WaitResult:
        """Wait for a specific number of events"""
        data = {
            'expected_count': expected_count,
            'timeout_seconds': timeout_seconds
        }
        if source:
            data['source'] = source
        if event_type:
            data['event_type'] = event_type
            
        response = self._request('POST', '/events/wait', data)
        return WaitResult(**response)
    
    # === Checkpoint Operations ===
    
    def create_checkpoint(self, automaton_name: str, processed_count: int,
                          last_processed_id: Optional[str] = None) -> bool:
        """Create or update a checkpoint"""
        data = {
            'automaton_name': automaton_name,
            'processed_count': processed_count
        }
        if last_processed_id:
            data['last_processed_id'] = last_processed_id
            
        response = self._request('POST', '/checkpoints', data)
        return response.get('success', False)
    
    def get_checkpoint(self, automaton_name: str) -> Optional[Dict]:
        """Get checkpoint state"""
        response = self._request('GET', f'/checkpoints/{automaton_name}')
        return response
    
    def wait_for_checkpoint(self, automaton_name: str, expected_count: int,
                            timeout_seconds: int = 5) -> WaitResult:
        """Wait for checkpoint to reach expected count"""
        data = {
            'expected_count': expected_count,
            'timeout_seconds': timeout_seconds
        }
        response = self._request('POST', f'/checkpoints/{automaton_name}/wait', data)
        return WaitResult(**response)
    
    # === Utility Operations ===
    
    def sleep(self, seconds: float) -> None:
        """Sleep for specified duration"""
        data = {'seconds': seconds}
        self._request('POST', '/utils/sleep', data)
    
    def query_database(self, query: str, parameters: Optional[List[Any]] = None) -> List[Dict]:
        """Execute a database query"""
        data = {
            'query': query,
            'parameters': parameters or []
        }
        response = self._request('POST', '/utils/database/query', data)
        return response.get('rows', [])
    
    # === High-Level Test Helpers ===
    
    def generate_events(self, source: str, event_type: str, count: int,
                        payload_template: Optional[Dict] = None,
                        interval_ms: Optional[int] = None) -> List[str]:
        """Generate multiple events with optional interval"""
        event_ids = []
        
        for i in range(count):
            # Process payload template
            payload = payload_template or {}
            if isinstance(payload, dict):
                # Replace {{index}} placeholder
                payload_str = json.dumps(payload)
                payload_str = payload_str.replace('{{index}}', str(i))
                payload = json.loads(payload_str)
            
            # Create event
            event_id = self.create_event(source, event_type, payload)
            event_ids.append(event_id)
            
            # Sleep between events if interval specified
            if interval_ms and i < count - 1:
                time.sleep(interval_ms / 1000.0)
        
        return event_ids
    
    def assert_event_count(self, expected: int, source: Optional[str] = None,
                           event_type: Optional[str] = None, comparison: str = "equals"):
        """Assert event count matches expectation"""
        actual = self.count_events(source, event_type)
        
        if comparison == "equals" and actual != expected:
            raise AssertionError(f"Expected {expected} events, got {actual}")
        elif comparison == "greater_than" and actual <= expected:
            raise AssertionError(f"Expected more than {expected} events, got {actual}")
        elif comparison == "greater_than_or_equal" and actual < expected:
            raise AssertionError(f"Expected at least {expected} events, got {actual}")
        elif comparison == "less_than" and actual >= expected:
            raise AssertionError(f"Expected less than {expected} events, got {actual}")
        elif comparison == "less_than_or_equal" and actual > expected:
            raise AssertionError(f"Expected at most {expected} events, got {actual}")
    
    def run_scenario_step(self, step: Dict) -> Any:
        """Execute a single test scenario step"""
        action = step.get('type')
        
        if action == 'generate_events':
            return self.generate_events(
                source=step['source'],
                event_type=step['event_type'],
                count=step['count'],
                payload_template=step.get('payload_template'),
                interval_ms=step.get('interval')
            )
        
        elif action == 'wait_for':
            condition = step.get('condition')
            if condition == 'event_count':
                result = self.wait_for_events(
                    expected_count=step['count'],
                    source=step.get('source'),
                    event_type=step.get('event_type'),
                    timeout_seconds=step.get('timeout', 5)
                )
                if not result.success:
                    raise TimeoutError(f"Timeout waiting for {step['count']} events")
                return result
        
        elif action == 'sleep':
            duration_str = step['duration']
            # Parse duration (e.g., "100ms", "2s")
            if duration_str.endswith('ms'):
                seconds = int(duration_str[:-2]) / 1000.0
            elif duration_str.endswith('s'):
                seconds = int(duration_str[:-1])
            else:
                seconds = float(duration_str)
            self.sleep(seconds)
        
        else:
            raise ValueError(f"Unknown action type: {action}")


# === Example Usage for VM Tests ===

def example_basic_test():
    """Example of using the bridge client in a VM test"""
    client = TestBridgeClient()
    
    # Generate some events
    print("Generating events...")
    event_ids = client.generate_events(
        source="test",
        event_type="test.event",
        count=10,
        payload_template={"test": True, "index": "{{index}}"},
        interval_ms=100
    )
    print(f"Created {len(event_ids)} events")
    
    # Wait for events to be processed
    print("Waiting for events...")
    result = client.wait_for_events(
        expected_count=10,
        source="test",
        timeout_seconds=5
    )
    print(f"Wait result: {result}")
    
    # Query events
    print("Querying events...")
    events = client.query_events(source="test", limit=5)
    for event in events:
        print(f"  - {event.id}: {event.event_type}")
    
    # Assert count
    print("Asserting event count...")
    client.assert_event_count(10, source="test")
    print("✓ All assertions passed!")


if __name__ == "__main__":
    # Run example if executed directly
    example_basic_test()