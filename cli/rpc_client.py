#!/usr/bin/env python3
"""
Sinex RPC Client - JSON-RPC 2.0 client for sinex-gateway service
"""

import json
import time
from typing import Optional, Dict, List, Any, Union
from datetime import datetime, timedelta
import urllib.request
import urllib.parse
import urllib.error


class SinexRPCError(Exception):
    """RPC-specific errors."""
    def __init__(self, code: int, message: str, data: Optional[Any] = None):
        self.code = code
        self.message = message
        self.data = data
        super().__init__(f"RPC Error {code}: {message}")


class SinexRPCClient:
    """JSON-RPC 2.0 client for sinex-gateway service."""
    
    def __init__(self, rpc_url: str = "http://127.0.0.1:9999", timeout: int = 30):
        self.rpc_url = rpc_url
        self.timeout = timeout
        self._request_id = 0
    
    def _next_id(self) -> int:
        """Get next request ID."""
        self._request_id += 1
        return self._request_id
    
    def _call_rpc(self, method: str, params: Dict[str, Any]) -> Any:
        """Make a JSON-RPC 2.0 call to the server."""
        request_data = {
            "jsonrpc": "2.0",
            "method": method,
            "params": params,
            "id": self._next_id()
        }
        
        try:
            # Convert to JSON
            request_json = json.dumps(request_data).encode('utf-8')
            
            # Create HTTP request
            req = urllib.request.Request(
                url=self.rpc_url,
                data=request_json,
                headers={
                    'Content-Type': 'application/json',
                    'Content-Length': str(len(request_json))
                },
                method='POST'
            )
            
            # Make request
            with urllib.request.urlopen(req, timeout=self.timeout) as response:
                response_data = json.loads(response.read().decode('utf-8'))
            
            # Check for JSON-RPC errors
            if 'error' in response_data:
                error = response_data['error']
                raise SinexRPCError(
                    error.get('code', -32603),
                    error.get('message', 'Unknown error'),
                    error.get('data')
                )
            
            return response_data.get('result')
            
        except urllib.error.URLError as e:
            if hasattr(e, 'code'):
                raise SinexRPCError(-32700, f"HTTP {e.code}: {e.reason}") from e
            else:
                raise SinexRPCError(-32700, f"Connection error: {e.reason}") from e
        except json.JSONDecodeError as e:
            raise SinexRPCError(-32700, f"Invalid JSON response: {e}") from e
        except Exception as e:
            raise SinexRPCError(-32603, f"Request failed: {e}") from e

    # Search Service Methods
    
    def search_events(
        self,
        text: Optional[str] = None,
        sources: Optional[List[str]] = None,
        event_types: Optional[List[str]] = None,
        start_time: Optional[datetime] = None,
        end_time: Optional[datetime] = None,
        limit: int = 50,
        offset: int = 0
    ) -> List[Dict[str, Any]]:
        """Search events using the search service."""
        params = {
            "text": text,
            "sources": sources or [],
            "event_types": event_types or [],
            "start_time": start_time.isoformat() if start_time else None,
            "end_time": end_time.isoformat() if end_time else None,
            "limit": limit,
            "offset": offset
        }
        
        return self._call_rpc("search.search_events", params)
    
    # Analytics Service Methods
    
    def get_event_count_by_source(self, days_back: int = 7) -> Dict[str, int]:
        """Get event count by source."""
        params = {"days_back": days_back}
        return self._call_rpc("analytics.event_count_by_source", params)
    
    def get_activity_heatmap(
        self,
        bucket_size_minutes: int = 60,
        limit: int = 100
    ) -> List[Dict[str, Any]]:
        """Get activity heatmap data."""
        params = {
            "bucket_size_minutes": bucket_size_minutes,
            "limit": limit
        }
        return self._call_rpc("analytics.activity_heatmap", params)
    
    # Utility methods for CLI compatibility
    
    def query_events_compatible(
        self,
        source: Optional[str] = None,
        event_type: Optional[str] = None,
        since: Optional[str] = None,
        until: Optional[str] = None,
        last: Optional[str] = None,
        limit: int = 50,
        host: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        """
        Query events with CLI-compatible parameters.
        
        Returns events in a format compatible with the existing CLI display functions.
        """
        # Parse time parameters
        start_time = None
        end_time = None
        
        if since:
            start_time = self._parse_datetime(since)
        if until:
            end_time = self._parse_datetime(until)
        if last:
            time_delta = self._parse_time_delta(last)
            start_time = datetime.now() - time_delta
        
        # Prepare sources and event types
        sources = [source] if source else []
        event_types = [event_type] if event_type else []
        
        # Make RPC call
        results = self.search_events(
            text=None,  # Not filtering by text for basic query
            sources=sources,
            event_types=event_types,
            start_time=start_time,
            end_time=end_time,
            limit=limit,
            offset=0
        )
        
        # Convert RPC results to CLI-compatible format
        compatible_events = []
        for result in results:
            # Parse payload if it's a string
            payload = result.get('payload', {})
            if isinstance(payload, str):
                try:
                    payload = json.loads(payload)
                except json.JSONDecodeError:
                    payload = {'raw': payload}
            
            # Convert RPC search result to event dict format expected by CLI
            event = {
                'id': result['event_id'],
                'source': result['source'],
                'event_type': result['event_type'],
                'ts_ingest': self._parse_datetime(result['timestamp']),
                'ts_orig': None,  # RPC doesn't return this separately
                'host': result.get('host', 'unknown'),
                'ingestor_version': None,
                'payload_schema_id': None,
                'payload': payload
            }
            
            # Apply host filter client-side if needed
            if host and event['host'] != host:
                continue
                
            compatible_events.append(event)
        
        return compatible_events
    
    def get_sources_statistics(self) -> List[Dict[str, Any]]:
        """Get sources statistics compatible with CLI sources command."""
        # Get event counts by source
        counts = self.get_event_count_by_source(days_back=365)  # Get all-time stats
        
        # Convert to CLI-compatible format
        sources_stats = []
        for source, count in counts.items():
            sources_stats.append({
                'source': source,
                'event_count': count,
                'event_type_count': 1,  # RPC doesn't provide this breakdown
                'host_count': 1,  # RPC doesn't provide this breakdown
                'first_event': datetime.now() - timedelta(days=30),  # Placeholder
                'last_event': datetime.now(),  # Placeholder
                'avg_ingest_delay': None  # RPC doesn't provide this
            })
        
        return sorted(sources_stats, key=lambda x: x['event_count'], reverse=True)
    
    # Helper methods
    
    def _parse_datetime(self, date_str: str) -> datetime:
        """Parse datetime string in various formats."""
        try:
            return datetime.fromisoformat(date_str)
        except ValueError:
            pass

        formats = [
            '%Y-%m-%dT%H:%M:%S.%fZ',  # ISO format with microseconds
            '%Y-%m-%dT%H:%M:%SZ',     # ISO format without microseconds
            '%Y-%m-%dT%H:%M:%S',      # ISO format no timezone
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
    
    def _parse_time_delta(self, time_str: str) -> timedelta:
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
    
    def ping(self) -> bool:
        """Check if RPC server is responding."""
        try:
            # Try a simple analytics call
            self.get_event_count_by_source(days_back=1)
            return True
        except Exception:
            return False
    
    def health_check(self) -> Dict[str, Any]:
        """Get basic health information from RPC server."""
        try:
            counts = self.get_event_count_by_source(days_back=1)
            total_events = sum(counts.values())
            return {
                'status': 'healthy',
                'rpc_url': self.rpc_url,
                'total_events_last_day': total_events,
                'active_sources': len(counts),
                'response_time_ms': None  # Could measure this
            }
        except Exception as e:
            return {
                'status': 'unhealthy',
                'rpc_url': self.rpc_url,
                'error': str(e)
            }


# Convenience functions for backward compatibility

def create_client(rpc_url: str = None) -> SinexRPCClient:
    """Create a configured RPC client."""
    if rpc_url is None:
        # Try common environment variables
        import os
        rpc_url = os.environ.get('SINEX_RPC_URL', 'http://127.0.0.1:9999')
    
    return SinexRPCClient(rpc_url)


def test_connection(rpc_url: str = None) -> bool:
    """Test if RPC server is reachable."""
    client = create_client(rpc_url)
    return client.ping()
