#!/usr/bin/env python3
"""
Sinex RPC Client - JSON-RPC 2.0 client for sinex-gateway service
"""

import json
import ssl
import time
from typing import Optional, Dict, List, Any, Union, Tuple
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
    
    def __init__(
        self,
        rpc_url: str = "https://127.0.0.1:9999",
        timeout: int = 30,
        token: Optional[str] = None,
        ca_cert_path: Optional[str] = None,
        client_cert_path: Optional[str] = None,
        client_key_path: Optional[str] = None,
    ):
        parsed = urllib.parse.urlparse(rpc_url)
        if parsed.scheme and parsed.scheme != "https":
            raise ValueError("RPC URL must use https://; gateway requires TLS.")
        if not parsed.scheme:
            raise ValueError("RPC URL must include a scheme (https://...).")

        self.rpc_url = rpc_url
        self.timeout = timeout
        self._request_id = 0
        self.token = token
        self._ssl_context = None
        if parsed.scheme == "https":
            if ca_cert_path:
                self._ssl_context = ssl.create_default_context(cafile=ca_cert_path)
            else:
                self._ssl_context = ssl.create_default_context()
            if client_cert_path or client_key_path:
                if not client_cert_path or not client_key_path:
                    raise ValueError(
                        "Both client cert and key are required for mTLS "
                        "(SINEX_RPC_CLIENT_CERT + SINEX_RPC_CLIENT_KEY)."
                    )
                self._ssl_context.load_cert_chain(
                    certfile=client_cert_path,
                    keyfile=client_key_path,
                )
    
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
            headers = {
                'Content-Type': 'application/json',
                'Content-Length': str(len(request_json))
            }
            if self.token:
                headers['Authorization'] = f"Bearer {self.token}"

            req = urllib.request.Request(
                url=self.rpc_url,
                data=request_json,
                headers=headers,
                method='POST'
            )
            
            # Make request
            urlopen_kwargs = {"timeout": self.timeout}
            if self._ssl_context is not None:
                urlopen_kwargs["context"] = self._ssl_context
            with urllib.request.urlopen(req, **urlopen_kwargs) as response:
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

    def call(self, method: str, params: Dict[str, Any]) -> Any:
        """Expose raw RPC calls for ad-hoc methods (e.g., telemetry)."""
        return self._call_rpc(method, params)

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
    
    # Utility methods for CLI formatting
    def query_events(
        self,
        source: Optional[str] = None,
        event_type: Optional[str] = None,
        since: Optional[str] = None,
        until: Optional[str] = None,
        last: Optional[str] = None,
        limit: int = 50,
        host: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        """Query events with CLI-friendly parameters."""
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
        
        # Convert RPC results to CLI format
        formatted_events = []
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
                
            formatted_events.append(event)
        
        return formatted_events

    # Replay Control Methods

    def replay_create_operation(
        self,
        actor: str,
        scope: Dict[str, Any],
    ) -> Dict[str, Any]:
        result = self._call_rpc(
            "replay.create_operation",
            {"actor": actor, "scope": scope},
        )
        operation = result.get("operation")
        if operation is None:
            raise SinexRPCError(-32603, "RPC response missing operation payload")
        return operation

    def replay_preview_operation(
        self,
        operation_id: str,
    ) -> Tuple[Dict[str, Any], Dict[str, Any]]:
        result = self._call_rpc(
            "replay.preview_operation",
            {"operation_id": operation_id},
        )
        operation = result.get("operation")
        preview = result.get("preview")
        if operation is None or preview is None:
            raise SinexRPCError(-32603, "RPC response missing preview payload")
        return operation, preview

    def replay_approve_operation(
        self,
        operation_id: str,
        approver: str,
    ) -> Dict[str, Any]:
        result = self._call_rpc(
            "replay.approve_operation",
            {"operation_id": operation_id, "approver": approver},
        )
        operation = result.get("operation")
        if operation is None:
            raise SinexRPCError(-32603, "RPC response missing operation payload")
        return operation

    def replay_execute_operation(
        self,
        operation_id: str,
        executor: str,
    ) -> Dict[str, Any]:
        result = self._call_rpc(
            "replay.execute_operation",
            {"operation_id": operation_id, "executor": executor},
        )
        operation = result.get("operation")
        if operation is None:
            raise SinexRPCError(-32603, "RPC response missing operation payload")
        return operation

    def replay_cancel_operation(
        self,
        operation_id: str,
        reason: Optional[str] = None,
    ) -> Dict[str, Any]:
        payload: Dict[str, Any] = {"operation_id": operation_id}
        if reason:
            payload["reason"] = reason
        result = self._call_rpc("replay.cancel_operation", payload)
        operation = result.get("operation")
        if operation is None:
            raise SinexRPCError(-32603, "RPC response missing operation payload")
        return operation

    def replay_operation_status(self, operation_id: str) -> Dict[str, Any]:
        result = self._call_rpc(
            "replay.operation_status",
            {"operation_id": operation_id},
        )
        operation = result.get("operation")
        if operation is None:
            raise SinexRPCError(-32603, "RPC response missing operation payload")
        return operation

    def replay_list_operations(
        self,
        state: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        payload: Dict[str, Any] = {}
        if state:
            payload["state"] = state
        result = self._call_rpc("replay.list_operations", payload)
        operations = result.get("operations")
        if operations is None:
            raise SinexRPCError(-32603, "RPC response missing operations list")
        return operations
    
    def get_sources_statistics(self, limit: int = 100) -> List[Dict[str, Any]]:
        """Get detailed per-source statistics."""
        result = self._call_rpc("analytics.sources_statistics", {"limit": limit})
        sources_stats = []
        for entry in result:
            first_event = entry.get("first_event")
            last_event = entry.get("last_event")
            sources_stats.append({
                "source": entry["source"],
                "event_count": entry["event_count"],
                "event_type_count": entry.get("event_type_count", 0),
                "host_count": entry.get("host_count", 0),
                "first_event": self._parse_datetime(first_event) if first_event else None,
                "last_event": self._parse_datetime(last_event) if last_event else None,
                "avg_ingest_delay": entry.get("avg_ingest_delay"),
            })
        return sources_stats
    
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

def create_client(
    rpc_url: str = None,
    token: str = None,
    ca_cert_path: str = None,
    client_cert_path: str = None,
    client_key_path: str = None,
) -> SinexRPCClient:
    """Create a configured RPC client."""
    # Try common environment variables
    import os
    if rpc_url is None:
        rpc_url = os.environ.get('SINEX_RPC_URL', 'https://127.0.0.1:9999')
    if token is None:
        token = os.environ.get('SINEX_RPC_TOKEN')
    if ca_cert_path is None:
        ca_cert_path = os.environ.get('SINEX_RPC_CA_CERT')
    if client_cert_path is None:
        client_cert_path = os.environ.get('SINEX_RPC_CLIENT_CERT')
    if client_key_path is None:
        client_key_path = os.environ.get('SINEX_RPC_CLIENT_KEY')
    
    return SinexRPCClient(
        rpc_url,
        token=token,
        ca_cert_path=ca_cert_path,
        client_cert_path=client_cert_path,
        client_key_path=client_key_path,
    )


def test_connection(
    rpc_url: str = None,
    token: str = None,
    ca_cert_path: str = None,
    client_cert_path: str = None,
    client_key_path: str = None,
) -> bool:
    """Test if RPC server is reachable."""
    client = create_client(
        rpc_url,
        token=token,
        ca_cert_path=ca_cert_path,
        client_cert_path=client_cert_path,
        client_key_path=client_key_path,
    )
    return client.ping()
