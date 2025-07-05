#!/usr/bin/env python3
"""
Integration tests for the Sinex CLI (exo.py) with real database
"""

import os
import sys
import json
import subprocess
import tempfile
from datetime import datetime, timedelta
from pathlib import Path

import pytest
import psycopg2
from click.testing import CliRunner

# Add CLI directory to path so we can import exo
cli_dir = os.path.join(os.path.dirname(__file__), '..', '..', '..', 'cli')
sys.path.insert(0, cli_dir)
import exo


@pytest.fixture(scope="module")
def test_db():
    """Create a test database connection."""
    # Use the same database URL as the Rust tests
    db_url = os.environ.get('DATABASE_URL', 'postgresql:///sinex_test?host=/run/postgresql')
    
    try:
        conn = psycopg2.connect(db_url)
        conn.autocommit = True
        yield conn
    finally:
        if 'conn' in locals():
            conn.close()


@pytest.fixture
def setup_test_data(test_db):
    """Insert test data before each test."""
    cursor = test_db.cursor()
    
    # Clean up any existing test data
    cursor.execute("""
        DELETE FROM raw.events 
        WHERE source LIKE 'test_%' 
        OR host = 'test-host'
    """)
    
    # Insert test events
    test_events = [
        {
            'source': 'test_filesystem',
            'event_type': 'file_created',
            'host': 'test-host',
            'payload': json.dumps({'path': '/test/file1.txt', 'size': 1024})
        },
        {
            'source': 'test_filesystem',
            'event_type': 'file_modified',
            'host': 'test-host',
            'payload': json.dumps({'path': '/test/file1.txt', 'size': 2048})
        },
        {
            'source': 'test_terminal',
            'event_type': 'command_executed',
            'host': 'test-host',
            'payload': json.dumps({'command': 'ls -la', 'exit_code': 0})
        },
        {
            'source': 'sinex',
            'event_type': 'agent.heartbeat',
            'host': 'test-host',
            'payload': json.dumps({
                'agent_name': 'test-agent',
                'status': 'running',
                'uptime_seconds': 3600,
                'events_processed_session': 100
            })
        }
    ]
    
    for event in test_events:
        cursor.execute("""
            INSERT INTO raw.events (source, event_type, host, payload)
            VALUES (%(source)s, %(event_type)s, %(host)s, %(payload)s::jsonb)
        """, event)
    
    # Insert test schema
    cursor.execute("""
        INSERT INTO sinex_schemas.event_payload_schemas 
        (event_source, event_type, schema_version, json_schema_definition, description)
        VALUES ('test_filesystem', 'file_created', '1.0.0', 
                '{"type": "object", "properties": {"path": {"type": "string"}}}'::jsonb,
                'Test file creation schema')
        ON CONFLICT (event_source, event_type, schema_version) DO NOTHING
    """)
    
    # Insert test agent manifest
    cursor.execute("""
        INSERT INTO sinex_schemas.agent_manifests 
        (agent_name, description, version, status, produces_event_types)
        VALUES ('test-agent', 'Test agent for integration tests', '1.0.0', 'stable',
                '{"test_filesystem": ["file_created", "file_modified"]}'::jsonb)
        ON CONFLICT (agent_name) DO UPDATE SET
            last_heartbeat_ts = NOW(),
            version = EXCLUDED.version
    """)
    
    yield cursor
    
    # Cleanup after test
    cursor.execute("""
        DELETE FROM raw.events 
        WHERE source LIKE 'test_%' 
        OR host = 'test-host'
    """)
    cursor.execute("""
        DELETE FROM sinex_schemas.event_payload_schemas 
        WHERE event_source LIKE 'test_%'
    """)
    cursor.execute("""
        DELETE FROM sinex_schemas.agent_manifests 
        WHERE agent_name = 'test-agent'
    """)


class TestCLIIntegration:
    """Integration tests for CLI with real database."""
    
    @pytest.fixture
    def runner(self):
        """Create a CLI test runner."""
        return CliRunner()
    
    def test_query_real_data(self, runner, setup_test_data):
        """Test querying real data from database."""
        result = runner.invoke(exo.cli, ['query', '--host', 'test-host', '--limit', '10'])
        
        assert result.exit_code == 0
        assert 'test_filesystem' in result.output
        assert 'file_created' in result.output
        assert 'test-host' in result.output
    
    def test_query_with_source_filter(self, runner, setup_test_data):
        """Test querying with source filter."""
        result = runner.invoke(exo.cli, ['query', '--source', 'test_filesystem', '--limit', '10'])
        
        assert result.exit_code == 0
        assert 'test_filesystem' in result.output
        assert 'file_created' in result.output
        # Should not include terminal events
        assert 'command_executed' not in result.output
    
    def test_query_json_output(self, runner, setup_test_data):
        """Test JSON output format with real data."""
        result = runner.invoke(exo.cli, [
            'query', 
            '--source', 'test_filesystem',
            '--output-format', 'json',
            '--limit', '10'
        ])
        
        assert result.exit_code == 0
        
        # Parse JSON output
        data = json.loads(result.output)
        assert isinstance(data, list)
        assert len(data) >= 2  # We inserted 2 filesystem events
        
        # Verify structure
        for event in data:
            assert 'source' in event
            assert 'event_type' in event
            assert 'payload' in event
            assert event['source'] == 'test_filesystem'
    
    def test_query_time_filters(self, runner, setup_test_data):
        """Test time-based filtering."""
        # Query events from last hour
        result = runner.invoke(exo.cli, ['query', '--last', '1h', '--host', 'test-host'])
        
        assert result.exit_code == 0
        # Should find our test events
        assert 'test_filesystem' in result.output
        
        # Query events from last second (should be empty)
        result = runner.invoke(exo.cli, ['query', '--last', '1s', '--host', 'test-host'])
        
        assert result.exit_code == 0
        assert 'No events found' in result.output or 'test_filesystem' not in result.output
    
    def test_schema_list_real(self, runner, setup_test_data):
        """Test schema list with real data."""
        result = runner.invoke(exo.cli, ['schema', 'list'])
        
        assert result.exit_code == 0
        assert 'test_filesystem' in result.output
        assert 'file_created' in result.output
        assert '1.0.0' in result.output
    
    def test_schema_get_real(self, runner, setup_test_data):
        """Test getting a specific schema."""
        result = runner.invoke(exo.cli, ['schema', 'get', 'test_filesystem/file_created'])
        
        assert result.exit_code == 0
        assert 'Test file creation schema' in result.output
        assert '"type": "object"' in result.output
    
    def test_agent_list_real(self, runner, setup_test_data):
        """Test agent list with real data."""
        result = runner.invoke(exo.cli, ['agent', 'list'])
        
        assert result.exit_code == 0
        assert 'test-agent' in result.output
        assert '1.0.0' in result.output
        assert 'stable' in result.output
    
    def test_agent_status_real(self, runner, setup_test_data):
        """Test agent status with real data."""
        result = runner.invoke(exo.cli, ['agent', 'status', 'test-agent'])
        
        assert result.exit_code == 0
        assert 'test-agent' in result.output
        assert 'Test agent for integration tests' in result.output
        assert 'test_filesystem' in result.output
    
    def test_sources_real(self, runner, setup_test_data):
        """Test sources command with real data."""
        result = runner.invoke(exo.cli, ['sources'])
        
        assert result.exit_code == 0
        # Should show our test sources
        assert 'test_filesystem' in result.output or 'Event Sources' in result.output
    
    def test_stats_real(self, runner, setup_test_data):
        """Test stats command with real data."""
        result = runner.invoke(exo.cli, ['stats'])
        
        assert result.exit_code == 0
        assert 'Total Events' in result.output
        # Stats should include our test events
    
    def test_csv_output_real(self, runner, setup_test_data):
        """Test CSV output with real data."""
        result = runner.invoke(exo.cli, [
            'query',
            '--host', 'test-host',
            '--output-format', 'csv',
            '--limit', '5'
        ])
        
        assert result.exit_code == 0
        lines = result.output.strip().split('\n')
        assert len(lines) > 1  # Header + data
        
        # Check CSV structure
        assert 'source' in lines[0]
        assert 'event_type' in lines[0]
        assert 'test_filesystem' in result.output


class TestCLIErrorHandling:
    """Test CLI error handling with database."""
    
    @pytest.fixture
    def runner(self):
        return CliRunner()
    
    def test_invalid_source_query(self, runner, setup_test_data):
        """Test querying with non-existent source."""
        result = runner.invoke(exo.cli, ['query', '--source', 'nonexistent_source'])
        
        assert result.exit_code == 0
        assert 'No events found' in result.output or len(result.output.strip()) < 100
    
    def test_invalid_time_format(self, runner):
        """Test invalid time format."""
        result = runner.invoke(exo.cli, ['query', '--since', 'invalid-date'])
        
        assert result.exit_code != 0
        assert 'Error' in result.output or 'Unable to parse' in result.output
    
    def test_schema_not_found(self, runner):
        """Test getting non-existent schema."""
        result = runner.invoke(exo.cli, ['schema', 'get', 'nonexistent/schema'])
        
        assert result.exit_code == 0
        assert 'not found' in result.output
    
    def test_agent_not_found(self, runner):
        """Test agent status for non-existent agent."""
        result = runner.invoke(exo.cli, ['agent', 'status', 'nonexistent-agent'])
        
        assert result.exit_code == 0
        assert 'not found' in result.output


class TestCLIWithSubprocess:
    """Test CLI by invoking it as a subprocess (most realistic)."""
    
    def setup_method(self):
        """Set up test environment."""
        self.cli_path = Path(__file__).parent.parent.parent.parent / 'cli' / 'exo.py'
        assert self.cli_path.exists(), f"CLI script not found at {self.cli_path}"
    
    def run_cli(self, args):
        """Run CLI as subprocess."""
        cmd = ['python3', str(self.cli_path)] + args
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            env={**os.environ, 'DATABASE_URL': os.environ.get('DATABASE_URL', 'postgresql:///sinex_test')}
        )
        return result
    
    def test_subprocess_query(self, setup_test_data):
        """Test CLI via subprocess."""
        result = self.run_cli(['query', '--host', 'test-host', '--limit', '5'])
        
        assert result.returncode == 0
        assert 'test_filesystem' in result.stdout
    
    def test_subprocess_json_output(self, setup_test_data):
        """Test JSON output via subprocess."""
        result = self.run_cli([
            'query',
            '--source', 'test_filesystem',
            '--output-format', 'json',
            '--limit', '10'
        ])
        
        assert result.returncode == 0
        data = json.loads(result.stdout)
        assert isinstance(data, list)
        assert len(data) > 0
    
    def test_subprocess_error_handling(self):
        """Test error handling via subprocess."""
        result = self.run_cli(['query', '--last', '1x'])  # Invalid time format
        
        assert result.returncode != 0
        assert 'Error' in result.stderr or 'Invalid' in result.stdout


if __name__ == '__main__':
    pytest.main([__file__, '-v'])