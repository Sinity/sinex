#!/usr/bin/env python3
"""
Tests for the Sinex CLI (exo.py)
"""

import os
import sys
import json
import tempfile
from datetime import datetime, timedelta
from unittest import mock
from pathlib import Path

import pytest
from click.testing import CliRunner
import psycopg2
from psycopg2.extras import RealDictCursor

# Add parent directory to path so we can import the CLI
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..', 'cli'))
import exo


class TestTimeParsing:
    """Test time parsing utilities."""
    
    def test_parse_time_delta(self):
        """Test parsing time delta strings."""
        assert exo.parse_time_delta('1h') == timedelta(hours=1)
        assert exo.parse_time_delta('30m') == timedelta(minutes=30)
        assert exo.parse_time_delta('2d') == timedelta(days=2)
        assert exo.parse_time_delta('1w') == timedelta(weeks=1)
        assert exo.parse_time_delta('45s') == timedelta(seconds=45)
        
        with pytest.raises(ValueError):
            exo.parse_time_delta('1x')  # Invalid unit
        
        with pytest.raises(ValueError):
            exo.parse_time_delta('abc')  # No number
    
    def test_parse_datetime(self):
        """Test parsing datetime strings."""
        # Full datetime
        dt = exo.parse_datetime('2025-01-09 14:30:00')
        assert dt.year == 2025
        assert dt.month == 1
        assert dt.day == 9
        assert dt.hour == 14
        assert dt.minute == 30
        assert dt.second == 0
        
        # Date only
        dt = exo.parse_datetime('2025-01-09')
        assert dt.year == 2025
        assert dt.month == 1
        assert dt.day == 9
        assert dt.hour == 0
        
        # Time only (uses today's date)
        dt = exo.parse_datetime('14:30')
        assert dt.hour == 14
        assert dt.minute == 30
        assert dt.date() == datetime.now().date()
        
        # Invalid format
        with pytest.raises(ValueError):
            exo.parse_datetime('not-a-date')


class TestCLICommands:
    """Test CLI commands."""
    
    @pytest.fixture
    def runner(self):
        """Create a CLI test runner."""
        return CliRunner()
    
    @pytest.fixture
    def mock_db(self):
        """Mock database connection."""
        with mock.patch('exo.get_db_connection') as mock_conn:
            mock_cursor = mock.MagicMock()
            mock_conn.return_value.__enter__.return_value.cursor.return_value.__enter__.return_value = mock_cursor
            yield mock_cursor
    
    def test_query_basic(self, runner, mock_db):
        """Test basic query command."""
        # Mock query results
        mock_db.fetchall.return_value = [
            {
                'id': b'test-id',
                'source': 'filesystem',
                'event_type': 'file_created',
                'ts_ingest': datetime.now(),
                'ts_orig': None,
                'host': 'test-host',
                'ingestor_version': '0.1.0',
                'payload_schema_id': None,
                'payload': {'path': '/test/file.txt', 'size': 1024}
            }
        ]
        
        result = runner.invoke(exo.cli, ['query', '--limit', '10'])
        assert result.exit_code == 0
        assert 'filesystem' in result.output
        assert 'file_created' in result.output
    
    def test_query_with_filters(self, runner, mock_db):
        """Test query with various filters."""
        mock_db.fetchall.return_value = []
        
        # Test source filter
        result = runner.invoke(exo.cli, ['query', '--source', 'hyprland'])
        assert result.exit_code == 0
        assert mock_db.execute.called
        
        # Check SQL includes source filter
        sql = mock_db.execute.call_args[0][0]
        assert 'source = %s' in sql
        params = mock_db.execute.call_args[0][1]
        assert 'hyprland' in params
        
        # Test time filter
        result = runner.invoke(exo.cli, ['query', '--last', '1h'])
        assert result.exit_code == 0
        
        # Test combined filters
        result = runner.invoke(exo.cli, [
            'query', 
            '--source', 'kitty',
            '--event-type', 'command_executed',
            '--limit', '5'
        ])
        assert result.exit_code == 0
    
    def test_query_output_formats(self, runner, mock_db):
        """Test different output formats."""
        mock_db.fetchall.return_value = [
            {
                'id': b'test-id',
                'source': 'test',
                'event_type': 'test_event',
                'ts_ingest': datetime.now(),
                'ts_orig': None,
                'host': 'test-host',
                'ingestor_version': '0.1.0',
                'payload_schema_id': None,
                'payload': {'test': 'data'}
            }
        ]
        
        # JSON output
        result = runner.invoke(exo.cli, ['query', '--output-format', 'json'])
        assert result.exit_code == 0
        data = json.loads(result.output)
        assert isinstance(data, list)
        assert len(data) == 1
        assert data[0]['source'] == 'test'
        
        # CSV output
        result = runner.invoke(exo.cli, ['query', '--output-format', 'csv'])
        assert result.exit_code == 0
        assert 'source,event_type' in result.output or 'test,test_event' in result.output
    
    def test_schema_list(self, runner, mock_db):
        """Test schema list command."""
        mock_db.fetchall.return_value = [
            {
                'id': b'schema-id',
                'event_source': 'filesystem',
                'event_type': 'file_created',
                'schema_version': '1.0.0',
                'description': 'File creation event',
                'created_at': datetime.now(),
                'is_active': True
            }
        ]
        
        result = runner.invoke(exo.cli, ['schema', 'list'])
        assert result.exit_code == 0
        assert 'filesystem' in result.output
        assert 'file_created' in result.output
        assert '1.0.0' in result.output
    
    def test_schema_get(self, runner, mock_db):
        """Test schema get command."""
        mock_db.fetchone.return_value = {
            'event_source': 'test',
            'event_type': 'test_event',
            'schema_version': '1.0.0',
            'is_active': True,
            'created_at': datetime.now(),
            'description': 'Test schema',
            'json_schema_definition': {
                'type': 'object',
                'properties': {
                    'test': {'type': 'string'}
                }
            }
        }
        
        # Test by source/type
        result = runner.invoke(exo.cli, ['schema', 'get', 'test/test_event'])
        assert result.exit_code == 0
        assert 'Test schema' in result.output
        
        # Test schema not found
        mock_db.fetchone.return_value = None
        result = runner.invoke(exo.cli, ['schema', 'get', 'nonexistent'])
        assert result.exit_code == 0
        assert 'not found' in result.output
    
    def test_agent_list(self, runner, mock_db):
        """Test agent list command."""
        mock_db.fetchall.return_value = [
            {
                'agent_name': 'unified-collector',
                'description': 'Monitors filesystem changes',
                'version': '0.3.0',
                'status': 'stable',
                'produces_event_types': {
                    'filesystem': ['file_created', 'file_modified']
                },
                'last_seen_heartbeat': datetime.now() - timedelta(minutes=2),
                'registered_at': datetime.now() - timedelta(days=1)
            }
        ]
        
        result = runner.invoke(exo.cli, ['agent', 'list'])
        assert result.exit_code == 0
        assert 'unified-collector' in result.output
        assert '0.3.0' in result.output
        assert 'stable' in result.output
    
    def test_agent_status(self, runner, mock_db):
        """Test agent status command."""
        # Mock agent manifest
        mock_db.fetchone.side_effect = [
            {
                'agent_name': 'test-agent',
                'version': '1.0.0',
                'status': 'stable',
                'description': 'Test agent',
                'registered_at': datetime.now(),
                'produces_event_types': {
                    'test': ['event1', 'event2']
                }
            },
            {'dlq_count': 5}  # DLQ count
        ]
        
        # Mock heartbeats and errors
        mock_db.fetchall.side_effect = [
            # Heartbeats
            [
                {
                    'payload': {
                        'agent_name': 'test-agent',
                        'status': 'running',
                        'uptime_seconds': 3600,
                        'events_processed_session': 1000,
                        'dlq_size': 2
                    },
                    'ts_ingest': datetime.now()
                }
            ],
            # Errors
            [
                {
                    'payload': {
                        'agent_name': 'test-agent',
                        'severity': 'warning',
                        'error_message': 'Connection timeout',
                        'error_context': 'Database connection'
                    },
                    'ts_ingest': datetime.now()
                }
            ]
        ]
        
        result = runner.invoke(exo.cli, ['agent', 'status', 'test-agent'])
        assert result.exit_code == 0
        assert 'test-agent' in result.output
        assert 'stable' in result.output
        assert 'DLQ Events: 5' in result.output
    
    def test_sources_command(self, runner, mock_db):
        """Test sources command."""
        mock_db.fetchall.return_value = [
            {
                'source': 'filesystem',
                'event_count': 10000,
                'event_type_count': 5,
                'host_count': 2,
                'first_event': datetime.now() - timedelta(days=30),
                'last_event': datetime.now(),
                'avg_ingest_delay': 0.125
            }
        ]
        
        result = runner.invoke(exo.cli, ['sources'])
        assert result.exit_code == 0
        assert 'filesystem' in result.output
        assert '10,000' in result.output
        assert '0.12s' in result.output
    
    def test_stats_command(self, runner, mock_db):
        """Test stats command."""
        mock_db.fetchone.return_value = {'total': 50000}
        mock_db.fetchall.side_effect = [
            # Daily counts
            [
                {
                    'day': datetime.now().date(),
                    'count': 5000,
                    'sources': 3
                }
            ],
            # Schema usage
            [
                {
                    'event_source': 'filesystem',
                    'event_type': 'file_created',
                    'schema_version': '1.0.0',
                    'usage_count': 1000
                }
            ],
            # Agent health
            [
                {
                    'agent_name': 'test-agent',
                    'status': 'running',
                    'last_heartbeat': datetime.now()
                }
            ]
        ]
        
        result = runner.invoke(exo.cli, ['stats'])
        assert result.exit_code == 0
        assert '50,000' in result.output
        assert 'Daily Activity' in result.output
        assert 'Agent Health' in result.output


class TestEventSummaryExtraction:
    """Test event summary extraction logic."""
    
    def test_hyprland_summaries(self):
        """Test Hyprland event summary extraction."""
        # Window focused
        summary = exo.extract_event_summary(
            'hyprland',
            'window_focused',
            {
                'app_class': 'firefox',
                'window_title': 'GitHub - Very Long Title That Should Be Truncated'
            }
        )
        assert 'firefox:' in summary
        assert len(summary) <= 60
        
        # Workspace changed
        summary = exo.extract_event_summary(
            'hyprland',
            'workspace_changed',
            {'workspace_id': 3}
        )
        assert summary == 'Workspace 3'
        
        # State snapshot
        summary = exo.extract_event_summary(
            'hyprland',
            'state_snapshot',
            {
                'clients': [1, 2, 3],
                'workspaces': [1, 2]
            }
        )
        assert '3 windows, 2 workspaces' in summary
    
    def test_kitty_summaries(self):
        """Test Kitty terminal event summary extraction."""
        summary = exo.extract_event_summary(
            'terminal.kitty',
            'command_executed',
            {
                'command_string': 'git commit -m "Very long commit message that should be truncated"',
                'exit_code': 0
            }
        )
        assert '[0]' in summary
        assert 'git commit' in summary
        assert len(summary) <= 60
    
    def test_filesystem_summaries(self):
        """Test filesystem event summary extraction."""
        # File created
        summary = exo.extract_event_summary(
            'filesystem',
            'file_created',
            {'path': '/home/user/documents/test.txt'}
        )
        assert summary == 'test.txt'
        
        # File renamed
        summary = exo.extract_event_summary(
            'filesystem',
            'file_renamed',
            {
                'path': '/tmp/old_name.txt',
                'new_path': '/tmp/new_name.txt'
            }
        )
        assert summary == 'old_name.txt → new_name.txt'
    
    def test_sinex_summaries(self):
        """Test Sinex agent event summary extraction."""
        # Heartbeat
        summary = exo.extract_event_summary(
            'sinex',
            'agent.heartbeat',
            {
                'agent_name': 'unified-collector',
                'status': 'running'
            }
        )
        assert summary == 'unified-collector: running'
        
        # Error
        summary = exo.extract_event_summary(
            'sinex',
            'agent.error',
            {
                'agent_name': 'unified-collector',
                'severity': 'warning'
            }
        )
        assert summary == 'unified-collector [warning]'
    
    def test_fallback_summary(self):
        """Test fallback summary extraction."""
        # Unknown event with common fields
        summary = exo.extract_event_summary(
            'unknown',
            'unknown_event',
            {
                'message': 'This is a test message',
                'other_field': 'ignored'
            }
        )
        assert summary == 'This is a test message'
        
        # No recognizable fields
        summary = exo.extract_event_summary(
            'unknown',
            'unknown_event',
            {'random': 'data'}
        )
        assert "{'random': 'data'}" in summary


class TestJQFilter:
    """Test JQ filter functionality."""
    
    @mock.patch('subprocess.run')
    @mock.patch('tempfile.NamedTemporaryFile')
    def test_apply_jq_filter_success(self, mock_temp, mock_run):
        """Test successful JQ filter application."""
        # Mock temp file
        mock_file = mock.MagicMock()
        mock_file.name = '/tmp/test.json'
        mock_temp.return_value.__enter__.return_value = mock_file
        
        # Mock jq output
        mock_run.return_value.stdout = '[{"filtered": "data"}]'
        mock_run.return_value.check = True
        
        events = [
            {'payload': {'original': 'data', 'extra': 'field'}}
        ]
        
        result = exo.apply_jq_filter(events, '.filtered')
        
        assert len(result) == 1
        assert result[0]['payload'] == {'filtered': 'data'}
    
    @mock.patch('subprocess.run')
    def test_apply_jq_filter_error(self, mock_run):
        """Test JQ filter error handling."""
        mock_run.side_effect = Exception("JQ error")
        
        events = [{'payload': {'test': 'data'}}]
        result = exo.apply_jq_filter(events, 'invalid')
        
        # Should return original events on error
        assert result == events


class TestOutputFormats:
    """Test different output format functions."""
    
    def test_output_json(self, capsys):
        """Test JSON output formatting."""
        events = [
            {
                'id': b'\x01\x02\x03',
                'source': 'test',
                'ts_ingest': datetime(2025, 1, 9, 12, 0, 0),
                'ts_orig': None,
                'payload': {'test': 'data'}
            }
        ]
        
        exo.output_json(events)
        captured = capsys.readouterr()
        
        data = json.loads(captured.out)
        assert len(data) == 1
        assert data[0]['source'] == 'test'
        assert data[0]['ts_ingest'] == '2025-01-09T12:00:00'
        assert isinstance(data[0]['id'], str)
    
    def test_output_csv(self, capsys):
        """Test CSV output formatting."""
        events = [
            {
                'source': 'test',
                'event_type': 'test_event',
                'payload': {'test': 'data'}
            }
        ]
        
        exo.output_csv(events)
        captured = capsys.readouterr()
        
        assert 'source,event_type,payload' in captured.out
        assert 'test,test_event' in captured.out
        assert '""test""' in captured.out and '""data""' in captured.out
    
    @mock.patch('yaml.dump')
    def test_output_yaml(self, mock_yaml, capsys):
        """Test YAML output formatting."""
        events = [
            {
                'source': 'test',
                'ts_ingest': datetime(2025, 1, 9, 12, 0, 0)
            }
        ]
        
        exo.output_yaml(events)
        
        # Verify datetime was converted
        assert mock_yaml.called
        call_args = mock_yaml.call_args[0][0]
        assert call_args[0]['ts_ingest'] == '2025-01-09T12:00:00'


class TestErrorHandling:
    """Test error handling in CLI."""
    
    @pytest.fixture
    def runner(self):
        """Create a CLI test runner."""
        return CliRunner()
    
    def test_main_exception_handling(self, runner):
        """Test that CLI handles invalid commands gracefully."""
        # Test with an invalid command
        result = runner.invoke(exo.cli, ['invalid-command'])
        assert result.exit_code != 0
        # Just verify it doesn't crash completely
    
    def test_database_connection_error(self, runner):
        """Test handling of database connection errors."""
        with mock.patch('exo.get_db_connection') as mock_conn:
            mock_conn.side_effect = psycopg2.OperationalError("Connection failed")
            
            result = runner.invoke(exo.cli, ['query'])
            assert result.exit_code != 0
            # The error might be in output or exception, just check exit code for now


if __name__ == '__main__':
    pytest.main([__file__, '-v'])