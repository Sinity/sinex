#!/usr/bin/env python3
"""
Comprehensive test suite for the Sinex CLI (exo.py) covering all gaps and edge cases.
"""

import os
import sys
import json
import tempfile
import csv
import io
import subprocess
from datetime import datetime, timedelta
from unittest import mock
from pathlib import Path

import pytest
import yaml
from click.testing import CliRunner
import psycopg2
from psycopg2.extras import RealDictCursor

# Add CLI directory to path so we can import exo
cli_dir = os.path.join(os.path.dirname(__file__), '..', '..', '..', 'cli')
sys.path.insert(0, cli_dir)
import exo


class TestDLQCommands:
    """Test Dead Letter Queue commands."""
    
    @pytest.fixture
    def runner(self):
        return CliRunner()
    
    @pytest.fixture
    def mock_db(self):
        """Mock database connection."""
        with mock.patch('exo.get_db_connection') as mock_conn:
            mock_cursor = mock.MagicMock()
            mock_conn.return_value.__enter__.return_value.cursor.return_value.__enter__.return_value = mock_cursor
            yield mock_cursor
    
    def test_dlq_list_basic(self, runner, mock_db):
        """Test basic DLQ list command."""
        mock_db.fetchall.return_value = [
            {
                'id': b'test-dlq-id',
                'agent_name': 'test-agent',
                'source': 'test-source',
                'event_type': 'test-event',
                'failure_reason': 'Validation failed',
                'retry_count': 2,
                'created_at': datetime.now(),
                'last_retry_at': datetime.now() - timedelta(hours=1),
                'payload': {'test': 'data'}
            }
        ]
        
        result = runner.invoke(exo.cli, ['dlq', 'list'])
        assert result.exit_code == 0
        assert 'test-agent' in result.output
        assert 'test-source' in result.output
        assert 'Validation failed' in result.output
    
    def test_dlq_list_with_filters(self, runner, mock_db):
        """Test DLQ list with filters."""
        mock_db.fetchall.return_value = []
        
        result = runner.invoke(exo.cli, [
            'dlq', 'list',
            '--agent', 'test-agent',
            '--source', 'test-source',
            '--event-type', 'test-event',
            '--limit', '10'
        ])
        assert result.exit_code == 0
        
        # Verify SQL filters were applied
        assert mock_db.execute.called
        sql = mock_db.execute.call_args[0][0]
        assert 'agent_name = %s' in sql
        assert 'source = %s' in sql
        assert 'event_type = %s' in sql
    
    def test_dlq_show(self, runner, mock_db):
        """Test DLQ show command."""
        mock_db.fetchone.return_value = {
            'id': b'test-dlq-id',
            'agent_name': 'test-agent',
            'source': 'test-source',
            'event_type': 'test-event',
            'failure_reason': 'Schema validation failed',
            'retry_count': 3,
            'created_at': datetime.now(),
            'last_retry_at': datetime.now() - timedelta(minutes=30),
            'payload': {'test': 'data', 'complex': {'nested': 'value'}}
        }
        
        result = runner.invoke(exo.cli, ['dlq', 'show', 'test-dlq-id'])
        assert result.exit_code == 0
        assert 'test-agent' in result.output
        assert 'Schema validation failed' in result.output
        assert 'Retry Count: 3' in result.output
    
    def test_dlq_show_not_found(self, runner, mock_db):
        """Test DLQ show for non-existent ID."""
        mock_db.fetchone.return_value = None
        
        result = runner.invoke(exo.cli, ['dlq', 'show', 'nonexistent-id'])
        assert result.exit_code == 0
        assert 'not found' in result.output
    
    def test_dlq_retry(self, runner, mock_db):
        """Test DLQ retry command."""
        mock_db.fetchone.return_value = {
            'id': b'test-dlq-id',
            'agent_name': 'test-agent',
            'source': 'test-source',
            'event_type': 'test-event',
            'payload': {'test': 'data'}
        }
        
        result = runner.invoke(exo.cli, ['dlq', 'retry', 'test-dlq-id'])
        assert result.exit_code == 0
        assert 'Retrying' in result.output or 'reprocessing' in result.output
    
    def test_dlq_retry_dry_run(self, runner, mock_db):
        """Test DLQ retry in dry-run mode."""
        mock_db.fetchone.return_value = {
            'id': b'test-dlq-id',
            'agent_name': 'test-agent',
            'source': 'test-source',
            'event_type': 'test-event',
            'payload': {'test': 'data'}
        }
        
        result = runner.invoke(exo.cli, ['dlq', 'retry', 'test-dlq-id', '--dry-run'])
        assert result.exit_code == 0
        assert 'DRY RUN' in result.output or 'would' in result.output
    
    def test_dlq_resolve(self, runner, mock_db):
        """Test DLQ resolve command."""
        mock_db.fetchone.return_value = {
            'id': b'test-dlq-id',
            'agent_name': 'test-agent'
        }
        
        result = runner.invoke(exo.cli, [
            'dlq', 'resolve', 'test-dlq-id', 'fixed-upstream'
        ])
        assert result.exit_code == 0
        assert 'Resolved' in result.output or 'marked' in result.output
    
    def test_dlq_stats(self, runner, mock_db):
        """Test DLQ stats command."""
        mock_db.fetchall.return_value = [
            {
                'agent_name': 'test-agent',
                'total_dlq': 50,
                'avg_retry_count': 2.5,
                'oldest_dlq': datetime.now() - timedelta(days=7),
                'common_failures': ['Schema validation', 'Network timeout']
            }
        ]
        
        result = runner.invoke(exo.cli, ['dlq', 'stats', '--days', '30'])
        assert result.exit_code == 0
        assert 'test-agent' in result.output
        assert '50' in result.output
    
    def test_dlq_purge(self, runner, mock_db):
        """Test DLQ purge command."""
        mock_db.fetchone.return_value = {'count': 25}
        
        result = runner.invoke(exo.cli, [
            'dlq', 'purge',
            '--agent', 'test-agent',
            '--category', 'resolved',
            '--older-than', '30d',
            '--dry-run'
        ])
        assert result.exit_code == 0
        assert 'DRY RUN' in result.output
        assert '25' in result.output


class TestBlobCommands:
    """Test blob storage commands."""
    
    @pytest.fixture
    def runner(self):
        return CliRunner()
    
    @pytest.fixture
    def mock_db(self):
        """Mock database connection."""
        with mock.patch('exo.get_db_connection') as mock_conn:
            mock_cursor = mock.MagicMock()
            mock_conn.return_value.__enter__.return_value.cursor.return_value.__enter__.return_value = mock_cursor
            yield mock_cursor
    
    def test_blob_list_basic(self, runner, mock_db):
        """Test basic blob list command."""
        mock_db.fetchall.return_value = [
            {
                'id': b'test-blob-id',
                'file_path': '/test/file.txt',
                'description': 'Test file',
                'mime_type': 'text/plain',
                'file_size': 1024,
                'created_at': datetime.now(),
                'annex_key': 'SHA256E-s1024--abc123'
            }
        ]
        
        result = runner.invoke(exo.cli, ['blob', 'list'])
        assert result.exit_code == 0
        assert 'test-blob-id' in result.output or 'file.txt' in result.output
        assert 'Test file' in result.output
    
    def test_blob_list_with_mime_filter(self, runner, mock_db):
        """Test blob list with MIME type filter."""
        mock_db.fetchall.return_value = []
        
        result = runner.invoke(exo.cli, [
            'blob', 'list',
            '--mime-type', 'image/jpeg',
            '--limit', '20'
        ])
        assert result.exit_code == 0
        
        # Verify MIME type filter
        assert mock_db.execute.called
        sql = mock_db.execute.call_args[0][0]
        assert 'mime_type = %s' in sql
    
    def test_blob_ingest_git_annex_not_available(self, runner, mock_db):
        """Test blob ingest when git-annex is not available."""
        with mock.patch('subprocess.run') as mock_run:
            mock_run.side_effect = FileNotFoundError("git-annex not found")
            
            result = runner.invoke(exo.cli, [
                'blob', 'ingest',
                '/nonexistent/file.txt',
                '--description', 'Test file',
                '--annex-repo', '/test/repo'
            ])
            assert result.exit_code != 0
            assert 'git-annex' in result.output
    
    def test_blob_ingest_file_not_found(self, runner, mock_db):
        """Test blob ingest with non-existent file."""
        result = runner.invoke(exo.cli, [
            'blob', 'ingest',
            '/definitely/nonexistent/file.txt',
            '--description', 'Test file',
            '--annex-repo', '/test/repo'
        ])
        assert result.exit_code != 0
        assert 'not found' in result.output or 'does not exist' in result.output
    
    def test_blob_get_not_found(self, runner, mock_db):
        """Test blob get for non-existent ID."""
        mock_db.fetchone.return_value = None
        
        result = runner.invoke(exo.cli, ['blob', 'get', 'nonexistent-id'])
        assert result.exit_code == 0
        assert 'not found' in result.output
    
    def test_blob_verify_no_annex(self, runner, mock_db):
        """Test blob verify when git-annex is not available."""
        with mock.patch('subprocess.run') as mock_run:
            mock_run.side_effect = FileNotFoundError("git-annex not found")
            
            result = runner.invoke(exo.cli, [
                'blob', 'verify',
                '--annex-repo', '/test/repo'
            ])
            assert result.exit_code != 0
            assert 'git-annex' in result.output


class TestStatsCommandEdgeCases:
    """Test stats command edge cases."""
    
    @pytest.fixture
    def runner(self):
        return CliRunner()
    
    @pytest.fixture
    def mock_db(self):
        """Mock database connection."""
        with mock.patch('exo.get_db_connection') as mock_conn:
            mock_cursor = mock.MagicMock()
            mock_conn.return_value.__enter__.return_value.cursor.return_value.__enter__.return_value = mock_cursor
            yield mock_cursor
    
    def test_stats_empty_database(self, runner, mock_db):
        """Test stats with empty database."""
        mock_db.fetchone.return_value = {'total': 0}
        mock_db.fetchall.return_value = []
        
        result = runner.invoke(exo.cli, ['stats'])
        assert result.exit_code == 0
        assert 'Total Events: 0' in result.output
    
    def test_stats_with_time_filter(self, runner, mock_db):
        """Test stats with time period filter."""
        mock_db.fetchone.return_value = {'total': 1000}
        mock_db.fetchall.side_effect = [
            [{'day': datetime.now().date(), 'count': 100, 'sources': 3}],
            [{'event_source': 'test', 'event_type': 'test', 'usage_count': 50}],
            [{'agent_name': 'test', 'status': 'running', 'last_heartbeat': datetime.now()}]
        ]
        
        result = runner.invoke(exo.cli, ['stats', '--days', '7'])
        assert result.exit_code == 0
        assert '1,000' in result.output
    
    def test_stats_database_error(self, runner, mock_db):
        """Test stats command with database error."""
        mock_db.fetchone.side_effect = psycopg2.Error("Database error")
        
        result = runner.invoke(exo.cli, ['stats'])
        assert result.exit_code != 0


class TestQueryCommandEdgeCases:
    """Test query command edge cases and complex scenarios."""
    
    @pytest.fixture
    def runner(self):
        return CliRunner()
    
    @pytest.fixture
    def mock_db(self):
        """Mock database connection."""
        with mock.patch('exo.get_db_connection') as mock_conn:
            mock_cursor = mock.MagicMock()
            mock_conn.return_value.__enter__.return_value.cursor.return_value.__enter__.return_value = mock_cursor
            yield mock_cursor
    
    def test_query_with_complex_payload_jq(self, runner, mock_db):
        """Test query with complex JQ filter."""
        mock_db.fetchall.return_value = [
            {
                'id': b'test-id',
                'source': 'test',
                'event_type': 'test',
                'ts_ingest': datetime.now(),
                'payload': {'nested': {'value': 'target'}}
            }
        ]
        
        with mock.patch('subprocess.run') as mock_run:
            mock_run.return_value.stdout = '[{"filtered": "data"}]'
            
            result = runner.invoke(exo.cli, [
                'query',
                '--payload-jq', '.nested.value',
                '--output-format', 'json'
            ])
            assert result.exit_code == 0
    
    def test_query_time_range_validation(self, runner, mock_db):
        """Test query with invalid time range."""
        mock_db.fetchall.return_value = []
        
        result = runner.invoke(exo.cli, [
            'query',
            '--since', '2025-01-10',
            '--until', '2025-01-09'  # Until is before since
        ])
        assert result.exit_code == 0  # Should succeed but return empty results
    
    def test_query_very_large_limit(self, runner, mock_db):
        """Test query with very large limit."""
        mock_db.fetchall.return_value = []
        
        result = runner.invoke(exo.cli, ['query', '--limit', '1000000'])
        assert result.exit_code == 0
        
        # Verify limit is applied
        assert mock_db.execute.called
        sql = mock_db.execute.call_args[0][0]
        assert 'LIMIT' in sql
    
    def test_query_zero_limit(self, runner, mock_db):
        """Test query with zero limit."""
        mock_db.fetchall.return_value = []
        
        result = runner.invoke(exo.cli, ['query', '--limit', '0'])
        assert result.exit_code == 0
        assert 'No events found' in result.output
    
    def test_query_negative_limit(self, runner, mock_db):
        """Test query with negative limit."""
        result = runner.invoke(exo.cli, ['query', '--limit', '-5'])
        assert result.exit_code != 0  # Should fail validation
    
    def test_query_unicode_in_filters(self, runner, mock_db):
        """Test query with Unicode characters in filters."""
        mock_db.fetchall.return_value = []
        
        result = runner.invoke(exo.cli, [
            'query',
            '--source', 'test-🚀',
            '--event-type', 'event-💡'
        ])
        assert result.exit_code == 0
    
    def test_query_special_characters_in_host(self, runner, mock_db):
        """Test query with special characters in host filter."""
        mock_db.fetchall.return_value = []
        
        result = runner.invoke(exo.cli, [
            'query',
            '--host', 'test-host.example.com'
        ])
        assert result.exit_code == 0


class TestOutputFormatEdgeCases:
    """Test output format edge cases."""
    
    @pytest.fixture
    def runner(self):
        return CliRunner()
    
    @pytest.fixture
    def mock_db(self):
        """Mock database connection."""
        with mock.patch('exo.get_db_connection') as mock_conn:
            mock_cursor = mock.MagicMock()
            mock_conn.return_value.__enter__.return_value.cursor.return_value.__enter__.return_value = mock_cursor
            yield mock_cursor
    
    def test_json_output_with_none_values(self, runner, mock_db):
        """Test JSON output with None values."""
        mock_db.fetchall.return_value = [
            {
                'id': b'test-id',
                'source': 'test',
                'event_type': 'test',
                'ts_ingest': datetime.now(),
                'ts_orig': None,
                'payload': {'key': None, 'value': 'test'}
            }
        ]
        
        result = runner.invoke(exo.cli, ['query', '--output-format', 'json'])
        assert result.exit_code == 0
        
        data = json.loads(result.output)
        assert data[0]['ts_orig'] is None
        assert data[0]['payload']['key'] is None
    
    def test_csv_output_with_special_characters(self, runner, mock_db):
        """Test CSV output with special characters."""
        mock_db.fetchall.return_value = [
            {
                'source': 'test,source',
                'event_type': 'test"event',
                'payload': {'message': 'Hello,\n"World"'}
            }
        ]
        
        result = runner.invoke(exo.cli, ['query', '--output-format', 'csv'])
        assert result.exit_code == 0
        
        # Parse CSV to verify proper escaping
        csv_reader = csv.DictReader(io.StringIO(result.output))
        rows = list(csv_reader)
        assert len(rows) == 1
        assert rows[0]['source'] == 'test,source'
        assert rows[0]['event_type'] == 'test"event'
    
    def test_yaml_output_with_complex_data(self, runner, mock_db):
        """Test YAML output with complex nested data."""
        mock_db.fetchall.return_value = [
            {
                'id': b'test-id',
                'source': 'test',
                'ts_ingest': datetime.now(),
                'payload': {
                    'nested': {
                        'array': [1, 2, 3],
                        'boolean': True,
                        'null_value': None
                    }
                }
            }
        ]
        
        result = runner.invoke(exo.cli, ['query', '--output-format', 'yaml'])
        assert result.exit_code == 0
        
        # Verify YAML can be parsed
        data = yaml.safe_load(result.output)
        assert isinstance(data, list)
        assert data[0]['payload']['nested']['array'] == [1, 2, 3]
        assert data[0]['payload']['nested']['boolean'] is True
        assert data[0]['payload']['nested']['null_value'] is None
    
    def test_table_output_with_very_long_text(self, runner, mock_db):
        """Test table output with very long text."""
        long_text = "x" * 1000
        mock_db.fetchall.return_value = [
            {
                'id': b'test-id',
                'source': 'test',
                'event_type': 'test',
                'ts_ingest': datetime.now(),
                'payload': {'long_field': long_text}
            }
        ]
        
        result = runner.invoke(exo.cli, ['query', '--output-format', 'table'])
        assert result.exit_code == 0
        # Should handle long text gracefully (truncate or format appropriately)
        assert len(result.output) > 0


class TestEventSummaryEdgeCases:
    """Test event summary extraction edge cases."""
    
    def test_summary_with_empty_payload(self):
        """Test summary extraction with empty payload."""
        summary = exo.extract_event_summary('test', 'test_event', {})
        assert summary == "{}"
    
    def test_summary_with_very_long_values(self):
        """Test summary extraction with very long values."""
        long_value = "x" * 200
        summary = exo.extract_event_summary(
            'test', 'test_event', 
            {'message': long_value}
        )
        assert len(summary) <= 60  # Should be truncated
        assert summary.endswith('...')
    
    def test_summary_with_non_string_values(self):
        """Test summary extraction with non-string values."""
        summary = exo.extract_event_summary(
            'test', 'test_event',
            {'count': 42, 'active': True, 'data': [1, 2, 3]}
        )
        assert "42" in summary or "True" in summary or "[1, 2, 3]" in summary
    
    def test_summary_with_unicode_characters(self):
        """Test summary extraction with Unicode characters."""
        summary = exo.extract_event_summary(
            'test', 'test_event',
            {'message': 'Hello 🌍 World! 🚀'}
        )
        assert '🌍' in summary
        assert '🚀' in summary
    
    def test_summary_hyprland_edge_cases(self):
        """Test Hyprland summary extraction edge cases."""
        # Very long window title
        long_title = "x" * 200
        summary = exo.extract_event_summary(
            'hyprland', 'window_focused',
            {'app_class': 'test', 'window_title': long_title}
        )
        assert len(summary) <= 60
        assert summary.startswith('test:')
        
        # Missing app_class
        summary = exo.extract_event_summary(
            'hyprland', 'window_focused',
            {'window_title': 'Test Window'}
        )
        assert 'Test Window' in summary
        
        # State snapshot with edge cases
        summary = exo.extract_event_summary(
            'hyprland', 'state_snapshot',
            {'clients': [], 'workspaces': []}
        )
        assert '0 windows, 0 workspaces' in summary
    
    def test_summary_filesystem_edge_cases(self):
        """Test filesystem summary extraction edge cases."""
        # Very long path
        long_path = "/very/long/path/" + "x" * 100 + "/file.txt"
        summary = exo.extract_event_summary(
            'filesystem', 'file_created',
            {'path': long_path}
        )
        assert 'file.txt' in summary
        
        # Path without filename
        summary = exo.extract_event_summary(
            'filesystem', 'file_created',
            {'path': '/tmp/'}
        )
        assert 'tmp' in summary or '/' in summary


class TestTimezoneHandling:
    """Test timezone handling edge cases."""
    
    @pytest.fixture
    def runner(self):
        return CliRunner()
    
    @pytest.fixture
    def mock_db(self):
        """Mock database connection."""
        with mock.patch('exo.get_db_connection') as mock_conn:
            mock_cursor = mock.MagicMock()
            mock_conn.return_value.__enter__.return_value.cursor.return_value.__enter__.return_value = mock_cursor
            yield mock_cursor
    
    def test_datetime_parsing_edge_cases(self):
        """Test datetime parsing edge cases."""
        # Test various valid formats
        dt = exo.parse_datetime('2025-01-09')
        assert dt.year == 2025
        assert dt.month == 1
        assert dt.day == 9
        
        dt = exo.parse_datetime('14:30')
        assert dt.hour == 14
        assert dt.minute == 30
        
        dt = exo.parse_datetime('2025-01-09 14:30')
        assert dt.year == 2025
        assert dt.hour == 14
        
        # Test invalid formats
        with pytest.raises(ValueError):
            exo.parse_datetime('invalid-date')
        
        with pytest.raises(ValueError):
            exo.parse_datetime('2025-13-01')  # Invalid month
        
        with pytest.raises(ValueError):
            exo.parse_datetime('25:00')  # Invalid hour
    
    def test_time_delta_parsing_edge_cases(self):
        """Test time delta parsing edge cases."""
        # Valid cases
        assert exo.parse_time_delta('1s') == timedelta(seconds=1)
        assert exo.parse_time_delta('0m') == timedelta(minutes=0)
        assert exo.parse_time_delta('1000d') == timedelta(days=1000)
        
        # Invalid cases
        with pytest.raises(ValueError):
            exo.parse_time_delta('1x')  # Invalid unit
        
        with pytest.raises(ValueError):
            exo.parse_time_delta('s')  # No number
        
        with pytest.raises(ValueError):
            exo.parse_time_delta('1.5h')  # Float number


class TestErrorHandlingComprehensive:
    """Test comprehensive error handling scenarios."""
    
    @pytest.fixture
    def runner(self):
        return CliRunner()
    
    def test_database_connection_timeout(self, runner):
        """Test database connection timeout."""
        with mock.patch('exo.get_db_connection') as mock_conn:
            mock_conn.side_effect = psycopg2.OperationalError("Connection timeout")
            
            result = runner.invoke(exo.cli, ['query'])
            assert result.exit_code != 0
    
    def test_database_permission_error(self, runner):
        """Test database permission error."""
        with mock.patch('exo.get_db_connection') as mock_conn:
            mock_conn.side_effect = psycopg2.Error("Permission denied")
            
            result = runner.invoke(exo.cli, ['stats'])
            assert result.exit_code != 0
    
    def test_interrupted_query(self, runner):
        """Test interrupted query (KeyboardInterrupt)."""
        with mock.patch('exo.get_db_connection') as mock_conn:
            mock_conn.return_value.__enter__.return_value.cursor.return_value.__enter__.return_value.fetchall.side_effect = KeyboardInterrupt()
            
            result = runner.invoke(exo.cli, ['query'])
            assert result.exit_code != 0
    
    def test_invalid_json_in_payload(self, runner):
        """Test handling of invalid JSON in payload."""
        with mock.patch('exo.get_db_connection') as mock_conn:
            mock_cursor = mock.MagicMock()
            mock_cursor.fetchall.return_value = [
                {
                    'id': b'test-id',
                    'source': 'test',
                    'event_type': 'test',
                    'ts_ingest': datetime.now(),
                    'payload': 'invalid-json'  # Not a dict
                }
            ]
            mock_conn.return_value.__enter__.return_value.cursor.return_value.__enter__.return_value = mock_cursor
            
            result = runner.invoke(exo.cli, ['query', '--output-format', 'json'])
            # Should handle gracefully
            assert result.exit_code == 0
    
    def test_memory_error_large_result(self, runner):
        """Test memory error with large result set."""
        with mock.patch('exo.get_db_connection') as mock_conn:
            mock_cursor = mock.MagicMock()
            mock_cursor.fetchall.side_effect = MemoryError("Not enough memory")
            mock_conn.return_value.__enter__.return_value.cursor.return_value.__enter__.return_value = mock_cursor
            
            result = runner.invoke(exo.cli, ['query', '--limit', '1000000'])
            assert result.exit_code != 0


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
        
        # Time only (uses today's date)
        dt = exo.parse_datetime('14:30')
        assert dt.hour == 14
        assert dt.minute == 30
        assert dt.date() == datetime.now().date()
        
        # Invalid format
        with pytest.raises(ValueError):
            exo.parse_datetime('not-a-date')


if __name__ == '__main__':
    pytest.main([__file__, '-v'])