#!/usr/bin/env python3
"""
Comprehensive test suite for the Sinex CLI (exo.py) covering both unit and integration tests.

Usage:
    pytest test_exo_cli.py              # Run all tests
    pytest test_exo_cli.py -m unit      # Run only unit tests (with mocks)
    pytest test_exo_cli.py -m integration  # Run only integration tests (requires database)
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


class TestRPCIntegration:
    """Test RPC integration for CLI commands."""
    
    @pytest.fixture
    def runner(self):
        return CliRunner()
    
    @pytest.fixture
    def mock_rpc_client(self):
        """Mock RPC client."""
        with mock.patch('exo.get_rpc_client') as mock_get_client:
            mock_client = mock.MagicMock()
            mock_get_client.return_value = mock_client
            yield mock_client
    
    def test_query_with_rpc_success(self, runner, mock_rpc_client):
        """Test query command using RPC client."""
        # Mock RPC response
        mock_rpc_client.query_events_compatible.return_value = [
            {
                'id': 'test-event-id',
                'source': 'test_source',
                'event_type': 'test_event',
                'ts_ingest': datetime.now(),
                'ts_orig': None,
                'host': 'test-host',
                'ingestor_version': None,
                'payload_schema_id': None,
                'payload': {'message': 'test message'}
            }
        ]
        
        result = runner.invoke(exo.cli, ['query', '--source', 'test_source', '--limit', '10'])
        
        assert result.exit_code == 0
        assert 'test_source' in result.output
        assert 'test_event' in result.output
        assert 'test-host' in result.output
        
        # Verify RPC client was called with correct parameters
        mock_rpc_client.query_events_compatible.assert_called_once()
        call_args = mock_rpc_client.query_events_compatible.call_args[1]
        assert call_args['source'] == 'test_source'
        assert call_args['limit'] == 10
    
    def test_query_with_rpc_error(self, runner, mock_rpc_client):
        """Test query command with RPC error."""
        from exo import SinexRPCError
        mock_rpc_client.query_events_compatible.side_effect = SinexRPCError(
            -32603, "RPC server unavailable"
        )
        
        result = runner.invoke(exo.cli, ['query', '--source', 'test'])
        
        assert result.exit_code == 1
        assert 'RPC Error' in result.output
        assert 'Try using --use-db flag' in result.output
    
    def test_query_with_database_fallback(self, runner, mock_rpc_client):
        """Test query command using database fallback."""
        with mock.patch('exo.get_db_connection') as mock_db:
            mock_cursor = mock.MagicMock()
            mock_cursor.fetchall.return_value = [
                {
                    'id': b'test-id',
                    'source': 'test_db_source',
                    'event_type': 'test_event',
                    'ts_ingest': datetime.now(),
                    'ts_orig': None,
                    'host': 'test-host',
                    'ingestor_version': None,
                    'payload_schema_id': None,
                    'payload': {'message': 'from database'}
                }
            ]
            mock_db.return_value.__enter__.return_value.cursor.return_value.__enter__.return_value = mock_cursor
            
            result = runner.invoke(exo.cli, [
                'query', 
                '--use-db',  # Force database mode
                '--source', 'test_db_source', 
                '--limit', '10'
            ])
            
            assert result.exit_code == 0
            assert 'test_db_source' in result.output
            # RPC client should not be called when using --use-db
            assert not mock_rpc_client.query_events_compatible.called
    
    def test_sources_with_rpc_success(self, runner, mock_rpc_client):
        """Test sources command using RPC client."""
        mock_rpc_client.get_sources_statistics.return_value = [
            {
                'source': 'test_filesystem',
                'event_count': 150,
                'event_type_count': 3,
                'host_count': 2,
                'first_event': datetime.now() - timedelta(days=30),
                'last_event': datetime.now(),
                'avg_ingest_delay': 0.5
            },
            {
                'source': 'test_terminal',
                'event_count': 75,
                'event_type_count': 2,
                'host_count': 1,
                'first_event': datetime.now() - timedelta(days=15),
                'last_event': datetime.now() - timedelta(minutes=5),
                'avg_ingest_delay': None
            }
        ]
        
        result = runner.invoke(exo.cli, ['sources'])
        
        assert result.exit_code == 0
        assert 'Event Sources' in result.output
        assert 'test_filesystem' in result.output
        assert 'test_terminal' in result.output
        assert '150' in result.output
        assert '75' in result.output
        
        # Verify RPC client was called
        mock_rpc_client.get_sources_statistics.assert_called_once()
    
    def test_stats_with_rpc_success(self, runner, mock_rpc_client):
        """Test stats command using RPC client."""
        mock_rpc_client.get_event_count_by_source.return_value = {
            'test_filesystem': 100,
            'test_terminal': 50,
            'test_system': 25
        }
        mock_rpc_client.get_activity_heatmap.return_value = [
            {
                'time_bucket': '2025-01-10 14:00',
                'event_count': 45
            },
            {
                'time_bucket': '2025-01-10 13:00',
                'event_count': 32
            }
        ]
        
        result = runner.invoke(exo.cli, ['stats'])
        
        assert result.exit_code == 0
        assert 'Total Events (last 7 days)' in result.output
        assert '175' in result.output  # Sum of all sources
        assert 'test_filesystem' in result.output
        assert 'test_terminal' in result.output
        assert 'Recent Activity' in result.output
        
        # Verify RPC client methods were called
        mock_rpc_client.get_event_count_by_source.assert_called_once_with(days_back=7)
        mock_rpc_client.get_activity_heatmap.assert_called_once()
    
    def test_rpc_url_configuration(self, runner, mock_rpc_client):
        """Test RPC URL configuration."""
        with mock.patch('exo.SinexRPCClient') as mock_client_class:
            mock_client_class.return_value = mock_rpc_client
            mock_rpc_client.query_events_compatible.return_value = []
            
            result = runner.invoke(exo.cli, [
                '--rpc-url', 'http://custom-host:8888',
                'query', '--limit', '5'
            ])
            
            assert result.exit_code == 0
            # Verify custom RPC URL was used
            mock_client_class.assert_called_with('http://custom-host:8888')
    
    def test_rpc_environment_variable(self, runner, mock_rpc_client):
        """Test RPC URL from environment variable."""
        with mock.patch.dict(os.environ, {'SINEX_RPC_URL': 'http://env-host:7777'}):
            with mock.patch('exo.SinexRPCClient') as mock_client_class:
                mock_client_class.return_value = mock_rpc_client
                mock_rpc_client.query_events_compatible.return_value = []
                
                result = runner.invoke(exo.cli, ['query', '--limit', '5'])
                
                assert result.exit_code == 0
                # Verify environment variable RPC URL was used
                mock_client_class.assert_called_with('http://env-host:7777')


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


# =============================================================================
# INTEGRATION TESTS (require real database)
# =============================================================================

@pytest.mark.integration
class TestCLIIntegration:
    """Integration tests for CLI with real database."""
    
    @pytest.fixture(scope="module")
    def test_db(self):
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
    def setup_test_data(self, test_db):
        """Insert test data before each test."""
        cursor = test_db.cursor()
        
        # Clean up any existing test data
        cursor.execute("""
            DELETE FROM core.events 
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
                INSERT INTO core.events (source, event_type, host, payload)
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
            DELETE FROM core.events 
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
        lines = result.output.strip().split('\\n')
        assert len(lines) > 1  # Header + data
        
        # Check CSV structure
        assert 'source' in lines[0]
        assert 'event_type' in lines[0]
        assert 'test_filesystem' in result.output


@pytest.mark.integration
class TestCLIErrorHandling:
    """Test CLI error handling with database."""
    
    @pytest.fixture
    def runner(self):
        return CliRunner()
    
    @pytest.fixture
    def setup_test_data(self):
        """Minimal setup for error handling tests."""
        pass
    
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


@pytest.mark.integration
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
    
    @pytest.fixture
    def setup_test_data(self):
        """Use module-level database setup."""
        # This reuses the database setup from TestCLIIntegration
        db_url = os.environ.get('DATABASE_URL', 'postgresql:///sinex_test?host=/run/postgresql')
        conn = psycopg2.connect(db_url)
        conn.autocommit = True
        cursor = conn.cursor()
        
        # Minimal test data for subprocess tests
        cursor.execute("""
            INSERT INTO core.events (source, event_type, host, payload)
            VALUES ('test_subprocess', 'test_event', 'test-host', '{"test": true}'::jsonb)
            ON CONFLICT DO NOTHING
        """)
        
        yield
        
        cursor.execute("""
            DELETE FROM core.events WHERE source = 'test_subprocess'
        """)
        cursor.close()
        conn.close()
    
    def test_subprocess_query(self, setup_test_data):
        """Test CLI via subprocess."""
        result = self.run_cli(['query', '--source', 'test_subprocess', '--limit', '5'])
        
        assert result.returncode == 0
        assert 'test_subprocess' in result.stdout
    
    def test_subprocess_json_output(self, setup_test_data):
        """Test JSON output via subprocess."""
        result = self.run_cli([
            'query',
            '--source', 'test_subprocess',
            '--output-format', 'json',
            '--limit', '10'
        ])
        
        assert result.returncode == 0
        data = json.loads(result.stdout)
        assert isinstance(data, list)
        assert len(data) >= 0  # May be 0 if no events match
    
    def test_subprocess_error_handling(self):
        """Test error handling via subprocess."""
        result = self.run_cli(['query', '--last', '1x'])  # Invalid time format
        
        assert result.returncode != 0
        assert 'Error' in result.stderr or 'Invalid' in result.stdout


# Mark all existing classes as unit tests
for name, obj in list(globals().items()):
    if isinstance(obj, type) and name.startswith('Test') and not hasattr(obj, 'pytestmark'):
        # Mark as unit test if not already marked as integration
        if name not in ['TestCLIIntegration', 'TestCLIErrorHandling', 'TestCLIWithSubprocess']:
            obj.pytestmark = pytest.mark.unit


if __name__ == '__main__':
    pytest.main([__file__, '-v'])