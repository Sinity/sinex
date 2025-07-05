#!/usr/bin/env python3
"""
Test coverage for previously untested exo CLI commands:
- blob management (git-annex integration)
- DLQ (Dead Letter Queue) management  
- stats command
- Additional utility functions
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

# Add CLI directory to path so we can import exo
cli_dir = os.path.join(os.path.dirname(__file__), '..', '..', '..', 'cli')
sys.path.insert(0, cli_dir)
import exo


class TestBlobCommands:
    """Test blob management commands (git-annex integration)."""
    
    @pytest.fixture
    def runner(self):
        return CliRunner()
    
    @pytest.fixture
    def mock_db(self):
        with mock.patch('exo.get_db_connection') as mock_conn:
            mock_cursor = mock.MagicMock()
            mock_conn.return_value.__enter__.return_value.cursor.return_value.__enter__.return_value = mock_cursor
            yield mock_cursor
            
    def test_blob_ingest_success(self, runner, mock_db):
        """Test successful blob ingestion."""
        # Create a temporary file to ingest
        with tempfile.NamedTemporaryFile(mode='w', delete=False) as f:
            f.write("test file content")
            temp_path = f.name
            
        try:
            # Mock git-annex commands
            with mock.patch('subprocess.run') as mock_run:
                mock_run.return_value.returncode = 0
                mock_run.return_value.stdout = "add test.txt ok"
                
                # Mock database insertion
                mock_db.execute.return_value = None
                
                result = runner.invoke(exo.cli, [
                    'blob', 'ingest', 
                    temp_path,
                    '--description', 'Test file',
                    '--annex-repo', '/tmp/test-repo'
                ])
                
                assert result.exit_code == 0
                assert 'Successfully ingested' in result.output
                assert mock_run.called
                assert mock_db.execute.called
                
        finally:
            os.unlink(temp_path)
            
    def test_blob_ingest_missing_file(self, runner):
        """Test blob ingestion with missing file."""
        result = runner.invoke(exo.cli, [
            'blob', 'ingest', 
            '/nonexistent/file.txt',
            '--annex-repo', '/tmp/test-repo'
        ])
        
        assert result.exit_code != 0
        assert 'does not exist' in result.output
        
    def test_blob_list(self, runner, mock_db):
        """Test blob listing."""
        # Mock blob data
        mock_db.fetchall.return_value = [
            {
                'id': b'blob-1',
                'original_path': '/home/user/document.pdf',
                'mime_type': 'application/pdf',
                'size_bytes': 1024000,
                'ingested_at': datetime.now(),
                'description': 'Important document',
                'annex_key': 'SHA256E-s1024000--abc123.pdf'
            },
            {
                'id': b'blob-2',
                'original_path': '/home/user/image.jpg',
                'mime_type': 'image/jpeg',
                'size_bytes': 512000,
                'ingested_at': datetime.now() - timedelta(days=1),
                'description': 'Profile picture',
                'annex_key': 'SHA256E-s512000--def456.jpg'
            }
        ]
        
        result = runner.invoke(exo.cli, ['blob', 'list', '--limit', '10'])
        
        assert result.exit_code == 0
        assert 'document.pdf' in result.output
        assert 'image.jpg' in result.output
        assert 'application/pdf' in result.output
        assert 'Important document' in result.output
        
    def test_blob_list_with_mime_filter(self, runner, mock_db):
        """Test blob listing with MIME type filter."""
        mock_db.fetchall.return_value = [
            {
                'id': b'blob-1',
                'original_path': '/test/image.jpg',
                'mime_type': 'image/jpeg',
                'size_bytes': 512000,
                'ingested_at': datetime.now(),
                'description': 'Test image',
                'annex_key': 'SHA256E-s512000--test.jpg'
            }
        ]
        
        result = runner.invoke(exo.cli, ['blob', 'list', '--mime-type', 'image/jpeg'])
        
        assert result.exit_code == 0
        assert 'image.jpg' in result.output
        assert mock_db.execute.called
        
        # Check that MIME type filter was applied
        sql = mock_db.execute.call_args[0][0]
        assert 'mime_type = %s' in sql
        
    def test_blob_get(self, runner, mock_db):
        """Test blob retrieval."""
        # Mock blob metadata
        mock_db.fetchone.return_value = {
            'original_path': '/home/user/document.pdf',
            'annex_key': 'SHA256E-s1024000--abc123.pdf',
            'description': 'Important document'
        }
        
        with mock.patch('subprocess.run') as mock_run:
            mock_run.return_value.returncode = 0
            mock_run.return_value.stdout = "/tmp/retrieved_file.pdf"
            
            result = runner.invoke(exo.cli, [
                'blob', 'get', 'test-blob-id',
                '--output', '/tmp/output.pdf',
                '--annex-repo', '/tmp/test-repo'
            ])
            
            assert result.exit_code == 0
            assert 'Retrieved blob' in result.output
            assert mock_run.called
            
    def test_blob_get_not_found(self, runner, mock_db):
        """Test blob retrieval for non-existent blob."""
        mock_db.fetchone.return_value = None
        
        result = runner.invoke(exo.cli, ['blob', 'get', 'nonexistent-id'])
        
        assert result.exit_code != 0
        assert 'not found' in result.output
        
    def test_blob_verify(self, runner):
        """Test blob integrity verification."""
        with mock.patch('subprocess.run') as mock_run:
            mock_run.return_value.returncode = 0
            mock_run.return_value.stdout = "fsck ok (checksum...)"
            
            result = runner.invoke(exo.cli, [
                'blob', 'verify',
                '--annex-repo', '/tmp/test-repo'
            ])
            
            assert result.exit_code == 0
            assert 'Verification complete' in result.output
            assert mock_run.called
            
            # Check that git annex fsck was called
            call_args = mock_run.call_args[0][0]
            assert 'git' in call_args
            assert 'annex' in call_args
            assert 'fsck' in call_args
            
    def test_blob_verify_fast(self, runner):
        """Test fast blob verification."""
        with mock.patch('subprocess.run') as mock_run:
            mock_run.return_value.returncode = 0
            
            result = runner.invoke(exo.cli, [
                'blob', 'verify',
                '--annex-repo', '/tmp/test-repo',
                '--fast'
            ])
            
            assert result.exit_code == 0
            assert mock_run.called
            
            # Check that --fast flag was passed to git annex
            call_args = mock_run.call_args[0][0]
            assert '--fast' in call_args


class TestDLQCommands:
    """Test Dead Letter Queue management commands."""
    
    @pytest.fixture
    def runner(self):
        return CliRunner()
    
    @pytest.fixture
    def mock_db(self):
        with mock.patch('exo.get_db_connection') as mock_conn:
            mock_cursor = mock.MagicMock()
            mock_conn.return_value.__enter__.return_value.cursor.return_value.__enter__.return_value = mock_cursor
            yield mock_cursor
            
    def test_dlq_list(self, runner, mock_db):
        """Test DLQ entry listing."""
        # Mock DLQ entries
        mock_db.fetchall.return_value = [
            {
                'id': b'dlq-1',
                'source': 'filesystem',
                'event_type': 'file_created',
                'error_category': 'validation_failed',
                'error_message': 'Schema validation failed',
                'retry_count': 2,
                'created_at': datetime.now() - timedelta(hours=2),
                'last_retry_at': datetime.now() - timedelta(hours=1),
                'agent_name': 'unified-collector'
            },
            {
                'id': b'dlq-2',
                'source': 'terminal',
                'event_type': 'command_executed',
                'error_category': 'processing_error',
                'error_message': 'Database connection timeout',
                'retry_count': 0,
                'created_at': datetime.now() - timedelta(minutes=30),
                'last_retry_at': None,
                'agent_name': 'unified-collector'
            }
        ]
        
        result = runner.invoke(exo.cli, ['dlq', 'list', '--limit', '10'])
        
        assert result.exit_code == 0
        assert 'validation_failed' in result.output
        assert 'processing_error' in result.output
        assert 'filesystem' in result.output
        assert 'terminal' in result.output
        
    def test_dlq_list_with_filters(self, runner, mock_db):
        """Test DLQ listing with filters."""
        mock_db.fetchall.return_value = []
        
        result = runner.invoke(exo.cli, [
            'dlq', 'list',
            '--agent', 'unified-collector',
            '--source', 'filesystem',
            '--event-type', 'file_created',
            '--category', 'validation_failed'
        ])
        
        assert result.exit_code == 0
        assert mock_db.execute.called
        
        # Check that filters were applied
        sql = mock_db.execute.call_args[0][0]
        assert 'agent_name = %s' in sql
        assert 'source = %s' in sql
        assert 'event_type = %s' in sql
        assert 'error_category = %s' in sql
        
    def test_dlq_show(self, runner, mock_db):
        """Test showing detailed DLQ entry."""
        # Mock DLQ entry details
        mock_db.fetchone.return_value = {
            'id': b'dlq-1',
            'source': 'filesystem',
            'event_type': 'file_created',
            'original_payload': {'path': '/test/file.txt', 'size': 1024},
            'error_category': 'validation_failed',
            'error_message': 'Required field "timestamp" missing',
            'error_details': {'field': 'timestamp', 'expected': 'ISO8601'},
            'retry_count': 3,
            'max_retries': 5,
            'created_at': datetime.now() - timedelta(hours=2),
            'last_retry_at': datetime.now() - timedelta(hours=1),
            'agent_name': 'unified-collector'
        }
        
        result = runner.invoke(exo.cli, ['dlq', 'show', 'dlq-1'])
        
        assert result.exit_code == 0
        assert 'dlq-1' in result.output
        assert 'validation_failed' in result.output
        assert 'Required field "timestamp" missing' in result.output
        assert '/test/file.txt' in result.output
        assert 'Retry Count: 3/5' in result.output
        
    def test_dlq_show_not_found(self, runner, mock_db):
        """Test showing non-existent DLQ entry."""
        mock_db.fetchone.return_value = None
        
        result = runner.invoke(exo.cli, ['dlq', 'show', 'nonexistent-id'])
        
        assert result.exit_code != 0
        assert 'not found' in result.output
        
    def test_dlq_retry(self, runner, mock_db):
        """Test retrying a DLQ entry."""
        # Mock DLQ entry
        mock_db.fetchone.return_value = {
            'id': b'dlq-1',
            'source': 'filesystem',
            'event_type': 'file_created',
            'original_payload': {'path': '/test/file.txt'},
            'retry_count': 2,
            'max_retries': 5
        }
        
        # Mock successful retry
        mock_db.execute.return_value = None
        
        result = runner.invoke(exo.cli, ['dlq', 'retry', 'dlq-1'])
        
        assert result.exit_code == 0
        assert 'Successfully retried' in result.output
        assert mock_db.execute.called
        
    def test_dlq_retry_dry_run(self, runner, mock_db):
        """Test DLQ retry dry run."""
        mock_db.fetchone.return_value = {
            'id': b'dlq-1',
            'source': 'filesystem',
            'event_type': 'file_created',
            'original_payload': {'path': '/test/file.txt'},
            'retry_count': 2,
            'max_retries': 5
        }
        
        result = runner.invoke(exo.cli, ['dlq', 'retry', 'dlq-1', '--dry-run'])
        
        assert result.exit_code == 0
        assert 'DRY RUN' in result.output
        assert 'Would retry' in result.output
        
    def test_dlq_resolve(self, runner, mock_db):
        """Test resolving a DLQ entry."""
        mock_db.fetchone.return_value = {
            'id': b'dlq-1',
            'source': 'filesystem',
            'event_type': 'file_created'
        }
        
        mock_db.execute.return_value = None
        
        result = runner.invoke(exo.cli, [
            'dlq', 'resolve', 'dlq-1',
            '--resolution', 'fixed_upstream'
        ])
        
        assert result.exit_code == 0
        assert 'Successfully resolved' in result.output
        assert mock_db.execute.called
        
    def test_dlq_stats(self, runner, mock_db):
        """Test DLQ statistics."""
        # Mock statistics data
        mock_db.fetchall.side_effect = [
            # Overall stats
            [
                {
                    'total_entries': 150,
                    'by_category': {
                        'validation_failed': 80,
                        'processing_error': 45,
                        'timeout': 25
                    },
                    'by_agent': {
                        'unified-collector': 120,
                        'promo-worker': 30
                    }
                }
            ],
            # Daily trends
            [
                {
                    'date': datetime.now().date() - timedelta(days=2),
                    'entries_created': 20,
                    'entries_resolved': 15
                },
                {
                    'date': datetime.now().date() - timedelta(days=1),
                    'entries_created': 35,
                    'entries_resolved': 25
                },
                {
                    'date': datetime.now().date(),
                    'entries_created': 10,
                    'entries_resolved': 8
                }
            ]
        ]
        
        result = runner.invoke(exo.cli, ['dlq', 'stats', '--days', '7'])
        
        assert result.exit_code == 0
        assert 'DLQ Statistics' in result.output
        assert '150' in result.output  # Total entries
        assert 'validation_failed' in result.output
        assert 'unified-collector' in result.output
        
    def test_dlq_purge(self, runner, mock_db):
        """Test DLQ purging."""
        # Mock purge count
        mock_db.execute.return_value = None
        mock_db.fetchone.return_value = {'purged_count': 25}
        
        result = runner.invoke(exo.cli, [
            'dlq', 'purge',
            '--category', 'resolved',
            '--older-than', '7d'
        ])
        
        assert result.exit_code == 0
        assert 'Successfully purged 25 entries' in result.output
        assert mock_db.execute.called
        
    def test_dlq_purge_dry_run(self, runner, mock_db):
        """Test DLQ purge dry run."""
        mock_db.fetchone.return_value = {'count': 42}
        
        result = runner.invoke(exo.cli, [
            'dlq', 'purge',
            '--category', 'resolved',
            '--older-than', '7d',
            '--dry-run'
        ])
        
        assert result.exit_code == 0
        assert 'DRY RUN' in result.output
        assert 'Would purge 42 entries' in result.output


class TestStatsCommand:
    """Test enhanced database statistics command."""
    
    @pytest.fixture
    def runner(self):
        return CliRunner()
    
    @pytest.fixture
    def mock_db(self):
        with mock.patch('exo.get_db_connection') as mock_conn:
            mock_cursor = mock.MagicMock()
            mock_conn.return_value.__enter__.return_value.cursor.return_value.__enter__.return_value = mock_cursor
            yield mock_cursor
            
    def test_stats_comprehensive(self, runner, mock_db):
        """Test comprehensive stats output."""
        # Mock various statistics queries
        mock_db.fetchone.side_effect = [
            {'total_events': 1500000},  # Total events
            {'total_sources': 8},       # Total sources
            {'total_schemas': 25},      # Total schemas
            {'avg_events_per_day': 5000},  # Daily average
            {'db_size': '2.5 GB'},      # Database size
        ]
        
        mock_db.fetchall.side_effect = [
            # Top sources
            [
                {'source': 'filesystem', 'event_count': 800000, 'percentage': 53.3},
                {'source': 'hyprland', 'event_count': 400000, 'percentage': 26.7},
                {'source': 'terminal', 'event_count': 200000, 'percentage': 13.3},
                {'source': 'clipboard', 'event_count': 100000, 'percentage': 6.7}
            ],
            # Event types
            [
                {'event_type': 'file_modified', 'event_count': 500000, 'percentage': 33.3},
                {'event_type': 'window_focused', 'event_count': 300000, 'percentage': 20.0},
                {'event_type': 'file_created', 'event_count': 200000, 'percentage': 13.3}
            ],
            # Daily activity
            [
                {'date': datetime.now().date() - timedelta(days=6), 'event_count': 4800},
                {'date': datetime.now().date() - timedelta(days=5), 'event_count': 5200},
                {'date': datetime.now().date() - timedelta(days=4), 'event_count': 4900},
                {'date': datetime.now().date() - timedelta(days=3), 'event_count': 5100},
                {'date': datetime.now().date() - timedelta(days=2), 'event_count': 5300},
                {'date': datetime.now().date() - timedelta(days=1), 'event_count': 5000},
                {'date': datetime.now().date(), 'event_count': 2500}
            ],
            # Agent health
            [
                {
                    'agent_name': 'unified-collector',
                    'last_heartbeat': datetime.now() - timedelta(minutes=2),
                    'status': 'running',
                    'events_processed': 1450000
                },
                {
                    'agent_name': 'promo-worker',
                    'last_heartbeat': datetime.now() - timedelta(minutes=1),
                    'status': 'running',
                    'events_processed': 50000
                }
            ]
        ]
        
        result = runner.invoke(exo.cli, ['stats'])
        
        assert result.exit_code == 0
        assert 'Database Statistics' in result.output
        assert '1,500,000' in result.output  # Total events
        assert 'filesystem' in result.output
        assert 'hyprland' in result.output
        assert 'file_modified' in result.output
        assert 'unified-collector' in result.output
        assert 'running' in result.output
        
    def test_stats_error_handling(self, runner, mock_db):
        """Test stats command error handling."""
        mock_db.fetchone.side_effect = psycopg2.Error("Database error")
        
        result = runner.invoke(exo.cli, ['stats'])
        
        assert result.exit_code != 0
        assert 'Error' in result.output


class TestUtilityFunctions:
    """Test utility functions that might not be covered elsewhere."""
    
    def test_format_duration_edge_cases(self):
        """Test duration formatting with edge cases."""
        # Test boundary values
        assert exo.format_duration(0) == '0.00s'
        assert exo.format_duration(1) == '1.00s'
        assert exo.format_duration(59) == '59.00s'
        assert exo.format_duration(60) == '1m 0.00s'
        assert exo.format_duration(3600) == '1h 0m 0.00s'
        assert exo.format_duration(3661) == '1h 1m 1.00s'
        
        # Test large values
        assert '24h' in exo.format_duration(86400)  # 1 day
        assert '25h' in exo.format_duration(90000)  # More than 1 day
        
        # Test fractional seconds
        assert '1.50s' in exo.format_duration(1.5)
        assert '0.25s' in exo.format_duration(0.25)
        
    def test_parse_time_delta_comprehensive(self):
        """Test comprehensive time delta parsing."""
        # Basic units
        assert exo.parse_time_delta('30s') == timedelta(seconds=30)
        assert exo.parse_time_delta('15m') == timedelta(minutes=15)
        assert exo.parse_time_delta('2h') == timedelta(hours=2)
        assert exo.parse_time_delta('3d') == timedelta(days=3)
        assert exo.parse_time_delta('1w') == timedelta(weeks=1)
        
        # Edge cases
        assert exo.parse_time_delta('0s') == timedelta(seconds=0)
        assert exo.parse_time_delta('1000d') == timedelta(days=1000)
        
        # Invalid formats
        with pytest.raises(ValueError):
            exo.parse_time_delta('')
        with pytest.raises(ValueError):
            exo.parse_time_delta('abc')
        with pytest.raises(ValueError):
            exo.parse_time_delta('10x')
        with pytest.raises(ValueError):
            exo.parse_time_delta('10')  # No unit
            
    def test_parse_datetime_comprehensive(self):
        """Test comprehensive datetime parsing."""
        # Full datetime formats
        dt1 = exo.parse_datetime('2025-01-10 15:30:45')
        assert dt1.year == 2025
        assert dt1.month == 1
        assert dt1.day == 10
        assert dt1.hour == 15
        assert dt1.minute == 30
        assert dt1.second == 45
        
        # Date only
        dt2 = exo.parse_datetime('2025-01-10')
        assert dt2.year == 2025
        assert dt2.month == 1
        assert dt2.day == 10
        assert dt2.hour == 0
        assert dt2.minute == 0
        assert dt2.second == 0
        
        # Time only (uses current date)
        dt3 = exo.parse_datetime('15:30')
        assert dt3.hour == 15
        assert dt3.minute == 30
        assert dt3.date() == datetime.now().date()
        
        # Time with seconds
        dt4 = exo.parse_datetime('15:30:45')
        assert dt4.hour == 15
        assert dt4.minute == 30
        assert dt4.second == 45
        
        # Invalid formats
        with pytest.raises(ValueError):
            exo.parse_datetime('')
        with pytest.raises(ValueError):
            exo.parse_datetime('not-a-date')
        with pytest.raises(ValueError):
            exo.parse_datetime('2025-13-01')  # Invalid month
        with pytest.raises(ValueError):
            exo.parse_datetime('25:30')  # Invalid hour


class TestIntegrationScenarios:
    """Test complex integration scenarios."""
    
    @pytest.fixture
    def runner(self):
        return CliRunner()
    
    def test_command_chaining_scenario(self, runner):
        """Test a realistic scenario of chaining multiple commands."""
        # This would test a workflow like:
        # 1. Query for recent events
        # 2. Check agent status
        # 3. Review DLQ entries
        # 4. Get statistics
        
        with mock.patch('exo.get_db_connection') as mock_conn:
            mock_cursor = mock.MagicMock()
            mock_conn.return_value.__enter__.return_value.cursor.return_value.__enter__.return_value = mock_cursor
            
            # Mock different responses for different commands
            mock_cursor.fetchall.side_effect = [
                # Query results
                [{'id': b'event-1', 'source': 'filesystem', 'event_type': 'file_created'}],
                # Agent list
                [{'agent_name': 'unified-collector', 'status': 'running'}],
                # DLQ entries
                [{'id': b'dlq-1', 'error_category': 'validation_failed'}]
            ]
            
            # Test query command
            result1 = runner.invoke(exo.cli, ['query', '--limit', '5'])
            assert result1.exit_code == 0
            
            # Test agent command
            result2 = runner.invoke(exo.cli, ['agent', 'list'])
            assert result2.exit_code == 0
            
            # Test DLQ command
            result3 = runner.invoke(exo.cli, ['dlq', 'list', '--limit', '5'])
            assert result3.exit_code == 0
            
    def test_error_propagation(self, runner):
        """Test that errors are properly propagated across command levels."""
        with mock.patch('exo.get_db_connection') as mock_conn:
            mock_conn.side_effect = psycopg2.OperationalError("Connection failed")
            
            # All database-dependent commands should fail gracefully
            commands_to_test = [
                ['query'],
                ['schema', 'list'],
                ['agent', 'list'],
                ['dlq', 'list'],
                ['sources'],
                ['stats']
            ]
            
            for cmd in commands_to_test:
                result = runner.invoke(exo.cli, cmd)
                assert result.exit_code != 0, f"Command {cmd} should fail with DB error"


if __name__ == '__main__':
    pytest.main([__file__, '-v'])