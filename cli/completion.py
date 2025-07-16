#!/usr/bin/env python3
"""
CLI Autocomplete System for Sinex CLI
Provides shell completion for sources, event types, and other database values
"""

import os
import sys
from pathlib import Path
from typing import List, Optional, Dict, Any

import psycopg2
from psycopg2.extras import RealDictCursor


def get_db_connection():
    """Get database connection using environment variable or default."""
    db_url = os.environ.get('DATABASE_URL', 'postgresql://localhost/sinex')
    return psycopg2.connect(db_url, cursor_factory=RealDictCursor)


def get_sources() -> List[str]:
    """Get all unique event sources from the database."""
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT DISTINCT source FROM core.events ORDER BY source")
                return [row['source'] for row in cur.fetchall()]
    except Exception:
        # Fallback to common sources if DB unavailable
        return ['hyprland', 'terminal.kitty', 'filesystem', 'sinex', 'shell.atuin']


def get_event_types(source: Optional[str] = None) -> List[str]:
    """Get event types, optionally filtered by source."""
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                if source:
                    cur.execute(
                        "SELECT DISTINCT event_type FROM core.events WHERE source = %s ORDER BY event_type",
                        (source,)
                    )
                else:
                    cur.execute("SELECT DISTINCT event_type FROM core.events ORDER BY event_type")
                return [row['event_type'] for row in cur.fetchall()]
    except Exception:
        # Fallback to common event types
        return [
            'window.focused', 'workspace.changed', 'command.executed', 
            'file.created', 'file.modified', 'automaton.heartbeat'
        ]


def get_hosts() -> List[str]:
    """Get all unique hosts from the database."""
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT DISTINCT host FROM core.events WHERE host IS NOT NULL ORDER BY host")
                return [row['host'] for row in cur.fetchall()]
    except Exception:
        return []


def get_automata() -> List[str]:
    """Get all automaton names from manifests."""
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT DISTINCT automaton_name FROM sinex_schemas.automaton_manifests ORDER BY automaton_name")
                return [row['automaton_name'] for row in cur.fetchall()]
    except Exception:
        return []


def get_blob_ids() -> List[str]:
    """Get blob IDs from source material registry (recent ones)."""
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT blob_id::text FROM raw.source_material_registry 
                    ORDER BY staged_at DESC 
                    LIMIT 100
                """)
                return [row['blob_id'] for row in cur.fetchall()]
    except Exception:
        return []


def get_event_ids() -> List[str]:
    """Get recent event IDs (for archive/restore operations)."""
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT event_id::text FROM core.events 
                    ORDER BY ts_ingest DESC 
                    LIMIT 50
                """)
                return [row['event_id'] for row in cur.fetchall()]
    except Exception:
        return []


def get_schema_identifiers() -> List[str]:
    """Get schema identifiers in source/type format."""
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT event_source, event_type, schema_version
                    FROM sinex_schemas.event_payload_schemas
                    WHERE is_active = true
                    ORDER BY event_source, event_type
                """)
                results = []
                for row in cur.fetchall():
                    results.append(f"{row['event_source']}/{row['event_type']}")
                    results.append(f"{row['event_source']}/{row['event_type']}/{row['schema_version']}")
                return results
    except Exception:
        return []


def generate_bash_completion() -> str:
    """Generate bash completion script."""
    return '''#!/bin/bash
# Sinex CLI Bash Completion

_sinex_completion() {
    local cur prev opts
    COMPREPLY=()
    cur="${COMP_WORDS[COMP_CWORD]}"
    prev="${COMP_WORDS[COMP_CWORD-1]}"
    
    # Top-level commands
    if [[ ${COMP_CWORD} == 1 ]]; then
        opts="query sources stats schema automaton blob dlq event-archive explore replay restore completion"
        COMPREPLY=( $(compgen -W "${opts}" -- ${cur}) )
        return 0
    fi
    
    # Command-specific completions
    case "${COMP_WORDS[1]}" in
        query)
            case "${prev}" in
                --source|-s)
                    local sources=$(python3 -c "from cli.completion import get_sources; print(' '.join(get_sources()))" 2>/dev/null)
                    COMPREPLY=( $(compgen -W "${sources}" -- ${cur}) )
                    return 0
                    ;;
                --event-type|-t)
                    local source=""
                    # Look for --source in previous args
                    for ((i=2; i<COMP_CWORD; i++)); do
                        if [[ "${COMP_WORDS[i]}" == "--source" || "${COMP_WORDS[i]}" == "-s" ]]; then
                            source="${COMP_WORDS[i+1]}"
                            break
                        fi
                    done
                    local event_types
                    if [[ -n "${source}" ]]; then
                        event_types=$(python3 -c "from cli.completion import get_event_types; print(' '.join(get_event_types('${source}')))" 2>/dev/null)
                    else
                        event_types=$(python3 -c "from cli.completion import get_event_types; print(' '.join(get_event_types()))" 2>/dev/null)
                    fi
                    COMPREPLY=( $(compgen -W "${event_types}" -- ${cur}) )
                    return 0
                    ;;
                --host)
                    local hosts=$(python3 -c "from cli.completion import get_hosts; print(' '.join(get_hosts()))" 2>/dev/null)
                    COMPREPLY=( $(compgen -W "${hosts}" -- ${cur}) )
                    return 0
                    ;;
                --output-format)
                    COMPREPLY=( $(compgen -W "table json csv yaml" -- ${cur}) )
                    return 0
                    ;;
                *)
                    opts="--source -s --event-type -t --since --until --last -l --limit -n --host --payload-jq --output-format"
                    COMPREPLY=( $(compgen -W "${opts}" -- ${cur}) )
                    return 0
                    ;;
            esac
            ;;
        schema)
            case "${COMP_WORDS[2]}" in
                list)
                    case "${prev}" in
                        --source|-s)
                            local sources=$(python3 -c "from cli.completion import get_sources; print(' '.join(get_sources()))" 2>/dev/null)
                            COMPREPLY=( $(compgen -W "${sources}" -- ${cur}) )
                            return 0
                            ;;
                        --event-type|-t)
                            local event_types=$(python3 -c "from cli.completion import get_event_types; print(' '.join(get_event_types()))" 2>/dev/null)
                            COMPREPLY=( $(compgen -W "${event_types}" -- ${cur}) )
                            return 0
                            ;;
                        *)
                            opts="--source -s --event-type -t --active-only"
                            COMPREPLY=( $(compgen -W "${opts}" -- ${cur}) )
                            return 0
                            ;;
                    esac
                    ;;
                get)
                    if [[ ${COMP_CWORD} == 3 ]]; then
                        local schemas=$(python3 -c "from cli.completion import get_schema_identifiers; print(' '.join(get_schema_identifiers()))" 2>/dev/null)
                        COMPREPLY=( $(compgen -W "${schemas}" -- ${cur}) )
                        return 0
                    fi
                    ;;
                *)
                    if [[ ${COMP_CWORD} == 2 ]]; then
                        COMPREPLY=( $(compgen -W "list get" -- ${cur}) )
                        return 0
                    fi
                    ;;
            esac
            ;;
        automaton)
            case "${COMP_WORDS[2]}" in
                list)
                    case "${prev}" in
                        --status|-s)
                            COMPREPLY=( $(compgen -W "development stable deprecated" -- ${cur}) )
                            return 0
                            ;;
                        *)
                            opts="--status -s"
                            COMPREPLY=( $(compgen -W "${opts}" -- ${cur}) )
                            return 0
                            ;;
                    esac
                    ;;
                status)
                    if [[ ${COMP_CWORD} == 3 ]]; then
                        local agents=$(python3 -c "from cli.completion import get_automata; print(' '.join(get_automata()))" 2>/dev/null)
                        COMPREPLY=( $(compgen -W "${agents}" -- ${cur}) )
                        return 0
                    fi
                    ;;
                *)
                    if [[ ${COMP_CWORD} == 2 ]]; then
                        COMPREPLY=( $(compgen -W "list status" -- ${cur}) )
                        return 0
                    fi
                    ;;
            esac
            ;;
        blob)
            case "${COMP_WORDS[2]}" in
                ingest)
                    case "${prev}" in
                        --description|-d|--annex-repo|-r)
                            return 0
                            ;;
                        *)
                            opts="--description -d --annex-repo -r"
                            COMPREPLY=( $(compgen -W "${opts}" -- ${cur}) )
                            if [[ ${COMP_CWORD} == 3 ]]; then
                                COMPREPLY+=( $(compgen -f -- ${cur}) )
                            fi
                            return 0
                            ;;
                    esac
                    ;;
                list)
                    case "${prev}" in
                        --limit|-n|--mime-type|-m)
                            return 0
                            ;;
                        *)
                            opts="--limit -n --mime-type -m"
                            COMPREPLY=( $(compgen -W "${opts}" -- ${cur}) )
                            return 0
                            ;;
                    esac
                    ;;
                get)
                    case "${prev}" in
                        --output|-o|--annex-repo|-r)
                            return 0
                            ;;
                        *)
                            opts="--output -o --annex-repo -r"
                            COMPREPLY=( $(compgen -W "${opts}" -- ${cur}) )
                            return 0
                            ;;
                    esac
                    ;;
                verify)
                    case "${prev}" in
                        --annex-repo|-r)
                            return 0
                            ;;
                        *)
                            opts="--annex-repo -r --fast"
                            COMPREPLY=( $(compgen -W "${opts}" -- ${cur}) )
                            return 0
                            ;;
                    esac
                    ;;
                stage)
                    case "${prev}" in
                        --source-id|-s|--comment|-c|--tags|-t|--annex-repo|-r)
                            return 0
                            ;;
                        *)
                            opts="--source-id -s --comment -c --tags -t --annex-repo -r"
                            COMPREPLY=( $(compgen -W "${opts}" -- ${cur}) )
                            if [[ ${COMP_CWORD} == 3 ]]; then
                                COMPREPLY+=( $(compgen -f -- ${cur}) )
                            fi
                            return 0
                            ;;
                    esac
                    ;;
                archive)
                    case "${prev}" in
                        --reason|-r)
                            return 0
                            ;;
                        *)
                            opts="--reason -r --dry-run --force"
                            COMPREPLY=( $(compgen -W "${opts}" -- ${cur}) )
                            if [[ ${COMP_CWORD} == 3 ]]; then
                                local blob_ids=$(python3 -c "from cli.completion import get_blob_ids; print(' '.join(get_blob_ids()))" 2>/dev/null)
                                COMPREPLY+=( $(compgen -W "${blob_ids}" -- ${cur}) )
                            fi
                            return 0
                            ;;
                    esac
                    ;;
                *)
                    if [[ ${COMP_CWORD} == 2 ]]; then
                        COMPREPLY=( $(compgen -W "ingest list get verify stage archive" -- ${cur}) )
                        return 0
                    fi
                    ;;
            esac
            ;;
        dlq)
            case "${COMP_WORDS[2]}" in
                list)
                    case "${prev}" in
                        --automaton|-a)
                            local agents=$(python3 -c "from cli.completion import get_automata; print(' '.join(get_automata()))" 2>/dev/null)
                            COMPREPLY=( $(compgen -W "${agents}" -- ${cur}) )
                            return 0
                            ;;
                        --source|-s)
                            local sources=$(python3 -c "from cli.completion import get_sources; print(' '.join(get_sources()))" 2>/dev/null)
                            COMPREPLY=( $(compgen -W "${sources}" -- ${cur}) )
                            return 0
                            ;;
                        --event-type|-t)
                            local event_types=$(python3 -c "from cli.completion import get_event_types; print(' '.join(get_event_types()))" 2>/dev/null)
                            COMPREPLY=( $(compgen -W "${event_types}" -- ${cur}) )
                            return 0
                            ;;
                        --category|-c)
                            COMPREPLY=( $(compgen -W "retryable permanent system user" -- ${cur}) )
                            return 0
                            ;;
                        --output-format)
                            COMPREPLY=( $(compgen -W "table json csv" -- ${cur}) )
                            return 0
                            ;;
                        *)
                            opts="--automaton -a --source -s --event-type -t --category -c --limit -n --include-resolved --output-format"
                            COMPREPLY=( $(compgen -W "${opts}" -- ${cur}) )
                            return 0
                            ;;
                    esac
                    ;;
                stats)
                    case "${prev}" in
                        --automaton|-a)
                            local agents=$(python3 -c "from cli.completion import get_automata; print(' '.join(get_automata()))" 2>/dev/null)
                            COMPREPLY=( $(compgen -W "${agents}" -- ${cur}) )
                            return 0
                            ;;
                        *)
                            opts="--automaton -a --days -d"
                            COMPREPLY=( $(compgen -W "${opts}" -- ${cur}) )
                            return 0
                            ;;
                    esac
                    ;;
                resolve)
                    case "${prev}" in
                        --resolution)
                            COMPREPLY=( $(compgen -W "manual purged" -- ${cur}) )
                            return 0
                            ;;
                        *)
                            opts="--resolution --dry-run"
                            COMPREPLY=( $(compgen -W "${opts}" -- ${cur}) )
                            return 0
                            ;;
                    esac
                    ;;
                purge)
                    case "${prev}" in
                        --automaton|-a)
                            local agents=$(python3 -c "from cli.completion import get_automata; print(' '.join(get_automata()))" 2>/dev/null)
                            COMPREPLY=( $(compgen -W "${agents}" -- ${cur}) )
                            return 0
                            ;;
                        --category|-c)
                            COMPREPLY=( $(compgen -W "retryable permanent system user" -- ${cur}) )
                            return 0
                            ;;
                        *)
                            opts="--automaton -a --category -c --older-than --resolved-only --dry-run --force"
                            COMPREPLY=( $(compgen -W "${opts}" -- ${cur}) )
                            return 0
                            ;;
                    esac
                    ;;
                *)
                    if [[ ${COMP_CWORD} == 2 ]]; then
                        COMPREPLY=( $(compgen -W "list show retry resolve stats purge" -- ${cur}) )
                        return 0
                    fi
                    ;;
            esac
            ;;
        event-archive)
            case "${prev}" in
                --reason|-r)
                    return 0
                    ;;
                *)
                    opts="--reason -r --dry-run --force"
                    COMPREPLY=( $(compgen -W "${opts}" -- ${cur}) )
                    if [[ ${COMP_CWORD} == 2 ]]; then
                        local event_ids=$(python3 -c "from cli.completion import get_event_ids; print(' '.join(get_event_ids()))" 2>/dev/null)
                        COMPREPLY+=( $(compgen -W "${event_ids}" -- ${cur}) )
                    fi
                    return 0
                    ;;
            esac
            ;;
        explore)
            case "${COMP_WORDS[2]}" in
                coverage)
                    case "${prev}" in
                        --satellite|-s)
                            COMPREPLY=( $(compgen -W "fs-watcher terminal-satellite desktop-satellite system-satellite" -- ${cur}) )
                            return 0
                            ;;
                        --time-range|-t)
                            COMPREPLY=( $(compgen -W "1h 6h 12h 1d 3d 1w 2w" -- ${cur}) )
                            return 0
                            ;;
                        --source)
                            local sources=$(python3 -c "from cli.completion import get_sources; print(' '.join(get_sources()))" 2>/dev/null)
                            COMPREPLY=( $(compgen -W "${sources}" -- ${cur}) )
                            return 0
                            ;;
                        *)
                            opts="--satellite -s --time-range -t --source"
                            COMPREPLY=( $(compgen -W "${opts}" -- ${cur}) )
                            return 0
                            ;;
                    esac
                    ;;
                source-state)
                    case "${prev}" in
                        --satellite|-s)
                            COMPREPLY=( $(compgen -W "fs-watcher terminal-satellite desktop-satellite system-satellite" -- ${cur}) )
                            return 0
                            ;;
                        *)
                            opts="--satellite -s --verbose -v"
                            COMPREPLY=( $(compgen -W "${opts}" -- ${cur}) )
                            return 0
                            ;;
                    esac
                    ;;
                missing-events)
                    case "${prev}" in
                        --satellite|-s)
                            COMPREPLY=( $(compgen -W "fs-watcher terminal-satellite desktop-satellite system-satellite" -- ${cur}) )
                            return 0
                            ;;
                        --time-range|-t)
                            COMPREPLY=( $(compgen -W "1h 6h 12h 1d 3d 1w" -- ${cur}) )
                            return 0
                            ;;
                        *)
                            opts="--satellite -s --time-range -t"
                            COMPREPLY=( $(compgen -W "${opts}" -- ${cur}) )
                            return 0
                            ;;
                    esac
                    ;;
                curate)
                    case "${prev}" in
                        --time-range|-t)
                            COMPREPLY=( $(compgen -W "1h 6h 12h 1d 3d 1w 2w" -- ${cur}) )
                            return 0
                            ;;
                        --source|-s)
                            local sources=$(python3 -c "from cli.completion import get_sources; print(' '.join(get_sources()))" 2>/dev/null)
                            COMPREPLY=( $(compgen -W "${sources}" -- ${cur}) )
                            return 0
                            ;;
                        --event-type|-e)
                            local event_types=$(python3 -c "from cli.completion import get_event_types; print(' '.join(get_event_types()))" 2>/dev/null)
                            COMPREPLY=( $(compgen -W "${event_types}" -- ${cur}) )
                            return 0
                            ;;
                        --limit|-n)
                            return 0
                            ;;
                        *)
                            opts="--time-range -t --source -s --event-type -e --limit -n --auto-resolve"
                            COMPREPLY=( $(compgen -W "${opts}" -- ${cur}) )
                            return 0
                            ;;
                    esac
                    ;;
                *)
                    if [[ ${COMP_CWORD} == 2 ]]; then
                        COMPREPLY=( $(compgen -W "coverage source-state missing-events curate" -- ${cur}) )
                        return 0
                    fi
                    ;;
            esac
            ;;
        completion)
            case "${COMP_WORDS[2]}" in
                install|generate)
                    if [[ ${COMP_CWORD} == 3 ]]; then
                        COMPREPLY=( $(compgen -W "bash zsh fish" -- ${cur}) )
                        return 0
                    fi
                    ;;
                *)
                    if [[ ${COMP_CWORD} == 2 ]]; then
                        COMPREPLY=( $(compgen -W "install generate" -- ${cur}) )
                        return 0
                    fi
                    ;;
            esac
            ;;
    esac
}

complete -F _sinex_completion exo
complete -F _sinex_completion ./exo.py
'''


def generate_zsh_completion() -> str:
    """Generate zsh completion script."""
    return '''#compdef exo

_sinex_sources() {
    local sources
    sources=($(python3 -c "from cli.completion import get_sources; print(' '.join(get_sources()))" 2>/dev/null))
    _describe 'sources' sources
}

_sinex_event_types() {
    local event_types source
    # Try to find source from command line
    for ((i=1; i<=$#words; i++)); do
        if [[ $words[i] == "--source" || $words[i] == "-s" ]]; then
            source=$words[i+1]
            break
        fi
    done
    
    if [[ -n $source ]]; then
        event_types=($(python3 -c "from cli.completion import get_event_types; print(' '.join(get_event_types('$source')))" 2>/dev/null))
    else
        event_types=($(python3 -c "from cli.completion import get_event_types; print(' '.join(get_event_types()))" 2>/dev/null))
    fi
    _describe 'event types' event_types
}

_sinex_hosts() {
    local hosts
    hosts=($(python3 -c "from cli.completion import get_hosts; print(' '.join(get_hosts()))" 2>/dev/null))
    _describe 'hosts' hosts
}

_sinex_agents() {
    local agents
    agents=($(python3 -c "from cli.completion import get_automata; print(' '.join(get_automata()))" 2>/dev/null))
    _describe 'agents' agents
}

_sinex_schemas() {
    local schemas
    schemas=($(python3 -c "from cli.completion import get_schema_identifiers; print(' '.join(get_schema_identifiers()))" 2>/dev/null))
    _describe 'schemas' schemas
}

_exo() {
    local context state line
    typeset -A opt_args
    
    _arguments -C \
        '1: :->command' \
        '*: :->args'
    
    case $state in
        command)
            _values "commands" \
                "query[Query events from database]" \
                "sources[List event sources]" \
                "stats[Show database statistics]" \
                "schema[Schema management]" \
                "automaton[Automaton management]" \
                "blob[Blob storage management]" \
                "dlq[Dead letter queue management]" \
                "event-archive[Archive specific events]" \
                "explore[Explore and curate data]" \
                "replay[Replay automaton processing]" \
                "restore[Restore archived events]" \
                "completion[Shell completion management]"
            ;;
        args)
            case $words[2] in
                query)
                    _arguments \
                        '(-s --source)'{-s,--source}'[Filter by source]:source:_sinex_sources' \
                        '(-t --event-type)'{-t,--event-type}'[Filter by event type]:event-type:_sinex_event_types' \
                        '--since[Show events since]:datetime:' \
                        '--until[Show events until]:datetime:' \
                        '(-l --last)'{-l,--last}'[Show events from last]:timespan:' \
                        '(-n --limit)'{-n,--limit}'[Maximum events to show]:limit:' \
                        '--host[Filter by host]:host:_sinex_hosts' \
                        '--payload-jq[JQ filter for payload]:jq-filter:' \
                        '--output-format[Output format]:format:(table json csv yaml)'
                    ;;
                schema)
                    case $words[3] in
                        list)
                            _arguments \
                                '(-s --source)'{-s,--source}'[Filter by source]:source:_sinex_sources' \
                                '(-t --event-type)'{-t,--event-type}'[Filter by event type]:event-type:_sinex_event_types' \
                                '--active-only[Show only active schemas]'
                            ;;
                        get)
                            _arguments \
                                '1:schema:_sinex_schemas'
                            ;;
                        *)
                            _values "schema commands" \
                                "list[List schemas]" \
                                "get[Get specific schema]"
                            ;;
                    esac
                    ;;
                automaton)
                    case $words[3] in
                        list)
                            _arguments \
                                '(-s --status)'{-s,--status}'[Filter by status]:status:(development stable deprecated)'
                            ;;
                        status)
                            _arguments \
                                '1:automaton:_sinex_automata'
                            ;;
                        *)
                            _values "automaton commands" \
                                "list[List automata]" \
                                "status[Show automaton status]"
                            ;;
                    esac
                    ;;
                blob)
                    case $words[3] in
                        ingest)
                            _arguments \
                                '1:file:_files' \
                                '(-d --description)'{-d,--description}'[Description]:description:' \
                                '(-r --annex-repo)'{-r,--annex-repo}'[Repository path]:path:_directories'
                            ;;
                        list)
                            _arguments \
                                '(-n --limit)'{-n,--limit}'[Number to show]:limit:' \
                                '(-m --mime-type)'{-m,--mime-type}'[Filter by MIME type]:mime-type:'
                            ;;
                        get)
                            _arguments \
                                '1:blob-id:' \
                                '(-o --output)'{-o,--output}'[Output file]:file:_files' \
                                '(-r --annex-repo)'{-r,--annex-repo}'[Repository path]:path:_directories'
                            ;;
                        verify)
                            _arguments \
                                '(-r --annex-repo)'{-r,--annex-repo}'[Repository path]:path:_directories' \
                                '--fast[Fast verification]'
                            ;;
                        stage)
                            _arguments \
                                '1:file:_files' \
                                '(-s --source-id)'{-s,--source-id}'[Source identifier]:source-id:' \
                                '(-c --comment)'{-c,--comment}'[User comment]:comment:' \
                                '(-t --tags)'{-t,--tags}'[Tags (comma-separated)]:tags:' \
                                '(-r --annex-repo)'{-r,--annex-repo}'[Repository path]:path:_directories'
                            ;;
                        archive)
                            _arguments \
                                '1:blob-id:' \
                                '(-r --reason)'{-r,--reason}'[Reason for archiving]:reason:' \
                                '--dry-run[Show what would be archived]' \
                                '--force[Archive without confirmation]'
                            ;;
                        *)
                            _values "blob commands" \
                                "ingest[Ingest file]" \
                                "list[List blobs]" \
                                "get[Get blob]" \
                                "verify[Verify integrity]" \
                                "stage[Stage source material]" \
                                "archive[Archive blob and derived events]"
                            ;;
                    esac
                    ;;
                dlq)
                    case $words[3] in
                        list)
                            _arguments \
                                '(-a --automaton)'{-a,--automaton}'[Filter by automaton]:automaton:_sinex_automata' \
                                '(-s --source)'{-s,--source}'[Filter by source]:source:_sinex_sources' \
                                '(-t --event-type)'{-t,--event-type}'[Filter by event type]:event-type:_sinex_event_types' \
                                '(-c --category)'{-c,--category}'[Filter by category]:category:(retryable permanent system user)' \
                                '(-n --limit)'{-n,--limit}'[Maximum entries]:limit:' \
                                '--include-resolved[Include resolved entries]' \
                                '--output-format[Output format]:format:(table json csv)'
                            ;;
                        stats)
                            _arguments \
                                '(-a --automaton)'{-a,--automaton}'[Filter by automaton]:automaton:_sinex_automata' \
                                '(-d --days)'{-d,--days}'[Number of days]:days:'
                            ;;
                        show|retry)
                            _arguments \
                                '1:dlq-id:'
                            ;;
                        resolve)
                            _arguments \
                                '1:dlq-id:' \
                                '--resolution[Resolution type]:resolution:(manual purged)' \
                                '--dry-run[Show what would be resolved]'
                            ;;
                        purge)
                            _arguments \
                                '(-a --automaton)'{-a,--automaton}'[Purge by automaton]:automaton:_sinex_automata' \
                                '(-c --category)'{-c,--category}'[Purge by category]:category:(retryable permanent system user)' \
                                '--older-than[Purge entries older than]:timespan:' \
                                '--resolved-only[Only purge resolved entries]' \
                                '--dry-run[Show what would be purged]' \
                                '--force[Skip confirmation]'
                            ;;
                        *)
                            _values "dlq commands" \
                                "list[List DLQ entries]" \
                                "show[Show DLQ entry details]" \
                                "retry[Retry DLQ entry]" \
                                "resolve[Resolve DLQ entry]" \
                                "stats[Show DLQ statistics]" \
                                "purge[Purge DLQ entries]"
                            ;;
                    esac
                    ;;
                event-archive)
                    _arguments \
                        '1:event-id:' \
                        '(-r --reason)'{-r,--reason}'[Reason for archiving]:reason:' \
                        '--dry-run[Show what would be archived]' \
                        '--force[Archive without confirmation]'
                    ;;
                explore)
                    case $words[3] in
                        coverage)
                            _arguments \
                                '(-s --satellite)'{-s,--satellite}'[Satellite name]:satellite:(fs-watcher terminal-satellite desktop-satellite system-satellite)' \
                                '(-t --time-range)'{-t,--time-range}'[Time range]:time-range:(1h 6h 12h 1d 3d 1w 2w)' \
                                '--source[Filter by source]:source:_sinex_sources'
                            ;;
                        source-state)
                            _arguments \
                                '(-s --satellite)'{-s,--satellite}'[Satellite name]:satellite:(fs-watcher terminal-satellite desktop-satellite system-satellite)' \
                                '(-v --verbose)'{-v,--verbose}'[Verbose output]'
                            ;;
                        missing-events)
                            _arguments \
                                '(-s --satellite)'{-s,--satellite}'[Satellite name]:satellite:(fs-watcher terminal-satellite desktop-satellite system-satellite)' \
                                '(-t --time-range)'{-t,--time-range}'[Time range]:time-range:(1h 6h 12h 1d 3d 1w)'
                            ;;
                        curate)
                            _arguments \
                                '(-t --time-range)'{-t,--time-range}'[Time range]:time-range:(1h 6h 12h 1d 3d 1w 2w)' \
                                '(-s --source)'{-s,--source}'[Filter by source]:source:_sinex_sources' \
                                '(-e --event-type)'{-e,--event-type}'[Filter by event type]:event-type:_sinex_event_types' \
                                '(-n --limit)'{-n,--limit}'[Maximum groups to show]:limit:' \
                                '--auto-resolve[Auto-resolve obvious duplicates]'
                            ;;
                        *)
                            _values "explore commands" \
                                "coverage[Analyze coverage gaps]" \
                                "source-state[Inspect source state]" \
                                "missing-events[Detect missing events]" \
                                "curate[Interactive data curation]"
                            ;;
                    esac
                    ;;
            esac
            ;;
    esac
}

_exo "$@"
'''


def generate_fish_completion() -> str:
    """Generate fish completion script."""
    return '''# Fish completion for exo command

# Function to get sources from database
function __sinex_sources
    python3 -c "from cli.completion import get_sources; print('\\n'.join(get_sources()))" 2>/dev/null
end

# Function to get event types from database
function __sinex_event_types
    python3 -c "from cli.completion import get_event_types; print('\\n'.join(get_event_types()))" 2>/dev/null
end

# Function to get hosts from database
function __sinex_hosts
    python3 -c "from cli.completion import get_hosts; print('\\n'.join(get_hosts()))" 2>/dev/null
end

# Function to get automata from database
function __sinex_automata
    python3 -c "from cli.completion import get_automata; print('\\n'.join(get_automata()))" 2>/dev/null
end

# Function to get schema identifiers
function __sinex_schemas
    python3 -c "from cli.completion import get_schema_identifiers; print('\\n'.join(get_schema_identifiers()))" 2>/dev/null
end

# Top-level commands
complete -c exo -f -n '__fish_use_subcommand' -a 'query' -d 'Query events from database'
complete -c exo -f -n '__fish_use_subcommand' -a 'sources' -d 'List event sources'
complete -c exo -f -n '__fish_use_subcommand' -a 'stats' -d 'Show database statistics'
complete -c exo -f -n '__fish_use_subcommand' -a 'schema' -d 'Schema management'
complete -c exo -f -n '__fish_use_subcommand' -a 'automaton' -d 'Automaton management'
complete -c exo -f -n '__fish_use_subcommand' -a 'blob' -d 'Blob storage management'
complete -c exo -f -n '__fish_use_subcommand' -a 'dlq' -d 'Dead letter queue management'
complete -c exo -f -n '__fish_use_subcommand' -a 'event-archive' -d 'Archive specific events'
complete -c exo -f -n '__fish_use_subcommand' -a 'explore' -d 'Explore and curate data'
complete -c exo -f -n '__fish_use_subcommand' -a 'replay' -d 'Replay automaton processing'
complete -c exo -f -n '__fish_use_subcommand' -a 'restore' -d 'Restore archived events'
complete -c exo -f -n '__fish_use_subcommand' -a 'completion' -d 'Shell completion management'

# Query command completions
complete -c exo -f -n '__fish_seen_subcommand_from query' -s s -l source -d 'Filter by source' -a '(__sinex_sources)'
complete -c exo -f -n '__fish_seen_subcommand_from query' -s t -l event-type -d 'Filter by event type' -a '(__sinex_event_types)'
complete -c exo -f -n '__fish_seen_subcommand_from query' -l since -d 'Show events since datetime'
complete -c exo -f -n '__fish_seen_subcommand_from query' -l until -d 'Show events until datetime'
complete -c exo -f -n '__fish_seen_subcommand_from query' -s l -l last -d 'Show events from last N time'
complete -c exo -f -n '__fish_seen_subcommand_from query' -s n -l limit -d 'Maximum number of events'
complete -c exo -f -n '__fish_seen_subcommand_from query' -l host -d 'Filter by host' -a '(__sinex_hosts)'
complete -c exo -f -n '__fish_seen_subcommand_from query' -l payload-jq -d 'JQ filter for payload'
complete -c exo -f -n '__fish_seen_subcommand_from query' -l output-format -d 'Output format' -a 'table json csv yaml'

# Schema subcommands
complete -c exo -f -n '__fish_seen_subcommand_from schema; and not __fish_seen_subcommand_from list get' -a 'list' -d 'List schemas'
complete -c exo -f -n '__fish_seen_subcommand_from schema; and not __fish_seen_subcommand_from list get' -a 'get' -d 'Get specific schema'

# Schema list completions
complete -c exo -f -n '__fish_seen_subcommand_from schema; and __fish_seen_subcommand_from list' -s s -l source -d 'Filter by source' -a '(__sinex_sources)'
complete -c exo -f -n '__fish_seen_subcommand_from schema; and __fish_seen_subcommand_from list' -s t -l event-type -d 'Filter by event type' -a '(__sinex_event_types)'
complete -c exo -f -n '__fish_seen_subcommand_from schema; and __fish_seen_subcommand_from list' -l active-only -d 'Show only active schemas'

# Schema get completions
complete -c exo -f -n '__fish_seen_subcommand_from schema; and __fish_seen_subcommand_from get' -a '(__sinex_schemas)'

# Automaton subcommands
complete -c exo -f -n '__fish_seen_subcommand_from automaton; and not __fish_seen_subcommand_from list status' -a 'list' -d 'List automata'
complete -c exo -f -n '__fish_seen_subcommand_from automaton; and not __fish_seen_subcommand_from list status' -a 'status' -d 'Show automaton status'

# Automaton list completions
complete -c exo -f -n '__fish_seen_subcommand_from automaton; and __fish_seen_subcommand_from list' -s s -l status -d 'Filter by status' -a 'development stable deprecated'

# Automaton status completions
complete -c exo -f -n '__fish_seen_subcommand_from automaton; and __fish_seen_subcommand_from status' -a '(__sinex_automata)'

# Blob subcommands
complete -c exo -f -n '__fish_seen_subcommand_from blob; and not __fish_seen_subcommand_from ingest list get verify stage archive' -a 'ingest' -d 'Ingest file'
complete -c exo -f -n '__fish_seen_subcommand_from blob; and not __fish_seen_subcommand_from ingest list get verify stage archive' -a 'list' -d 'List blobs'
complete -c exo -f -n '__fish_seen_subcommand_from blob; and not __fish_seen_subcommand_from ingest list get verify stage archive' -a 'get' -d 'Get blob'
complete -c exo -f -n '__fish_seen_subcommand_from blob; and not __fish_seen_subcommand_from ingest list get verify stage archive' -a 'verify' -d 'Verify integrity'
complete -c exo -f -n '__fish_seen_subcommand_from blob; and not __fish_seen_subcommand_from ingest list get verify stage archive' -a 'stage' -d 'Stage source material'
complete -c exo -f -n '__fish_seen_subcommand_from blob; and not __fish_seen_subcommand_from ingest list get verify stage archive' -a 'archive' -d 'Archive blob and derived events'

# Blob ingest completions
complete -c exo -n '__fish_seen_subcommand_from blob; and __fish_seen_subcommand_from ingest' -s d -l description -d 'Description'
complete -c exo -n '__fish_seen_subcommand_from blob; and __fish_seen_subcommand_from ingest' -s r -l annex-repo -d 'Repository path'

# Blob list completions
complete -c exo -f -n '__fish_seen_subcommand_from blob; and __fish_seen_subcommand_from list' -s n -l limit -d 'Number to show'
complete -c exo -f -n '__fish_seen_subcommand_from blob; and __fish_seen_subcommand_from list' -s m -l mime-type -d 'Filter by MIME type'

# Blob get completions
complete -c exo -n '__fish_seen_subcommand_from blob; and __fish_seen_subcommand_from get' -s o -l output -d 'Output file'
complete -c exo -n '__fish_seen_subcommand_from blob; and __fish_seen_subcommand_from get' -s r -l annex-repo -d 'Repository path'

# Blob verify completions
complete -c exo -n '__fish_seen_subcommand_from blob; and __fish_seen_subcommand_from verify' -s r -l annex-repo -d 'Repository path'
complete -c exo -f -n '__fish_seen_subcommand_from blob; and __fish_seen_subcommand_from verify' -l fast -d 'Fast verification'

# Blob stage completions
complete -c exo -n '__fish_seen_subcommand_from blob; and __fish_seen_subcommand_from stage' -s s -l source-id -d 'Source identifier'
complete -c exo -n '__fish_seen_subcommand_from blob; and __fish_seen_subcommand_from stage' -s c -l comment -d 'User comment'
complete -c exo -n '__fish_seen_subcommand_from blob; and __fish_seen_subcommand_from stage' -s t -l tags -d 'Tags (comma-separated)'
complete -c exo -n '__fish_seen_subcommand_from blob; and __fish_seen_subcommand_from stage' -s r -l annex-repo -d 'Repository path'

# Blob archive completions
complete -c exo -f -n '__fish_seen_subcommand_from blob; and __fish_seen_subcommand_from archive' -s r -l reason -d 'Reason for archiving'
complete -c exo -f -n '__fish_seen_subcommand_from blob; and __fish_seen_subcommand_from archive' -l dry-run -d 'Show what would be archived'
complete -c exo -f -n '__fish_seen_subcommand_from blob; and __fish_seen_subcommand_from archive' -l force -d 'Archive without confirmation'

# DLQ subcommands
complete -c exo -f -n '__fish_seen_subcommand_from dlq; and not __fish_seen_subcommand_from list show retry resolve stats purge' -a 'list' -d 'List DLQ entries'
complete -c exo -f -n '__fish_seen_subcommand_from dlq; and not __fish_seen_subcommand_from list show retry resolve stats purge' -a 'show' -d 'Show DLQ entry details'
complete -c exo -f -n '__fish_seen_subcommand_from dlq; and not __fish_seen_subcommand_from list show retry resolve stats purge' -a 'retry' -d 'Retry DLQ entry'
complete -c exo -f -n '__fish_seen_subcommand_from dlq; and not __fish_seen_subcommand_from list show retry resolve stats purge' -a 'resolve' -d 'Resolve DLQ entry'
complete -c exo -f -n '__fish_seen_subcommand_from dlq; and not __fish_seen_subcommand_from list show retry resolve stats purge' -a 'stats' -d 'Show DLQ statistics'
complete -c exo -f -n '__fish_seen_subcommand_from dlq; and not __fish_seen_subcommand_from list show retry resolve stats purge' -a 'purge' -d 'Purge DLQ entries'

# DLQ list completions
complete -c exo -f -n '__fish_seen_subcommand_from dlq; and __fish_seen_subcommand_from list' -s a -l automaton -d 'Filter by automaton' -a '(__sinex_automata)'
complete -c exo -f -n '__fish_seen_subcommand_from dlq; and __fish_seen_subcommand_from list' -s s -l source -d 'Filter by source' -a '(__sinex_sources)'
complete -c exo -f -n '__fish_seen_subcommand_from dlq; and __fish_seen_subcommand_from list' -s t -l event-type -d 'Filter by event type' -a '(__sinex_event_types)'
complete -c exo -f -n '__fish_seen_subcommand_from dlq; and __fish_seen_subcommand_from list' -s c -l category -d 'Filter by category' -a 'retryable permanent system user'
complete -c exo -f -n '__fish_seen_subcommand_from dlq; and __fish_seen_subcommand_from list' -s n -l limit -d 'Maximum entries'
complete -c exo -f -n '__fish_seen_subcommand_from dlq; and __fish_seen_subcommand_from list' -l include-resolved -d 'Include resolved entries'
complete -c exo -f -n '__fish_seen_subcommand_from dlq; and __fish_seen_subcommand_from list' -l output-format -d 'Output format' -a 'table json csv'

# DLQ stats completions
complete -c exo -f -n '__fish_seen_subcommand_from dlq; and __fish_seen_subcommand_from stats' -s a -l automaton -d 'Filter by automaton' -a '(__sinex_automata)'
complete -c exo -f -n '__fish_seen_subcommand_from dlq; and __fish_seen_subcommand_from stats' -s d -l days -d 'Number of days'

# DLQ resolve completions
complete -c exo -f -n '__fish_seen_subcommand_from dlq; and __fish_seen_subcommand_from resolve' -l resolution -d 'Resolution type' -a 'manual purged'
complete -c exo -f -n '__fish_seen_subcommand_from dlq; and __fish_seen_subcommand_from resolve' -l dry-run -d 'Show what would be resolved'

# DLQ purge completions
complete -c exo -f -n '__fish_seen_subcommand_from dlq; and __fish_seen_subcommand_from purge' -s a -l automaton -d 'Purge by automaton' -a '(__sinex_automata)'
complete -c exo -f -n '__fish_seen_subcommand_from dlq; and __fish_seen_subcommand_from purge' -s c -l category -d 'Purge by category' -a 'retryable permanent system user'
complete -c exo -f -n '__fish_seen_subcommand_from dlq; and __fish_seen_subcommand_from purge' -l older-than -d 'Purge entries older than'
complete -c exo -f -n '__fish_seen_subcommand_from dlq; and __fish_seen_subcommand_from purge' -l resolved-only -d 'Only purge resolved entries'
complete -c exo -f -n '__fish_seen_subcommand_from dlq; and __fish_seen_subcommand_from purge' -l dry-run -d 'Show what would be purged'
complete -c exo -f -n '__fish_seen_subcommand_from dlq; and __fish_seen_subcommand_from purge' -l force -d 'Skip confirmation'

# Event-archive command completions  
complete -c exo -f -n '__fish_seen_subcommand_from event-archive' -s r -l reason -d 'Reason for archiving'
complete -c exo -f -n '__fish_seen_subcommand_from event-archive' -l dry-run -d 'Show what would be archived'
complete -c exo -f -n '__fish_seen_subcommand_from event-archive' -l force -d 'Archive without confirmation'

# Explore subcommands
complete -c exo -f -n '__fish_seen_subcommand_from explore; and not __fish_seen_subcommand_from coverage source-state missing-events curate' -a 'coverage' -d 'Analyze coverage gaps'
complete -c exo -f -n '__fish_seen_subcommand_from explore; and not __fish_seen_subcommand_from coverage source-state missing-events curate' -a 'source-state' -d 'Inspect source state'
complete -c exo -f -n '__fish_seen_subcommand_from explore; and not __fish_seen_subcommand_from coverage source-state missing-events curate' -a 'missing-events' -d 'Detect missing events'
complete -c exo -f -n '__fish_seen_subcommand_from explore; and not __fish_seen_subcommand_from coverage source-state missing-events curate' -a 'curate' -d 'Interactive data curation'

# Explore coverage completions
complete -c exo -f -n '__fish_seen_subcommand_from explore; and __fish_seen_subcommand_from coverage' -s s -l satellite -d 'Satellite name' -a 'fs-watcher terminal-satellite desktop-satellite system-satellite'
complete -c exo -f -n '__fish_seen_subcommand_from explore; and __fish_seen_subcommand_from coverage' -s t -l time-range -d 'Time range' -a '1h 6h 12h 1d 3d 1w 2w'
complete -c exo -f -n '__fish_seen_subcommand_from explore; and __fish_seen_subcommand_from coverage' -l source -d 'Filter by source' -a '(__sinex_sources)'

# Explore source-state completions
complete -c exo -f -n '__fish_seen_subcommand_from explore; and __fish_seen_subcommand_from source-state' -s s -l satellite -d 'Satellite name' -a 'fs-watcher terminal-satellite desktop-satellite system-satellite'
complete -c exo -f -n '__fish_seen_subcommand_from explore; and __fish_seen_subcommand_from source-state' -s v -l verbose -d 'Verbose output'

# Explore missing-events completions
complete -c exo -f -n '__fish_seen_subcommand_from explore; and __fish_seen_subcommand_from missing-events' -s s -l satellite -d 'Satellite name' -a 'fs-watcher terminal-satellite desktop-satellite system-satellite'
complete -c exo -f -n '__fish_seen_subcommand_from explore; and __fish_seen_subcommand_from missing-events' -s t -l time-range -d 'Time range' -a '1h 6h 12h 1d 3d 1w'

# Explore curate completions
complete -c exo -f -n '__fish_seen_subcommand_from explore; and __fish_seen_subcommand_from curate' -s t -l time-range -d 'Time range' -a '1h 6h 12h 1d 3d 1w 2w'
complete -c exo -f -n '__fish_seen_subcommand_from explore; and __fish_seen_subcommand_from curate' -s s -l source -d 'Filter by source' -a '(__sinex_sources)'
complete -c exo -f -n '__fish_seen_subcommand_from explore; and __fish_seen_subcommand_from curate' -s e -l event-type -d 'Filter by event type' -a '(__sinex_event_types)'
complete -c exo -f -n '__fish_seen_subcommand_from explore; and __fish_seen_subcommand_from curate' -s n -l limit -d 'Maximum groups to show'
complete -c exo -f -n '__fish_seen_subcommand_from explore; and __fish_seen_subcommand_from curate' -l auto-resolve -d 'Auto-resolve obvious duplicates'

# Completion subcommands
complete -c exo -f -n '__fish_seen_subcommand_from completion; and not __fish_seen_subcommand_from install generate' -a 'install' -d 'Install shell completion'
complete -c exo -f -n '__fish_seen_subcommand_from completion; and not __fish_seen_subcommand_from install generate' -a 'generate' -d 'Generate completion script'
complete -c exo -f -n '__fish_seen_subcommand_from completion; and __fish_seen_subcommand_from install' -a 'bash zsh fish' -d 'Shell type'
complete -c exo -f -n '__fish_seen_subcommand_from completion; and __fish_seen_subcommand_from generate' -a 'bash zsh fish' -d 'Shell type'
'''


def install_completion(shell: str, completion_dir: Optional[str] = None) -> bool:
    """Install completion script for specified shell."""
    try:
        if shell == 'bash':
            content = generate_bash_completion()
            default_dir = Path.home() / '.bash_completion.d'
            filename = 'exo'
        elif shell == 'zsh':
            content = generate_zsh_completion()
            default_dir = Path.home() / '.zsh' / 'completions'
            filename = '_exo'
        elif shell == 'fish':
            content = generate_fish_completion()
            default_dir = Path.home() / '.config' / 'fish' / 'completions'
            filename = 'exo.fish'
        else:
            print(f"Unsupported shell: {shell}")
            return False
        
        # Use provided directory or default
        target_dir = Path(completion_dir) if completion_dir else default_dir
        target_dir.mkdir(parents=True, exist_ok=True)
        
        target_file = target_dir / filename
        target_file.write_text(content)
        
        print(f"Installed {shell} completion to: {target_file}")
        
        if shell == 'bash':
            print("Add this to your ~/.bashrc:")
            print(f"source {target_file}")
        elif shell == 'zsh':
            print("Add this to your ~/.zshrc:")
            print(f"fpath=({target_dir} $fpath)")
            print("autoload -U compinit && compinit")
        elif shell == 'fish':
            print("Fish will automatically load completions from ~/.config/fish/completions/")
        
        return True
        
    except Exception as e:
        print(f"Failed to install {shell} completion: {e}")
        return False


if __name__ == '__main__':
    if len(sys.argv) < 2:
        print("Usage: completion.py <shell> [install-dir]")
        print("Shells: bash, zsh, fish")
        sys.exit(1)
    
    shell = sys.argv[1]
    install_dir = sys.argv[2] if len(sys.argv) > 2 else None
    
    if shell in ['bash', 'zsh', 'fish']:
        if install_completion(shell, install_dir):
            sys.exit(0)
        else:
            sys.exit(1)
    elif shell == 'sources':
        print(' '.join(get_sources()))
    elif shell == 'event-types':
        source = sys.argv[2] if len(sys.argv) > 2 else None
        print(' '.join(get_event_types(source)))
    elif shell == 'hosts':
        print(' '.join(get_hosts()))
    elif shell == 'automata':
        print(' '.join(get_automata()))
    elif shell == 'schemas':
        print(' '.join(get_schema_identifiers()))
    elif shell == 'blob-ids':
        print(' '.join(get_blob_ids()))
    elif shell == 'event-ids':
        print(' '.join(get_event_ids()))
    else:
        print(f"Unknown command: {shell}")
        sys.exit(1)