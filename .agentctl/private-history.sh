#!/usr/bin/env bash
set -euo pipefail

if [[ $# -ne 1 ]]; then
    printf '%s\n' 'expected one private fixture directory' >&2
    exit 2
fi
if [[ ! -r "$1/history.db" || ! -r "$1/expected.json" ]]; then
    printf '%s\n' 'the fixture needs readable history.db and expected.json files' >&2
    exit 2
fi

export SINEX_TEST_HISTORY_DIR="$1"
exec cargo run --locked --quiet -p xtask -- test -p sinexd \
    --test-threads 1 --run-ignored only \
    -E 'test(an_external_history_survives_a_stopped_admission_and_remains_queryable)'
