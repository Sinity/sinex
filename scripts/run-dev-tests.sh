#!/usr/bin/env bash
set -euo pipefail

pushd crate/lib/sinex-schema >/dev/null
DATABASE_URL="${DATABASE_URL_SUPERUSER:?missing superuser url}" cargo run -- up
popd

extra_args=()
if [ "${SINEX_DEVTEST_NO_FAIL_FAST:-0}" != "0" ]; then
  extra_args+=(--no-fail-fast)
fi

export SINEX_GATEWAY_ADMIN_TOKEN_FILE="${SINEX_GATEWAY_ADMIN_TOKEN_FILE:-$PWD/secret/sample-admin-token}"

PROPTEST_CASES="${PROPTEST_CASES:-64}" \
CARGO_TERM_COLOR="${CARGO_TERM_COLOR:-always}" \
SQLX_OFFLINE="${SQLX_OFFLINE:-1}" \
cargo nextest run --workspace --profile reliable "${extra_args[@]}"
