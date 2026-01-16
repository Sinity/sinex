#!/usr/bin/env bash
set -e

# Generate self-signed CA and certs for mTLS testing
# Usage: ./generate_tls_fixtures.sh <output_dir>

OUT_DIR=${1:-"tests/fixtures/tls"}
mkdir -p "$OUT_DIR"

echo "Generating generic mTLS fixtures in $OUT_DIR..."

cd "$OUT_DIR"

# 1. CA
if [ ! -f ca.pem ]; then
    echo "Generating CA..."
    openssl genrsa -out ca.key 2048
    openssl req -new -x509 -days 3650 -key ca.key -out ca.pem -subj "/CN=Sinex Test CA"
fi

# 2. Server Cert
if [ ! -f server.pem ]; then
    echo "Generating Server Cert..."
    openssl genrsa -out server-key.pem 2048
    openssl req -new -key server-key.pem -out server.csr -subj "/CN=localhost" \
        -addext "subjectAltName = DNS:localhost,IP:127.0.0.1"
    openssl x509 -req -days 365 -in server.csr -CA ca.pem -CAkey ca.key -CAcreateserial \
        -out server.pem -extensions v3_req -extfile <(echo "[v3_req]
subjectAltName = DNS:localhost,IP:127.0.0.1")
fi

# 3. Client Cert
if [ ! -f client.pem ]; then
    echo "Generating Client Cert..."
    openssl genrsa -out client-key.pem 2048
    openssl req -new -key client-key.pem -out client.csr -subj "/CN=sinex-client"
    openssl x509 -req -days 365 -in client.csr -CA ca.pem -CAkey ca.key -CAcreateserial \
        -out client.pem
fi

echo "Done."
