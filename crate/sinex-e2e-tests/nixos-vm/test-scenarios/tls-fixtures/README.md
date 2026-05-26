# TLS Test Fixtures

This directory contains TLS certificates for mTLS integration testing.

## Generated Certificates

- **CA Certificate**: `ca-cert.pem` + `ca-key.pem`
  - Used to sign both server and client certificates
  - Valid for 365 days from generation

- **Server Certificate**: `server-cert.pem` + `server-key.pem`
  - CN: localhost
  - Used by sinex-gateway for TLS termination

- **Client Certificate**: `client-cert.pem` + `client-key.pem`
  - CN: test-client
  - Valid client cert for mTLS authentication

- **Expired Client Certificate**: `expired-client-cert.pem` + `expired-client-key.pem`
  - CN: expired-client
  - Backdated to 2023-01-01 to 2023-01-02
  - Used for negative testing (should be rejected)

## Regenerating Certificates

```bash
cd tests/e2e/nixos-vm/test-scenarios/tls-fixtures

# Generate CA
openssl genrsa -out ca-key.pem 2048
openssl req -new -x509 -key ca-key.pem -out ca-cert.pem -days 365 \
  -subj "/CN=Test CA"

# Generate server cert
openssl genrsa -out server-key.pem 2048
openssl req -new -key server-key.pem -out server-csr.pem \
  -subj "/CN=localhost"
openssl x509 -req -in server-csr.pem -CA ca-cert.pem -CAkey ca-key.pem \
  -CAcreateserial -out server-cert.pem -days 365

# Generate client cert
openssl genrsa -out client-key.pem 2048
openssl req -new -key client-key.pem -out client-csr.pem \
  -subj "/CN=test-client"
openssl x509 -req -in client-csr.pem -CA ca-cert.pem -CAkey ca-key.pem \
  -CAcreateserial -out client-cert.pem -days 365

# Generate expired client cert
openssl genrsa -out expired-client-key.pem 2048
openssl req -new -key expired-client-key.pem -out expired-client-csr.pem \
  -subj "/CN=expired-client"
openssl x509 -req -in expired-client-csr.pem -CA ca-cert.pem -CAkey ca-key.pem \
  -set_serial 03 -out expired-client-cert.pem \
  -not_before 20230101000000Z -not_after 20230102000000Z
```

## Security Notice

**These certificates are for testing only!** Do not use in production.
- Private keys are committed to the repository
- Certificate validity is minimal
- No real verification of identity
