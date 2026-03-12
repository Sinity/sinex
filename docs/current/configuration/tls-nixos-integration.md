# TLS NixOS Integration

This guide covers TLS configuration for Sinex in NixOS deployments using declarative configuration and secret management.

## Overview

Sinex TLS in NixOS involves:
- **Secret management** via agenix or similar tools
- **Declarative configuration** through NixOS module options
- **Service-level TLS enforcement** for gateway and NATS
- **Automatic environment variable setup** from module configuration

## Prerequisites

- NixOS system with Sinex module imported
- agenix (or similar) configured for secret management
- Valid TLS certificates (production) or development certificates

## Gateway TLS Configuration

### Basic Module Setup

The Sinex NixOS module exposes TLS configuration through the `services.sinex.core.gateway` options:

```nix
{
  services.sinex = {
    enable = true;

    core.gateway = {
      enable = true;
      listenAddress = "0.0.0.0:9999";  # Bind address

      # TLS certificate paths
      tlsCertFile = "/path/to/server.pem";
      tlsKeyFile = "/path/to/server-key.pem";
      tlsClientCAFile = "/path/to/ca.pem";  # Optional: for mTLS

      # Force mTLS even on loopback (optional)
      requireClientTLS = false;  # Default: false
    };
  };
}
```

### Module Options

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `tlsCertFile` | `nullOr path` | `null` | Path to server certificate |
| `tlsKeyFile` | `nullOr path` | `null` | Path to server private key |
| `tlsClientCAFile` | `nullOr path` | `null` | CA bundle for client verification (mTLS) |
| `requireClientTLS` | `bool` | `false` | Force mTLS on loopback connections |

**Note**: The module validates that both `tlsCertFile` and `tlsKeyFile` are set when the gateway is enabled.

### Environment Variable Mapping

The NixOS module automatically sets environment variables from module options:

| Module Option | Environment Variable |
|---------------|---------------------|
| `tlsCertFile` | `SINEX_GATEWAY_TLS_CERT` |
| `tlsKeyFile` | `SINEX_GATEWAY_TLS_KEY` |
| `tlsClientCAFile` | `SINEX_GATEWAY_TLS_CLIENT_CA` |
| `requireClientTLS = true` | `SINEX_GATEWAY_REQUIRE_CLIENT_TLS=1` |

See `nixos/modules/node-services.nix:131-134` for implementation.

### mTLS Enforcement

**Automatic**: mTLS is **required** when binding to non-loopback addresses (e.g., `0.0.0.0:9999`, `192.168.1.10:9999`).

**Manual override**: Set `requireClientTLS = true` to enforce mTLS even on loopback (`127.0.0.1`) addresses.

```nix
{
  services.sinex.core.gateway = {
    listenAddress = "127.0.0.1:9999";  # Loopback binding
    requireClientTLS = true;  # Force mTLS anyway
    tlsClientCAFile = "/run/secrets/sinex-ca";
  };
}
```

## NATS TLS Configuration

### Node-Level NATS TLS

NATS TLS is configured at the **node level** through environment variables in the `satellites.defaults.env` section:

```nix
{
  services.sinex.satellites = {
    enable = true;
    nats.servers = [ "tls://nats.example.com:4222" ];

    defaults.env = {
      # Server verification (minimum for TLS)
      SINEX_NATS_CA_CERT = "/run/secrets/nats-ca";

      # Client certificates (for mTLS)
      SINEX_NATS_CLIENT_CERT = "/run/secrets/nats-client-cert";
      SINEX_NATS_CLIENT_KEY = "/run/secrets/nats-client-key";
    };

    filesystem.enable = true;
    terminal.enable = true;
  };
}
```

### NATS Environment Variables

| Variable | Purpose | Required |
|----------|---------|----------|
| `SINEX_NATS_URL` | Set via `nats.servers` | Yes |
| `SINEX_NATS_REQUIRE_TLS` | Enforce TLS validation | Recommended |
| `SINEX_NATS_CA_CERT` | CA certificate path | For server verification |
| `SINEX_NATS_CLIENT_CERT` | Client certificate path | For mTLS |
| `SINEX_NATS_CLIENT_KEY` | Client private key path | For mTLS |

**Note**: Use `tls://` scheme in `nats.servers` URLs to enable TLS.

### Complete Remote Satellite Example

For edge deployments connecting to remote NATS:

```nix
{
  services.sinex = {
    enable = true;

    # Disable local core services
    core.enable = false;

    satellites = {
      enable = true;
      nats.servers = [ "tls://core.example.net:4222" ];

      defaults.env = {
        SINEX_NATS_CA_CERT = config.sinex.secrets.paths."sinex-remote-nats-ca";
        SINEX_NATS_CLIENT_CERT = config.sinex.secrets.paths."sinex-remote-nats-cert";
        SINEX_NATS_CLIENT_KEY = config.sinex.secrets.paths."sinex-remote-nats-key";
        SINEX_EDGE_MODE = "1";  # No DB dependency, NATS KV only
      };

      filesystem.enable = true;
      terminal.enable = true;
    };
  };

  # Disable local services
  services.nats.enable = lib.mkForce false;
  services.postgresql.enable = lib.mkForce false;
}
```

See `nixos/example-remote-satellite.nix` for complete working example.

## Secret Management with agenix

### Installing agenix

Add to `flake.nix`:

```nix
{
  inputs = {
    agenix.url = "github:ryantm/agenix";
  };

  outputs = { nixpkgs, agenix, ... }: {
    nixosConfigurations.myhost = nixpkgs.lib.nixosSystem {
      modules = [
        agenix.nixosModules.default
        ./configuration.nix
      ];
    };
  };
}
```

### Defining Secrets

Create `secrets/secrets.nix`:

```nix
let
  sinnix-prime = "ssh-ed25519 AAAAC3... host-key";
  user1 = "ssh-ed25519 AAAAC3... user1-key";
in
{
  "sinex-gateway-cert.age".publicKeys = [ sinnix-prime user1 ];
  "sinex-gateway-key.age".publicKeys = [ sinnix-prime user1 ];
  "sinex-ca.age".publicKeys = [ sinnix-prime user1 ];
  "nats-ca.age".publicKeys = [ sinnix-prime user1 ];
  "nats-client-cert.age".publicKeys = [ sinnix-prime user1 ];
  "nats-client-key.age".publicKeys = [ sinnix-prime user1 ];
}
```

### Encrypting Certificates

```bash
# Ensure development certificates exist (auto-generated by preflight)
cd /path/to/sinex
xtask doctor --fix

# Encrypt with agenix
agenix -e secrets/sinex-gateway-cert.age
# Paste contents of .sinex/tls/server.pem, save and exit

agenix -e secrets/sinex-gateway-key.age
# Paste contents of .sinex/tls/server-key.pem, save and exit

agenix -e secrets/sinex-ca.age
# Paste contents of .sinex/tls/ca.pem, save and exit
```

**Important**: For development, the auto-generated certificates are fine. Production certificates should come from a proper CA (Let's Encrypt, internal PKI).

### Using agenix Secrets in NixOS

```nix
{
  # Import secrets
  age.secrets = {
    sinex-gateway-cert = {
      file = ../secrets/sinex-gateway-cert.age;
      owner = "sinex";
      group = "sinex";
      mode = "0440";
    };
    sinex-gateway-key = {
      file = ../secrets/sinex-gateway-key.age;
      owner = "sinex";
      group = "sinex";
      mode = "0440";
    };
    sinex-ca = {
      file = ../secrets/sinex-ca.age;
      owner = "sinex";
      group = "sinex";
      mode = "0440";
    };
  };

  # Reference in Sinex module
  services.sinex.core.gateway = {
    enable = true;
    tlsCertFile = config.age.secrets.sinex-gateway-cert.path;
    tlsKeyFile = config.age.secrets.sinex-gateway-key.path;
    tlsClientCAFile = config.age.secrets.sinex-ca.path;
  };
}
```

Secrets are decrypted at boot to `/run/agenix/secret-name` with proper ownership and permissions.

## Development vs Production

### Development Setup

For local development and testing:

1. **Certificates are generated automatically** by preflight:
   ```bash
   xtask doctor --fix
   ```

2. **Or encrypt with agenix** (recommended for consistent setup):
   ```bash
   agenix -e secrets/dev-gateway-cert.age
   # Paste contents of .sinex/tls/server.pem, save and exit
   ```

### Production Setup

For production deployments:

1. **Obtain certificates** from Let's Encrypt or internal CA:
   ```bash
   # Example with certbot
   certbot certonly --standalone -d gateway.example.com
   ```

2. **Encrypt with agenix**:
   ```bash
   agenix -e secrets/prod-gateway-cert.age
   # Paste contents of /etc/letsencrypt/live/gateway.example.com/fullchain.pem

   agenix -e secrets/prod-gateway-key.age
   # Paste contents of /etc/letsencrypt/live/gateway.example.com/privkey.pem
   ```

3. **Configure renewal** with post-renewal hook:
   ```nix
   {
     security.acme = {
       acceptTerms = true;
       defaults.email = "admin@example.com";

       certs."gateway.example.com" = {
         postRun = ''
           # Re-encrypt renewed certificate
           ${pkgs.agenix}/bin/agenix -e /path/to/secrets/prod-gateway-cert.age < $RENEWED_LINEAGE/fullchain.pem
           ${pkgs.agenix}/bin/agenix -e /path/to/secrets/prod-gateway-key.age < $RENEWED_LINEAGE/privkey.pem

           # Restart gateway
           systemctl restart sinex-gateway
         '';
       };
     };
   }
   ```

### Behind a Reverse Proxy

If running behind nginx, HAProxy, or cloud load balancer:

```nix
{
  # Gateway binds to loopback only
  services.sinex.core.gateway = {
    listenAddress = "127.0.0.1:9999";
    # TLS still required, but can use self-signed for backend
    tlsCertFile = "${./dev-certs}/server.pem";
    tlsKeyFile = "${./dev-certs}/server-key.pem";
    # No client CA needed - proxy does client auth
  };

  # Proxy handles TLS termination
  services.nginx.virtualHosts."gateway.example.com" = {
    enableACME = true;
    forceSSL = true;
    locations."/" = {
      proxyPass = "https://127.0.0.1:9999";
      # Optional: client cert verification at proxy level
      extraConfig = ''
        ssl_client_certificate /etc/ssl/client-ca.pem;
        ssl_verify_client optional;
        proxy_set_header X-Client-Cert $ssl_client_cert;
      '';
    };
  };
}
```

## Certificate Rotation

### Manual Rotation

1. **Regenerate certificates**:
   ```bash
   xtask reset --yes --tls
   # Or obtain new certificates from CA
   ```

2. **Re-encrypt with agenix**:
   ```bash
   agenix -e secrets/sinex-gateway-cert.age
   # Update with new certificate content from .sinex/tls/server.pem
   ```

3. **Deploy and restart**:
   ```bash
   nixos-rebuild switch
   # Services automatically restart with new secrets
   ```

### Automated Rotation

For Let's Encrypt with automatic renewal:

```nix
{
  security.acme.certs."gateway.example.com" = {
    postRun = ''
      # Copy to agenix secrets location
      cp $RENEWED_LINEAGE/fullchain.pem /etc/nixos/secrets/gateway-cert.pem
      cp $RENEWED_LINEAGE/privkey.pem /etc/nixos/secrets/gateway-key.pem

      # Re-encrypt
      cd /etc/nixos
      ${pkgs.agenix}/bin/agenix -e secrets/sinex-gateway-cert.age < secrets/gateway-cert.pem
      ${pkgs.agenix}/bin/agenix -e secrets/sinex-gateway-key.age < secrets/gateway-key.pem

      # Trigger rebuild
      systemctl restart sinex-gateway
    '';
  };
}
```

### Monitoring Expiration

Use systemd timers or monitoring systems:

```nix
{
  systemd.services.sinex-cert-check = {
    description = "Check Sinex certificate expiration";
    script = ''
      ${pkgs.openssl}/bin/openssl x509 -in ${config.age.secrets.sinex-gateway-cert.path} -noout -checkend 604800
      if [ $? -ne 0 ]; then
        echo "Certificate expires within 7 days!"
        exit 1
      fi
    '';
  };

  systemd.timers.sinex-cert-check = {
    wantedBy = [ "timers.target" ];
    timerConfig = {
      OnCalendar = "daily";
      Persistent = true;
    };
  };
}
```

## Troubleshooting

### Certificate Not Found

**Error**: `SINEX_GATEWAY_TLS_CERT is required for TCP bindings`

**Fix**: Ensure `tlsCertFile` and `tlsKeyFile` are set in module options.

### Permission Denied

**Error**: `Permission denied: /run/agenix/sinex-gateway-key`

**Fix**: Check agenix secret ownership:
```nix
{
  age.secrets.sinex-gateway-key = {
    file = ../secrets/sinex-gateway-key.age;
    owner = "sinex";  # Must match service user
    group = "sinex";
    mode = "0440";
  };
}
```

### mTLS Required but No Client CA

**Error**: `SINEX_GATEWAY_TLS_CLIENT_CA is required when mTLS is enforced`

**Fix**: Either:
1. Bind to loopback: `listenAddress = "127.0.0.1:9999"`
2. Provide client CA: `tlsClientCAFile = config.age.secrets.sinex-ca.path`

### NATS TLS Connection Failures

**Error**: `NATS connection failed: certificate verify failed`

**Check**:
1. Verify CA certificate path is correct
2. Ensure URL uses `tls://` scheme
3. Check certificate hasn't expired:
   ```bash
   openssl x509 -in /run/agenix/nats-ca -noout -dates
   ```

### Certificate Chain Issues

**Error**: `unable to get local issuer certificate`

**Fix**: Ensure CA certificate includes full chain (intermediate + root CAs):
```bash
# Combine certificates
cat intermediate.pem root.pem > ca-bundle.pem

# Encrypt bundle
agenix -e secrets/sinex-ca.age < ca-bundle.pem
```

## Security Considerations

1. **Never commit secrets** - Use agenix or similar tools
2. **Restrict secret access** - Set proper `owner` and `mode` in agenix config
3. **Rotate regularly** - Automate certificate renewal and rotation
4. **Monitor expiration** - Use systemd timers or monitoring systems
5. **Enable mTLS in production** - Especially for exposed services
6. **Use strong keys** - Generated certificates use 2048-bit RSA (minimum)
7. **Audit secret access** - Check `/run/agenix` permissions after deployment

## Verification

### After Deployment

```bash
# Check service status
systemctl status sinex-gateway

# Verify TLS configuration
sudo -u sinex openssl s_client -connect localhost:9999 \
  -CAfile /run/agenix/sinex-ca \
  -showcerts

# Check certificate expiration
openssl x509 -in /run/agenix/sinex-gateway-cert -noout -dates
```

### Integration Testing

```nix
{
  # Add to NixOS test
  nodes.server = {
    services.sinex.core.gateway = {
      enable = true;
      tlsCertFile = "${./test-certs}/server.pem";
      tlsKeyFile = "${./test-certs}/server-key.pem";
    };
  };

  testScript = ''
    server.wait_for_unit("sinex-gateway")
    server.succeed("curl --cacert ${./test-certs}/ca.pem https://localhost:9999")
  '';
}
```

## Complete Examples

### Single-Node Development Setup

```nix
{
  # Auto-generated dev certs in .sinex/tls/
  # Encrypt with agenix: agenix -e secrets/dev-gateway-cert.age

  age.secrets = {
    sinex-dev-cert.file = ../secrets/dev-gateway-cert.age;
    sinex-dev-key.file = ../secrets/dev-gateway-key.age;
    sinex-dev-ca.file = ../secrets/dev-ca.age;
  };

  services.sinex = {
    enable = true;

    database.autoSetup = true;
    nats.autoSetup = true;

    core = {
      enable = true;
      gateway = {
        enable = true;
        listenAddress = "127.0.0.1:9999";
        tlsCertFile = config.age.secrets.sinex-dev-cert.path;
        tlsKeyFile = config.age.secrets.sinex-dev-key.path;
        tlsClientCAFile = config.age.secrets.sinex-dev-ca.path;
      };
    };

    satellites = {
      enable = true;
      filesystem.enable = true;
      terminal.enable = true;
    };
  };
}
```

### Production Multi-Node Setup

**Core server** (runs ingestd + gateway + NATS):

```nix
{
  age.secrets = {
    gateway-cert.file = ../secrets/prod-gateway-cert.age;
    gateway-key.file = ../secrets/prod-gateway-key.age;
    gateway-ca.file = ../secrets/prod-ca.age;
  };

  services.sinex = {
    enable = true;

    core = {
      enable = true;
      gateway = {
        enable = true;
        listenAddress = "0.0.0.0:9999";  # Network exposed
        tlsCertFile = config.age.secrets.gateway-cert.path;
        tlsKeyFile = config.age.secrets.gateway-key.path;
        tlsClientCAFile = config.age.secrets.gateway-ca.path;  # mTLS required
      };
    };

    nats = {
      enable = true;
      tls = {
        certFile = config.age.secrets.nats-cert.path;
        keyFile = config.age.secrets.nats-key.path;
        caFile = config.age.secrets.nats-ca.path;
      };
    };
  };
}
```

**Edge satellite** (runs only collectors):

```nix
{
  age.secrets = {
    nats-ca.file = ../secrets/prod-nats-ca.age;
    nats-client-cert.file = ../secrets/satellite-nats-cert.age;
    nats-client-key.file = ../secrets/satellite-nats-key.age;
  };

  services.sinex = {
    enable = true;

    core.enable = false;  # No ingestd/gateway

    satellites = {
      enable = true;
      nats.servers = [ "tls://core.example.net:4222" ];

      defaults.env = {
        SINEX_NATS_CA_CERT = config.age.secrets.nats-ca.path;
        SINEX_NATS_CLIENT_CERT = config.age.secrets.nats-client-cert.path;
        SINEX_NATS_CLIENT_KEY = config.age.secrets.nats-client-key.path;
        SINEX_EDGE_MODE = "1";
      };

      filesystem.enable = true;
      terminal.enable = true;
    };
  };
}
```

## Related Documentation

- [TLS Setup Guide](tls-setup.md) - Development certificate generation and verification
- NixOS module options: `/realm/project/sinnix/docs/`
- Remote satellite example: `/realm/project/sinnix/nixos/example-remote-satellite.nix`
- [Security Architecture](../architecture/security-architecture.md) - TLS enforcement policies
