# TLS / NixOS Integration

This is the declarative TLS guide for Sinex on NixOS.

The intended split is:

- [tls-setup.md](tls-setup.md) for generic TLS behavior
- this document for module options and secret wiring

## Gateway: typed options

Gateway TLS is already first-class in the NixOS module:

```nix
services.sinex.core.gateway = {
  enable = true;
  listenAddress = "127.0.0.1:9999";

  tlsCertFile = config.age.secrets.gateway-cert.path;
  tlsKeyFile = config.age.secrets.gateway-key.path;

  # only needed when mTLS is enforced
  tlsClientCAFile = config.age.secrets.gateway-clients-ca.path;
  requireClientTLS = false;
};
```

Rules:

- gateway always needs `tlsCertFile` and `tlsKeyFile`
- non-loopback binds must enable mTLS
- when mTLS is enabled, `tlsClientCAFile` is required

For simple single-host setups, gateway certs can also be auto-generated:

```nix
services.sinex.core.gateway.autoGenerateTls = true;
```

## NATS: typed TLS options

NATS TLS should use the typed module surface instead of generic env injection:

```nix
services.sinex.nodes.nats = {
  servers = [ "tls://core.example.net:4222" ];
  tls = {
    requireTls = true;
    caCertFile = config.age.secrets.nats-ca.path;
    clientCertFile = config.age.secrets.nats-client-cert.path;
    clientKeyFile = config.age.secrets.nats-client-key.path;
  };
  auth = {
    credsFile = config.age.secrets.nats-client-creds.path;
  };
};
```

These options export the corresponding `SINEX_NATS_*` variables for both core services and nodes.

## What still belongs in `defaults.env`

Use `services.sinex.nodes.defaults.env` for application behavior flags, not for primary transport wiring.

Good example:

```nix
services.sinex.nodes.defaults.env = {
  SINEX_EDGE_MODE = "1";
};
```

Bad example:

```nix
# avoid this when a typed option already exists
services.sinex.nodes.defaults.env = {
  SINEX_NATS_CA_CERT = "/run/secrets/nats-ca";
};
```

## agenix pattern

Typical secret wiring:

```nix
age.secrets = {
  gateway-cert.file = ../secrets/gateway-cert.age;
  gateway-key.file = ../secrets/gateway-key.age;
  gateway-clients-ca.file = ../secrets/gateway-clients-ca.age;
  nats-ca.file = ../secrets/nats-ca.age;
  nats-client-cert.file = ../secrets/nats-client-cert.age;
  nats-client-key.file = ../secrets/nats-client-key.age;
};
```

Then reference those paths from the typed options shown above.

## Recommended shapes

### Local workstation

```nix
services.sinex.core.gateway = {
  autoGenerateTls = true;
  listenAddress = "127.0.0.1:9999";
};
```

### Remote node / satellite

```nix
services.sinex = {
  core.enable = false;

  nodes.nats = {
    servers = [ "tls://core.example.net:4222" ];
    tls = {
      requireTls = true;
      caCertFile = config.age.secrets.nats-ca.path;
      clientCertFile = config.age.secrets.nats-client-cert.path;
      clientKeyFile = config.age.secrets.nats-client-key.path;
    };
    auth.credsFile = config.age.secrets.nats-client-creds.path;
  };

  nodes.defaults.env.SINEX_EDGE_MODE = "1";
};
```

## Failure modes

### Gateway assertion failure

If evaluation says gateway TLS files are missing, set:

- `services.sinex.core.gateway.tlsCertFile`
- `services.sinex.core.gateway.tlsKeyFile`

or enable:

- `services.sinex.core.gateway.autoGenerateTls = true`

### Gateway mTLS assertion failure

If binding beyond loopback, set:

- `services.sinex.core.gateway.requireClientTLS = true`
- `services.sinex.core.gateway.tlsClientCAFile = ...`

### NATS client certificate mismatch

If you provide one of:

- `clientCertFile`
- `clientKeyFile`

you must provide both.

### NATS auth mode mismatch

If evaluation says multiple NATS auth modes are configured, keep exactly one of:

- `services.sinex.nodes.nats.auth.tokenFile`
- `services.sinex.nodes.nats.auth.credsFile`
- `services.sinex.nodes.nats.auth.nkeySeedFile`

## References

- [TLS Setup](tls-setup.md)
- [Security](../security.md)
- [nixos/example-remote-node.nix](../../../nixos/example-remote-node.nix)
