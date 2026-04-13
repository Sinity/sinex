# Sinex Secrets

Encrypted secrets managed with [agenix](https://github.com/ryantm/agenix).
Mirror of the Sinnix host layout.

## Encrypting

```
agenix -e nixos/secret/sinex-local-db.age \
  -r "ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAAIDwD8IB2eVfw6X7z9AqBBGjrqOIOCJ4tden1we7mCqOy sinity@sinnix-prime" \
  -r "ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAAILrplBDI4Rrb1hyzqYO7f8/2pmFWupC7C2+hYkBAkOdF root@sinnix-prime" \
  -r "ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAAIEsBMzW1MeF+qcxatMh4nvrQSl3jjAMQyMa+h7egmQyT root@sinnix-ethereal"
```

Replace the plaintext via stdin before running the command.

## Files

- `sinex-local-db.age` – local dev database password
- `sinex-gateway-admin-token.age` – gateway RPC admin bearer token (**required to enable the gateway**)
- `sinex-grafana-secret-key.age` – optional Grafana signing/encryption key override
- `sinex-remote-db.age` – remote satellite DB credential
- `sinex-remote-nats-*.age` – TLS CA/cert/key for remote satellites

## Creating the gateway admin token

Generate a random token and encrypt it:

```
head -c 32 /dev/urandom | base64 | tr -d '=' > /tmp/gateway-token
agenix -e nixos/secret/sinex-gateway-admin-token.age \
  -r "ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAAIDwD8IB2eVfw6X7z9AqBBGjrqOIOCJ4tden1we7mCqOy sinity@sinnix-prime" \
  -r "ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAAILrplBDI4Rrb1hyzqYO7f8/2pmFWupC7C2+hYkBAkOdF root@sinnix-prime" \
  -r "ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAAIEsBMzW1MeF+qcxatMh4nvrQSl3jjAMQyMa+h7egmQyT root@sinnix-ethereal" < /tmp/gateway-token
rm /tmp/gateway-token
```

Decrypt locally for debugging with the target-user SSH key (the NixOS module now also
adds this identity alongside the host key when available):

```
nix shell nixpkgs#rage -c rage -d -i ~/.ssh/id_ed25519 nixos/secret/<name>.age
```
