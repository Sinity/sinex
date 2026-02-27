# Sinex Secrets

Encrypted secrets managed with [agenix](https://github.com/ryantm/agenix).
Mirror of the Sinnix host layout.

## Encrypting

```
agenix -e secret/sinex-local-db.age \
  -r "ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAAIDwD8IB2eVfw6X7z9AqBBGjrqOIOCJ4tden1we7mCqOy sinity@sinnix-prime" \
  -r "ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAAIA8aHYDIVHK5J4pkbtIPq8AbWH3Jc2HW28UHfGBrg50P root@sinnix-prime" \
  -r "ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAAIEsBMzW1MeF+qcxatMh4nvrQSl3jjAMQyMa+h7egmQyT root@sinnix-ethereal"
```

Replace the plaintext via stdin before running the command.

## Files

- `sinex-local-db.age` – local dev database password
- `sinex-gateway-admin-token.age` – gateway RPC admin bearer token (**required to enable the gateway**)
- `sinex-remote-db.age` – remote satellite DB credential
- `sinex-remote-nats-*.age` – TLS CA/cert/key for remote satellites

## Creating the gateway admin token

Generate a random token and encrypt it:

```
head -c 32 /dev/urandom | base64 | tr -d '=' > /tmp/gateway-token
agenix -e secret/sinex-gateway-admin-token.age \
  -r "ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAAIDwD8IB2eVfw6X7z9AqBBGjrqOIOCJ4tden1we7mCqOy sinity@sinnix-prime" \
  -r "ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAAIA8aHYDIVHK5J4pkbtIPq8AbWH3Jc2HW28UHfGBrg50P root@sinnix-prime" \
  -r "ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAAIEsBMzW1MeF+qcxatMh4nvrQSl3jjAMQyMa+h7egmQyT root@sinnix-ethereal" < /tmp/gateway-token
rm /tmp/gateway-token
```

Decrypt locally for debugging with:

```
AGEIDENTITY=/etc/ssh/ssh_host_ed25519_key agenix -d secret/<name>.age
```
