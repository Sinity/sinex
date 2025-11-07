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
- `sinex-remote-db.age` – remote satellite DB credential
- `sinex-remote-nats-*.age` – TLS CA/cert/key for remote satellites

Decrypt locally for debugging with:

```
AGEIDENTITY=/etc/ssh/ssh_host_ed25519_key agenix -d secret/<name>.age
```
