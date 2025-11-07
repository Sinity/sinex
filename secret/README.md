# Local deployment secrets

This directory contains public examples only. Real encrypted payloads and the
recipient manifest are operator state: keep them outside version control.

## Setup

Copy `secrets.example.nix` to the ignored `secrets.nix`, replace the placeholder
recipients, and create the `.age` files required by your deployment. The Sinex
NixOS module discovers encrypted files from
`services.sinex.secrets.secretsDirectory`; it defaults to this directory for a
local checkout and can point at a private deployment overlay for flake consumers.

Use `agenix -e <path>.age` with your own recipient manifest. Do not commit the
resulting ciphertext or real recipient keys to this repository.

`sample-admin-token` is deliberately non-secret test data.
