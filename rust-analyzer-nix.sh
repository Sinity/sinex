#!/usr/bin/env bash
# Wrapper to run rust-analyzer inside nix-shell for NixOS users
exec nix-shell --run "rust-analyzer $@"
