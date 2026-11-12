{
  description = "Sinex: a durable record of one person's digital life";
  inputs.nixpkgs.url = "github:NixOS/nixpkgs/nixos-unstable";
  inputs.crane.url = "github:ipetkov/crane/eb35abda9f232cc6610b1d1e3200d15c49b7ac54";
  inputs.rust-overlay = {
    url = "github:oxalica/rust-overlay/4748ec2f5ed4a881474ed4c98aa71a5308cdac8d";
    inputs.nixpkgs.follows = "nixpkgs";
  };
  outputs =
    {
      self,
      nixpkgs,
      crane,
      rust-overlay,
    }:
    let
      system = "x86_64-linux";
      pkgs = import nixpkgs {
        inherit system;
        overlays = [ rust-overlay.overlays.default ];
        config.allowUnfreePredicate = pkg: builtins.elem (pkgs.lib.getName pkg) [ "timescaledb" ];
      };
      toolchain = pkgs.rust-bin.fromRustupToolchainFile ./rust-toolchain.toml;
      nextest = pkgs.cargo-nextest;
      llvmTools = "${toolchain}/lib/rustlib/${pkgs.stdenv.hostPlatform.rust.rustcTarget}/bin";
      postgres = import ./nix/postgresql.nix {
        postgresql = pkgs.postgresql_18;
        extensions = p: [ p.pgvector ];
      };
      build = import ./nix/build.nix {
        inherit
          pkgs
          crane
          toolchain
          postgres
          ;
        source = self.outPath;
        revision = "${
          builtins.substring 0 40 (
            self.rev or self.dirtyRev or (throw "Sinex requires a Git source revision")
          )
        };dirty=${if self ? rev then "false" else "true"}";
      };
      package = build.package;
    in
    {
      packages.${system} = {
        default = package;
        sinex-dependencies = build.dependencies;
        sinexd = package;
        sinexctl = package;
        postgresql = postgres;
      };
      nixosModules.default = import ./nix/module.nix self;
      # VM tests: the deployed stack, a satellite capture node, the restore
      # drill. Built on demand, not by `package`.
      checks.${system} = {
        postgresql-uuid-bounds = import ./nix/tests/postgresql-uuid-bounds.nix { inherit pkgs postgres; };
        module = import ./nix/tests/module.nix self pkgs;
        vm-stack = pkgs.testers.runNixOSTest (import ./nix/tests/stack.nix self);
        vm-topology = pkgs.testers.runNixOSTest (import ./nix/tests/topology.nix self);
        vm-restore = pkgs.testers.runNixOSTest (import ./nix/tests/restore.nix self);
      };
      devShells.${system}.default = pkgs.mkShell {
        packages = [
          toolchain
          pkgs.git
          nextest
          pkgs.cargo-llvm-cov
          pkgs.cargo-fuzz
          postgres
          pkgs.nats-server
          pkgs.python3
          pkgs.pkg-config
          pkgs.mold
          pkgs.poppler-utils
        ];
        # mold links the test binaries several times faster than the default
        # linker; linking was a third to a half of each test binary's build.
        # A fixed build identity for development builds, so sinexd is not
        # rebuilt on every edit or commit; packages carry the real revision.
        SINEX_BUILD = "0000000000000000000000000000000000000000;dirty=true";
        RUST_SRC_PATH = "${toolchain}/lib/rustlib/src/rust/library";
        LLVM_COV = "${llvmTools}/llvm-cov";
        LLVM_PROFDATA = "${llvmTools}/llvm-profdata";
        # mold compresses the debug sections it writes: a 137 MB test binary
        # becomes 75.5 MB, and panic backtraces keep file and line (std reads
        # zlib-compressed DWARF). On the write-bound /realm drive, link output
        # is build time.
        CARGO_TARGET_X86_64_UNKNOWN_LINUX_GNU_RUSTFLAGS = "-C link-arg=-fuse-ld=mold -C link-arg=-Wl,--compress-debug-sections=zlib";
        shellHook = ''
          # Optional host placement; an explicit CARGO_TARGET_DIR is left as is.
          if [ -z "''${CARGO_TARGET_DIR:-}" ] && command -v sinnix-cargo-target >/dev/null 2>&1; then
            if cargo_target="$(sinnix-cargo-target)"; then
              export CARGO_TARGET_DIR="$cargo_target"
            fi
          fi
          export CARGO_TARGET_DIR="''${CARGO_TARGET_DIR:-$PWD/target}"
          if [ -d .githooks ] && git rev-parse --is-inside-work-tree >/dev/null 2>&1 \
            && ! git config --get core.hooksPath >/dev/null; then
            git config --local core.hooksPath .githooks
          fi
        '';
      };
    };
}
