"""Real Cargo graph controls over a disposable copy of the native manifests."""
import argparse
import json
import os
from pathlib import Path
import shutil
import subprocess
import tempfile

ROOT = Path(__file__).resolve().parents[1]


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--before-checker", type=Path, help="explicit pre-fix xtask executable for the routing regression reproduction")
    before = parser.parse_args().before_checker
    report = Path(tempfile.mkdtemp(prefix="sinex-dependency-boundaries-"))
    results = []

    def run(label, args, cwd=ROOT, expected=None, env=None):
        result = subprocess.run(args, cwd=cwd, text=True, capture_output=True, env=env)
        (report / f"{label}.stdout").write_text(result.stdout)
        (report / f"{label}.stderr").write_text(result.stderr)
        results.append({"case": label, "argv": [str(x) for x in args], "cwd": str(cwd), "exit": result.returncode})
        if expected is None:
            assert result.returncode == 0, f"{label} failed: {result.stderr}"
        else:
            assert result.returncode != 0, f"{label} unexpectedly passed"
            assert expected in result.stderr, f"{label}: missing {expected!r}: {result.stderr}"
        return result

    try:
        run("build-checker", ["cargo", "build", "--locked", "--offline", "-p", "xtask"])
        run("checker-tests", ["cargo", "test", "--locked", "--offline", "-p", "xtask"])
        target = Path(os.environ.get("CARGO_TARGET_DIR", ROOT / "target"))
        binary = target.resolve() / "debug/xtask"
        run("native-graph", [binary, "boundaries", ROOT])
        # The existing frontend selects --workspace; exclude products for this
        # database-free forwarding control rather than changing that contract.
        excludes = [arg for name in ["sinex-model", "sinex-protocol", "sinex-store", "sinexd", "sinexctl"] for arg in ["--exclude", name]]
        run("normal-check-frontend", [binary, "check", "--locked", "--offline", *excludes])
        # Cargo's ordinary selectors still reach the native operation. Metadata
        # covers all workspace features/targets, including explicit -F requests.
        for feature in [["--features", "sinex-store/product"], ["--features=sinex-store/product"], ["-F", "sinex-store/product"], ["-Fsinex-store/product"]]:
            run(f"ordinary-selection-{len(results)}", [binary, "check", "--locked", "--offline", "--package=xtask", *excludes, *feature])
        copied = report / "workspace"
        copied.mkdir()
        for name in ["Cargo.toml", "Cargo.lock"]:
            shutil.copy2(ROOT / name, copied / name)
        for name in ["crate", "xtask"]:
            shutil.copytree(ROOT / name, copied / name)
        run("copied-graph", [binary, "boundaries", copied])
        manifest = copied / "crate/sinex-protocol/Cargo.toml"
        original = manifest.read_text()
        lock = (copied / "Cargo.lock").read_text()
        for case, header, declaration in [
            ("normal", "dependencies", 'store_alias = { package="sinex-store", path="../sinex-store" }'),
            ("optional", "dependencies", 'sinex-store = { path="../sinex-store", optional=true }'),
            ("development", "dev-dependencies", 'sinex-store = { path="../sinex-store" }'),
            ("build", "build-dependencies", 'sinex-store = { path="../sinex-store" }'),
            ("foreign-target", "target.'cfg(target_os = \"none\")'.dependencies", 'sinex-store = { path="../sinex-store" }'),
        ]:
            # Existing normal table must be extended rather than redeclared.
            text = original + ("\n" if header == "dependencies" else f"\n[{header}]\n") + declaration + "\n"
            manifest.write_text(text)
            run(f"{case}-resolve", ["cargo", "generate-lockfile", "--offline"], copied)
            run(f"forbidden-{case}", [binary, "boundaries", copied], expected="forbidden dependency: sinex-protocol -> sinex-store")
            if case == "normal":
                # These are actual copied native manifests with an illegal edge.
                # Cargo itself accepts the alternate workspace while xtask must
                # refuse graph-routing overrides before validating/compiling.
                (report / "forbidden-protocol-manifest.toml").write_text(text)
                if before:
                    for index, option in enumerate([["--manifest-path", str(copied / "Cargo.toml")], [f"--manifest-path={copied / 'Cargo.toml'}"]]):
                        run(f"pre-fix-routing-bypass-{index}", [before.resolve(), "check", *option, "--offline", *excludes])
                for frontend in ["check", "build", "test"]:
                    for option in [["--manifest-path", str(copied / "Cargo.toml")], [f"--manifest-path={copied / 'Cargo.toml'}"], ["--config", "net.offline=true"], ["--config=net.offline=true"], ["--lockfile-path", str(copied / "Cargo.lock")], ["-C", str(copied)]]:
                        label = f"routing-{frontend}-{len(results)}"
                        run(label, [binary, frontend, *option], expected="unsupported Cargo routing override")
                for frontend in ["check", "build", "test"]:
                    run(f"{frontend}-forbidden-edge", [binary, frontend], cwd=copied, expected="forbidden dependency: sinex-protocol -> sinex-store")
            manifest.write_text(original)
            (copied / "Cargo.lock").write_text(lock)
        daemon = copied / "crate/sinexd/Cargo.toml"
        daemon_text = daemon.read_text()
        daemon.write_text(daemon_text.replace('"runtime-tokio", "sqlite"', '"runtime-tokio", "sqlite", "postgres"'))
        run("postgres-resolve", ["cargo", "generate-lockfile", "--offline"], copied)
        run("forbidden-postgres-owner", [binary, "boundaries", copied], expected="forbidden dependency: sinexd -> sqlx")
        daemon.write_text(daemon_text)
        (copied / "Cargo.lock").write_text(lock)
        model = copied / "crate/sinex-model/Cargo.toml"
        model_text = model.read_text()
        model.write_text(model_text + '\ntokio = { version="1", optional=true }\n')
        run("pure-resolve", ["cargo", "generate-lockfile", "--offline"], copied)
        run("forbidden-pure-runtime", [binary, "boundaries", copied], expected="forbidden dependency: sinex-model -> tokio")
        model.write_text(model_text)
        (copied / "Cargo.lock").write_text(lock)
        wrapper = report / "neutral-wrapper"
        (wrapper / "src").mkdir(parents=True)
        (wrapper / "src/lib.rs").write_text("")
        (wrapper / "Cargo.toml").write_text('[package]\nname="neutral-wrapper"\nversion="0.1.0"\nedition="2024"\n[dependencies]\ntokio="1"\n')
        model.write_text(model_text + f'\nneutral-wrapper = {{ path="{wrapper}" }}\n')
        run("wrapper-resolve", ["cargo", "generate-lockfile", "--offline"], copied)
        run("forbidden-transitive-runtime", [binary, "boundaries", copied], expected="forbidden transitive dependency: sinex-model -> tokio")
        model.write_text(model_text)
        (copied / "Cargo.lock").write_text(lock)
        workspace = copied / "Cargo.toml"
        workspace_text = workspace.read_text()
        workspace.write_text(workspace_text.replace('"xtask"]', '"xtask", "surprise"]'))
        extra = copied / "surprise"
        (extra / "src").mkdir(parents=True)
        (extra / "src/lib.rs").write_text("")
        (extra / "Cargo.toml").write_text('[package]\nname="sinex-surprise"\nversion="0.1.0"\nedition="2024"\n')
        run("unknown-resolve", ["cargo", "generate-lockfile", "--offline"], copied)
        run("unknown-workspace", [binary, "boundaries", copied], expected="unassigned workspace package: sinex-surprise")
        workspace.write_text(workspace_text)
        (copied / "Cargo.lock").write_text(lock)
        manifest.write_text(original + '\nthis is not valid TOML\n')
        run("malformed-manifest", [binary, "boundaries", copied], expected="cargo metadata failed")
        run("check-propagates-refusal", [binary, "check"], cwd=copied, expected="cargo metadata failed")
        manifest.write_text(original)
        run("restored-graph", [binary, "boundaries", copied])
        empty_path = report / "empty-path"
        empty_path.mkdir()
        run("missing-cargo", [binary, "boundaries", copied], expected="could not execute cargo metadata", env={**os.environ, "PATH": str(empty_path)})
        fake = empty_path / "cargo"
        fake.write_text("#!/bin/sh\nprintf '{}'\n")
        fake.chmod(0o700)
        run("malformed-metadata", [binary, "boundaries", copied], expected="invalid cargo metadata", env={**os.environ, "PATH": str(empty_path)})
        status = "passed"
    except BaseException:
        status = "failed"
        raise
    finally:
        (report / "result.json").write_text(json.dumps({"status": status, "cases": results}, indent=2) + "\n")
        print(f"dependency boundary evidence: {report}")


if __name__ == "__main__":
    main()
