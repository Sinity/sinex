#!/usr/bin/env python3
"""Split Rust test modules into *_test.rs files.

The tool is intentionally conservative:
- it scans crate/, tests/, and xtask/ by default
- it skips tests.rs and existing *_test.rs unless --include-split-test-files is set
- it only handles files with exactly one true inline `mod tests { ... }`
- it writes sibling `<stem>_test.rs` files and leaves `#[path = "..."] mod tests;`
- it fails instead of overwriting an existing target file
- with --canonicalize-existing-split, it moves existing implicit `mod tests;`
  module files (`foo/tests.rs`, `tests.rs`) to explicit sibling *_test.rs files

Run without --apply for a dry-run inventory.
"""

from __future__ import annotations

import argparse
import json
from collections import defaultdict
from dataclasses import dataclass
from pathlib import Path


DEFAULT_ROOTS = ("crate", "tests", "xtask")


@dataclass
class Candidate:
    source: Path
    target: Path
    module_name: str
    cfg_start: int
    module_end: int
    attrs_after_cfg: str
    body: str
    merge_existing: bool = False


@dataclass
class SplitModuleCandidate:
    source: Path
    current_module_file: Path
    target: Path
    decl_start: int
    decl_end: int


def is_skipped(
    path: Path,
    *,
    include_test_directories: bool,
    include_split_test_files: bool,
    include_tests_rs: bool,
) -> bool:
    if path.name.endswith("_test.rs") and not include_split_test_files:
        return True
    if path.name == "tests.rs" and not include_tests_rs:
        return True
    return (not include_test_directories) and "tests" in path.parts


def iter_rust_files(
    roots: list[Path],
    *,
    include_test_directories: bool,
    include_split_test_files: bool,
    include_tests_rs: bool,
) -> list[Path]:
    files: list[Path] = []
    for root in roots:
        if root.is_file() and root.suffix == ".rs":
            if not is_skipped(
                root,
                include_test_directories=include_test_directories,
                include_split_test_files=include_split_test_files,
                include_tests_rs=include_tests_rs,
            ):
                files.append(root)
            continue
        for path in root.rglob("*.rs"):
            if not is_skipped(
                path,
                include_test_directories=include_test_directories,
                include_split_test_files=include_split_test_files,
                include_tests_rs=include_tests_rs,
            ):
                files.append(path)
    return sorted(files)


def line_start(text: str, idx: int) -> int:
    return text.rfind("\n", 0, idx) + 1


def next_nonblank_line(text: str, start: int) -> tuple[int, str] | None:
    pos = start
    while pos < len(text):
        end = text.find("\n", pos)
        if end == -1:
            end = len(text)
        line = text[pos:end]
        if line.strip():
            return pos, line
        pos = end + 1
    return None


def find_matching_brace(text: str, open_idx: int) -> int:
    depth = 0
    i = open_idx
    line_comment = False
    block_depth = 0
    string_quote: str | None = None
    raw_hashes: int | None = None
    char_lit = False

    while i < len(text):
        ch = text[i]
        nxt = text[i + 1] if i + 1 < len(text) else ""

        if line_comment:
            if ch == "\n":
                line_comment = False
            i += 1
            continue

        if block_depth:
            if ch == "/" and nxt == "*":
                block_depth += 1
                i += 2
                continue
            if ch == "*" and nxt == "/":
                block_depth -= 1
                i += 2
                continue
            i += 1
            continue

        if raw_hashes is not None:
            if ch == '"' and text.startswith("#" * raw_hashes, i + 1):
                i += raw_hashes + 1
                raw_hashes = None
            else:
                i += 1
            continue

        if string_quote:
            if ch == "\\":
                i += 2
                continue
            if ch == string_quote:
                string_quote = None
            i += 1
            continue

        if char_lit:
            if ch == "\\":
                i += 2
                continue
            if ch == "'":
                char_lit = False
            i += 1
            continue

        if ch == "/" and nxt == "/":
            line_comment = True
            i += 2
            continue
        if ch == "/" and nxt == "*":
            block_depth = 1
            i += 2
            continue
        if ch == "r":
            j = i + 1
            while j < len(text) and text[j] == "#":
                j += 1
            if j < len(text) and text[j] == '"':
                raw_hashes = j - i - 1
                i = j + 1
                continue
        if ch == '"':
            string_quote = ch
            i += 1
            continue
        if ch == "'" and not (nxt.isalpha() or nxt == "_"):
            char_lit = True
            i += 1
            continue
        if ch == "{":
            depth += 1
        elif ch == "}":
            depth -= 1
            if depth == 0:
                return i
        i += 1

    raise ValueError("unmatched brace")


def dedent_body(body: str) -> str:
    if body.startswith("\n"):
        body = body[1:]
    if body.endswith("\n"):
        body = body[:-1]
    lines = body.splitlines()
    indents = [
        len(line) - len(line.lstrip(" "))
        for line in lines
        if line.strip()
    ]
    if indents:
        width = min(indents)
        if width:
            lines = [line[width:] if len(line) >= width else line for line in lines]
    return "\n".join(lines).rstrip() + "\n"


def find_candidates(path: Path) -> list[Candidate]:
    import re

    text = path.read_text()
    candidates: list[Candidate] = []
    search = 0
    module_pattern = re.compile(r"^(?P<vis>pub(?:\([^)]*\))?\s+)?mod\s+(?P<name>[A-Za-z_][A-Za-z0-9_]*)\s*\{")
    while True:
        cfg = text.find("#[cfg", search)
        if cfg == -1:
            break
        cfg_line_end = text.find("\n", cfg)
        if cfg_line_end == -1:
            cfg_line_end = len(text)
        cfg_line = text[cfg:cfg_line_end]
        if cfg_line.strip() != "#[cfg(test)]":
            search = cfg_line_end + 1
            continue
        cfg_start = line_start(text, cfg)
        probe = cfg_line_end + 1
        while True:
            next_line = next_nonblank_line(text, probe)
            if next_line is None:
                break
            line_pos, line = next_line
            stripped = line.strip()
            if stripped.startswith("#["):
                probe = text.find("\n", line_pos)
                if probe == -1:
                    break
                probe += 1
                continue
            module_match = module_pattern.match(stripped)
            if module_match:
                open_idx = text.find("{", line_pos)
                close_idx = find_matching_brace(text, open_idx)
                after = close_idx + 1
                if after < len(text) and text[after] == "\n":
                    after += 1
                module_name = module_match.group("name")
                if module_name == "tests":
                    target_name = f"{path.stem}_test.rs"
                else:
                    target_name = f"{path.stem}_{module_name}.rs"
                target = inline_test_target(path, target_name)
                candidates.append(
                    Candidate(
                        source=path,
                        target=target,
                        module_name=module_name,
                        cfg_start=cfg_start,
                        module_end=after,
                        attrs_after_cfg=text[cfg_line_end + 1 : line_pos],
                        body=dedent_body(text[open_idx + 1 : close_idx]),
                    )
                )
                search = after
                break
            search = line_pos + len(line)
            break
        else:
            search = cfg_line_end + 1
            continue
        if search <= cfg:
            search = cfg_line_end + 1
    return candidates


def split_candidate(candidate: Candidate) -> None:
    split_candidates_for_source([candidate])


def rust_path_attr_target(source: Path, target: Path) -> str:
    return target.relative_to(source.parent).as_posix()


def inline_test_target(path: Path, target_name: str) -> Path:
    if path.parent.name == "tests":
        return path.with_name(path.stem) / target_name
    return path.with_name(target_name)


def split_candidates_for_source(candidates: list[Candidate]) -> None:
    if not candidates:
        return
    source = candidates[0].source
    if any(candidate.source != source for candidate in candidates):
        raise ValueError("split_candidates_for_source received multiple sources")
    target_names = [candidate.target for candidate in candidates]
    if len(target_names) != len(set(target_names)):
        raise ValueError(f"multiple candidates share a target in {source}")
    for candidate in candidates:
        if candidate.target.exists() and not candidate.merge_existing:
            raise FileExistsError(f"target already exists: {candidate.target}")
    text = source.read_text()
    for candidate in sorted(candidates, key=lambda item: item.cfg_start, reverse=True):
        indent = text[candidate.cfg_start : text.find("#[cfg(test)]", candidate.cfg_start)]
        replacement = (
            f"{indent}#[cfg(test)]\n"
            f"{candidate.attrs_after_cfg}"
            f"{indent}#[path = \"{rust_path_attr_target(source, candidate.target)}\"]\n"
            f"{indent}mod {candidate.module_name};\n"
        )
        text = text[: candidate.cfg_start] + replacement + text[candidate.module_end :]
    source.write_text(text)
    for candidate in candidates:
        candidate.target.parent.mkdir(parents=True, exist_ok=True)
        if candidate.merge_existing and candidate.target.exists():
            candidate.target.write_text(
                merge_existing_target(candidate.body, candidate.target.read_text())
            )
        else:
            candidate.target.write_text(candidate.body)


def target_name_for_source(path: Path) -> str:
    if path.name in {"lib.rs", "main.rs"}:
        return f"{path.stem}_test.rs"
    if path.name == "mod.rs":
        return f"{path.parent.name}_test.rs"
    return f"{path.stem}_test.rs"


def implicit_tests_module_files(path: Path) -> list[Path]:
    if path.name in {"lib.rs", "main.rs", "mod.rs"}:
        return [path.with_name("tests.rs"), path.with_name("tests") / "mod.rs"]
    module_dir = path.with_name(path.stem)
    return [module_dir / "tests.rs", module_dir / "tests" / "mod.rs"]


def find_existing_split_candidate(path: Path) -> SplitModuleCandidate | None:
    import re

    text = path.read_text()
    pattern = re.compile(
        r"(?m)^(?P<indent>[ \t]*)#\[cfg\(test\)\]\n(?P=indent)mod tests;\n?"
    )
    matches = list(pattern.finditer(text))
    if len(matches) != 1:
        return None
    module_file = next(
        (candidate for candidate in implicit_tests_module_files(path) if candidate.exists()),
        None,
    )
    target = path.with_name(target_name_for_source(path))
    if module_file is None:
        return None
    return SplitModuleCandidate(
        source=path,
        current_module_file=module_file,
        target=target,
        decl_start=matches[0].start(),
        decl_end=matches[0].end(),
    )


def find_existing_split_candidates(
    files: list[Path],
) -> tuple[list[SplitModuleCandidate], list[dict[str, str]]]:
    candidates: list[SplitModuleCandidate] = []
    skipped: list[dict[str, str]] = []
    for path in files:
        candidate = find_existing_split_candidate(path)
        if candidate is None:
            continue
        if candidate.target.exists():
            skipped.append({"path": str(path), "reason": f"target exists: {candidate.target}"})
            continue
        candidates.append(candidate)
    return candidates, skipped


def canonicalize_existing_split(candidate: SplitModuleCandidate) -> None:
    text = candidate.source.read_text()
    indent = text[candidate.decl_start : text.find("#[cfg(test)]", candidate.decl_start)]
    replacement = (
        f"{indent}#[cfg(test)]\n"
        f"{indent}#[path = \"{candidate.target.name}\"]\n"
        f"{indent}mod tests;\n"
    )
    candidate.source.write_text(
        text[: candidate.decl_start] + replacement + text[candidate.decl_end :]
    )
    candidate.current_module_file.rename(candidate.target)
    try:
        candidate.current_module_file.parent.rmdir()
    except OSError:
        pass


def test_function_names(text: str) -> set[str]:
    import re

    return set(re.findall(r"(?m)^\s*(?:async\s+)?fn\s+([A-Za-z0-9_]+)", text))


def top_level_test_function_blocks(text: str) -> list[tuple[str, str]]:
    import re

    blocks: list[tuple[str, str]] = []
    pattern = re.compile(
        r"(?m)^(?P<attrs>(?:#\[[^\n]*\]\n)*)\s*(?:async\s+)?fn\s+(?P<name>[A-Za-z0-9_]+)"
    )
    for match in pattern.finditer(text):
        open_idx = text.find("{", match.end())
        if open_idx == -1:
            continue
        close_idx = find_matching_brace(text, open_idx)
        end = close_idx + 1
        if end < len(text) and text[end] == "\n":
            end += 1
        blocks.append((match.group("name"), text[match.start() : end].rstrip() + "\n"))
    return blocks


def merge_existing_target(source_body: str, existing: str) -> str:
    source_names = test_function_names(source_body)
    additions = [
        block
        for name, block in top_level_test_function_blocks(existing)
        if name not in source_names
    ]
    if not additions:
        return source_body
    return (
        source_body.rstrip()
        + "\n\n// Preserved from the pre-existing split test file during inline extraction.\n"
        + "\n".join(block.rstrip() for block in additions)
        + "\n"
    )


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--apply", action="store_true")
    parser.add_argument("--merge-existing", action="store_true")
    parser.add_argument("--canonicalize-existing-split", action="store_true")
    parser.add_argument(
        "--include-test-directories",
        action="store_true",
        help="also scan Rust files below tests/ directories",
    )
    parser.add_argument(
        "--include-split-test-files",
        action="store_true",
        help="also scan *_test.rs files for nested inline test modules",
    )
    parser.add_argument(
        "--include-tests-rs",
        action="store_true",
        help="also scan files named tests.rs for nested inline test modules",
    )
    parser.add_argument("--root", action="append", default=[])
    parser.add_argument("--json", action="store_true")
    args = parser.parse_args()

    roots = [Path(root) for root in (args.root or DEFAULT_ROOTS)]
    files = iter_rust_files(
        roots,
        include_test_directories=args.include_test_directories,
        include_split_test_files=args.include_split_test_files,
        include_tests_rs=args.include_tests_rs,
    )
    found: list[Candidate] = []
    split_found: list[SplitModuleCandidate] = []
    skipped: list[dict[str, str]] = []
    split_skipped: list[dict[str, str]] = []
    for path in files:
        try:
            candidates = find_candidates(path)
        except ValueError as error:
            skipped.append({"path": str(path), "reason": f"parse error: {error}"})
            continue
        if not candidates:
            continue
        target_counts: dict[Path, int] = {}
        for candidate in candidates:
            target_counts[candidate.target] = target_counts.get(candidate.target, 0) + 1
        if duplicate_targets := [
            str(target) for target, count in target_counts.items() if count > 1
        ]:
            skipped.append(
                {
                    "path": str(path),
                    "reason": f"multiple modules would share target(s): {', '.join(duplicate_targets)}",
                }
            )
            continue
        for candidate in candidates:
            if candidate.target.exists():
                if not args.merge_existing:
                    skipped.append(
                        {
                            "path": str(path),
                            "reason": f"target exists: {candidate.target}",
                        }
                    )
                    continue
                candidate.merge_existing = True
            found.append(candidate)

    if args.canonicalize_existing_split:
        split_found, split_skipped = find_existing_split_candidates(files)

    if args.apply:
        candidates_by_source: dict[Path, list[Candidate]] = defaultdict(list)
        for candidate in found:
            candidates_by_source[candidate.source].append(candidate)
        for candidates in candidates_by_source.values():
            split_candidates_for_source(candidates)
        for candidate in split_found:
            canonicalize_existing_split(candidate)

    summary = {
        "applied": args.apply,
        "candidate_count": len(found),
        "existing_split_candidate_count": len(split_found),
        "skipped_count": len(skipped),
        "existing_split_skipped_count": len(split_skipped),
        "candidates": [
            {
                "source": str(item.source),
                "target": str(item.target),
                "module_name": item.module_name,
                "merge_existing": item.merge_existing,
            }
            for item in found
        ],
        "existing_split_candidates": [
            {
                "source": str(item.source),
                "current_module_file": str(item.current_module_file),
                "target": str(item.target),
            }
            for item in split_found
        ],
        "skipped": skipped,
        "existing_split_skipped": split_skipped,
    }

    if args.json:
        print(json.dumps(summary, indent=2))
    else:
        action = "split" if args.apply else "would split"
        print(f"{action} {len(found)} inline test module(s); skipped {len(skipped)}")
        for item in found:
            print(f"{item.source} -> {item.target}")
        if args.canonicalize_existing_split:
            split_action = "canonicalized" if args.apply else "would canonicalize"
            print(
                f"{split_action} {len(split_found)} existing split module(s); "
                f"skipped {len(split_skipped)}"
            )
            for item in split_found:
                print(f"{item.current_module_file} -> {item.target}")
        for item in skipped:
            print(f"SKIP {item['path']}: {item['reason']}")
        for item in split_skipped:
            print(f"SKIP {item['path']}: {item['reason']}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
