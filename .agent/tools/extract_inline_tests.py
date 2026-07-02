#!/usr/bin/env python3
"""Split inline Rust #[cfg(test)] mod tests blocks into *_test.rs files.

The tool is intentionally conservative:
- it skips existing *_test.rs, tests.rs, and paths under tests/
- it only handles files with exactly one true inline `mod tests { ... }`
- it writes sibling `<stem>_test.rs` files and leaves `#[path = "..."] mod tests;`
- it fails instead of overwriting an existing target file

Run without --apply for a dry-run inventory.
"""

from __future__ import annotations

import argparse
import json
from dataclasses import dataclass
from pathlib import Path


DEFAULT_ROOTS = ("crate", "xtask/src")


@dataclass
class Candidate:
    source: Path
    target: Path
    cfg_start: int
    module_end: int
    body: str
    merge_existing: bool = False


def is_skipped(path: Path) -> bool:
    return (
        path.name.endswith("_test.rs")
        or path.name == "tests.rs"
        or "tests" in path.parts
    )


def iter_rust_files(roots: list[Path]) -> list[Path]:
    files: list[Path] = []
    for root in roots:
        if root.is_file() and root.suffix == ".rs":
            files.append(root)
            continue
        for path in root.rglob("*.rs"):
            if not is_skipped(path):
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
        if ch == "'":
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
    text = path.read_text()
    candidates: list[Candidate] = []
    search = 0
    while True:
        cfg = text.find("#[cfg", search)
        if cfg == -1:
            break
        cfg_line_end = text.find("\n", cfg)
        if cfg_line_end == -1:
            cfg_line_end = len(text)
        cfg_line = text[cfg:cfg_line_end]
        if "test" not in cfg_line:
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
            if stripped.startswith("mod tests {"):
                open_idx = text.find("{", line_pos)
                close_idx = find_matching_brace(text, open_idx)
                after = close_idx + 1
                if after < len(text) and text[after] == "\n":
                    after += 1
                target = path.with_name(f"{path.stem}_test.rs")
                candidates.append(
                    Candidate(
                        source=path,
                        target=target,
                        cfg_start=cfg_start,
                        module_end=after,
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
    if candidate.target.exists() and not candidate.merge_existing:
        raise FileExistsError(f"target already exists: {candidate.target}")
    text = candidate.source.read_text()
    indent = text[candidate.cfg_start : text.find("#[cfg(test)]", candidate.cfg_start)]
    replacement = (
        f"{indent}#[cfg(test)]\n"
        f"{indent}#[path = \"{candidate.target.name}\"]\n"
        f"{indent}mod tests;\n"
    )
    candidate.source.write_text(
        text[: candidate.cfg_start] + replacement + text[candidate.module_end :]
    )
    if candidate.merge_existing and candidate.target.exists():
        candidate.target.write_text(merge_existing_target(candidate.body, candidate.target.read_text()))
    else:
        candidate.target.write_text(candidate.body)


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
    parser.add_argument("--root", action="append", default=[])
    parser.add_argument("--json", action="store_true")
    args = parser.parse_args()

    roots = [Path(root) for root in (args.root or DEFAULT_ROOTS)]
    files = iter_rust_files(roots)
    found: list[Candidate] = []
    skipped: list[dict[str, str]] = []
    for path in files:
        candidates = find_candidates(path)
        if not candidates:
            continue
        if len(candidates) != 1:
            skipped.append({"path": str(path), "reason": "multiple inline test modules"})
            continue
        candidate = candidates[0]
        if candidate.target.exists():
            if not args.merge_existing:
                skipped.append({"path": str(path), "reason": f"target exists: {candidate.target}"})
                continue
            candidate.merge_existing = True
        found.append(candidate)

    if args.apply:
        for candidate in found:
            split_candidate(candidate)

    summary = {
        "applied": args.apply,
        "candidate_count": len(found),
        "skipped_count": len(skipped),
        "candidates": [
            {
                "source": str(item.source),
                "target": str(item.target),
                "merge_existing": item.merge_existing,
            }
            for item in found
        ],
        "skipped": skipped,
    }

    if args.json:
        print(json.dumps(summary, indent=2))
    else:
        action = "split" if args.apply else "would split"
        print(f"{action} {len(found)} inline test module(s); skipped {len(skipped)}")
        for item in found:
            print(f"{item.source} -> {item.target}")
        for item in skipped:
            print(f"SKIP {item['path']}: {item['reason']}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
