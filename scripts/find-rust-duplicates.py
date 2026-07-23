#!/usr/bin/env python3
"""Find exact duplicated Rust blocks without external dependencies."""

from __future__ import annotations

import argparse
import hashlib
from collections import defaultdict
from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True, order=True)
class Location:
    path: Path
    line: int


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "paths",
        nargs="*",
        type=Path,
        default=[Path("crates"), Path("tests")],
        help="files or directories to scan (default: crates tests)",
    )
    parser.add_argument("--window", type=int, default=8, help="significant lines per block")
    parser.add_argument("--min-chars", type=int, default=180)
    parser.add_argument("--limit", type=int, default=25)
    parser.add_argument("--deny", action="store_true", help="fail when duplicates are found")
    return parser.parse_args()


def rust_files(paths: list[Path]) -> list[Path]:
    files: set[Path] = set()
    for path in paths:
        if path.is_file() and path.suffix == ".rs":
            files.add(path)
        elif path.is_dir():
            files.update(
                candidate
                for candidate in path.rglob("*.rs")
                if "target" not in candidate.parts
            )
    return sorted(files)


def significant_lines(path: Path) -> list[tuple[str, int]]:
    lines = []
    for line_number, raw in enumerate(path.read_text(encoding="utf-8").splitlines(), 1):
        normalized = " ".join(raw.split())
        if (
            not normalized
            or normalized.startswith("//")
            or normalized in {"{", "}", "};"}
        ):
            continue
        lines.append((normalized, line_number))
    return lines


def duplicate_groups(
    files: list[Path], window: int, min_chars: int
) -> list[tuple[str, list[Location]]]:
    blocks: dict[str, list[tuple[Location, str]]] = defaultdict(list)
    for path in files:
        lines = significant_lines(path)
        for index in range(len(lines) - window + 1):
            chunk = lines[index : index + window]
            text = "\n".join(line for line, _ in chunk)
            if len(text) < min_chars:
                continue
            digest = hashlib.sha256(text.encode()).hexdigest()
            blocks[digest].append((Location(path, chunk[0][1]), text))

    groups = []
    for occurrences in blocks.values():
        locations = sorted({location for location, _ in occurrences})
        if len(locations) < 2:
            continue
        if not any(
            left.path != right.path or abs(left.line - right.line) > window * 2
            for index, left in enumerate(locations)
            for right in locations[index + 1 :]
        ):
            continue
        groups.append((occurrences[0][1], locations))
    return sorted(groups, key=lambda group: (-len(group[1]), -len(group[0])))


def shifted_duplicate(
    locations: list[Location],
    reported: list[list[Location]],
    window: int,
) -> bool:
    for prior in reported:
        if len(prior) != len(locations):
            continue
        if all(
            current.path == previous.path
            and abs(current.line - previous.line) <= window
            for current, previous in zip(locations, prior)
        ):
            return True
    return False


def main() -> int:
    args = parse_args()
    groups = duplicate_groups(
        rust_files(args.paths),
        window=args.window,
        min_chars=args.min_chars,
    )
    reported: list[list[Location]] = []
    for text, locations in groups:
        if shifted_duplicate(locations, reported, args.window):
            continue
        reported.append(locations)
        rendered = ", ".join(f"{item.path}:{item.line}" for item in locations)
        print(f"{len(locations)} copies: {rendered}")
        print(f"  {text.splitlines()[0]}")
        if len(reported) >= args.limit:
            break

    if not reported:
        print("No duplicated Rust blocks found.")
    return int(args.deny and bool(reported))


if __name__ == "__main__":
    raise SystemExit(main())
