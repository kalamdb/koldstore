#!/usr/bin/env python3
"""Read release version metadata from workspace Cargo.toml for CI."""

from __future__ import annotations

import argparse
import os
import re
import sys
from pathlib import Path

import tomllib

ROOT = Path(__file__).resolve().parents[1]
WORKSPACE_CARGO = ROOT / "Cargo.toml"

SEMVER_RE = re.compile(r"^[0-9]+\.[0-9]+\.[0-9]+([-.][0-9A-Za-z.-]+)?$")


def get_workspace_version() -> str:
    cargo = tomllib.loads(WORKSPACE_CARGO.read_text(encoding="utf-8"))
    try:
        version = cargo["workspace"]["package"]["version"]
    except KeyError as error:
        raise SystemExit("Missing [workspace.package].version in Cargo.toml") from error
    if not isinstance(version, str) or not SEMVER_RE.match(version):
        raise SystemExit(f"Invalid workspace.package.version: {version!r}")
    return version


def github_outputs() -> dict[str, str]:
    version = get_workspace_version()
    tag = f"v{version}"
    return {
        "version": version,
        "tag": tag,
        "release_tag": tag,
    }


def emit_github_outputs(output_path: Path | None) -> int:
    outputs = github_outputs()
    rendered = "\n".join(f"{key}={value}" for key, value in outputs.items()) + "\n"
    if output_path is None:
        sys.stdout.write(rendered)
        return 0
    with output_path.open("a", encoding="utf-8") as handle:
        handle.write(rendered)
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Read pg-koldstore release version metadata from Cargo.toml"
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    outputs_parser = subparsers.add_parser(
        "github-outputs", help="Emit GitHub Actions outputs"
    )
    outputs_parser.add_argument(
        "--github-output",
        type=Path,
        default=Path(os.environ["GITHUB_OUTPUT"])
        if "GITHUB_OUTPUT" in os.environ
        else None,
        help="Path to the GitHub Actions output file",
    )
    outputs_parser.set_defaults(
        func=lambda args: emit_github_outputs(args.github_output)
    )

    return parser


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()
    return args.func(args)


if __name__ == "__main__":
    raise SystemExit(main())
