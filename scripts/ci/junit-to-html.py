#!/usr/bin/env python3
"""Convert a nextest/JUnit XML report into a standalone HTML summary.

Uses only the Python standard library so CI does not need extra packages.
"""

from __future__ import annotations

import argparse
import html
import os
import sys
import xml.etree.ElementTree as ET
from dataclasses import dataclass, field
from pathlib import Path


@dataclass
class Case:
    classname: str
    name: str
    time: float
    status: str
    message: str = ""
    detail: str = ""


@dataclass
class Suite:
    name: str
    tests: int = 0
    failures: int = 0
    errors: int = 0
    skipped: int = 0
    time: float = 0.0
    cases: list[Case] = field(default_factory=list)


def _text(node: ET.Element | None) -> str:
    if node is None:
        return ""
    parts: list[str] = []
    if node.text:
        parts.append(node.text)
    for child in node:
        if child.tail:
            parts.append(child.tail)
    return "".join(parts).strip()


def parse_junit(path: Path) -> list[Suite]:
    root = ET.parse(path).getroot()
    suite_nodes: list[ET.Element]
    if root.tag == "testsuites":
        suite_nodes = list(root.findall("testsuite"))
    elif root.tag == "testsuite":
        suite_nodes = [root]
    else:
        raise SystemExit(f"unsupported JUnit root element: {root.tag}")

    suites: list[Suite] = []
    for suite_node in suite_nodes:
        suite = Suite(
            name=suite_node.attrib.get("name") or "tests",
            tests=int(suite_node.attrib.get("tests") or 0),
            failures=int(suite_node.attrib.get("failures") or 0),
            errors=int(suite_node.attrib.get("errors") or 0),
            skipped=int(suite_node.attrib.get("skipped") or 0),
            time=float(suite_node.attrib.get("time") or 0.0),
        )
        for case_node in suite_node.findall("testcase"):
            status = "passed"
            message = ""
            detail = ""
            failure = case_node.find("failure")
            error = case_node.find("error")
            skipped = case_node.find("skipped")
            if failure is not None:
                status = "failed"
                message = failure.attrib.get("message") or ""
                detail = _text(failure) or failure.attrib.get("type") or ""
            elif error is not None:
                status = "error"
                message = error.attrib.get("message") or ""
                detail = _text(error) or error.attrib.get("type") or ""
            elif skipped is not None:
                status = "skipped"
                message = skipped.attrib.get("message") or _text(skipped)
            suite.cases.append(
                Case(
                    classname=case_node.attrib.get("classname") or "",
                    name=case_node.attrib.get("name") or "",
                    time=float(case_node.attrib.get("time") or 0.0),
                    status=status,
                    message=message,
                    detail=detail,
                )
            )
        suites.append(suite)
    return suites


def render_html(suites: list[Suite], title: str) -> str:
    total = sum(s.tests for s in suites)
    failures = sum(s.failures for s in suites)
    errors = sum(s.errors for s in suites)
    skipped = sum(s.skipped for s in suites)
    passed = max(total - failures - errors - skipped, 0)
    duration = sum(s.time for s in suites)
    ok = failures == 0 and errors == 0

    rows: list[str] = []
    for suite in suites:
        for case in suite.cases:
            cls = html.escape(case.classname)
            name = html.escape(case.name)
            status = html.escape(case.status)
            msg = html.escape(case.message)
            detail = html.escape(case.detail)
            detail_html = (
                f"<pre>{detail}</pre>" if detail else (f"<span class='muted'>{msg}</span>" if msg else "")
            )
            rows.append(
                "<tr class='{status}'>"
                f"<td>{status}</td>"
                f"<td><code>{cls}</code></td>"
                f"<td><code>{name}</code></td>"
                f"<td>{case.time:.3f}s</td>"
                f"<td>{detail_html}</td>"
                "</tr>"
            )

    body_rows = "\n".join(rows) if rows else "<tr><td colspan='5'>No test cases</td></tr>"
    return f"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>{html.escape(title)}</title>
  <style>
    :root {{ color-scheme: light dark; font-family: ui-sans-serif, system-ui, sans-serif; }}
    body {{ margin: 1.5rem; line-height: 1.4; }}
    h1 {{ margin: 0 0 .5rem; font-size: 1.4rem; }}
    .meta {{ display: flex; flex-wrap: wrap; gap: .75rem 1.25rem; margin: 1rem 0 1.5rem; }}
    .pill {{ padding: .25rem .65rem; border-radius: 999px; background: #e8eef7; }}
    .ok {{ background: #d9f7e4; }}
    .bad {{ background: #ffe0e0; }}
    table {{ width: 100%; border-collapse: collapse; font-size: .92rem; }}
    th, td {{ border-bottom: 1px solid #ccc4; text-align: left; padding: .45rem .5rem; vertical-align: top; }}
    th {{ position: sticky; top: 0; background: Canvas; }}
    tr.failed, tr.error {{ background: #ffecec55; }}
    tr.skipped {{ opacity: .75; }}
    code, pre {{ font-family: ui-monospace, SFMono-Regular, Menlo, monospace; font-size: .85rem; }}
    pre {{ white-space: pre-wrap; margin: .25rem 0 0; max-height: 18rem; overflow: auto; }}
    .muted {{ color: #666; }}
  </style>
</head>
<body>
  <h1>{html.escape(title)}</h1>
  <div class="meta">
    <span class="pill {"ok" if ok else "bad"}">{"PASS" if ok else "FAIL"}</span>
    <span class="pill">total {total}</span>
    <span class="pill">passed {passed}</span>
    <span class="pill">failed {failures}</span>
    <span class="pill">errors {errors}</span>
    <span class="pill">skipped {skipped}</span>
    <span class="pill">{duration:.2f}s</span>
  </div>
  <table>
    <thead>
      <tr><th>Status</th><th>Suite</th><th>Test</th><th>Time</th><th>Details</th></tr>
    </thead>
    <tbody>
{body_rows}
    </tbody>
  </table>
</body>
</html>
"""


def write_summary_md(suites: list[Suite], title: str, html_name: str) -> str:
    total = sum(s.tests for s in suites)
    failures = sum(s.failures for s in suites)
    errors = sum(s.errors for s in suites)
    skipped = sum(s.skipped for s in suites)
    passed = max(total - failures - errors - skipped, 0)
    duration = sum(s.time for s in suites)
    status = "PASS" if failures == 0 and errors == 0 else "FAIL"
    lines = [
        f"### {title}",
        "",
        f"- **Result:** {status}",
        f"- **Tests:** {passed} passed / {failures} failed / {errors} errors / {skipped} skipped / {total} total",
        f"- **Duration:** {duration:.2f}s",
        f"- **Report:** `{html_name}` (see job artifacts)",
        "",
    ]
    bad = [
        case
        for suite in suites
        for case in suite.cases
        if case.status in {"failed", "error"}
    ]
    if bad:
        lines.append("| Status | Test | Message |")
        lines.append("| --- | --- | --- |")
        for case in bad[:40]:
            label = f"{case.classname}::{case.name}" if case.classname else case.name
            msg = (case.message or case.detail or "").replace("\n", " ").strip()
            if len(msg) > 160:
                msg = msg[:157] + "..."
            lines.append(f"| {case.status} | `{label}` | {msg} |")
        if len(bad) > 40:
            lines.append(f"| … | {len(bad) - 40} more | |")
        lines.append("")
    return "\n".join(lines)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("junit", type=Path, help="Path to JUnit XML")
    parser.add_argument("-o", "--output", type=Path, required=True, help="HTML output path")
    parser.add_argument("--title", default="Test report", help="HTML/document title")
    parser.add_argument(
        "--summary-md",
        type=Path,
        help="Optional Markdown summary path (also append to $GITHUB_STEP_SUMMARY when set)",
    )
    args = parser.parse_args()

    if not args.junit.is_file():
        print(f"error: JUnit report not found: {args.junit}", file=sys.stderr)
        return 2

    suites = parse_junit(args.junit)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(render_html(suites, args.title), encoding="utf-8")
    print(f"wrote HTML report: {args.output}")

    if args.summary_md is not None:
        summary = write_summary_md(suites, args.title, args.output.name)
        args.summary_md.parent.mkdir(parents=True, exist_ok=True)
        args.summary_md.write_text(summary, encoding="utf-8")
        github_summary = Path(os.environ["GITHUB_STEP_SUMMARY"]) if "GITHUB_STEP_SUMMARY" in os.environ else None
        if github_summary is not None:
            with github_summary.open("a", encoding="utf-8") as handle:
                handle.write(summary)
                handle.write("\n")
        print(f"wrote Markdown summary: {args.summary_md}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
