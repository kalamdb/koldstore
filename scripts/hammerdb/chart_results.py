#!/usr/bin/env python3
"""Render 3-arm HammerDB compare results as readable SVG bar charts."""

from __future__ import annotations

import argparse
import json
from pathlib import Path


COLORS = {
    "baseline": "#2563eb",
    "hot_only": "#16a34a",
    "hot_cold": "#ca8a04",
}

LABELS = {
    "baseline": "Baseline",
    "hot_only": "Hot only",
    "hot_cold": "Hot + cold",
}


def fmt_int(n: float | int) -> str:
    if isinstance(n, float):
        if n >= 100:
            return f"{n:,.0f}"
        return f"{n:,.2f}"
    return f"{n:,}"


def bar_chart(
    *,
    title: str,
    subtitle: str,
    values: list[tuple[str, float]],
    unit: str,
    width: int = 1000,
    height: int = 560,
) -> str:
    """Grouped vertical bars with axes, gridlines, and in-bar labels."""
    left, right, top, bottom = 90, 40, 100, 90
    plot_w = width - left - right
    plot_h = height - top - bottom
    max_v = max((v for _, v in values), default=1) or 1
    n = max(len(values), 1)
    gap = 36
    bar_w = (plot_w - gap * (n - 1)) / n

    grid = []
    for frac in (0.0, 0.25, 0.5, 0.75, 1.0):
        y = top + plot_h * (1 - frac)
        val = max_v * frac
        grid.append(
            f'<line x1="{left}" y1="{y:.1f}" x2="{width - right}" y2="{y:.1f}" '
            f'stroke="#cbd5e1" stroke-width="1"/>'
            f'<text x="{left - 12}" y="{y + 4:.1f}" text-anchor="end" '
            f'fill="#475569" font-size="13" font-family="Helvetica, Arial, sans-serif">'
            f"{fmt_int(val)}</text>"
        )

    bars = []
    for i, (key, value) in enumerate(values):
        x = left + i * (bar_w + gap)
        h = plot_h * (value / max_v)
        y = top + plot_h - h
        color = COLORS.get(key, "#64748b")
        label = LABELS.get(key, key)
        # Value label above bar (or inside if tall enough)
        if h < plot_h * 0.85:
            label_y = y - 12
            label_fill = "#0f172a"
        else:
            label_y = y + 28
            label_fill = "#ffffff"
        bars.append(
            f"""
  <rect x="{x:.1f}" y="{y:.1f}" width="{bar_w:.1f}" height="{max(h, 2):.1f}"
        rx="8" fill="{color}" stroke="#0f172a" stroke-width="1"/>
  <text x="{x + bar_w/2:.1f}" y="{label_y:.1f}" text-anchor="middle"
        fill="{label_fill}" font-size="20" font-weight="700"
        font-family="Helvetica, Arial, sans-serif">{fmt_int(value)}</text>
  <text x="{x + bar_w/2:.1f}" y="{height - 36}" text-anchor="middle"
        fill="#0f172a" font-size="16" font-weight="600"
        font-family="Helvetica, Arial, sans-serif">{label}</text>
"""
        )

    return f"""<?xml version="1.0" encoding="UTF-8"?>
<svg xmlns="http://www.w3.org/2000/svg" width="{width}" height="{height}"
     viewBox="0 0 {width} {height}" role="img">
  <rect width="{width}" height="{height}" fill="#f8fafc"/>
  <rect x="0" y="0" width="{width}" height="84" fill="#e2e8f0"/>
  <text x="{width/2}" y="36" text-anchor="middle" fill="#0f172a"
        font-size="26" font-weight="700" font-family="Helvetica, Arial, sans-serif">{title}</text>
  <text x="{width/2}" y="62" text-anchor="middle" fill="#475569"
        font-size="14" font-family="Helvetica, Arial, sans-serif">{subtitle}</text>
  <text x="24" y="120" fill="#64748b" font-size="12"
        font-family="Helvetica, Arial, sans-serif">{unit}</text>
  {''.join(grid)}
  <line x1="{left}" y1="{top}" x2="{left}" y2="{top + plot_h}" stroke="#334155" stroke-width="2"/>
  <line x1="{left}" y1="{top + plot_h}" x2="{width - right}" y2="{top + plot_h}"
        stroke="#334155" stroke-width="2"/>
  {''.join(bars)}
</svg>
"""


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--results", required=True)
    parser.add_argument("--out-dir", required=True)
    args = parser.parse_args()

    report = json.loads(Path(args.results).read_text(encoding="utf-8"))
    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    arms = ["baseline", "hot_only", "hot_cold"]
    subtitle = (
        f"TPROC-C · {report['warehouses']} WH · {report['virtual_users']} VU · "
        f"{report['duration_minutes']} min · HISTORY-only manage · "
        f"{report['read_iters']} PK read iters"
    )

    nopm = bar_chart(
        title="HammerDB NOPM (OLTP mix — mostly does not read HISTORY)",
        subtitle=subtitle,
        values=[(a, report["arms"][a]["nopm"]) for a in arms],
        unit="NOPM",
    )
    hist = bar_chart(
        title="HISTORY PK lookup latency (proves hot / hot+cold path)",
        subtitle=subtitle + " · lower is faster",
        values=[(a, report["arms"][a]["reads"]["history_pk_ms"]) for a in arms],
        unit="ms total",
    )
    cust = bar_chart(
        title="Customer PK lookup latency (unmanaged hot table)",
        subtitle=subtitle + " · should stay flat across arms",
        values=[(a, report["arms"][a]["reads"]["customer_pk_ms"]) for a in arms],
        unit="ms total",
    )

    (out_dir / "hammerdb-nopm.svg").write_text(nopm, encoding="utf-8")
    (out_dir / "hammerdb-history-reads.svg").write_text(hist, encoding="utf-8")
    (out_dir / "hammerdb-customer-reads.svg").write_text(cust, encoding="utf-8")

    # Drop the old misleading storage-only / tpm dark charts if present.
    for stale in ("hammerdb-tpm.svg", "hammerdb-storage.svg"):
        p = out_dir / stale
        if p.exists():
            p.unlink()

    print(f"wrote {out_dir / 'hammerdb-nopm.svg'}")
    print(f"wrote {out_dir / 'hammerdb-history-reads.svg'}")
    print(f"wrote {out_dir / 'hammerdb-customer-reads.svg'}")


if __name__ == "__main__":
    main()
