#!/usr/bin/env python3
"""Generate reproducible SVG result charts for the project report.

The report figures are generated from the final GKE summary numbers selected
for discussion. The script intentionally uses only the Python standard library
so it works in Cloud Shell and WSL without matplotlib/numpy.
"""

from __future__ import annotations

import argparse
import html
from pathlib import Path
from typing import Iterable


POLICIES = ["baseline", "A", "B", "C"]
POLICY_LABELS = {
    "baseline": "Baseline",
    "A": "Age-based",
    "B": "Throttled",
    "C": "Pressure-aware",
}
PROFILE_LABELS = {
    "none": "No pressure",
    "cpu": "CPU pressure",
    "io": "I/O pressure",
}
COLORS = {
    "baseline": "#64748b",
    "A": "#2563eb",
    "B": "#f97316",
    "C": "#16a34a",
}

# Final GKE result suites selected for the report:
# none = 20260524T050113Z-gke-suite-none
# cpu  = 20260524T064324Z-gke-suite-cpu
# io   = 20260524T081648Z-gke-suite-io
LATENCY = {
    "none": {
        "baseline": {
            "ALL": {"count": 811, "errors": 0, "throughput": 13.503, "p50": 60.495, "p95": 464.777, "p99": 1069.216, "max": 1239.200},
            "GET": {"count": 543, "errors": 0, "throughput": 9.107, "p50": 48.126, "p95": 250.647, "p99": 998.518, "max": 1131.244},
            "PUT": {"count": 268, "errors": 0, "throughput": 4.474, "p50": 132.614, "p95": 686.258, "p99": 1125.659, "max": 1239.200},
        },
        "A": {
            "ALL": {"count": 761, "errors": 0, "throughput": 12.863, "p50": 53.678, "p95": 517.194, "p99": 1108.235, "max": 3607.213},
            "GET": {"count": 524, "errors": 0, "throughput": 8.877, "p50": 45.052, "p95": 269.914, "p99": 1004.370, "max": 1267.179},
            "PUT": {"count": 237, "errors": 0, "throughput": 4.021, "p50": 130.027, "p95": 793.300, "p99": 1176.905, "max": 3607.213},
        },
        "B": {
            "ALL": {"count": 766, "errors": 0, "throughput": 12.738, "p50": 59.812, "p95": 474.307, "p99": 1119.501, "max": 3009.906},
            "GET": {"count": 530, "errors": 0, "throughput": 8.967, "p50": 50.306, "p95": 289.176, "p99": 1021.553, "max": 1424.902},
            "PUT": {"count": 236, "errors": 0, "throughput": 3.965, "p50": 133.474, "p95": 811.068, "p99": 1505.489, "max": 3009.906},
        },
        "C": {
            "ALL": {"count": 836, "errors": 0, "throughput": 13.983, "p50": 57.226, "p95": 539.957, "p99": 977.065, "max": 1154.287},
            "GET": {"count": 583, "errors": 0, "throughput": 9.751, "p50": 47.155, "p95": 262.401, "p99": 863.411, "max": 1154.287},
            "PUT": {"count": 253, "errors": 0, "throughput": 4.269, "p50": 128.008, "p95": 721.696, "p99": 1006.465, "max": 1082.661},
        },
    },
    "cpu": {
        "baseline": {
            "ALL": {"count": 816, "errors": 0, "throughput": 13.648, "p50": 57.458, "p95": 449.759, "p99": 1069.191, "max": 2065.606},
            "GET": {"count": 551, "errors": 0, "throughput": 9.346, "p50": 47.332, "p95": 248.870, "p99": 1067.519, "max": 2065.606},
            "PUT": {"count": 265, "errors": 0, "throughput": 4.432, "p50": 128.409, "p95": 703.880, "p99": 1050.265, "max": 1491.172},
        },
        "A": {
            "ALL": {"count": 812, "errors": 0, "throughput": 13.454, "p50": 55.171, "p95": 454.205, "p99": 1105.419, "max": 2617.746},
            "GET": {"count": 572, "errors": 0, "throughput": 9.489, "p50": 47.110, "p95": 239.647, "p99": 945.651, "max": 2617.746},
            "PUT": {"count": 240, "errors": 0, "throughput": 3.978, "p50": 141.955, "p95": 724.537, "p99": 1375.356, "max": 2177.675},
        },
        "B": {
            "ALL": {"count": 797, "errors": 0, "throughput": 13.480, "p50": 53.742, "p95": 491.810, "p99": 1104.792, "max": 3910.827},
            "GET": {"count": 566, "errors": 0, "throughput": 9.577, "p50": 46.231, "p95": 245.860, "p99": 845.484, "max": 3398.305},
            "PUT": {"count": 231, "errors": 0, "throughput": 3.917, "p50": 123.435, "p95": 962.200, "p99": 1193.695, "max": 3910.827},
        },
        "C": {
            "ALL": {"count": 827, "errors": 0, "throughput": 13.807, "p50": 55.276, "p95": 523.150, "p99": 1056.998, "max": 2108.190},
            "GET": {"count": 580, "errors": 0, "throughput": 9.683, "p50": 47.131, "p95": 254.259, "p99": 1055.601, "max": 1093.251},
            "PUT": {"count": 247, "errors": 0, "throughput": 4.176, "p50": 132.094, "p95": 631.394, "p99": 1066.837, "max": 2108.190},
        },
    },
    "io": {
        "baseline": {
            "ALL": {"count": 688, "errors": 0, "throughput": 11.531, "p50": 55.306, "p95": 647.909, "p99": 1071.196, "max": 1688.634},
            "GET": {"count": 483, "errors": 0, "throughput": 8.122, "p50": 47.574, "p95": 162.530, "p99": 327.626, "max": 1115.544},
            "PUT": {"count": 205, "errors": 0, "throughput": 3.452, "p50": 289.700, "p95": 858.876, "p99": 1084.633, "max": 1688.634},
        },
        "A": {
            "ALL": {"count": 175, "errors": 4, "throughput": 2.957, "p50": 119.529, "p95": 1517.036, "p99": 3270.696, "max": 6956.014},
            "GET": {"count": 117, "errors": 0, "throughput": 2.001, "p50": 57.828, "p95": 908.339, "p99": 1376.307, "max": 6956.014},
            "PUT": {"count": 58, "errors": 4, "throughput": 1.182, "p50": 187.135, "p95": 2106.672, "p99": 3756.524, "max": 5385.477},
        },
        "B": {
            "ALL": {"count": 263, "errors": 0, "throughput": 4.460, "p50": 131.067, "p95": 1841.436, "p99": 2675.589, "max": 2958.953},
            "GET": {"count": 180, "errors": 0, "throughput": 3.056, "p50": 64.449, "p95": 1028.737, "p99": 1254.981, "max": 1478.786},
            "PUT": {"count": 83, "errors": 0, "throughput": 1.415, "p50": 582.358, "p95": 2616.966, "p99": 2926.149, "max": 2958.953},
        },
        "C": {
            "ALL": {"count": 638, "errors": 0, "throughput": 10.664, "p50": 56.406, "p95": 719.885, "p99": 1140.505, "max": 1907.571},
            "GET": {"count": 426, "errors": 0, "throughput": 7.120, "p50": 47.216, "p95": 270.728, "p99": 879.760, "max": 1317.597},
            "PUT": {"count": 212, "errors": 0, "throughput": 3.600, "p50": 170.135, "p95": 867.619, "p99": 1551.961, "max": 1907.571},
        },
    },
}

MIGRATION = {
    "none": {
        "baseline": {"done": 0, "backlog": 180, "drain": 0.000},
        "A": {"done": 40, "backlog": 99, "drain": 0.288},
        "B": {"done": 43, "backlog": 107, "drain": 0.287},
        "C": {"done": 36, "backlog": 112, "drain": 0.243},
    },
    "cpu": {
        "baseline": {"done": 0, "backlog": 315, "drain": 0.000},
        "A": {"done": 67, "backlog": 223, "drain": 0.231},
        "B": {"done": 67, "backlog": 214, "drain": 0.238},
        "C": {"done": 61, "backlog": 236, "drain": 0.205},
    },
    "io": {
        "baseline": {"done": 0, "backlog": 255, "drain": 0.000},
        "A": {"done": 52, "backlog": 56, "drain": 0.481},
        "B": {"done": 49, "backlog": 84, "drain": 0.368},
        "C": {"done": 61, "backlog": 201, "drain": 0.233},
    },
}

FIGURES = [
    "01_put_p99_heatmap.svg",
    "02_get_p99_heatmap.svg",
    "02_io_put_percentile_bands.svg",
    "03_migration_latency_tradeoff.svg",
    "04_io_resilience_panel.svg",
]

OLD_FIGURES = [
    "01_system_architecture.svg",
    "02_experiment_flow.svg",
    "03_put_p99_under_pressure.svg",
    "04_key_findings.svg",
    "put_p99_pressure_bar.svg",
    "latency_migration_tradeoff.svg",
    "io_error_rate.svg",
    "results_dashboard.svg",
]


def esc(value: object) -> str:
    return html.escape(str(value), quote=True)


def svg_root(width: int, height: int, body: Iterable[str]) -> str:
    return "\n".join(
        [
            f'<svg xmlns="http://www.w3.org/2000/svg" width="{width}" height="{height}" viewBox="0 0 {width} {height}">',
            "<defs>",
            '<filter id="shadow" x="-20%" y="-20%" width="140%" height="140%">',
            '<feDropShadow dx="0" dy="8" stdDeviation="8" flood-color="#0f172a" flood-opacity="0.10"/>',
            "</filter>",
            "</defs>",
            '<rect width="100%" height="100%" fill="#ffffff"/>',
            *body,
            "</svg>",
            "",
        ]
    )


def write_svg(path: Path, width: int, height: int, body: Iterable[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(svg_root(width, height, body), encoding="utf-8")


def text(x: float, y: float, value: object, size: int = 18, weight: int | str = 400,
         fill: str = "#0f172a", anchor: str = "start") -> str:
    return (
        f'<text x="{x:.1f}" y="{y:.1f}" font-family="Inter,Segoe UI,Arial,sans-serif" '
        f'font-size="{size}" font-weight="{weight}" fill="{fill}" text-anchor="{anchor}">{esc(value)}</text>'
    )


def line(x1: float, y1: float, x2: float, y2: float, stroke: str = "#cbd5e1", width: float = 1.0) -> str:
    return f'<line x1="{x1:.1f}" y1="{y1:.1f}" x2="{x2:.1f}" y2="{y2:.1f}" stroke="{stroke}" stroke-width="{width}"/>'


def rect(x: float, y: float, w: float, h: float, fill: str, rx: float = 0, stroke: str = "none",
         sw: float = 1.0) -> str:
    return (
        f'<rect x="{x:.1f}" y="{y:.1f}" width="{w:.1f}" height="{h:.1f}" '
        f'rx="{rx:.1f}" fill="{fill}" stroke="{stroke}" stroke-width="{sw:.1f}"/>'
    )


def circle(x: float, y: float, r: float, fill: str, stroke: str = "none", sw: float = 1.0) -> str:
    return f'<circle cx="{x:.1f}" cy="{y:.1f}" r="{r:.1f}" fill="{fill}" stroke="{stroke}" stroke-width="{sw:.1f}"/>'


def title_block(title: str, subtitle: str) -> list[str]:
    return [
        text(70, 70, title, 38, 900),
        text(70, 108, subtitle, 18, 500, "#475569"),
    ]


def legend(x: float, y: float, policies: Iterable[str]) -> list[str]:
    out: list[str] = []
    cx = x
    for policy in policies:
        out.append(rect(cx, y - 13, 18, 18, COLORS[policy], 4))
        out.append(text(cx + 26, y + 3, POLICY_LABELS[policy], 15, 700, "#334155"))
        cx += 170 if policy == "C" else 140
    return out


def interpolate(c1: tuple[int, int, int], c2: tuple[int, int, int], t: float) -> tuple[int, int, int]:
    return tuple(round(a + (b - a) * t) for a, b in zip(c1, c2))


def rgb_hex(rgb: tuple[int, int, int]) -> str:
    return "#{:02x}{:02x}{:02x}".format(*rgb)


def heat_color(value: float, max_value: float) -> str:
    t = max(0.0, min(value / max_value, 1.0))
    if t < 0.45:
        return rgb_hex(interpolate((220, 252, 231), (254, 240, 138), t / 0.45))
    if t < 0.75:
        return rgb_hex(interpolate((254, 240, 138), (251, 146, 60), (t - 0.45) / 0.30))
    return rgb_hex(interpolate((251, 146, 60), (220, 38, 38), (t - 0.75) / 0.25))


def y_grid(x: float, y: float, w: float, h: float, max_value: float, ticks: list[float],
           suffix: str = "") -> list[str]:
    out: list[str] = []
    for tick in ticks:
        ty = y + h - (tick / max_value) * h
        out.append(line(x, ty, x + w, ty, "#e2e8f0", 1))
        out.append(text(x - 12, ty + 5, f"{int(tick)}{suffix}", 13, 600, "#64748b", "end"))
    out.append(line(x, y, x, y + h, "#94a3b8", 1.5))
    out.append(line(x, y + h, x + w, y + h, "#94a3b8", 1.5))
    return out


def percent_drop(old: float, new: float) -> float:
    return (old - new) / old * 100.0


def error_rate(profile: str, policy: str, op: str) -> float:
    row = LATENCY[profile][policy][op]
    if row["count"] == 0:
        return 0.0
    return row["errors"] / row["count"] * 100.0


def cleanup_figures(out_dir: Path) -> None:
    for name in OLD_FIGURES:
        path = out_dir / name
        if path.exists():
            path.unlink()


def put_p99_heatmap(out_dir: Path) -> None:
    width, height = 1600, 940
    body = title_block(
        "PUT p99 Latency Heatmap",
        "Darker cells are worse; this chart shows where background migration hurts foreground writes.",
    )
    x0, y0 = 310, 225
    cell_w, cell_h = 290, 150
    max_v = 4000
    header_lines = {
        "baseline": ("Baseline", ""),
        "A": ("Age-based", ""),
        "B": ("Throttled", ""),
        "C": ("Pressure-", "aware"),
    }

    for ci, policy in enumerate(POLICIES):
        x = x0 + ci * cell_w
        h1, h2 = header_lines[policy]
        body.append(text(x + cell_w / 2, y0 - 46, h1, 18, 900, "#334155", "middle"))
        if h2:
            body.append(text(x + cell_w / 2, y0 - 22, h2, 18, 900, "#334155", "middle"))
    for ri, profile in enumerate(["none", "cpu", "io"]):
        y = y0 + ri * cell_h
        body.append(text(x0 - 38, y + cell_h / 2 + 7, PROFILE_LABELS[profile], 20, 900, "#0f172a", "end"))
        for ci, policy in enumerate(POLICIES):
            x = x0 + ci * cell_w
            value = LATENCY[profile][policy]["PUT"]["p99"]
            stroke = "#14532d" if policy == "C" else "#ffffff"
            sw = 4.0 if policy == "C" else 2.0
            body.append(rect(x, y, cell_w - 14, cell_h - 14, heat_color(value, max_v), 18, stroke, sw))
            body.append(text(x + cell_w / 2 - 7, y + 67, f"{value:.0f} ms", 28, 900, "#0f172a", "middle"))
            if profile in ["cpu", "io"] and policy == "C":
                a = LATENCY[profile]["A"]["PUT"]["p99"]
                drop = percent_drop(a, value)
                pill_w = 160
                pill_x = x + (cell_w - pill_w) / 2 - 7
                body.append(rect(pill_x, y + 94, pill_w, 32, "#dcfce7", 16))
                body.append(text(x + cell_w / 2 - 7, y + 116, f"{drop:.1f}% lower vs A", 14, 900, "#15803d", "middle"))

    # Color scale.
    sx, sy = 310, 730
    body.append(text(sx, sy - 22, "Scale: lower PUT p99 is better", 16, 800, "#334155"))
    for i in range(120):
        v = i / 119 * max_v
        body.append(rect(sx + i * 5, sy, 5, 24, heat_color(v, max_v), 0))
    for tick in [0, 1000, 2000, 3000, 4000]:
        tx = sx + tick / max_v * 595
        body.append(line(tx, sy + 24, tx, sy + 34, "#64748b", 1))
        body.append(text(tx, sy + 55, tick, 12, 700, "#64748b", "middle"))

    body.append(text(310, 845, "Takeaway: under I/O pressure, Age-based and Throttled policies show severe write-tail amplification.", 17, 700, "#334155"))
    body.append(text(310, 875, "Pressure-aware remains much closer to baseline while avoiding foreground errors.", 17, 700, "#334155"))
    write_svg(out_dir / "01_put_p99_heatmap.svg", width, height, body)


def get_p99_heatmap(out_dir: Path) -> None:
    width, height = 1600, 940
    body = title_block(
        "GET p99 Latency Heatmap",
        "Read-tail latency is shown separately because GET and PUT are affected by migration in different ways.",
    )
    x0, y0 = 310, 225
    cell_w, cell_h = 290, 150
    max_v = 1600
    header_lines = {
        "baseline": ("Baseline", ""),
        "A": ("Age-based", ""),
        "B": ("Throttled", ""),
        "C": ("Pressure-", "aware"),
    }

    for ci, policy in enumerate(POLICIES):
        x = x0 + ci * cell_w
        h1, h2 = header_lines[policy]
        body.append(text(x + cell_w / 2, y0 - 46, h1, 18, 900, "#334155", "middle"))
        if h2:
            body.append(text(x + cell_w / 2, y0 - 22, h2, 18, 900, "#334155", "middle"))
    for ri, profile in enumerate(["none", "cpu", "io"]):
        y = y0 + ri * cell_h
        body.append(text(x0 - 38, y + cell_h / 2 + 7, PROFILE_LABELS[profile], 20, 900, "#0f172a", "end"))
        for ci, policy in enumerate(POLICIES):
            x = x0 + ci * cell_w
            value = LATENCY[profile][policy]["GET"]["p99"]
            stroke = "#14532d" if policy == "C" else "#ffffff"
            sw = 4.0 if policy == "C" else 2.0
            body.append(rect(x, y, cell_w - 14, cell_h - 14, heat_color(value, max_v), 18, stroke, sw))
            body.append(text(x + cell_w / 2 - 7, y + 67, f"{value:.0f} ms", 28, 900, "#0f172a", "middle"))
            if profile == "io" and policy == "C":
                a = LATENCY[profile]["A"]["GET"]["p99"]
                drop = percent_drop(a, value)
                pill_w = 160
                pill_x = x + (cell_w - pill_w) / 2 - 7
                body.append(rect(pill_x, y + 94, pill_w, 32, "#dcfce7", 16))
                body.append(text(x + cell_w / 2 - 7, y + 116, f"{drop:.1f}% lower vs A", 14, 900, "#15803d", "middle"))

    sx, sy = 310, 730
    body.append(text(sx, sy - 22, "Scale: lower GET p99 is better", 16, 800, "#334155"))
    for i in range(120):
        v = i / 119 * max_v
        body.append(rect(sx + i * 5, sy, 5, 24, heat_color(v, max_v), 0))
    for tick in [0, 400, 800, 1200, 1600]:
        tx = sx + tick / max_v * 595
        body.append(line(tx, sy + 24, tx, sy + 34, "#64748b", 1))
        body.append(text(tx, sy + 55, tick, 12, 700, "#64748b", "middle"))

    body.append(text(310, 845, "Takeaway: GET tail latency is less dominated by writes, so CPU-pressure differences are smaller and noisier.", 17, 700, "#334155"))
    body.append(text(310, 875, "Under I/O pressure, Pressure-aware still reduces read-tail amplification compared with Age-based.", 17, 700, "#334155"))
    write_svg(out_dir / "02_get_p99_heatmap.svg", width, height, body)


def io_put_percentile_bands(out_dir: Path) -> None:
    width, height = 1600, 940
    chart_x, chart_y = 155, 215
    chart_w, chart_h = 900, 470
    y_max = 4200
    body = title_block(
        "I/O Pressure PUT Latency Percentile Bands",
        "Summary percentile bands show how the write tail expands under I/O contention.",
    )
    body += legend(890, 108, POLICIES)
    body += y_grid(chart_x, chart_y, chart_w, chart_h, y_max, [0, 1000, 2000, 3000, 4000])
    body.append(text(55, 450, "PUT latency (ms)", 16, 800, "#334155", "middle")
                .replace("<text ", '<text transform="rotate(-90 55 450)" ', 1))

    def y_of(value: float) -> float:
        return chart_y + chart_h - value / y_max * chart_h

    spacing = chart_w / len(POLICIES)
    for i, policy in enumerate(POLICIES):
        x = chart_x + spacing * (i + 0.5)
        row = LATENCY["io"][policy]["PUT"]
        p50, p95, p99 = row["p50"], row["p95"], row["p99"]
        body.append(line(x, y_of(p50), x, y_of(p99), COLORS[policy], 10))
        body.append(line(x - 36, y_of(p95), x + 36, y_of(p95), "#0f172a", 3))
        body.append(circle(x, y_of(p50), 13, "#ffffff", COLORS[policy], 4))
        body.append(circle(x, y_of(p99), 15, COLORS[policy], "#ffffff", 3))
        body.append(text(x, chart_y + chart_h + 44, POLICY_LABELS[policy], 18, 900, "#0f172a", "middle"))
        body.append(text(x, y_of(p99) - 22, f"{p99:.0f}", 15, 900, "#0f172a", "middle"))

    # Readout table keeps detailed numbers out of the plotting area.
    table_x, table_y = 1115, 225
    body.append(rect(table_x - 25, table_y - 45, 465, 330, "#ffffff", 18, "#cbd5e1", 1.4))
    body.append(text(table_x, table_y, "I/O PUT percentiles", 22, 900, "#0f172a"))
    body.append(text(table_x, table_y + 40, "Policy", 14, 900, "#64748b"))
    body.append(text(table_x + 220, table_y + 40, "p50", 14, 900, "#64748b", "end"))
    body.append(text(table_x + 305, table_y + 40, "p95", 14, 900, "#64748b", "end"))
    body.append(text(table_x + 390, table_y + 40, "p99", 14, 900, "#64748b", "end"))
    for i, policy in enumerate(POLICIES):
        y = table_y + 78 + i * 48
        row = LATENCY["io"][policy]["PUT"]
        if i % 2 == 0:
            body.append(rect(table_x - 10, y - 27, 420, 36, "#f8fafc", 8))
        body.append(rect(table_x, y - 17, 14, 14, COLORS[policy], 3))
        body.append(text(table_x + 24, y, POLICY_LABELS[policy], 14, 800, "#334155"))
        body.append(text(table_x + 220, y, f'{row["p50"]:.0f}', 14, 800, "#334155", "end"))
        body.append(text(table_x + 305, y, f'{row["p95"]:.0f}', 14, 800, "#334155", "end"))
        body.append(text(table_x + 390, y, f'{row["p99"]:.0f}', 14, 900, "#0f172a", "end"))

    legend_y = 760
    body.append(circle(180, legend_y, 10, "#16a34a"))
    body.append(text(205, legend_y + 5, "filled circle = p99", 14, 700, "#475569"))
    body.append(line(380, legend_y, 445, legend_y, "#0f172a", 3))
    body.append(text(462, legend_y + 5, "black tick = p95", 14, 700, "#475569"))
    body.append(circle(660, legend_y, 9, "#ffffff", "#16a34a", 3))
    body.append(text(685, legend_y + 5, "open circle = p50", 14, 700, "#475569"))
    body.append(text(155, 840, "Takeaway: Strategy A/B have acceptable-looking medians but huge p95-p99 tails.", 17, 700, "#334155"))
    body.append(text(155, 870, "Pressure-aware keeps the tail band lower while maintaining zero foreground errors.", 17, 700, "#334155"))
    write_svg(out_dir / "02_io_put_percentile_bands.svg", width, height, body)


def migration_latency_tradeoff(out_dir: Path) -> None:
    width, height = 1600, 940
    chart_x, chart_y = 150, 215
    chart_w, chart_h = 930, 470
    x_max, y_max = 75, 4200
    body = title_block(
        "Migration Progress vs Foreground Write Tail",
        "Good policies move right without moving upward: more REPL_TO_EC work, lower PUT p99 latency.",
    )
    body += legend(930, 108, ["A", "B", "C"])

    for tick in [0, 15, 30, 45, 60, 75]:
        tx = chart_x + tick / x_max * chart_w
        body.append(line(tx, chart_y, tx, chart_y + chart_h, "#e2e8f0", 1))
        body.append(text(tx, chart_y + chart_h + 28, tick, 13, 700, "#64748b", "middle"))
    body += y_grid(chart_x, chart_y, chart_w, chart_h, y_max, [0, 1000, 2000, 3000, 4000])
    body.append(text(chart_x + chart_w / 2, chart_y + chart_h + 70, "Completed REPL_TO_EC tasks", 16, 800, "#334155", "middle"))
    body.append(text(55, 450, "PUT p99 latency (ms)", 16, 800, "#334155", "middle")
                .replace("<text ", '<text transform="rotate(-90 55 450)" ', 1))

    for profile in ["cpu", "io"]:
        for policy in ["A", "B", "C"]:
            done = MIGRATION[profile][policy]["done"]
            p99 = LATENCY[profile][policy]["PUT"]["p99"]
            x = chart_x + done / x_max * chart_w
            y = chart_y + chart_h - p99 / y_max * chart_h
            r = 16 if policy == "C" else 13
            if profile == "cpu":
                body.append(circle(x, y, r, COLORS[policy], "#ffffff", 3))
            else:
                body.append(
                    f'<rect x="{x-r:.1f}" y="{y-r:.1f}" width="{2*r:.1f}" height="{2*r:.1f}" '
                    f'transform="rotate(45 {x:.1f} {y:.1f})" fill="{COLORS[policy]}" stroke="#ffffff" stroke-width="3"/>'
                )
            body.append(text(x, y + 5, policy, 13, 900, "#ffffff", "middle"))

    table_x, table_y = 1165, 225
    body.append(rect(table_x - 25, table_y - 45, 330, 360, "#ffffff", 18, "#cbd5e1", 1.4))
    body.append(text(table_x, table_y, "Point readout", 22, 900, "#0f172a"))
    body.append(text(table_x, table_y + 40, "Profile", 14, 900, "#64748b"))
    body.append(text(table_x + 78, table_y + 40, "Policy", 14, 900, "#64748b"))
    body.append(text(table_x + 155, table_y + 40, "Done", 14, 900, "#64748b"))
    body.append(text(table_x + 220, table_y + 40, "p99", 14, 900, "#64748b"))
    row_i = 0
    for profile in ["cpu", "io"]:
        for policy in ["A", "B", "C"]:
            y = table_y + 78 + row_i * 40
            row_i += 1
            if row_i % 2 == 1:
                body.append(rect(table_x - 10, y - 26, 290, 33, "#f8fafc", 8))
            body.append(text(table_x, y, profile.upper(), 14, 800, "#334155"))
            body.append(rect(table_x + 82, y - 17, 14, 14, COLORS[policy], 3))
            body.append(text(table_x + 104, y, policy, 14, 900, "#334155"))
            body.append(text(table_x + 155, y, MIGRATION[profile][policy]["done"], 14, 800, "#334155"))
            body.append(text(table_x + 220, y, f'{LATENCY[profile][policy]["PUT"]["p99"]:.0f}', 14, 900, "#0f172a"))

    body.append(circle(170, 765, 10, "#64748b"))
    body.append(text(195, 771, "circle = CPU pressure", 14, 700, "#475569"))
    body.append(f'<rect x="395" y="755" width="20" height="20" transform="rotate(45 405 765)" fill="#64748b"/>')
    body.append(text(430, 771, "diamond = I/O pressure", 14, 700, "#475569"))
    body.append(text(150, 840, "Takeaway: under I/O pressure, Pressure-aware completes the most REPL_TO_EC tasks", 17, 700, "#334155"))
    body.append(text(150, 870, "while keeping PUT p99 far below Age-based and Throttled.", 17, 700, "#334155"))
    write_svg(out_dir / "03_migration_latency_tradeoff.svg", width, height, body)


def io_resilience_panel(out_dir: Path) -> None:
    width, height = 1600, 940
    body = title_block(
        "I/O Pressure Resilience",
        "A policy is useful only if it preserves throughput and avoids foreground errors while migration runs.",
    )
    body += legend(910, 108, POLICIES)

    # Throughput chart.
    left_x, top_y = 150, 220
    chart_w, chart_h = 560, 430
    body.append(text(left_x, top_y - 35, "Overall throughput", 24, 900, "#0f172a"))
    body += y_grid(left_x, top_y, chart_w, chart_h, 14, [0, 3, 6, 9, 12], " rps")
    for i, policy in enumerate(POLICIES):
        value = LATENCY["io"][policy]["ALL"]["throughput"]
        bar_w = 70
        x = left_x + 85 + i * 110
        h = value / 14 * chart_h
        y = top_y + chart_h - h
        body.append(rect(x, y, bar_w, h, COLORS[policy], 10))
        body.append(text(x + bar_w / 2, y - 12, f"{value:.1f}", 15, 900, "#0f172a", "middle"))
        body.append(text(x + bar_w / 2, top_y + chart_h + 34, policy, 15, 900, "#475569", "middle"))

    # Error chart.
    right_x = 875
    body.append(text(right_x, top_y - 35, "PUT error rate", 24, 900, "#0f172a"))
    body += y_grid(right_x, top_y, chart_w, chart_h, 8, [0, 2, 4, 6, 8], "%")
    for i, policy in enumerate(POLICIES):
        value = error_rate("io", policy, "PUT")
        bar_w = 70
        x = right_x + 85 + i * 110
        h = value / 8 * chart_h
        y = top_y + chart_h - h
        color = "#dc2626" if value > 0 else COLORS[policy]
        body.append(rect(x, y, bar_w, max(h, 2), color, 10))
        body.append(text(x + bar_w / 2, y - 12, f"{value:.1f}%", 15, 900, "#0f172a", "middle"))
        body.append(text(x + bar_w / 2, top_y + chart_h + 34, policy, 15, 900, "#475569", "middle"))

    body.append(rect(150, 735, 1280, 100, "#ffffff", 18, "#bbf7d0", 1.5))
    body.append(text(185, 777, "Result: Pressure-aware keeps 10.7 rps overall throughput and 0% PUT errors under I/O pressure.", 18, 800, "#14532d"))
    body.append(text(185, 810, "Age-based drops to 3.0 rps and 6.9% PUT errors.", 18, 800, "#14532d"))
    write_svg(out_dir / "04_io_resilience_panel.svg", width, height, body)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--out-dir", type=Path, default=Path("experiments/figures"))
    args = parser.parse_args()

    cleanup_figures(args.out_dir)
    put_p99_heatmap(args.out_dir)
    get_p99_heatmap(args.out_dir)
    io_put_percentile_bands(args.out_dir)
    migration_latency_tradeoff(args.out_dir)
    io_resilience_panel(args.out_dir)

    print(f"Wrote figures to {args.out_dir}")
    for name in FIGURES:
        print(args.out_dir / name)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
