#!/usr/bin/env python3
"""Generate poster-ready figures from selected representative GKE runs.

The selected values are mirrored in
experiments/final_results/gke_selected_representative_runs.csv. These are
compact single-run snapshots for poster layout work, so captions should say
"selected representative runs" instead of implying they are aggregate repeat
statistics.

This script intentionally uses Pillow only. It avoids depending on the local
NumPy/Matplotlib environment, which can be fragile on Cloud Shell/WSL machines.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Iterable

try:
    from PIL import Image, ImageColor, ImageDraw, ImageFont
except ImportError as exc:  # pragma: no cover - dependency message for humans
    raise SystemExit("Pillow is required. Use the bundled Codex Python or install pillow.") from exc


ROOT = Path(__file__).resolve().parents[1]
OUT_DIR = ROOT / "experiments" / "figures" / "poster_selected_runs"

POLICIES = ["Baseline", "Age-based", "Throttled", "Pressure-aware"]
PROFILES = ["No pressure", "CPU pressure", "I/O pressure"]

SHORT_POLICY = {
    "Baseline": "Base",
    "Age-based": "A",
    "Throttled": "B",
    "Pressure-aware": "C",
}

RUN_LABELS = {
    "No pressure": "20260607T150833Z-gke-formal-none-r2",
    "CPU pressure": "20260607T202559Z-gke-formal-cpu-r3",
    "I/O pressure": "20260606T153612Z-gke-multinode-io-r1",
}

DATA = {
    "No pressure": {
        "Baseline": {"get_p99": 40.4, "put_p99": 35.9, "repl_done": 0},
        "Age-based": {"get_p99": 50.1, "put_p99": 37.4, "repl_done": 41},
        "Throttled": {"get_p99": 50.3, "put_p99": 36.1, "repl_done": 40},
        "Pressure-aware": {"get_p99": 56.0, "put_p99": 40.5, "repl_done": 36},
    },
    "CPU pressure": {
        "Baseline": {"get_p99": 37.0, "put_p99": 35.4, "repl_done": 0},
        "Age-based": {"get_p99": 57.0, "put_p99": 38.8, "repl_done": 64},
        "Throttled": {"get_p99": 55.6, "put_p99": 40.6, "repl_done": 66},
        "Pressure-aware": {"get_p99": 113.1, "put_p99": 40.1, "repl_done": 59},
    },
    "I/O pressure": {
        "Baseline": {"get_p99": 44.7, "put_p99": 42.8, "repl_done": 0},
        "Age-based": {"get_p99": 75.0, "put_p99": 55.9, "repl_done": 66},
        "Throttled": {"get_p99": 79.0, "put_p99": 64.7, "repl_done": 66},
        "Pressure-aware": {"get_p99": 55.6, "put_p99": 52.1, "repl_done": 16},
    },
}

COLORS = {
    "surface": "#FCFCFD",
    "panel": "#FFFFFF",
    "navy": "#1F2A56",
    "ink": "#1F2430",
    "muted": "#6F768A",
    "grid": "#E6E8F0",
    "axis": "#C9CEDA",
    "blue": "#5477C4",
    "gold": "#FFE15B",
    "gold_light": "#FFF4C2",
    "orange": "#F0986E",
    "orange_dark": "#804126",
    "red": "#EF4444",
    "green": "#71B436",
    "green_dark": "#386411",
    "teal": "#1D7F95",
    "gray": "#7A828F",
    "gray_light": "#E2E5EA",
}

POLICY_COLORS = {
    "Baseline": COLORS["gray"],
    "Age-based": COLORS["blue"],
    "Throttled": COLORS["orange"],
    "Pressure-aware": COLORS["green"],
}


def rgb(hex_color: str) -> tuple[int, int, int]:
    return ImageColor.getrgb(hex_color)


def blend(a: str, b: str, t: float) -> tuple[int, int, int]:
    ar, ag, ab = rgb(a)
    br, bg, bb = rgb(b)
    return (
        int(ar + (br - ar) * t),
        int(ag + (bg - ag) * t),
        int(ab + (bb - ab) * t),
    )


def heat_color(value: float, vmin: float = 30.0, vmax: float = 120.0) -> tuple[int, int, int]:
    t = max(0.0, min(1.0, (value - vmin) / (vmax - vmin)))
    if t < 0.55:
        return blend(COLORS["gold_light"], COLORS["gold"], t / 0.55)
    if t < 0.82:
        return blend(COLORS["gold"], COLORS["orange"], (t - 0.55) / 0.27)
    return blend(COLORS["orange"], COLORS["red"], (t - 0.82) / 0.18)


def font(size: int, bold: bool = False) -> ImageFont.FreeTypeFont | ImageFont.ImageFont:
    candidates = [
        "C:/Windows/Fonts/arialbd.ttf" if bold else "C:/Windows/Fonts/arial.ttf",
        "C:/Windows/Fonts/segoeuib.ttf" if bold else "C:/Windows/Fonts/segoeui.ttf",
        "/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf" if bold else "/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf",
    ]
    for candidate in candidates:
        try:
            if Path(candidate).exists():
                return ImageFont.truetype(candidate, size=size)
        except OSError:
            pass
    return ImageFont.load_default()


@dataclass
class Canvas:
    width: int
    height: int
    scale: int = 2

    def __post_init__(self) -> None:
        self.image = Image.new("RGB", (self.width * self.scale, self.height * self.scale), rgb(COLORS["surface"]))
        self.draw = ImageDraw.Draw(self.image)

    def xy(self, *coords: float) -> tuple[int, ...]:
        return tuple(int(round(c * self.scale)) for c in coords)

    def f(self, size: int, bold: bool = False) -> ImageFont.FreeTypeFont | ImageFont.ImageFont:
        return font(size * self.scale, bold)

    def text(self, xy: tuple[float, float], text: str, size: int, color: str = COLORS["ink"], bold: bool = False, anchor: str | None = None) -> None:
        self.draw.text(self.xy(*xy), text, fill=rgb(color), font=self.f(size, bold), anchor=anchor)

    def centered(self, box: tuple[float, float, float, float], text: str, size: int, color: str = COLORS["ink"], bold: bool = False) -> None:
        x1, y1, x2, y2 = box
        self.draw.text(self.xy((x1 + x2) / 2, (y1 + y2) / 2), text, fill=rgb(color), font=self.f(size, bold), anchor="mm")

    def rect(self, box: tuple[float, float, float, float], fill: str | tuple[int, int, int] | None, outline: str | None = None, width: int = 1, radius: int = 0) -> None:
        fill_rgb = rgb(fill) if isinstance(fill, str) else fill
        outline_rgb = rgb(outline) if outline else None
        scaled_width = max(1, width * self.scale)
        if radius:
            self.draw.rounded_rectangle(self.xy(*box), radius=radius * self.scale, fill=fill_rgb, outline=outline_rgb, width=scaled_width)
        else:
            self.draw.rectangle(self.xy(*box), fill=fill_rgb, outline=outline_rgb, width=scaled_width)

    def line(self, coords: Iterable[float], fill: str = COLORS["axis"], width: int = 1) -> None:
        self.draw.line(self.xy(*coords), fill=rgb(fill), width=max(1, width * self.scale))

    def circle(self, center: tuple[float, float], radius: float, fill: str, outline: str = "#FFFFFF", width: int = 2) -> None:
        x, y = center
        self.draw.ellipse(self.xy(x - radius, y - radius, x + radius, y + radius), fill=rgb(fill), outline=rgb(outline), width=max(1, width * self.scale))

    def save(self, name: str) -> None:
        OUT_DIR.mkdir(parents=True, exist_ok=True)
        output = self.image.resize((self.width, self.height), Image.Resampling.LANCZOS)
        output.save(OUT_DIR / f"{name}.png", dpi=(300, 300))


def panel(c: Canvas, box: tuple[int, int, int, int]) -> None:
    c.rect(box, COLORS["panel"], outline="#E3E7F0", width=1, radius=22)


def draw_legend(c: Canvas, x: int, y: int, items: list[tuple[str, str]]) -> None:
    offset = 0
    for label, color in items:
        c.rect((x + offset, y, x + offset + 20, y + 20), color, radius=5)
        c.text((x + offset + 28, y - 1), label, 18, COLORS["muted"], bold=True)
        offset += 170


def plot_heatmaps() -> None:
    c = Canvas(2300, 1020)
    c.text((70, 60), "Foreground p99 latency under selected representative runs", 42, COLORS["navy"], bold=True)
    c.text((70, 112), "Lower is better. Green outline marks pressure-aware policy. Runs: none r2, CPU r3, I/O r1.", 22, COLORS["muted"])

    panels = [
        ("PUT p99 latency", "put_p99", 70),
        ("GET p99 latency", "get_p99", 1190),
    ]
    for title, metric, x0 in panels:
        panel(c, (x0, 170, x0 + 1040, 860))
        c.text((x0 + 42, 210), title, 32, COLORS["navy"], bold=True)
        left = x0 + 205
        top = 310
        cell_w = 200
        cell_h = 150
        for i, policy in enumerate(POLICIES):
            label = policy.replace("Pressure-aware", "Pressure-\naware")
            c.centered((left + i * cell_w, top - 88, left + (i + 1) * cell_w, top - 20), label, 18, COLORS["ink"], bold=True)
        for r, profile in enumerate(PROFILES):
            c.text((x0 + 44, top + r * cell_h + 54), profile, 20, COLORS["ink"], bold=True)
            row_values = [DATA[profile][p][metric] for p in POLICIES]
            row_min = min(row_values)
            for i, policy in enumerate(POLICIES):
                value = DATA[profile][policy][metric]
                box = (left + i * cell_w + 8, top + r * cell_h + 8, left + (i + 1) * cell_w - 8, top + (r + 1) * cell_h - 8)
                c.rect(box, heat_color(value), radius=18)
                if policy == "Pressure-aware":
                    c.rect(box, None, outline=COLORS["green_dark"], width=5, radius=18)
                c.centered(box, f"{value:.1f} ms", 25, "#0B1220", bold=True)
                if abs(value - row_min) < 1e-9:
                    c.rect((box[0] + 58, box[3] - 40, box[2] - 58, box[3] - 13), "#D8ECBD", radius=14)
                    c.centered((box[0] + 58, box[3] - 40, box[2] - 58, box[3] - 13), "lowest", 14, "#064E3B", bold=True)

    c.text((70, 920), "Caption suggestion: selected representative runs from three pressure profiles; p99 latency measured by in-cluster workload.", 20, COLORS["muted"])
    c.save("01_put_get_p99_heatmaps")


def draw_axes(c: Canvas, box: tuple[int, int, int, int], x_max: float, y_max: float, y_label: str) -> None:
    x1, y1, x2, y2 = box
    for tick in range(0, int(y_max) + 1, 25):
        ty = y2 - (tick / y_max) * (y2 - y1)
        c.line((x1, ty, x2, ty), COLORS["grid"], 1)
        c.text((x1 - 16, ty - 10), str(tick), 15, COLORS["muted"], anchor="ra")
    for tick in range(0, int(x_max) + 1, 20):
        tx = x1 + (tick / x_max) * (x2 - x1)
        c.line((tx, y2, tx, y2 + 7), COLORS["axis"], 2)
        c.text((tx, y2 + 18), str(tick), 15, COLORS["muted"], anchor="ma")
    c.line((x1, y1, x1, y2), COLORS["axis"], 2)
    c.line((x1, y2, x2, y2), COLORS["axis"], 2)
    c.centered((x1, y2 + 48, x2, y2 + 80), "Completed REPL_TO_EC tasks", 18, COLORS["ink"], bold=True)
    c.text((x1 - 70, y1 + 8), y_label, 17, COLORS["ink"], bold=True)


def plot_tradeoff() -> None:
    c = Canvas(2300, 1120)
    c.text((70, 60), "I/O pressure trade-off: latency protection vs. migration progress", 42, COLORS["navy"], bold=True)
    c.text((70, 112), "Representative I/O run r1. C completes fewer migrations while keeping foreground p99 close to the fastest policy.", 22, COLORS["muted"])
    draw_legend(c, 70, 170, [(p, POLICY_COLORS[p]) for p in POLICIES])

    charts = [
        ("PUT p99 vs. migration progress", "put_p99", (120, 270, 1060, 910)),
        ("GET p99 vs. migration progress", "get_p99", (1230, 270, 2170, 910)),
    ]
    x_max = 72.0
    y_max = 90.0
    profile = "I/O pressure"
    for title, metric, box in charts:
        panel(c, (box[0] - 52, box[1] - 82, box[2] + 42, box[3] + 112))
        c.text((box[0], box[1] - 55), title, 30, COLORS["navy"], bold=True)
        draw_axes(c, box, x_max, y_max, "p99 latency (ms)")
        x1, y1, x2, y2 = box
        if metric == "get_p99":
            label_offsets = {
                "Baseline": (18, -12),
                "Age-based": (-150, -48),
                "Throttled": (18, -8),
                "Pressure-aware": (18, -40),
            }
        else:
            label_offsets = {
                "Baseline": (18, -12),
                "Age-based": (-88, -40),
                "Throttled": (20, -10),
                "Pressure-aware": (18, -40),
            }
        for policy in POLICIES:
            x = DATA[profile][policy]["repl_done"]
            y = DATA[profile][policy][metric]
            px = x1 + (x / x_max) * (x2 - x1)
            py = y2 - (y / y_max) * (y2 - y1)
            radius = 22 if policy == "Pressure-aware" else 17
            c.circle((px, py), radius, POLICY_COLORS[policy], width=3)
            dx, dy = label_offsets[policy]
            value = f"{SHORT_POLICY[policy]}  {y:.1f} ms / {x:.0f} tasks"
            c.text((px + dx, py + dy), value, 17, COLORS["ink"], bold=(policy == "Pressure-aware"))

    c.text((70, 1040), "Caption suggestion: under I/O pressure, pressure-aware tiering trades migration progress for foreground latency protection.", 20, COLORS["muted"])
    c.save("02_latency_migration_tradeoff")


def plot_migration_bars() -> None:
    c = Canvas(2000, 1020)
    c.text((70, 60), "Completed background migration tasks", 42, COLORS["navy"], bold=True)
    c.text((70, 112), "Pressure-aware tiering intentionally reduces migration work when I/O pressure is detected.", 22, COLORS["muted"])
    draw_legend(c, 70, 165, [(p, POLICY_COLORS[p]) for p in POLICIES])
    panel(c, (70, 235, 1930, 895))

    chart = (180, 310, 1860, 800)
    x1, y1, x2, y2 = chart
    y_max = 75.0
    for tick in range(0, 76, 15):
        ty = y2 - (tick / y_max) * (y2 - y1)
        c.line((x1, ty, x2, ty), COLORS["grid"], 1)
        c.text((x1 - 20, ty - 10), str(tick), 17, COLORS["muted"], anchor="ra")
    c.line((x1, y1, x1, y2), COLORS["axis"], 2)
    c.line((x1, y2, x2, y2), COLORS["axis"], 2)
    c.text((92, 320), "tasks", 18, COLORS["ink"], bold=True)

    group_w = (x2 - x1) / len(PROFILES)
    bar_w = 70
    for g, profile in enumerate(PROFILES):
        gx = x1 + group_w * g + group_w / 2
        c.centered((gx - group_w / 2, y2 + 35, gx + group_w / 2, y2 + 72), profile, 21, COLORS["ink"], bold=True)
        for i, policy in enumerate(POLICIES):
            value = DATA[profile][policy]["repl_done"]
            bx = gx + (i - 1.5) * (bar_w + 14)
            by = y2 - (value / y_max) * (y2 - y1)
            c.rect((bx, by, bx + bar_w, y2), POLICY_COLORS[policy], radius=8)
            c.text((bx + bar_w / 2, by - 30), f"{value:.0f}", 18, COLORS["ink"], bold=True, anchor="ma")

    c.text((70, 940), "Caption suggestion: C completes 16 tasks under I/O pressure vs. 66 tasks for A/B in the selected I/O run.", 20, COLORS["muted"])
    c.save("03_completed_migration_bars")


def plot_io_focus() -> None:
    c = Canvas(2300, 980)
    c.text((70, 60), "I/O pressure: pressure-aware policy gates migration work", 42, COLORS["navy"], bold=True)
    c.text((70, 112), "Representative I/O run r1. C reduces background work while keeping foreground p99 in the same range.", 22, COLORS["muted"])
    metrics = [
        ("GET p99 latency", "get_p99", "ms"),
        ("PUT p99 latency", "put_p99", "ms"),
        ("Completed migration", "repl_done", "tasks"),
    ]
    profile = "I/O pressure"
    for col, (title, key, unit) in enumerate(metrics):
        x0 = 70 + col * 750
        panel(c, (x0, 200, x0 + 660, 820))
        c.text((x0 + 35, 245), title, 29, COLORS["navy"], bold=True)
        vals = [DATA[profile][policy][key] for policy in POLICIES]
        max_v = max(vals) * 1.22
        top = 330
        for i, policy in enumerate(POLICIES):
            y = top + i * 115
            c.text((x0 + 35, y + 24), policy, 21, COLORS["ink"], bold=True)
            bar_x = x0 + 245
            bar_w = 360 * (DATA[profile][policy][key] / max_v if max_v else 0)
            c.rect((bar_x, y, bar_x + bar_w, y + 48), POLICY_COLORS[policy], radius=10)
            value = DATA[profile][policy][key]
            label = f"{value:.1f} {unit}" if unit == "ms" else f"{value:.0f} {unit}"
            c.text((bar_x + bar_w + 16, y + 10), label, 19, COLORS["ink"], bold=True)
    c.text((70, 900), "Caption suggestion: lower p99 is better; fewer completed migrations means the policy delayed background work under contention.", 20, COLORS["muted"])
    c.save("04_io_pressure_focus_bars")


def plot_profile_latency_bars() -> None:
    c = Canvas(2300, 1020)
    c.text((70, 60), "Foreground p99 latency by pressure profile", 42, COLORS["navy"], bold=True)
    c.text((70, 112), "Grouped bars show both foreground operations without using a dense table.", 22, COLORS["muted"])
    draw_legend(c, 70, 165, [("PUT p99", COLORS["gold"]), ("GET p99", COLORS["teal"])])
    for col, profile in enumerate(PROFILES):
        x0 = 70 + col * 750
        panel(c, (x0, 235, x0 + 660, 870))
        c.text((x0 + 35, 285), profile, 30, COLORS["navy"], bold=True)
        max_v = 125.0
        for i, policy in enumerate(POLICIES):
            y = 370 + i * 120
            c.text((x0 + 35, y + 22), policy, 20, COLORS["ink"], bold=True)
            put = DATA[profile][policy]["put_p99"]
            get = DATA[profile][policy]["get_p99"]
            bar_x = x0 + 235
            put_w = 330 * put / max_v
            get_w = 330 * get / max_v
            c.rect((bar_x, y, bar_x + put_w, y + 35), COLORS["gold"], radius=8)
            c.rect((bar_x, y + 42, bar_x + get_w, y + 77), COLORS["teal"], radius=8)
            c.text((bar_x + put_w + 12, y + 5), f"{put:.1f}", 16, COLORS["ink"], bold=True)
            c.text((bar_x + get_w + 12, y + 47), f"{get:.1f}", 16, COLORS["ink"], bold=True)
    c.text((70, 940), "Caption suggestion: selected representative runs; all measurements use in-cluster workload traffic.", 20, COLORS["muted"])
    c.save("05_profile_latency_grouped_bars")


def main() -> None:
    plot_heatmaps()
    plot_tradeoff()
    plot_migration_bars()
    plot_io_focus()
    plot_profile_latency_bars()
    print(f"Wrote poster figures to {OUT_DIR}")


if __name__ == "__main__":
    main()
