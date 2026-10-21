"""Capture dashboard panels as PNGs, and refuse to save one that is not a panel.

The dashboard is verified by two test suites already -- one that checks every
panel queries a series the registry can produce, one that drives load and
checks every expression selects something. What neither can show is what the
thing *looks like* while the pipeline is under a fault, which is the one
question a reader with ninety seconds is actually asking. Hence images, in git,
beside the numbers.

**The verification matters more than the capture.** Grafana cannot rasterise a
panel by itself, and asked to do so it answers **HTTP 200** with a PNG reading
"No image renderer available/installed". A naive capture script saves that file
happily, the images land in the repository, and the documentation now contains
a picture of an error message where the evidence should be. This project has
been caught by a success code carrying a failure before.

So every capture is checked against the size that was requested. The
placeholder is always 478x208 whatever dimensions are asked for, so an image
whose dimensions do not match the request is rejected and the file is not
written. Size in bytes would be a weaker test -- a blank panel is small too,
and a blank panel is a real answer.

    docker compose --profile rendering up -d renderer
    python tasks.py screenshots

Run it while the signals are fresh. Most of these panels are rate() or
increase() over a short window, so a dashboard captured an hour after the fault
is a picture of an idle pipeline, correctly rendered.
"""

from __future__ import annotations

import argparse
import json
import struct
import sys
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path

import httpx
import structlog

from tools.chaos.support import REPO_ROOT

log = structlog.get_logger(__name__)

GRAFANA_URL = "http://localhost:3000"
DASHBOARD_UID = "transitpulse-pipeline"
OUTPUT_DIR = REPO_ROOT / "docs" / "dashboards"

# The dimensions Grafana returns when it has no renderer, whatever was asked
# for. Measured against this stack, not taken from the documentation.
PLACEHOLDER_SIZE = (478, 208)


@dataclass(frozen=True)
class Panel:
    """One panel to capture, named for what it answers rather than by id."""

    slug: str
    panel_id: int | None
    title: str
    width: int = 1000
    height: int = 420

    @property
    def path(self) -> str:
        # d-solo renders one panel; d renders the whole dashboard.
        return (
            f"/render/d-solo/{DASHBOARD_UID}/" if self.panel_id else f"/render/d/{DASHBOARD_UID}/"
        )


PANELS = (
    Panel("slo-compliance", 1, "SLO compliance - events processed within 2s", 700, 360),
    Panel("ingest-lag-p95", 2, "Ingest lag p95", 700, 360),
    Panel("ingest-lag-percentiles", 5, "Ingest lag percentiles"),
    Panel("quarantined-by-reason", 7, "Quarantined events by reason"),
    Panel("delivery-outcome-rate", 8, "Delivery outcome rate"),
    Panel("events-dead-lettered", 4, "Events dead-lettered (10m)", 700, 360),
    Panel("error-budget-burn", 15, "Error budget burn rate"),
    Panel("pipeline-health", None, "The whole dashboard", 1400, 1700),
)


def png_dimensions(payload: bytes) -> tuple[int, int] | None:
    """Width and height straight out of the IHDR chunk, or None if not a PNG."""
    if len(payload) < 24 or payload[:8] != b"\x89PNG\r\n\x1a\n":
        return None
    width, height = struct.unpack(">II", payload[16:24])
    return int(width), int(height)


def capture(
    panel: Panel, *, grafana: str, window: str, theme: str, scale: int
) -> tuple[bytes, str | None]:
    """Render one panel. Returns the bytes and a rejection reason, if any."""
    params = {
        "panelId": str(panel.panel_id) if panel.panel_id else None,
        "width": str(panel.width),
        "height": str(panel.height),
        "scale": str(scale),
        "from": f"now-{window}",
        "to": "now",
        "theme": theme,
        # Without a timezone the renderer uses the container's UTC, which is
        # fine, but pinning it means two captures of the same window are
        # comparable whatever the host is set to.
        "tz": "UTC",
    }
    response = httpx.get(
        f"{grafana}{panel.path}",
        params={k: v for k, v in params.items() if v is not None},
        auth=("admin", "admin"),
        timeout=120.0,
        follow_redirects=True,
    )
    if response.status_code != 200:
        return response.content, f"HTTP {response.status_code}"

    dimensions = png_dimensions(response.content)
    if dimensions is None:
        return response.content, "the response is not a PNG"
    if dimensions == PLACEHOLDER_SIZE:
        return response.content, (
            "Grafana returned its 'No image renderer available' placeholder; start it "
            "with: docker compose --profile rendering up -d renderer"
        )
    # Not an exact size match: the renderer clamps a solo panel to its own
    # minimum viewport, so a request for 700x360 legitimately comes back at
    # 1000x500. What has to hold is that the image is a rendered panel rather
    # than the placeholder -- which is a fixed 478x208 whatever was asked for,
    # so "larger than it in both dimensions" separates the two without
    # rejecting a render the renderer resized on its own.
    if dimensions[0] <= PLACEHOLDER_SIZE[0] or dimensions[1] <= PLACEHOLDER_SIZE[1]:
        return (
            response.content,
            f"image is {dimensions[0]}x{dimensions[1]}, no larger than the "
            f"{PLACEHOLDER_SIZE[0]}x{PLACEHOLDER_SIZE[1]} placeholder",
        )
    return response.content, None


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        prog="screenshots",
        description="Capture the dashboard panels as verified PNGs under docs/dashboards/.",
        epilog=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("--grafana", default=GRAFANA_URL, help=f"default: {GRAFANA_URL}")
    parser.add_argument(
        "--window", default="30m", help="dashboard time range, ending now (default: 30m)"
    )
    parser.add_argument("--theme", default="light", choices=["light", "dark"])
    parser.add_argument("--scale", type=int, default=2, help="device scale factor (default: 2)")
    parser.add_argument("--out", type=Path, default=OUTPUT_DIR, help=f"default: {OUTPUT_DIR.name}/")
    parser.add_argument(
        "--only", default=None, help="capture one panel by slug instead of all of them"
    )
    args = parser.parse_args(argv)

    panels = [p for p in PANELS if args.only is None or p.slug == args.only]
    if not panels:
        print(f"error: no panel with slug {args.only!r}", file=sys.stderr)
        return 2

    args.out.mkdir(parents=True, exist_ok=True)
    captured: list[dict[str, object]] = []
    failures = 0

    for panel in panels:
        payload, problem = capture(
            panel,
            grafana=args.grafana,
            window=args.window,
            theme=args.theme,
            scale=args.scale,
        )
        if problem is not None:
            failures += 1
            print(f"  FAIL  {panel.slug}: {problem}", file=sys.stderr)
            continue
        target = args.out / f"{panel.slug}.png"
        target.write_bytes(payload)
        dimensions = png_dimensions(payload) or (0, 0)
        captured.append(
            {
                "slug": panel.slug,
                "panel_id": panel.panel_id,
                "title": panel.title,
                "file": str(target.relative_to(REPO_ROOT)).replace("\\", "/"),
                "pixels": f"{dimensions[0]}x{dimensions[1]}",
                "bytes": len(payload),
            }
        )
        print(f"  ok    {panel.slug}  {dimensions[0]}x{dimensions[1]}  {len(payload):,} bytes")

    if captured:
        # A manifest, because a screenshot with no time range attached is an
        # assertion rather than evidence: a reader cannot tell which run it
        # shows or line it up against the result files.
        manifest = {
            "captured_at": datetime.now(UTC).isoformat(),
            "dashboard_uid": DASHBOARD_UID,
            "window": f"now-{args.window} to now",
            "theme": args.theme,
            "scale": args.scale,
            "panels": captured,
        }
        (args.out / "manifest.json").write_text(
            json.dumps(manifest, indent=2) + "\n", encoding="utf-8"
        )
        print(f"\n  {len(captured)} panel(s) -> {args.out.relative_to(REPO_ROOT)}/")

    return 1 if failures else 0


if __name__ == "__main__":  # pragma: no cover - module entry point
    raise SystemExit(main())
