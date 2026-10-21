"""Collect a nightly performance run into one table.

The gate's verdict is already carried by exit codes -- the replay harness
exits non-zero below ``--min-accuracy`` and each chaos scenario exits non-zero
if any check failed -- so this adds no judgement. What it adds is the numbers
in one place, written to the job summary, because a gate that only ever says
"pass" teaches nobody anything about drift.

Thresholds on a shared CI runner have to be loose enough not to flake, which
means the interesting signal is not the pass/fail but the trend underneath it.
Printing the measured rates every night is what makes that trend visible
without anyone downloading an artifact.

    python -m tools.perf_summary --out "$GITHUB_STEP_SUMMARY"
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[1]
REPLAY_DIR = REPO_ROOT / "perf"
CHAOS_DIR = REPO_ROOT / "docs" / "benchmarks"


def _load(path: Path) -> dict[str, Any] | None:
    try:
        payload: dict[str, Any] = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return None
    return payload


def replay_rows(directory: Path) -> list[tuple[str, str, str]]:
    rows: list[tuple[str, str, str]] = []
    for path in sorted(directory.glob("*.json")):
        report = _load(path)
        if report is None or "achieved_rate" not in report:
            continue
        verdict = "pass" if report["rate_accuracy"] >= 0.8 else "LOW"
        detail = (
            f"{report['achieved_rate']:,.1f}/s "
            f"({report['rate_accuracy']:.1%} of {report['target_rate']:,.0f}/s)"
        )
        if report.get("publisher_bound"):
            detail += " — publisher-bound"
        rows.append((path.stem, verdict, detail))
    return rows


def chaos_rows(directory: Path) -> list[tuple[str, str, str]]:
    rows: list[tuple[str, str, str]] = []
    for path in sorted(directory.glob("*.json")):
        result = _load(path)
        if result is None or "checks" not in result:
            continue
        failed = [check["name"] for check in result["checks"] if not check["passed"]]
        detail = f"{len(result['checks'])} checks"
        if failed:
            detail += " — failed: " + "; ".join(failed)
        rows.append((result["scenario"], "pass" if result["passed"] else "FAIL", detail))
    return rows


def render(rows: list[tuple[str, str, str]]) -> str:
    if not rows:
        return "No performance results were produced — the run failed before measuring anything.\n"
    lines = ["| Measurement | Result | Detail |", "|---|---|---|"]
    lines += [f"| `{name}` | {verdict} | {detail} |" for name, verdict, detail in rows]
    return "\n".join(lines) + "\n"


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        prog="perf_summary",
        description="Summarise a nightly performance run as a Markdown table.",
    )
    parser.add_argument(
        "--out",
        type=Path,
        default=None,
        help="append the table here (e.g. $GITHUB_STEP_SUMMARY); stdout if omitted",
    )
    parser.add_argument("--replay-dir", type=Path, default=REPLAY_DIR)
    parser.add_argument("--chaos-dir", type=Path, default=CHAOS_DIR)
    args = parser.parse_args(argv)

    rows = replay_rows(args.replay_dir) + chaos_rows(args.chaos_dir)
    table = "## Nightly performance gate\n\n" + render(rows)

    if args.out is None:
        print(table)
    else:
        with args.out.open("a", encoding="utf-8") as handle:
            handle.write(table)
        print(table)

    # Deliberately always zero. The gate's verdict belongs to the steps that
    # measured things; a summariser that could also fail the build would give
    # a run two ways to fail for one reason.
    return 0


if __name__ == "__main__":  # pragma: no cover - module entry point
    raise SystemExit(main())
