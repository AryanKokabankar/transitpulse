"""What a chaos experiment is, and how its conclusion is recorded.

An experiment that printed a narrative would be worth nothing: the whole point
is that a reviewer can run it rather than believe it. So a scenario produces a
:class:`ScenarioResult` -- a list of checks, each one a comparison between a
number that was expected and a number that was measured, plus the raw
measurements behind them -- and the process exits non-zero if any check failed.

The checks are written as comparisons rather than bare booleans because a
failure has to be diagnosable from the output alone. ``zero loss: expected
2000, measured 1997`` says what to look at; ``zero loss: False`` does not.
"""

from __future__ import annotations

import argparse
import json
import sys
import time
from collections.abc import Callable, Sequence
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import structlog

from tools.chaos.support import RESULTS_DIR
from transitpulse_core.config import Settings, get_settings
from transitpulse_core.telemetry import setup_telemetry

log = structlog.get_logger(__name__)


@dataclass(frozen=True)
class Check:
    """One assertion, carrying both numbers so a failure explains itself."""

    name: str
    passed: bool
    detail: str

    def as_dict(self) -> dict[str, Any]:
        return {"name": self.name, "passed": self.passed, "detail": self.detail}


def equals(name: str, expected: object, measured: object, *, unit: str = "") -> Check:
    suffix = f" {unit}" if unit else ""
    return Check(
        name=name,
        passed=expected == measured,
        detail=f"expected {expected!r}{suffix}, measured {measured!r}{suffix}",
    )


def at_least(name: str, floor: float, measured: float, *, unit: str = "") -> Check:
    suffix = f" {unit}" if unit else ""
    return Check(
        name=name,
        passed=measured >= floor,
        detail=f"expected at least {floor}{suffix}, measured {measured}{suffix}",
    )


def at_most(name: str, ceiling: float, measured: float, *, unit: str = "") -> Check:
    suffix = f" {unit}" if unit else ""
    return Check(
        name=name,
        passed=measured <= ceiling,
        detail=f"expected at most {ceiling}{suffix}, measured {measured}{suffix}",
    )


def holds(name: str, passed: bool, detail: str) -> Check:
    """For a property with no natural pair of numbers, e.g. a breaker's state."""
    return Check(name=name, passed=passed, detail=detail)


@dataclass
class ScenarioResult:
    """One run of one experiment: what it asked, what it found, what it proves."""

    scenario: str
    question: str
    checks: list[Check] = field(default_factory=list)
    measurements: dict[str, Any] = field(default_factory=dict)
    notes: list[str] = field(default_factory=list)
    seconds: float = 0.0
    started_at: str = ""

    @property
    def passed(self) -> bool:
        return all(check.passed for check in self.checks)

    def check(self, *checks: Check) -> None:
        self.checks.extend(checks)

    def measure(self, **values: Any) -> None:
        self.measurements.update(values)

    def note(self, *lines: str) -> None:
        """A caveat that travels with the numbers.

        Used for the things that would make a figure misleading if quoted
        without them -- a shortened cooldown, a Postgres running without
        fsync, a rate the limiter was still capping.
        """
        self.notes.extend(lines)

    def as_dict(self) -> dict[str, Any]:
        return {
            "scenario": self.scenario,
            "question": self.question,
            "passed": self.passed,
            "started_at": self.started_at,
            "seconds": round(self.seconds, 3),
            "checks": [check.as_dict() for check in self.checks],
            "measurements": self.measurements,
            "notes": self.notes,
        }

    def render(self) -> str:
        width = max((len(c.name) for c in self.checks), default=0)
        lines = [
            "",
            f"  chaos: {self.scenario}",
            "  " + "=" * 68,
            f"  {self.question}",
            "",
        ]
        for check in self.checks:
            mark = "PASS" if check.passed else "FAIL"
            lines.append(f"  [{mark}] {check.name.ljust(width)}  {check.detail}")
        if self.measurements:
            lines.extend(["", "  measurements"])
            lines.extend(_render_measurements(self.measurements, indent=4))
        if self.notes:
            lines.extend(["", "  notes"])
            lines.extend(f"    - {note}" for note in self.notes)
        verdict = "all checks passed" if self.passed else "CHECKS FAILED"
        lines.extend(["", f"  {verdict} in {self.seconds:,.2f}s", ""])
        return "\n".join(lines)


def _render_measurements(values: dict[str, Any], *, indent: int) -> list[str]:
    pad = " " * indent
    width = max((len(k) for k in values), default=0)
    out: list[str] = []
    for key, value in values.items():
        if isinstance(value, dict):
            out.append(f"{pad}{key}")
            out.extend(_render_measurements(value, indent=indent + 2))
        elif isinstance(value, float):
            out.append(f"{pad}{key.ljust(width)}  {value:,.4f}")
        elif isinstance(value, int) and not isinstance(value, bool):
            out.append(f"{pad}{key.ljust(width)}  {value:,}")
        else:
            out.append(f"{pad}{key.ljust(width)}  {value}")
    return out


def write_result(result: ScenarioResult, *, directory: Path | None = None) -> Path:
    """Commit the raw measurements beside the prose that will cite them."""
    target = directory or RESULTS_DIR
    target.mkdir(parents=True, exist_ok=True)
    path = target / f"{result.scenario}.json"
    path.write_text(json.dumps(result.as_dict(), indent=2) + "\n", encoding="utf-8")
    return path


ScenarioFn = Callable[[Settings, argparse.Namespace], ScenarioResult]


def execute(
    scenario: ScenarioFn,
    settings: Settings,
    args: argparse.Namespace,
    *,
    name: str,
) -> ScenarioResult:
    """Time one scenario and stamp it, whatever it concludes."""
    started = time.monotonic()
    started_at = datetime.now(UTC).isoformat()
    log.info("chaos.started", scenario=name)
    result = scenario(settings, args)
    result.seconds = time.monotonic() - started
    result.started_at = started_at
    log.info("chaos.finished", scenario=name, passed=result.passed, seconds=result.seconds)
    return result


def add_common_arguments(parser: argparse.ArgumentParser) -> argparse.ArgumentParser:
    parser.add_argument("--json", action="store_true", help="emit the result as JSON on stdout")
    parser.add_argument(
        "--no-write",
        action="store_true",
        help=f"do not write the result file under {RESULTS_DIR.name}/",
    )
    parser.add_argument(
        "--keep-rows",
        action="store_true",
        help="leave this run's rows in Postgres for inspection",
    )
    return parser


def report(result: ScenarioResult, args: argparse.Namespace) -> int:
    """Print, persist, and turn the verdict into an exit code."""
    if args.json:
        print(json.dumps(result.as_dict(), indent=2))
    else:
        print(result.render())
    if not args.no_write:
        path = write_result(result)
        if not args.json:
            print(f"  raw measurements: {path.relative_to(RESULTS_DIR.parents[1])}\n")
    return 0 if result.passed else 1


def scenario_main(
    scenario: ScenarioFn,
    parser: argparse.ArgumentParser,
    *,
    name: str,
    service_name: str,
    argv: Sequence[str] | None = None,
) -> int:
    """The per-scenario CLI, so each experiment is runnable on its own.

    Its own OTel service name, like every other driver in ``tools/``: a
    scenario is a client of the pipeline, and metrics it provokes must not be
    attributed to a service that was never running.
    """
    args = parser.parse_args(list(argv) if argv is not None else None)
    setup_telemetry(service_name, instrument_libraries=False)
    settings = get_settings()
    guard_local(settings)
    return report(execute(scenario, settings, args, name=name), args)


def guard_local(settings: Settings) -> None:
    """Refuse to break anything that is not a local stack.

    These scenarios kill processes, publish malformed payloads and delete rows.
    Every one of them is safe against docker-compose and none of them is safe
    against a deployment, so the check is here rather than in each scenario's
    documentation.
    """
    if not settings.is_local:
        print(
            f"error: refusing to run chaos experiments with TP_ENV={settings.env!r}; "
            "these kill processes and delete rows, and are for the local stack only",
            file=sys.stderr,
        )
        raise SystemExit(2)
