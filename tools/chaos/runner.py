"""The committed runner: one command that executes the experiments in order.

Separate from the per-scenario CLIs rather than replacing them. A reviewer
reproducing a claim wants the single experiment behind it, and
``python -m tools.chaos.ramp`` is that; a reviewer checking that the whole set
still holds wants ``all``, which is this.

Order matters and is fixed. The recovery experiment runs first, while the local
stack is quiet, because it is the one that kills a process and exhausts a
delivery budget -- and the ramp runs last, because it is the only one whose
numbers are sensitive to what else has been running.

Each scenario writes its own result file whatever the others do, so a run that
fails in the middle still leaves the evidence from the experiments that
finished. The exit code is the whole set's verdict.
"""

from __future__ import annotations

import argparse
import json
import sys
import time
from collections.abc import Callable, Sequence
from typing import Protocol, cast

import structlog

from tools.chaos import (
    duplicate_batch,
    feed_failure,
    kill_processor,
    malformed_feed,
    ramp,
)
from tools.chaos.experiment import ScenarioResult, execute, guard_local, write_result
from transitpulse_core.config import Settings, get_settings
from transitpulse_core.telemetry import setup_telemetry

log = structlog.get_logger(__name__)

SERVICE_NAME = "transitpulse-chaos"


class Scenario(Protocol):
    """The shape every experiment module exposes.

    Declared as callable attributes rather than methods because the things
    satisfying it are modules, not instances, and a module's functions take no
    ``self``. Writing it down at all is what lets the registry below be typed
    instead of a tuple of ``object``.
    """

    NAME: str
    QUESTION: str
    build_parser: Callable[[], argparse.ArgumentParser]
    run: Callable[[Settings, argparse.Namespace], ScenarioResult]


# In dependency order, not alphabetical: see the module docstring.
# Cast because mypy will not accept a module as a Protocol implementation in an
# annotated assignment, though the shape matches exactly. The Protocol still
# earns its place: everything downstream of this line is checked against it.
SCENARIOS: tuple[Scenario, ...] = cast(
    "tuple[Scenario, ...]",
    (
        kill_processor,
        malformed_feed,
        feed_failure,
        duplicate_batch,
        ramp,
    ),
)

_BY_NAME = {module.NAME: module for module in SCENARIOS}


def _list_scenarios() -> str:
    width = max(len(name) for name in _BY_NAME)
    lines = ["", "  chaos experiments", "  " + "-" * 68]
    for index, module in enumerate(SCENARIOS, start=1):
        lines.append(f"  {index}. {module.NAME.ljust(width)}  {module.QUESTION}")
    lines.extend(
        [
            "",
            "  python tasks.py chaos -- <name>        one experiment",
            "  python tasks.py chaos -- all           all of them, in this order",
            "",
        ]
    )
    return "\n".join(lines)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="chaos",
        description="Run the chaos experiments and record what they measured.",
        epilog=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "scenario",
        nargs="?",
        choices=[*_BY_NAME, "all"],
        help="which experiment to run (default: list them)",
    )
    parser.add_argument("--list", action="store_true", help="list the experiments and exit")
    parser.add_argument("--json", action="store_true", help="emit each result as JSON")
    parser.add_argument("--no-write", action="store_true", help="do not write the result files")
    parser.add_argument(
        "--keep-rows", action="store_true", help="leave each run's rows in Postgres"
    )
    parser.add_argument(
        "--keep-going",
        action="store_true",
        help="with `all`, run every experiment even after one fails its checks",
    )
    return parser


def _scenario_args(module: Scenario, args: argparse.Namespace) -> argparse.Namespace:
    """Each scenario's own defaults, with the runner's shared flags applied.

    Parsed from an empty argv rather than constructed by hand, so a scenario
    that gains a flag is picked up here without this function being touched.
    """
    scenario_args = module.build_parser().parse_args([])
    scenario_args.json = args.json
    scenario_args.no_write = args.no_write
    scenario_args.keep_rows = args.keep_rows
    return scenario_args


def run_all(names: Sequence[str], args: argparse.Namespace) -> list[ScenarioResult]:
    settings = get_settings()
    results: list[ScenarioResult] = []

    for name in names:
        module = _BY_NAME[name]
        result = execute(module.run, settings, _scenario_args(module, args), name=name)
        results.append(result)
        if not args.no_write:
            write_result(result)
        print(result.render())
        if not result.passed and not args.keep_going:
            print(
                f"error: {name} failed its checks; stopping. "
                "Pass --keep-going to run the rest anyway.",
                file=sys.stderr,
            )
            break
        if name != names[-1]:
            # The local stack's metric pipeline is cumulative and the scenarios
            # read counters off it. A gap between runs keeps one experiment's
            # increments from landing inside the next one's before/after
            # sample, which would attribute them to the wrong fault.
            time.sleep(args.settle_seconds)

    return results


def render_summary(results: Sequence[ScenarioResult]) -> str:
    width = max((len(r.scenario) for r in results), default=0)
    lines = ["", "  chaos summary", "  " + "=" * 68]
    for result in results:
        mark = "PASS" if result.passed else "FAIL"
        failed = sum(1 for c in result.checks if not c.passed)
        tail = "" if result.passed else f"  ({failed} of {len(result.checks)} checks failed)"
        lines.append(
            f"  [{mark}] {result.scenario.ljust(width)}  "
            f"{len(result.checks)} checks in {result.seconds:,.1f}s{tail}"
        )
    passed = sum(1 for r in results if r.passed)
    lines.extend(["", f"  {passed} of {len(results)} experiments passed", ""])
    return "\n".join(lines)


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    if args.list or args.scenario is None:
        print(_list_scenarios())
        return 0

    # Shared across the scenarios the runner drives, so they do not each have
    # to declare it; the per-scenario CLIs take it from their own parser.
    args.settle_seconds = 15.0

    setup_telemetry(SERVICE_NAME, instrument_libraries=False)
    settings = get_settings()
    guard_local(settings)

    names = list(_BY_NAME) if args.scenario == "all" else [args.scenario]
    results = run_all(names, args)

    if len(results) > 1:
        print(render_summary(results))
    if args.json:
        print(json.dumps([r.as_dict() for r in results], indent=2))

    return 0 if results and all(r.passed for r in results) else 1


if __name__ == "__main__":  # pragma: no cover - module entry point
    raise SystemExit(main())
