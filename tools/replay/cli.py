"""Command line for the load harness.

    replay --rate 500 --duration 10s
    replay --rate 2000 --duration 2m --fanout 20
    replay --rate 500 --duration 10s --dry-run     # pacing only, no broker

``--dry-run`` exists to answer the first question anyone should ask of a load
harness: is the limiter itself accurate? It runs the identical loop with the
publish removed, so a shortfall in a real run can be attributed to the pipeline
rather than to the tool measuring it.
"""

from __future__ import annotations

import argparse
import json
import re
import sys
from datetime import timedelta
from pathlib import Path

import structlog

from tools.replay.corpus import EmptyCorpusError, load_corpus
from tools.replay.harness import ReplayConfig, ReplayHarness
from transitpulse_core.config import get_settings
from transitpulse_core.pubsub import EventPublisher
from transitpulse_core.snapshots import build_snapshot_archive
from transitpulse_core.telemetry import setup_telemetry

log = structlog.get_logger(__name__)

_DURATION_PATTERN = re.compile(r"^(?P<value>\d+(?:\.\d+)?)(?P<unit>ms|s|m|h)?$")
_UNIT_SECONDS = {"ms": 0.001, "s": 1.0, "m": 60.0, "h": 3600.0}


def parse_duration(raw: str) -> timedelta:
    """Accept ``10s``, ``500ms``, ``2m``, ``1h`` or a bare number of seconds."""
    match = _DURATION_PATTERN.match(raw.strip().lower())
    if not match:
        raise argparse.ArgumentTypeError(
            f"invalid duration {raw!r}; expected something like 10s, 500ms, 2m or 1h"
        )
    seconds = float(match.group("value")) * _UNIT_SECONDS[match.group("unit") or "s"]
    if seconds <= 0:
        raise argparse.ArgumentTypeError("duration must be positive")
    return timedelta(seconds=seconds)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="replay",
        description="Replay the archived snapshot corpus into Pub/Sub at a target rate.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument(
        "--rate", type=float, required=True, metavar="N", help="target messages per second"
    )
    parser.add_argument(
        "--duration",
        type=parse_duration,
        required=True,
        metavar="Ns",
        help="how long to sustain the rate (10s, 500ms, 2m, 1h)",
    )
    parser.add_argument(
        "--fanout",
        type=int,
        default=1,
        metavar="K",
        help="clone each ping into K synthetic vehicles (default: 1, no fan-out)",
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=4,
        help="concurrent publish batches in flight (default: 4)",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=None,
        help="messages per publish batch (default: derived from the rate)",
    )
    parser.add_argument(
        "--agency",
        default=None,
        help="restrict the corpus to one agency prefix (default: the whole corpus)",
    )
    parser.add_argument(
        "--max-snapshots",
        type=int,
        default=None,
        metavar="N",
        help="load at most N snapshots from the corpus",
    )
    parser.add_argument("--seed", type=int, default=0, help="fan-out jitter seed (default: 0)")
    parser.add_argument(
        "--preserve-timestamps",
        action="store_true",
        help=(
            "keep the corpus's original event times instead of re-basing them to now; "
            "true replay semantics, and the messages will dedupe against existing rows"
        ),
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="run the loop without publishing, to verify the limiter's accuracy",
    )
    parser.add_argument("--json", action="store_true", help="emit the report as JSON on stdout")
    parser.add_argument(
        "--output",
        type=Path,
        default=None,
        metavar="PATH",
        help=(
            "write the report as JSON to a file. Prefer this over --json "
            "anywhere a machine reads the result: structured logs share "
            "stdout, so piping --json into a parser feeds it log lines first"
        ),
    )
    parser.add_argument(
        "--min-accuracy",
        type=float,
        default=0.95,
        metavar="F",
        help=(
            "achieved/target ratio below which the command exits non-zero "
            "(default: 0.95); the nightly performance gate tightens this"
        ),
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    settings = get_settings()

    # The harness is a client of the pipeline, not part of it, so it gets its
    # own service name in traces rather than impersonating the collector.
    setup_telemetry("transitpulse-replay", instrument_libraries=False)

    archive = build_snapshot_archive(settings)
    try:
        corpus = load_corpus(archive, agency=args.agency, limit=args.max_snapshots)
    except EmptyCorpusError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 2

    config = ReplayConfig(
        rate_per_second=args.rate,
        duration=args.duration,
        fanout=args.fanout,
        workers=args.workers,
        seed=args.seed,
        preserve_timestamps=args.preserve_timestamps,
        batch_size=args.batch_size,
        dry_run=args.dry_run,
    )

    publisher = None if args.dry_run else EventPublisher(settings)
    try:
        report = ReplayHarness(config, corpus, publisher=publisher).run()
    finally:
        if publisher is not None:
            publisher.close()

    if args.output is not None:
        # Written rather than printed, because the one caller that must be
        # able to trust this is a performance gate, and stdout already
        # carries the structured log stream.
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(json.dumps(report.as_dict(), indent=2) + "\n", encoding="utf-8")

    if args.json:
        print(json.dumps(report.as_dict(), indent=2))
    else:
        print(report.render())

    # A run that missed its target is a failed measurement rather than a
    # success with a caveat, so it exits non-zero -- which is what lets a
    # performance gate fail a build without parsing output. The threshold is a
    # flag because what counts as "met" belongs to the caller: a shared laptop
    # and a dedicated CI runner do not deserve the same bar.
    if report.rate_accuracy >= args.min_accuracy:
        return 0
    print(
        f"error: achieved {report.rate_accuracy:.1%} of the target rate, "
        f"below the --min-accuracy threshold of {args.min_accuracy:.1%}",
        file=sys.stderr,
    )
    return 1


if __name__ == "__main__":  # pragma: no cover - module entry point
    raise SystemExit(main())
