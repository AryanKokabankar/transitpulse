"""Tests for the replay CLI's argument handling.

Small surface, but it is the surface a benchmark is invoked through: a duration
silently parsed as 10 seconds when the operator wrote ``10m`` would produce a
number labelled with the wrong methodology, which is worse than no number.
"""

from __future__ import annotations

import argparse
from datetime import timedelta

import pytest

from tools.replay.cli import build_parser, parse_duration


class TestDurationParsing:
    @pytest.mark.parametrize(
        ("raw", "expected"),
        [
            ("10s", timedelta(seconds=10)),
            ("500ms", timedelta(milliseconds=500)),
            ("2m", timedelta(minutes=2)),
            ("1h", timedelta(hours=1)),
            ("0.5s", timedelta(milliseconds=500)),
            ("90", timedelta(seconds=90)),
        ],
    )
    def test_accepts_the_documented_forms(self, raw: str, expected: timedelta) -> None:
        assert parse_duration(raw) == expected

    def test_a_bare_number_means_seconds(self) -> None:
        assert parse_duration("30") == parse_duration("30s")

    def test_is_case_insensitive(self) -> None:
        assert parse_duration("10S") == timedelta(seconds=10)

    @pytest.mark.parametrize("raw", ["", "soon", "10 s", "-5s", "0s", "10x", "s10"])
    def test_rejects_anything_else(self, raw: str) -> None:
        # Rejected rather than coerced: a misread duration mislabels a result.
        with pytest.raises(argparse.ArgumentTypeError):
            parse_duration(raw)


class TestParser:
    def test_parses_the_documented_invocation(self) -> None:
        args = build_parser().parse_args(["--rate", "500", "--duration", "10s"])
        assert args.rate == 500.0
        assert args.duration == timedelta(seconds=10)
        assert args.fanout == 1
        assert args.preserve_timestamps is False
        assert args.dry_run is False

    def test_parses_fanout(self) -> None:
        args = build_parser().parse_args(["--rate", "2000", "--duration", "2m", "--fanout", "20"])
        assert args.fanout == 20

    @pytest.mark.parametrize("missing", [["--rate", "500"], ["--duration", "10s"]])
    def test_rate_and_duration_are_both_required(self, missing: list[str]) -> None:
        """Neither has a defensible default: a benchmark must state both."""
        with pytest.raises(SystemExit):
            build_parser().parse_args(missing)

    def test_the_pass_threshold_is_the_callers_choice(self) -> None:
        """A shared laptop and a dedicated CI runner do not deserve the same
        bar, so what counts as "target met" is a flag rather than a constant."""
        assert build_parser().parse_args(["--rate", "1", "--duration", "1s"]).min_accuracy == 0.95
        tightened = build_parser().parse_args(
            ["--rate", "1", "--duration", "1s", "--min-accuracy", "0.99"]
        )
        assert tightened.min_accuracy == 0.99

    def test_timestamps_can_be_preserved(self) -> None:
        args = build_parser().parse_args(
            ["--rate", "10", "--duration", "1s", "--preserve-timestamps"]
        )
        assert args.preserve_timestamps is True
