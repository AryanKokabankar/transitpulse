"""Checks that BENCHMARKS.md is backed by evidence that actually passed.

The document's whole claim is that every figure in it came from a committed
run anyone can repeat. That claim decays silently in two ways, and neither
shows up anywhere else: a result file goes stale or missing while the prose
citing it stays, or a run is committed whose own checks failed and the prose
quotes its numbers anyway.

So this asserts the cheap half of the contract -- that every experiment has a
result file, that each one reported success, and that the document and the
README point at them. It needs no Docker and runs on every push, in the same
spirit as the dashboard contract test: the expensive proof is running the
experiments, and this is what stops the cheap part rotting between runs.

It deliberately does **not** re-check the numbers in the prose against the
JSON. A test that parsed figures out of Markdown would be guessing at which
numbers were claims and which were prose, and would fail on a rewording while
passing on a wrong figure.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest

from tools.chaos.runner import SCENARIOS

REPO_ROOT = Path(__file__).resolve().parents[2]
RESULTS = REPO_ROOT / "docs" / "benchmarks"
BENCHMARKS = REPO_ROOT / "BENCHMARKS.md"
README = REPO_ROOT / "README.md"

SCENARIO_NAMES = [module.NAME for module in SCENARIOS]


def load(name: str) -> dict[str, Any]:
    payload: dict[str, Any] = json.loads((RESULTS / f"{name}.json").read_text(encoding="utf-8"))
    return payload


class TestEvidenceExists:
    @pytest.mark.parametrize("name", SCENARIO_NAMES)
    def test_every_experiment_has_a_committed_result(self, name: str) -> None:
        path = RESULTS / f"{name}.json"
        assert path.exists(), (
            f"{path.relative_to(REPO_ROOT)} is missing; run "
            f"`python tasks.py chaos -- {name}` and commit what it writes"
        )

    @pytest.mark.parametrize("name", SCENARIO_NAMES)
    def test_every_committed_result_passed_its_own_checks(self, name: str) -> None:
        """A result file with failing checks must not be committed as evidence.

        Quoting a number out of a run that failed is the specific dishonesty
        this document exists not to commit.
        """
        result = load(name)
        failed = [c["name"] for c in result["checks"] if not c["passed"]]
        assert result["passed"], f"{name} has failing checks: {failed}"

    @pytest.mark.parametrize("name", SCENARIO_NAMES)
    def test_every_result_actually_asserted_something(self, name: str) -> None:
        """`passed` is vacuously true for a run that checked nothing."""
        result = load(name)
        assert result["checks"], f"{name} recorded no checks at all"
        assert result["measurements"], f"{name} recorded no measurements"

    @pytest.mark.parametrize("name", SCENARIO_NAMES)
    def test_every_result_says_when_it_ran(self, name: str) -> None:
        """A benchmark with no timestamp cannot be told from a stale one."""
        assert load(name)["started_at"]

    def test_the_durability_comparison_is_committed_if_it_was_run(self) -> None:
        """Optional -- it needs a second Postgres -- but not optional to pass."""
        path = RESULTS / "durability.json"
        if not path.exists():
            pytest.skip("the durability comparison has not been run")
        result = json.loads(path.read_text(encoding="utf-8"))
        assert result["passed"], "the committed durability comparison failed its checks"


class TestDocumentation:
    def test_benchmarks_exists_and_is_not_a_stub(self) -> None:
        assert BENCHMARKS.exists()
        assert len(BENCHMARKS.read_text(encoding="utf-8")) > 2000

    def test_the_readme_points_at_it(self) -> None:
        assert "BENCHMARKS.md" in README.read_text(encoding="utf-8")

    @pytest.mark.parametrize("name", SCENARIO_NAMES)
    def test_every_experiment_is_mentioned_in_the_document(self, name: str) -> None:
        """An experiment nobody wrote up is an experiment nobody will rerun."""
        text = BENCHMARKS.read_text(encoding="utf-8")
        module = name.replace("-", "_")
        assert module in text or name in text, (
            f"{name} has a result file but is not mentioned in BENCHMARKS.md"
        )

    def test_the_fsync_caveat_is_stated(self) -> None:
        """Non-negotiable: every write-path figure here is inflated by it, and
        a reader who misses that has been misled by omission."""
        text = BENCHMARKS.read_text(encoding="utf-8")
        assert "fsync=off" in text
        assert "synchronous_commit" in text
