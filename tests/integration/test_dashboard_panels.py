"""Every panel in the committed dashboard must resolve against real metrics.

This exists because the dashboard was written before a single metric had ever
been emitted. Its PromQL was composed against *assumed* series names, and the
assumptions were not uniformly right: the SLO tile -- the one number the
project is judged on -- selected the 2s histogram bucket as ``le="2"`` and was
empty from the day it was committed, because Prometheus stores that boundary as
``le="2.0"``. Nothing complained. A "No data" panel is worse than no panel,
because the dashboard still looks finished.

Three choices make this test worth having rather than merely present:

**The expressions are read out of the committed JSON, never duplicated here.**
A test that restated the queries would pass while the dashboard was broken,
which is the exact failure being guarded against. Adding a panel whose query
matches nothing therefore fails CI instead of shipping quietly.

**Load is driven through the real pipeline, by subprocess.** The suite runs with
``TP_OTEL_ENABLED=false`` -- every test would otherwise pay OTLP backoff -- so
the drivers run as their own processes, exactly as they do by hand, and emit
through the real OTel pipeline into the real Collector and the real Prometheus.
Nothing here calls ``.record()`` on an instrument: a test that hand-fed the
metrics it then asserted on would be testing its own fixture.

**Non-empty is the assertion, not a value.** Which numbers are plausible
depends on the machine and the moment, and asserting on them would either be
vacuous or flaky. What must hold is that each query selects something -- that
the names, labels, units and recording rules all line up.

Run with: python tasks.py up && python tasks.py test-integration
"""

from __future__ import annotations

import json
import os
import re
import subprocess
import sys
import time
from collections.abc import Iterator
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import httpx
import pytest

pytestmark = pytest.mark.integration

REPO_ROOT = Path(__file__).resolve().parents[2]
DASHBOARD = REPO_ROOT / "observability" / "grafana" / "dashboards" / "transitpulse-pipeline.json"
RULES_DIR = REPO_ROOT / "observability" / "prometheus" / "rules"
PROMETHEUS = os.environ.get("TP_TEST_PROMETHEUS_URL", "http://localhost:9090")

# Grafana expands a dashboard variable before sending the query; Prometheus has
# never heard of one. Replaced with a matcher that selects everything, because
# what is under test is whether the series exist at all, not whether the
# template picker works.
_VARIABLE = re.compile(r"\$\{?(\w+)\}?")
_INTERVALS = {"$__rate_interval": "1m", "$__interval": "1m"}

# How long to wait for a driven signal to traverse the chain. The export
# interval, the Collector's batcher, the scrape interval and the rule
# evaluation interval are all in series, so the worst case is their sum and
# none of them is instant.
_SETTLE_TIMEOUT_SECONDS = 150.0
_SETTLE_POLL_SECONDS = 5.0


# ----------------------------------------------------------------------
# dashboard and rule parsing
# ----------------------------------------------------------------------
def _resolve(expr: str) -> str:
    for name, value in _INTERVALS.items():
        expr = expr.replace(name, value)
    return _VARIABLE.sub(".+", expr)


def iter_targets() -> Iterator[tuple[str, str]]:
    """Yield ``(panel id/refId, expression)`` for every query the dashboard runs."""
    dashboard = json.loads(DASHBOARD.read_text(encoding="utf-8"))
    for panel in dashboard["panels"]:
        if panel.get("type") == "row":
            continue
        for target in panel.get("targets") or []:
            expr = target.get("expr")
            if not expr:
                continue
            label = f"panel {panel['id']} {target.get('refId', '?')} - {panel['title']}"
            yield label, _resolve(expr)


TARGETS = list(iter_targets())


# ----------------------------------------------------------------------
# prometheus
# ----------------------------------------------------------------------
def _get(path: str, **params: str) -> dict[str, Any]:
    response = httpx.get(f"{PROMETHEUS}{path}", params=params or None, timeout=30.0)
    response.raise_for_status()
    payload: dict[str, Any] = (
        response.json() if response.text.startswith("{") else {"raw": response.text}
    )
    return payload


def query(expr: str) -> dict[str, Any]:
    return _get("/api/v1/query", query=expr)


def series_count(expr: str) -> int:
    result = query(expr)
    if result.get("status") != "success":
        return -1
    return len(result["data"]["result"])


# ----------------------------------------------------------------------
# fixtures
# ----------------------------------------------------------------------
@pytest.fixture(scope="module", autouse=True)
def _require_stack() -> None:
    if not os.environ.get("PUBSUB_EMULATOR_HOST"):
        pytest.skip("PUBSUB_EMULATOR_HOST is unset; run `python tasks.py up` first")
    try:
        _get("/-/ready")
    except (httpx.HTTPError, OSError) as exc:
        pytest.skip(f"Prometheus is not reachable at {PROMETHEUS}: {exc}")


@pytest.fixture(scope="module", autouse=True)
def _leave_the_subscription_empty() -> Iterator[None]:
    """Drain anything this module published, before and after it runs.

    The load driver normally consumes everything it publishes, but a run that
    fails partway through does not, and every integration module shares one
    emulator with no `seek`. Leftovers here surface as a message-count
    assertion failing in a completely unrelated module, which is a long way
    from the cause.
    """
    from google.cloud import pubsub_v1

    from transitpulse_core.config import get_settings

    def drain() -> None:
        settings = get_settings()
        subscriber = pubsub_v1.SubscriberClient()
        path = subscriber.subscription_path(settings.gcp_project, settings.subscription_processor)
        try:
            for _ in range(60):
                pulled = subscriber.pull(
                    request={"subscription": path, "max_messages": 1000}, timeout=2.0
                )
                if not pulled.received_messages:
                    break
                subscriber.acknowledge(
                    request={
                        "subscription": path,
                        "ack_ids": [m.ack_id for m in pulled.received_messages],
                    }
                )
        except Exception as exc:  # noqa: BLE001 - cleanup must not fail a suite
            # Said out loud rather than swallowed: a drain that could not
            # finish is exactly the condition that makes an unrelated module
            # fail later, and a silent pass here would hide the one clue.
            print(f"warning: could not fully drain the subscription: {exc}")
        finally:
            subscriber.close()

    drain()
    yield
    drain()


@pytest.fixture(scope="module")
def _seeded_corpus() -> None:
    """Make sure there is something to replay, synthesising one snapshot if not.

    The replay corpus is captured from a live agency feed and is deliberately
    not in git, so on a fresh clone -- and in CI -- there is nothing to drive
    load from. A synthetic snapshot is written instead, with current
    timestamps: a fixture's fixed epoch would decode into pings old enough to
    be quarantined as stale, leaving the corpus technically present and
    practically empty.
    """
    from transitpulse_core.config import get_settings
    from transitpulse_core.snapshots import build_snapshot_archive

    settings = get_settings()
    archive = build_snapshot_archive(settings)
    if archive.list_payloads(agency=settings.feed_agency, limit=1):
        return

    from tests.gtfs_fixtures import VehicleEntitySpec, build_vehicle_feed

    now = int(time.time())
    payload = build_vehicle_feed(
        *(
            VehicleEntitySpec(
                entity_id=f"synthetic-{i}",
                vehicle_id=f"synthetic_{i}",
                latitude=42.3601 + i * 0.001,
                longitude=-71.0589 - i * 0.001,
                timestamp=now - i,
            )
            for i in range(80)
        ),
        header_timestamp=now,
    )
    archive.put_payload(payload, agency=settings.feed_agency, fetched_at=datetime.now(UTC))


@pytest.fixture(scope="module")
def driven_load(_seeded_corpus: None) -> None:
    """Provoke every signal the dashboard reads, then wait for it to arrive.

    ``tools.smoke`` is the driver rather than a fixture written here, because
    it is also what a person runs by hand before opening Grafana. One
    implementation means the thing CI proves and the thing a human checks
    cannot drift apart.
    """
    completed = subprocess.run(
        [sys.executable, "-m", "tools.smoke", "--json"],
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
        timeout=600,
        env={**os.environ, "TP_OTEL_ENABLED": "true"},
    )
    if completed.returncode != 0:
        pytest.skip(
            "could not drive load through the pipeline "
            f"(tools.smoke exited {completed.returncode}): {completed.stderr[-2000:]}"
        )

    # The SLI recording rule is the last link in the chain -- it needs the
    # histogram scraped before it can record a quantile -- so waiting for it
    # covers everything upstream of it too.
    deadline = time.monotonic() + _SETTLE_TIMEOUT_SECONDS
    sentinel = "transitpulse:ingest_lag_seconds:p95"
    while time.monotonic() < deadline:
        if series_count(sentinel) > 0:
            return
        time.sleep(_SETTLE_POLL_SECONDS)
    pytest.fail(
        f"{sentinel} never appeared within {_SETTLE_TIMEOUT_SECONDS:.0f}s of a load run. "
        "Either the recording rules are not loaded (check /api/v1/rules) or metrics are "
        "not reaching Prometheus (check the otel-collector's 8889 endpoint)."
    )


# ----------------------------------------------------------------------
# tests
# ----------------------------------------------------------------------
def test_the_dashboard_has_targets_to_check() -> None:
    """Guard the guard: a parsing change that yielded nothing would pass silently."""
    assert len(TARGETS) >= 15, f"only found {len(TARGETS)} dashboard targets"


@pytest.mark.parametrize(("label", "expr"), TARGETS, ids=[label for label, _ in TARGETS])
def test_every_panel_target_returns_data(label: str, expr: str, driven_load: None) -> None:
    result = query(expr)
    assert result.get("status") == "success", (
        f"{label}: Prometheus rejected the expression: {result.get('error')}\n  {expr}"
    )
    assert result["data"]["result"], (
        f"{label}: the expression parses but selects nothing, so the panel renders "
        f"'No data'.\n  {expr}\n"
        "Check the series name against the Collector's OpenMetrics output -- the "
        "Prometheus text format spells histogram bucket boundaries differently "
        '(le="2" vs le="2.0") and only the OpenMetrics spelling is what gets stored.'
    )


def test_dashboard_annotation_queries_resolve(driven_load: None) -> None:
    """Annotations are queries too, and just as able to reference nothing.

    The dashboard shipped with one against ``transitpulse_build_info``, a
    metric no part of this codebase has ever emitted.

    What is asserted is that the expression is accepted and that the series it
    names exist -- not that the whole expression currently matches something.
    An annotation marks an instant by construction ("a process started in the
    last two minutes"), so its own result is empty almost all of the time, and
    a test that demanded otherwise would pass or fail on how long the load run
    happened to take.
    """
    dashboard = json.loads(DASHBOARD.read_text(encoding="utf-8"))
    annotations = [a for a in dashboard["annotations"]["list"] if a.get("expr")]
    assert annotations, "expected at least one annotation query"
    for annotation in annotations:
        expr = _resolve(annotation["expr"])
        result = query(expr)
        assert result.get("status") == "success", (
            f"{annotation['name']}: Prometheus rejected the expression: "
            f"{result.get('error')}\n  {expr}"
        )
        referenced = set(re.findall(r"\btransitpulse_[a-z0-9_]+\b", expr))
        assert referenced, f"{annotation['name']} names no transitpulse series: {expr}"
        for name in referenced:
            assert series_count(name) > 0, (
                f"{annotation['name']} is built on {name!r}, which no series exists for, "
                f"so it can never annotate anything.\n  {expr}"
            )


def test_every_recording_rule_is_healthy_and_records_something(driven_load: None) -> None:
    """A recording rule that errors keeps its last value and looks fine.

    Prometheus reports rule health separately from query results, so a rule
    that has started failing is invisible on the dashboard until the stale
    series expires.
    """
    groups = _get("/api/v1/rules", type="record")["data"]["groups"]
    recorded = [rule for group in groups for rule in group["rules"]]
    assert recorded, "no recording rules are loaded; check rule_files in prometheus.yml"
    for rule in recorded:
        assert rule["health"] == "ok", f"{rule['name']}: {rule.get('lastError')}"
        assert series_count(rule["name"]) > 0, f"{rule['name']} recorded no series"


def test_every_alert_rule_is_loaded_and_evaluating() -> None:
    """The alert files are mounted, parse, and every expression evaluates.

    Checked against the running Prometheus rather than with promtool, because
    promtool validates syntax while this validates that the rules Prometheus
    actually holds are the ones in the repository -- a rules directory that
    failed to mount is syntactically perfect and completely absent.
    """
    groups = _get("/api/v1/rules", type="alert")["data"]["groups"]
    loaded = {rule["name"] for group in groups for rule in group["rules"]}
    assert loaded, "no alert rules are loaded; check the rules bind mount in docker-compose"

    on_disk = set()
    for path in sorted(RULES_DIR.glob("*.yml")):
        on_disk.update(
            re.findall(r"^\s*-\s*alert:\s*(\S+)", path.read_text(encoding="utf-8"), re.M)
        )
    assert on_disk, f"no alert rules found on disk under {RULES_DIR}"
    assert on_disk == loaded, (
        "the alert rules Prometheus holds differ from the ones in the repository; "
        f"missing from Prometheus: {sorted(on_disk - loaded)}, "
        f"unknown on disk: {sorted(loaded - on_disk)}"
    )

    for group in groups:
        assert not group.get("lastError"), f"{group['name']}: {group['lastError']}"
        for rule in group["rules"]:
            assert rule["health"] == "ok", f"{rule['name']}: {rule.get('lastError')}"
