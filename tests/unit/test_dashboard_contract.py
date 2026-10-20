"""Static checks on the committed dashboard: no Docker, so CI runs them always.

``tests/integration/test_dashboard_panels.py`` is the real proof -- it drives
load and asserts that every panel selects something -- but it needs a
Collector, a Prometheus and a broker, so it is the slow half. These are the
checks that can be made from the files alone, and they catch the specific ways
this dashboard was already wrong:

* a panel querying ``transitpulse_dlq_depth``, an instrument that existed in
  the registry but that no code path ever set, so the tile could never have had
  a value;
* an annotation querying ``transitpulse_build_info``, which was never even in
  the registry;
* every panel pointing at datasource uid ``Prometheus`` while the provisioned
  datasource had an auto-generated uid, so each one rendered a datasource error
  rather than data.

The naming rule applied below -- dots to underscores, ``_total`` on counters,
unit as a suffix -- is written here rather than in the library on purpose.
Production code does not need to know what the Prometheus exporter will call
things; this is a statement of what this project *believes* the exporter does,
and the integration test is what confirms the belief against a live one.
"""

from __future__ import annotations

import json
import re
from pathlib import Path

import pytest
import yaml
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics.export import Histogram as HistogramData
from opentelemetry.sdk.metrics.export import InMemoryMetricReader
from opentelemetry.sdk.metrics.export import Sum as SumData

from transitpulse_core.telemetry import Metrics, build_metrics

REPO_ROOT = Path(__file__).resolve().parents[2]
OBSERVABILITY = REPO_ROOT / "observability"
DASHBOARD_DIR = OBSERVABILITY / "grafana" / "dashboards"
DATASOURCES = OBSERVABILITY / "grafana" / "datasources" / "datasources.yaml"
RULES_DIR = OBSERVABILITY / "prometheus" / "rules"

# OTel unit annotations, and what the exporter turns each into. Measured
# against a running Collector, not guessed: see the module docstring.
_UNIT_SUFFIXES = {"s": "seconds", "d": "days"}

_SERIES_REFERENCE = re.compile(r"\btransitpulse_[a-z0-9_]+\b")
_RECORDED_REFERENCE = re.compile(r"\btransitpulse:[a-z0-9_:]+\b")


def _dashboards() -> list[Path]:
    return sorted(DASHBOARD_DIR.glob("*.json"))


def _expected_series_names() -> set[str]:
    """Every series name the registry can produce, derived from the registry."""
    reader = InMemoryMetricReader()
    provider = MeterProvider(metric_readers=[reader])
    registry = build_metrics(provider.get_meter("transitpulse"))

    # Synchronous instruments only report after a recording, so each one is
    # exercised. Observable instruments report through their callback and need
    # no help. Reaching for the field names rather than listing them means a
    # new instrument is covered the moment it is added.
    for field in Metrics.__dataclass_fields__:
        instrument = getattr(registry, field)
        if hasattr(instrument, "record"):
            instrument.record(1.0)
        elif hasattr(instrument, "add"):
            instrument.add(1)
        elif hasattr(instrument, "set"):
            instrument.set(1)

    names: set[str] = set()
    data = reader.get_metrics_data()
    assert data is not None, "the in-memory reader collected nothing"
    for resource_metric in data.resource_metrics:
        for scope_metric in resource_metric.scope_metrics:
            for metric in scope_metric.metrics:
                base = metric.name.replace(".", "_")
                if metric.unit in _UNIT_SUFFIXES:
                    base = f"{base}_{_UNIT_SUFFIXES[metric.unit]}"
                # Which suffixes exist depends on the instrument's type, so
                # they are derived from it rather than applied to everything --
                # otherwise a panel querying a gauge as though it were a
                # counter would pass this test and render nothing.
                if isinstance(metric.data, HistogramData):
                    names.update(f"{base}{s}" for s in ("_bucket", "_count", "_sum"))
                elif isinstance(metric.data, SumData) and metric.data.is_monotonic:
                    names.add(f"{base}_total")
                else:
                    names.add(base)
    return names


def _recorded_rule_names() -> set[str]:
    names: set[str] = set()
    for path in sorted(RULES_DIR.glob("*.yml")):
        document = yaml.safe_load(path.read_text(encoding="utf-8"))
        for group in document.get("groups", []):
            for rule in group.get("rules", []):
                if "record" in rule:
                    names.add(rule["record"])
    return names


def _queries(dashboard: dict[str, object]) -> list[tuple[str, str]]:
    found: list[tuple[str, str]] = []
    panels = dashboard.get("panels", [])
    assert isinstance(panels, list)
    for panel in panels:
        if panel.get("type") == "row":
            continue
        for target in panel.get("targets") or []:
            if target.get("expr"):
                found.append((f"panel {panel['id']} {target.get('refId', '?')}", target["expr"]))
    annotations = dashboard.get("annotations", {})
    assert isinstance(annotations, dict)
    for annotation in annotations.get("list", []):
        if annotation.get("expr"):
            found.append((f"annotation {annotation['name']}", annotation["expr"]))
    return found


@pytest.fixture(scope="module")
def expected_series() -> set[str]:
    return _expected_series_names()


def test_the_registry_produces_the_names_the_project_documents(
    expected_series: set[str],
) -> None:
    """Pin the naming rule itself, so a unit change is visible as a test diff.

    ``transitpulse.ingest.lag`` carries ``unit="s"``, so it is queried as
    ``transitpulse_ingest_lag_seconds_bucket``. Dropping the unit -- an
    innocuous-looking edit -- renames every series the SLO depends on.
    """
    assert "transitpulse_ingest_lag_seconds_bucket" in expected_series
    assert "transitpulse_partitions_headroom_days" in expected_series
    assert "transitpulse_job_last_success_timestamp_seconds" in expected_series
    assert "transitpulse_events_processed_total" in expected_series
    # The instrument the dashboard used to query. Nothing emits it any more and
    # nothing should: a broker-side queue depth is not observable from inside
    # this codebase (ADR-007).
    assert "transitpulse_dlq_depth" not in expected_series


@pytest.mark.parametrize("path", _dashboards(), ids=lambda p: p.name)
def test_dashboard_is_valid_json_with_queries(path: Path) -> None:
    dashboard = json.loads(path.read_text(encoding="utf-8"))
    queries = _queries(dashboard)
    assert queries, f"{path.name} has no queries at all"
    for label, expr in queries:
        assert expr.strip(), f"{path.name} {label} has an empty expression"


@pytest.mark.parametrize("path", _dashboards(), ids=lambda p: p.name)
def test_every_metric_a_panel_queries_is_one_the_registry_emits(
    path: Path, expected_series: set[str]
) -> None:
    """The check that would have caught the dead DLQ tile before it shipped."""
    dashboard = json.loads(path.read_text(encoding="utf-8"))
    recorded = _recorded_rule_names()
    for label, expr in _queries(dashboard):
        for name in _SERIES_REFERENCE.findall(expr):
            assert name in expected_series, (
                f"{path.name} {label} queries {name!r}, which no instrument in the "
                "registry produces. Add it to libs/transitpulse_core/telemetry.py and "
                "emit it, or correct the query -- a panel for a metric nothing writes "
                "renders 'No data' forever and looks fine."
            )
        for name in _RECORDED_REFERENCE.findall(expr):
            assert name in recorded, (
                f"{path.name} {label} queries the recorded series {name!r}, which no "
                f"rule in {RULES_DIR.name}/ defines."
            )


@pytest.mark.parametrize("path", _dashboards(), ids=lambda p: p.name)
def test_every_panel_points_at_a_provisioned_datasource(path: Path) -> None:
    provisioned = {
        entry["uid"]
        for entry in yaml.safe_load(DATASOURCES.read_text(encoding="utf-8"))["datasources"]
        if "uid" in entry
    }
    assert provisioned, "no datasource in datasources.yaml declares an explicit uid"

    dashboard = json.loads(path.read_text(encoding="utf-8"))
    referenced: set[str] = set()
    for panel in dashboard["panels"]:
        source = panel.get("datasource")
        if isinstance(source, dict) and "uid" in source:
            referenced.add(source["uid"])
    for annotation in dashboard.get("annotations", {}).get("list", []):
        source = annotation.get("datasource")
        if isinstance(source, dict) and "uid" in source:
            referenced.add(source["uid"])

    unknown = referenced - provisioned
    assert not unknown, (
        f"{path.name} references datasource uid(s) {sorted(unknown)} that no provisioned "
        "datasource declares. Grafana renders a datasource error, not data."
    )


@pytest.mark.parametrize("path", _dashboards(), ids=lambda p: p.name)
def test_no_panel_uses_a_second_y_axis(path: Path) -> None:
    """A project convention, asserted rather than left to review.

    Two axes let any two series be made to look correlated by choosing the
    scales, so the rule is that they are never used -- which is only a rule if
    something checks.
    """
    dashboard = json.loads(path.read_text(encoding="utf-8"))
    for panel in dashboard["panels"]:
        for override in panel.get("fieldConfig", {}).get("overrides", []):
            for prop in override.get("properties", []):
                assert not (
                    prop.get("id") == "custom.axisPlacement" and prop.get("value") == "right"
                ), f"{path.name} panel {panel.get('id')} places a series on a right-hand axis"


def _alert_rules() -> list[dict[str, object]]:
    rules: list[dict[str, object]] = []
    for path in sorted(RULES_DIR.glob("*.yml")):
        document = yaml.safe_load(path.read_text(encoding="utf-8"))
        for group in document.get("groups", []):
            for rule in group.get("rules", []):
                if "alert" in rule:
                    rules.append(rule)
    return rules


# Gauges that describe shared state -- the database, not the reporter. A run of
# a batch job exports one of these and exits; the Collector then re-publishes
# that last value until metric_expiration, so without a stable identity the
# readings of several finished runs sit in Prometheus at once, every one of them
# stamped with the current scrape time. Aggregating across them is wrong in
# whichever direction it is done, and no timestamp arithmetic can separate
# them, so an alert on one of these must select a single reporter instead.
_SHARED_STATE_GAUGES = {
    "transitpulse_partitions_headroom_days": "transitpulse-partition-maintenance",
}


@pytest.mark.parametrize(("series", "owner"), sorted(_SHARED_STATE_GAUGES.items()))
def test_an_alert_on_shared_state_selects_one_authoritative_reporter(
    series: str, owner: str
) -> None:
    """Regression test for a page that fired while nothing was wrong.

    ``TransitPulsePartitionHeadroomLow`` read
    ``min by (table) (transitpulse_partitions_headroom_days)`` across every
    reporter. The intent was caution -- believe the shortest runway anyone
    reports -- but the effect was that a reading of 1, taken before the window
    was extended, outvoted the current 14 for as long as the Collector kept
    re-publishing it. Scoping the query to the job that owns the window is what
    makes the aggregation meaningful.
    """
    offenders = [
        rule["alert"]
        for rule in _alert_rules()
        if series in str(rule.get("expr", ""))
        and f'service_name="{owner}"' not in str(rule.get("expr", ""))
    ]
    assert offenders == [], (
        f"these alerts query {series} without restricting it to {owner!r}, "
        f"so a stale reading from a finished run can trigger them: {offenders}"
    )


def test_every_alert_carries_a_severity_and_a_runbook() -> None:
    """An alert nobody knows how to action is noise with extra steps."""
    incomplete = [
        rule["alert"]
        for rule in _alert_rules()
        if not rule.get("labels", {}).get("severity")
        or not rule.get("annotations", {}).get("runbook")
    ]
    assert incomplete == [], f"alerts missing severity or runbook: {incomplete}"
