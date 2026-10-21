"""The BigQuery table Terraform creates must match the rows the sink writes.

Two files describe the same table: ``_SCHEMA`` in the sink, which builds the
protobuf descriptor the Storage Write API appends with, and
``infra/terraform/schemas/vehicle_events.json``, which is what the table is
actually created from. Nothing keeps them in step, and the failure mode if
they drift is specific and bad: ``AppendRows`` rejects the batch for a field
the table does not have, at runtime, in the one service that cannot drop the
row it is holding.

So this asserts the columns and their types agree. It is the same argument as
the dashboard contract test -- the expensive proof is running the thing, and
this is what stops the cheap part rotting in between.

Regenerate the JSON rather than editing it by hand when a column is added.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest
from google.protobuf import descriptor_pb2

from transitpulse_core.sinks.bigquery import _SCHEMA

_T = descriptor_pb2.FieldDescriptorProto

SCHEMA_FILE = (
    Path(__file__).resolve().parents[2] / "infra" / "terraform" / "schemas" / "vehicle_events.json"
)

# How a protobuf field type has to be declared in BigQuery for the Storage
# Write API to accept it. A TIMESTAMP column takes int64 microseconds, which
# is why timestamps are TYPE_INT64 on the proto side and TIMESTAMP here.
EXPECTED_BQ_TYPE = {
    _T.TYPE_STRING: "STRING",
    _T.TYPE_INT64: "INTEGER",
    _T.TYPE_DOUBLE: "FLOAT",
    _T.TYPE_BOOL: "BOOLEAN",
}


@pytest.fixture(scope="module")
def table_schema() -> list[dict[str, str]]:
    if not SCHEMA_FILE.exists():
        pytest.fail(f"{SCHEMA_FILE} is missing; the BigQuery table has no schema to create from")
    fields: list[dict[str, str]] = json.loads(SCHEMA_FILE.read_text(encoding="utf-8"))
    return fields


def test_every_column_the_sink_writes_exists_in_the_table(
    table_schema: list[dict[str, str]],
) -> None:
    declared = {field["name"] for field in table_schema}
    written = {column.name for column in _SCHEMA}
    missing = written - declared
    assert not missing, f"the sink writes columns the table does not have: {sorted(missing)}"


def test_the_table_has_no_columns_the_sink_never_writes(
    table_schema: list[dict[str, str]],
) -> None:
    """A column nothing fills is not harmless: it is either dead weight or a
    rename that lost its data."""
    declared = {field["name"] for field in table_schema}
    written = {column.name for column in _SCHEMA}
    extra = declared - written
    assert not extra, f"the table declares columns nothing writes: {sorted(extra)}"


@pytest.mark.parametrize("column", _SCHEMA, ids=[c.name for c in _SCHEMA])
def test_each_column_is_declared_with_a_compatible_type(
    column: object, table_schema: list[dict[str, str]]
) -> None:
    by_name = {field["name"]: field for field in table_schema}
    name = column.name  # type: ignore[attr-defined]
    expected = (
        "TIMESTAMP"
        if column.is_timestamp  # type: ignore[attr-defined]
        else EXPECTED_BQ_TYPE[column.proto_type]  # type: ignore[attr-defined]
    )
    assert by_name[name]["type"] == expected, (
        f"{name} is {by_name[name]['type']} in the table but the sink appends it as {expected}"
    )


def test_the_partitioning_and_clustering_columns_are_present(
    table_schema: list[dict[str, str]],
) -> None:
    """Terraform partitions on event_time and clusters on route_id and agency.
    A table cannot be created partitioned on a column it does not have."""
    declared = {field["name"] for field in table_schema}
    for required in ("event_time", "route_id", "agency"):
        assert required in declared


def test_the_identity_column_is_required(table_schema: list[dict[str, str]]) -> None:
    """Analytical queries deduplicate on it (ADR-005), so a null would make a
    row impossible to collapse against its duplicates."""
    by_name = {field["name"]: field for field in table_schema}
    assert by_name["idempotency_key"]["mode"] == "REQUIRED"
