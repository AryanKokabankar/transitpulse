"""Centralised, validated configuration.

All TransitPulse settings come from the environment with the ``TP_`` prefix.
OpenTelemetry settings deliberately keep their spec-mandated ``OTEL_`` names so
that the standard SDK auto-configuration keeps working alongside this class.
"""

from __future__ import annotations

from functools import lru_cache
from typing import Literal
from zoneinfo import ZoneInfo

from pydantic import PostgresDsn, TypeAdapter, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

Environment = Literal["local", "dev", "prod"]
SnapshotBackend = Literal["auto", "local", "gcs"]


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_prefix="TP_",
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    env: Environment = "local"
    log_level: str = "INFO"

    # --- feed ---------------------------------------------------------
    # MBTA by default because it serves GTFS-Realtime over plain HTTPS with no
    # API key, so a fresh clone can capture a replay corpus immediately. The
    # MTA NYCT feed now answers 403 without a registered key; it is still a
    # one-line switch, and the pipeline is agency-agnostic by design.
    feed_url: str = "https://cdn.mbta.com/realtime/VehiclePositions.pb"
    feed_agency: str = "mbta"
    feed_timeout_seconds: float = 10.0
    feed_max_attempts: int = 4
    feed_retry_initial_seconds: float = 0.5
    feed_retry_max_seconds: float = 8.0

    # A dead agency feed must not turn every scheduler tick into a burst of
    # retries. After this many consecutive failed ticks the collector stops
    # calling upstream entirely and fails fast until the cooldown elapses.
    feed_breaker_failure_threshold: int = 5
    feed_breaker_reset_seconds: float = 60.0

    # --- static GTFS ---------------------------------------------------
    # The timetable the realtime feed is measured against. MBTA publishes a
    # single archive with no key, matching the realtime default above.
    gtfs_static_url: str = "https://cdn.mbta.com/MBTA_GTFS.zip"
    gtfs_download_timeout_seconds: float = 120.0

    # GTFS stop times are local to the agency and relative to a service day
    # that begins at local noon minus twelve hours, so schedule delay cannot
    # be computed in UTC. A wrong value here produces delays that are off by
    # whole hours while still looking plausible, so the loader checks it
    # against the agency.txt in the downloaded feed and warns on a mismatch.
    agency_timezone: str = "America/New_York"

    # --- pub/sub ------------------------------------------------------
    gcp_project: str = "transitpulse-local"
    topic_vehicle_events: str = "vehicle-events"
    subscription_processor: str = "vehicle-events-processor"
    topic_dlq: str = "vehicle-events-dlq"
    # Shared by the subscription's dead-letter policy and the processor's
    # poison-message guard. They have to agree: if the handler gave up later
    # than the broker dead-letters, the quarantine row would never be written,
    # and if it gave up earlier, the DLQ would never see anything.
    max_delivery_attempts: int = 5
    publish_max_batch_size: int = 100
    publish_max_latency_seconds: float = 0.05

    # --- postgres -----------------------------------------------------
    # Held as a plain str because psycopg wants a string and pydantic's Url
    # object would have to be unwrapped at every call site. Validity is still
    # enforced, by running the value through PostgresDsn in the validator
    # below -- so a malformed DSN fails at startup, not at first query.
    pg_dsn: str = "postgresql://transitpulse:transitpulse@localhost:5432/transitpulse"
    pg_pool_min: int = 2
    pg_pool_max: int = 10

    # --- bigquery -----------------------------------------------------
    bq_enabled: bool = False
    bq_dataset: str = "transitpulse"
    bq_table: str = "vehicle_events"
    bq_batch_size: int = 500
    bq_batch_timeout_seconds: float = 2.0

    # --- snapshots / replay corpus ------------------------------------
    # "auto" resolves to GCS outside the local environment and to disk inside
    # it. Explicit rather than inferred from whether a bucket name happens to
    # be set, because a bucket name has a default and silently writing a local
    # run's snapshots into a real bucket is the wrong failure mode.
    snapshot_backend: SnapshotBackend = "auto"
    snapshot_bucket: str = "transitpulse-snapshots"
    snapshot_local_dir: str = "./replay-corpus"

    # --- observability ------------------------------------------------
    otel_enabled: bool = True
    otel_console_export: bool = False

    @field_validator("log_level")
    @classmethod
    def _upper(cls, v: str) -> str:
        level = v.upper()
        allowed = {"DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"}
        if level not in allowed:
            raise ValueError(f"log_level must be one of {sorted(allowed)}, got {v!r}")
        return level

    @field_validator("pg_dsn")
    @classmethod
    def _valid_postgres_dsn(cls, v: str) -> str:
        """Fail fast on a malformed DSN instead of at the first query."""
        TypeAdapter(PostgresDsn).validate_python(v)
        return v

    @field_validator("agency_timezone")
    @classmethod
    def _known_timezone(cls, v: str) -> str:
        """Fail at startup rather than on the first schedule lookup."""
        ZoneInfo(v)
        return v

    @property
    def agency_tzinfo(self) -> ZoneInfo:
        return ZoneInfo(self.agency_timezone)

    @property
    def pg_dsn_str(self) -> str:
        """Alias kept so call sites read intentionally, not incidentally."""
        return self.pg_dsn

    @property
    def is_local(self) -> bool:
        return self.env == "local"


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    """Process-wide singleton. Cached so every module sees one instance."""
    return Settings()
