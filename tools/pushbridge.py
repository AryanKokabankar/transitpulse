"""Deliver pulled Pub/Sub messages to the processor the way push delivery does.

In production nothing like this exists: the subscription is a *push*
subscription and Pub/Sub POSTs each message to the processor's Cloud Run URL
directly. Locally the broker is the emulator, inside a container, and the
processor runs on the host, so the one thing the emulator cannot do is reach
it. Without a stand-in the local stack publishes into a topic nobody consumes
-- which is why six of the eleven dashboard panels had never once had a value
to show.

So this is a local parity shim in the same family as the Pub/Sub emulator
itself. It is deliberately thin, because its value depends entirely on the
processor seeing what Pub/Sub would send:

* the body is the real push envelope shape, built by the same
  :class:`~transitpulse_core.pubsub.PushEnvelope` field names that production
  parses, including the base64 ``data`` and the ``attributes`` map carrying
  ``traceparent``;
* ``deliveryAttempt`` is the broker's own count, taken from the pull response
  rather than tracked here, so the processor's poison-message guard sees the
  same number it would in production;
* **the status code is the acknowledgement**, exactly as in push: 2xx acks,
  anything else returns the message to the subscription for redelivery. A
  bridge that acked unconditionally would make the retry path untestable and
  every delivery look successful.

By default the processor is driven in-process over ASGI, so one command is
enough to make the local pipeline end to end. ``--url`` instead posts over real
HTTP to an already-running processor, which is the closer parity and what to
reach for when debugging anything involving the server itself.

    python -m tools.pushbridge --duration 30s
    python -m tools.pushbridge --max-messages 500 --url http://localhost:8081
"""

from __future__ import annotations

import argparse
import asyncio
import base64
import json
import sys
import time
from dataclasses import asdict, dataclass
from datetime import timedelta
from typing import Any

import httpx
import structlog
from google.api_core import exceptions as gcp_exceptions
from google.cloud import pubsub_v1

from tools.replay.cli import parse_duration
from transitpulse_core.config import Settings, get_settings

log = structlog.get_logger(__name__)

# Pulled in batches because the round trip to the emulator dominates: one
# message per pull caps the bridge well below what the processor can handle,
# and the whole point is to put the processor under enough load for a
# percentile to mean something.
_PULL_BATCH = 100
# How long a pull waits for messages before reporting an empty batch. Short,
# so a run that has drained the subscription ends promptly instead of sitting
# on a long poll.
_PULL_TIMEOUT_SECONDS = 5.0


@dataclass
class BridgeReport:
    """What the run delivered. Counted from responses, never from intent."""

    delivered: int = 0
    processed: int = 0
    quarantined: int = 0
    returned: int = 0
    failed: int = 0
    seconds: float = 0.0

    def render(self) -> str:
        return "\n".join(
            [
                "",
                "  push bridge results",
                "  " + "-" * 52,
                f"  delivered          {self.delivered:,}",
                f"  processed          {self.processed:,}",
                f"  quarantined        {self.quarantined:,}",
                f"  returned (nacked)  {self.returned:,}",
                f"  undeliverable      {self.failed:,}",
                f"  elapsed            {self.seconds:,.2f}s",
                "",
            ]
        )

    def as_dict(self) -> dict[str, Any]:
        payload: dict[str, Any] = asdict(self)
        payload["seconds"] = round(self.seconds, 3)
        return payload


def build_envelope(message: Any, subscription: str, delivery_attempt: int) -> dict[str, Any]:
    """Shape one pulled message as the JSON body a push subscription sends.

    The camelCase keys are not a style choice: they are what Pub/Sub puts on
    the wire, and ``PushEnvelope`` aliases its fields to them. Writing
    ``delivery_attempt`` here would parse to the default of 1 without
    complaint and quietly disable the processor's give-up logic.
    """
    return {
        "subscription": subscription,
        "deliveryAttempt": delivery_attempt,
        "message": {
            "data": base64.b64encode(bytes(message.data)).decode("ascii"),
            "attributes": dict(message.attributes),
            "messageId": message.message_id,
            "orderingKey": message.ordering_key,
            "publishTime": message.publish_time.rfc3339()
            if message.publish_time is not None
            else None,
        },
    }


async def _deliver(
    client: httpx.AsyncClient, envelope: dict[str, Any], report: BridgeReport
) -> bool:
    """POST one envelope. Returns whether Pub/Sub would consider it acked."""
    try:
        response = await client.post("/", json=envelope, timeout=30.0)
    except httpx.HTTPError as exc:
        # A transport failure is not an ack. Returning the message to the
        # subscription is the same decision push delivery makes.
        log.warning("pushbridge.transport_error", error=str(exc))
        report.failed += 1
        return False

    report.delivered += 1
    if response.status_code >= 300:
        report.returned += 1
        return False

    outcome = response.json().get("outcome") if response.content else None
    if outcome == "quarantined":
        report.quarantined += 1
    else:
        report.processed += 1
    return True


async def _run_async(
    client: httpx.AsyncClient,
    subscriber: pubsub_v1.SubscriberClient,
    subscription: str,
    *,
    deadline: float,
    max_messages: int | None,
    concurrency: int,
) -> BridgeReport:
    report = BridgeReport()
    started = time.monotonic()
    semaphore = asyncio.Semaphore(concurrency)

    async def one(envelope: dict[str, Any], ack_id: str) -> str | None:
        async with semaphore:
            acked = await _deliver(client, envelope, report)
        return ack_id if acked else None

    while time.monotonic() < deadline:
        if max_messages is not None and report.delivered >= max_messages:
            break
        remaining = _PULL_BATCH
        if max_messages is not None:
            remaining = min(remaining, max_messages - report.delivered)

        pulled = await asyncio.to_thread(
            subscriber.pull,
            request={"subscription": subscription, "max_messages": remaining},
            timeout=_PULL_TIMEOUT_SECONDS,
        )
        if not pulled.received_messages:
            break

        results = await asyncio.gather(
            *(
                one(
                    build_envelope(
                        received.message,
                        subscription,
                        # The emulator reports 0 when it is not tracking
                        # attempts; push delivery always sends at least 1.
                        max(1, received.delivery_attempt),
                    ),
                    received.ack_id,
                )
                for received in pulled.received_messages
            )
        )
        ack_ids = [ack_id for ack_id in results if ack_id is not None]
        if ack_ids:
            await asyncio.to_thread(
                subscriber.acknowledge,
                request={"subscription": subscription, "ack_ids": ack_ids},
            )
        # Everything else goes back immediately rather than waiting out the
        # ack deadline, so a redelivery is observable inside one short run.
        nack_ids = [
            received.ack_id
            for received, ack_id in zip(pulled.received_messages, results, strict=True)
            if ack_id is None
        ]
        if nack_ids:
            await asyncio.to_thread(
                subscriber.modify_ack_deadline,
                request={
                    "subscription": subscription,
                    "ack_ids": nack_ids,
                    "ack_deadline_seconds": 0,
                },
            )

    report.seconds = time.monotonic() - started
    return report


async def bridge(
    settings: Settings,
    *,
    duration: timedelta,
    max_messages: int | None,
    concurrency: int,
    url: str | None,
) -> BridgeReport:
    subscriber = pubsub_v1.SubscriberClient()
    subscription = subscriber.subscription_path(
        settings.gcp_project, settings.subscription_processor
    )
    deadline = time.monotonic() + duration.total_seconds()

    if url is not None:
        async with httpx.AsyncClient(base_url=url) as client:
            return await _run_async(
                client,
                subscriber,
                subscription,
                deadline=deadline,
                max_messages=max_messages,
                concurrency=concurrency,
            )

    # Imported here, not at module scope: in --url mode the processor runs in
    # another process and this one has no business opening its database pool.
    from services.processor.main import app

    # The lifespan is what builds the sinks, the enricher and the telemetry
    # pipeline, so entering it explicitly is not a detail -- an ASGI transport
    # does not run it, and without it the first request fails on an
    # uninitialised pool.
    async with app.router.lifespan_context(app):
        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(
            transport=transport, base_url="http://processor.local"
        ) as client:
            return await _run_async(
                client,
                subscriber,
                subscription,
                deadline=deadline,
                max_messages=max_messages,
                concurrency=concurrency,
            )


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="pushbridge",
        description="Pull from the processor subscription and deliver as Pub/Sub push would.",
    )
    parser.add_argument(
        "--duration",
        type=parse_duration,
        default=timedelta(seconds=60),
        metavar="D",
        help="stop after this long, even with messages left (default: 60s)",
    )
    parser.add_argument(
        "--max-messages",
        type=int,
        default=None,
        metavar="N",
        help="stop after delivering this many (default: until the subscription drains)",
    )
    parser.add_argument(
        "--concurrency",
        type=int,
        default=16,
        metavar="N",
        help="deliveries in flight at once (default: 16)",
    )
    parser.add_argument(
        "--url",
        default=None,
        metavar="URL",
        help="post to a running processor instead of driving it in-process",
    )
    parser.add_argument("--json", action="store_true", help="emit the report as JSON on stdout")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    settings = get_settings()

    try:
        report = asyncio.run(
            bridge(
                settings,
                duration=args.duration,
                max_messages=args.max_messages,
                concurrency=args.concurrency,
                url=args.url,
            )
        )
    except gcp_exceptions.NotFound:
        print(
            f"error: subscription {settings.subscription_processor!r} does not exist; "
            "run `python tasks.py topology` first",
            file=sys.stderr,
        )
        return 2

    if args.json:
        print(json.dumps(report.as_dict(), indent=2))
    else:
        print(report.render())
    return 0


if __name__ == "__main__":  # pragma: no cover - module entry point
    raise SystemExit(main())
