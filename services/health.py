"""Shared liveness and readiness endpoints.

Liveness and readiness are deliberately different checks. ``/healthz`` says
"this process is not wedged" and must never touch a dependency -- if it did, a
brief database blip would make Cloud Run kill and restart every healthy
instance at once. ``/readyz`` says "this instance can serve traffic" and does
check dependencies, so a struggling instance is removed from rotation without
being destroyed.
"""

from __future__ import annotations

from collections.abc import Callable

from fastapi import APIRouter, Response, status

ReadinessProbe = Callable[[], bool]


def build_health_router(
    service: str,
    *,
    readiness: ReadinessProbe | None = None,
) -> APIRouter:
    router = APIRouter(tags=["health"])

    @router.get("/healthz", include_in_schema=False)
    def healthz() -> dict[str, str]:
        return {"status": "ok", "service": service}

    @router.get("/readyz", include_in_schema=False)
    def readyz(response: Response) -> dict[str, object]:
        ready = readiness() if readiness is not None else True
        if not ready:
            response.status_code = status.HTTP_503_SERVICE_UNAVAILABLE
        return {"ready": ready, "service": service}

    return router
