# ---------------------------------------------------------------------------
# One image, every service. The entrypoint picks which one runs, so all four
# deployments share a single build, a single scan and a single digest -- and a
# dependency bump cannot leave one service behind.
#
# Multi-stage because the runtime needs none of the build toolchain: wheels are
# compiled in the builder and only the resulting site-packages crosses over.
# ---------------------------------------------------------------------------

# --- builder ---------------------------------------------------------------
FROM python:3.12-slim AS builder

ENV PIP_DISABLE_PIP_VERSION_CHECK=1 \
    PIP_NO_CACHE_DIR=1 \
    PYTHONDONTWRITEBYTECODE=1

# build-essential is needed for any sdist-only transitive dependency; it stays
# in this stage and never reaches the final image.
RUN apt-get update \
    && apt-get install -y --no-install-recommends build-essential \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /build

# Copy only what the dependency resolution needs first, so editing service code
# does not invalidate the (slow) install layer.
COPY pyproject.toml README.md ./
COPY libs/ ./libs/

RUN python -m venv /opt/venv
ENV PATH="/opt/venv/bin:$PATH"
RUN pip install --upgrade pip setuptools wheel && pip install .

# --- runtime ---------------------------------------------------------------
FROM python:3.12-slim AS runtime

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PYTHONFAULTHANDLER=1 \
    PATH="/opt/venv/bin:$PATH" \
    PYTHONPATH="/app"

# curl only, for the container healthcheck. No compilers, no package manager
# leftovers: this is the difference between a ~1.1GB image and a ~180MB one.
RUN apt-get update \
    && apt-get install -y --no-install-recommends curl \
    && rm -rf /var/lib/apt/lists/* \
    && useradd --create-home --shell /usr/sbin/nologin --uid 10001 transitpulse

COPY --from=builder /opt/venv /opt/venv

WORKDIR /app
COPY --chown=transitpulse:transitpulse services/ ./services/
COPY --chown=transitpulse:transitpulse tools/ ./tools/
COPY --chown=transitpulse:transitpulse migrations/ ./migrations/
COPY --chown=transitpulse:transitpulse alembic.ini ./
COPY --chown=transitpulse:transitpulse docker/entrypoint.sh /usr/local/bin/entrypoint.sh

RUN chmod +x /usr/local/bin/entrypoint.sh

# Non-root: Cloud Run does not require it, but running as root in a container
# that parses untrusted protobuf from the public internet is indefensible.
USER transitpulse

# Cloud Run injects PORT; 8080 is its default and the local default too.
ENV PORT=8080
EXPOSE 8080

HEALTHCHECK --interval=15s --timeout=3s --start-period=10s --retries=3 \
    CMD curl -fsS "http://localhost:${PORT}/healthz" || exit 1

# TP_SERVICE selects the process: collector | processor | api | job
ENV TP_SERVICE=api
ENTRYPOINT ["/usr/local/bin/entrypoint.sh"]
