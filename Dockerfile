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

# Prune the package managers out of the venv here, in the stage that built it.
# Doing it here reclaims the bytes, because the venv is copied afterwards. The
# same removal in the runtime stage does not reclaim anything -- a deletion in
# a later layer only writes whiteout entries over a layer that still carries
# the files, which was measured making the image 55 KB *larger*.
RUN rm -rf /opt/venv/lib/python3.12/site-packages/pip \
           /opt/venv/lib/python3.12/site-packages/pip-* \
           /opt/venv/lib/python3.12/site-packages/setuptools \
           /opt/venv/lib/python3.12/site-packages/setuptools-* \
           /opt/venv/lib/python3.12/site-packages/pkg_resources \
           /opt/venv/lib/python3.12/site-packages/wheel \
           /opt/venv/lib/python3.12/site-packages/wheel-* \
           /opt/venv/bin/pip /opt/venv/bin/pip3 /opt/venv/bin/pip3.12

# --- runtime ---------------------------------------------------------------
FROM python:3.12-slim AS runtime

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PYTHONFAULTHANDLER=1 \
    PATH="/opt/venv/bin:$PATH" \
    PYTHONPATH="/app"

# curl only, for the container healthcheck. No compilers and no build
# toolchain: those stay in the builder stage.
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

# Strip the base image's package managers out of the runtime.
#
# A security fix with a measured cause, not housekeeping. Trivy reported four
# HIGH findings against this image -- msgpack, urllib3 and setuptools -- and
# none came from the project's dependency tree: they were pip's *vendored*
# copies under site-packages/pip/_vendor, plus the base image's own setuptools.
# Upgrading our dependencies could not have fixed them, because we do not
# depend on them.
#
# The entrypoint only ever runs `uvicorn` or `python -m`, so nothing here needs
# pip, setuptools or wheel once the build is done. Removing them eliminates the
# findings at the source rather than suppressing them with an ignore file.
#
# This step does not shrink the image -- it cannot, for the layering reason
# given in the builder stage. What it changes is the filesystem a running
# container sees, which is why the scan comes back clean and why none of that
# code is reachable at runtime.
#
# pkg_resources goes too, and that is the one removal with real breakage
# potential: a dependency importing it would now fail at startup rather than in
# CI. So the same layer imports every service entrypoint, which is also why
# this step sits after the COPY -- there is nothing to import before it.
RUN rm -rf \
      /usr/local/lib/python3.12/site-packages/pip \
      /usr/local/lib/python3.12/site-packages/pip-* \
      /usr/local/lib/python3.12/site-packages/setuptools \
      /usr/local/lib/python3.12/site-packages/setuptools-* \
      /usr/local/lib/python3.12/site-packages/pkg_resources \
      /usr/local/lib/python3.12/site-packages/wheel \
      /usr/local/lib/python3.12/site-packages/wheel-* \
      /usr/local/bin/pip /usr/local/bin/pip3 /usr/local/bin/pip3.12 \
    && python -c "import services.api.main, services.collector.main, services.processor.main, services.jobs.gtfs_static_loader, services.jobs.partition_maintenance" \
    && echo "all service entrypoints import with no package manager present"

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
