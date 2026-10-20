#!/usr/bin/env bash
# Single entrypoint for every service in the image. TP_SERVICE selects the
# process; a typo fails loudly here rather than starting the wrong thing.
set -euo pipefail

PORT="${PORT:-8080}"
# Cloud Run bills per instance, so concurrency is set high and the worker
# count low -- these services are I/O bound on the broker and the database.
WORKERS="${TP_UVICORN_WORKERS:-1}"

case "${TP_SERVICE:-}" in
  collector)
    exec uvicorn services.collector.main:app \
      --host 0.0.0.0 --port "${PORT}" --workers "${WORKERS}" --no-access-log
    ;;
  processor)
    exec uvicorn services.processor.main:app \
      --host 0.0.0.0 --port "${PORT}" --workers "${WORKERS}" --no-access-log
    ;;
  api)
    exec uvicorn services.api.main:app \
      --host 0.0.0.0 --port "${PORT}" --workers "${WORKERS}"
    ;;
  job)
    # Cloud Run Jobs pass the job name plus any arguments.
    exec python -m "services.jobs.${TP_JOB:?TP_JOB must be set when TP_SERVICE=job}" "$@"
    ;;
  *)
    echo "TP_SERVICE must be one of: collector | processor | api | job (got '${TP_SERVICE:-unset}')" >&2
    exit 64
    ;;
esac
