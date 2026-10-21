#!/usr/bin/env python
"""Cross-platform task runner.

A Makefile is the convention for this and one is included for CI and Linux,
but ``make`` is not present on a stock Windows box and this project is
developed on one. This script is the portable equivalent, stdlib-only, so it
works before the virtualenv exists.

    python tasks.py bootstrap     # one-time: venv + dependencies + .env
    python tasks.py up            # start the local stack
    python tasks.py migrate       # apply database migrations
    python tasks.py check         # lint + typecheck + unit tests
    python tasks.py capture       # archive one feed snapshot for the corpus
    python tasks.py replay -- --rate 500 --duration 10s
    python tasks.py bridge -- --duration 30s   # deliver them to the processor
    python tasks.py help
"""

from __future__ import annotations

import argparse
import os
import shutil
import subprocess
import sys
from collections.abc import Sequence
from pathlib import Path

ROOT = Path(__file__).resolve().parent
VENV = ROOT / ".venv"
IS_WINDOWS = os.name == "nt"
BIN = VENV / ("Scripts" if IS_WINDOWS else "bin")
PY = BIN / ("python.exe" if IS_WINDOWS else "python")

COMPOSE = ["docker", "compose"]


# ----------------------------------------------------------------------
# helpers
# ----------------------------------------------------------------------
def load_dotenv() -> None:
    """Load .env into this process so subprocesses inherit it.

    Hand-rolled rather than using python-dotenv because this script has to run
    before the virtualenv exists, so it stays stdlib-only.

    This matters for more than convenience: the variables without the TP_
    prefix (PUBSUB_EMULATOR_HOST above all) are read from the environment by
    the Google SDKs themselves, not by Settings. Without them a task like
    `topology` does not fail loudly -- it falls through to Application Default
    Credentials and tries to create topics in a real GCP project.

    A real environment variable always wins over the file, so CI and the cloud
    deployments are unaffected.
    """
    env_file = ROOT / ".env"
    if not env_file.exists():
        return
    for raw in env_file.read_text(encoding="utf-8").splitlines():
        line = raw.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, _, value = line.partition("=")
        key = key.strip()
        value = value.strip().strip('"').strip("'")
        if key and key not in os.environ:
            os.environ[key] = value


def run(cmd: Sequence[str], *, env: dict[str, str] | None = None, check: bool = True) -> int:
    printable = " ".join(str(c) for c in cmd)
    print(f"\033[36m$ {printable}\033[0m", flush=True)
    merged = {**os.environ, **(env or {})}
    result = subprocess.run([str(c) for c in cmd], cwd=ROOT, env=merged)
    if check and result.returncode != 0:
        print(f"\033[31mfailed ({result.returncode}): {printable}\033[0m", file=sys.stderr)
        sys.exit(result.returncode)
    return result.returncode


def venv_python() -> Path:
    if not PY.exists():
        print("No .venv found. Run: python tasks.py bootstrap", file=sys.stderr)
        sys.exit(1)
    return PY


def require_docker() -> None:
    if shutil.which("docker") is None:
        print("Docker is not on PATH. Install Docker Desktop first.", file=sys.stderr)
        sys.exit(1)
    if subprocess.run([*COMPOSE, "version"], capture_output=True, cwd=ROOT).returncode != 0:
        print("`docker compose` is unavailable.", file=sys.stderr)
        sys.exit(1)
    # A present CLI with an unreachable daemon is the common Windows case:
    # Docker Desktop installed but not started.
    if subprocess.run(["docker", "info"], capture_output=True, cwd=ROOT).returncode != 0:
        print(
            "The Docker daemon is not responding. Start Docker Desktop and retry.",
            file=sys.stderr,
        )
        sys.exit(1)


# ----------------------------------------------------------------------
# tasks
# ----------------------------------------------------------------------
def task_bootstrap(_: argparse.Namespace) -> None:
    """Create .venv, install the project with dev extras, seed .env."""
    if not PY.exists():
        run([sys.executable, "-m", "venv", str(VENV)])
    run([str(PY), "-m", "pip", "install", "--upgrade", "pip", "setuptools", "wheel"])
    run([str(PY), "-m", "pip", "install", "-e", ".[dev]"])

    env_file = ROOT / ".env"
    if not env_file.exists():
        shutil.copyfile(ROOT / ".env.example", env_file)
        print("Created .env from .env.example")
    else:
        print(".env already exists, left untouched")
    print("\nNext: python tasks.py up")


def task_up(_: argparse.Namespace) -> None:
    """Start Postgres, the Pub/Sub emulator and the observability stack."""
    require_docker()
    run([*COMPOSE, "up", "-d", "--wait"])
    print(
        "\nStack is up:\n"
        "  Postgres    localhost:5432\n"
        "  Pub/Sub     localhost:8085 (emulator)\n"
        "  OTLP        localhost:4317\n"
        "  Jaeger      http://localhost:16686\n"
        "  Prometheus  http://localhost:9090\n"
        "  Grafana     http://localhost:3000  (admin/admin)\n"
        "\nNext: python tasks.py migrate && python tasks.py topology"
    )


def task_down(_: argparse.Namespace) -> None:
    """Stop the stack, keeping the Postgres volume."""
    require_docker()
    run([*COMPOSE, "down"])


def task_nuke(_: argparse.Namespace) -> None:
    """Stop the stack and delete its volumes. Destroys local data."""
    require_docker()
    run([*COMPOSE, "down", "-v"])


def task_ps(_: argparse.Namespace) -> None:
    require_docker()
    run([*COMPOSE, "ps"])


def task_logs(args: argparse.Namespace) -> None:
    require_docker()
    cmd = [*COMPOSE, "logs", "-f", "--tail", "100"]
    if args.rest:
        cmd.extend(args.rest)
    run(cmd, check=False)


def task_migrate(_: argparse.Namespace) -> None:
    """Apply Alembic migrations to the local database."""
    run([str(venv_python()), "-m", "alembic", "upgrade", "head"])


def task_topology(_: argparse.Namespace) -> None:
    """Create topics, subscription and DLQ wiring in the emulator."""
    run(
        [
            str(venv_python()),
            "-c",
            "from transitpulse_core.pubsub import ensure_local_topology;ensure_local_topology()",
        ]
    )


def task_capture(args: argparse.Namespace) -> None:
    """Fetch the live feed and archive it into the replay corpus."""
    run([str(venv_python()), "-m", "tools.capture", *args.rest])


def task_replay(args: argparse.Namespace) -> None:
    """Replay the corpus at a target rate: `replay -- --rate 500 --duration 10s`."""
    run([str(venv_python()), "-m", "tools.replay", *args.rest], check=False)


def task_bridge(args: argparse.Namespace) -> None:
    """Deliver published messages to the processor: `bridge -- --duration 30s`."""
    run([str(venv_python()), "-m", "tools.pushbridge", *args.rest], check=False)


def task_smoke(args: argparse.Namespace) -> None:
    """Provoke every metric the dashboard and alert rules read, once."""
    run([str(venv_python()), "-m", "tools.smoke", *args.rest])


def task_partitions(args: argparse.Namespace) -> None:
    """Create partitions ahead of now, prune old ones, report the headroom."""
    run([str(venv_python()), "-m", "services.jobs.partition_maintenance", *args.rest])


def task_replay_dlq(args: argparse.Namespace) -> None:
    """Re-validate dead-lettered events and republish the ones that now pass."""
    run([str(venv_python()), "-m", "services.jobs.dlq_replayer", *args.rest])


def task_chaos(args: argparse.Namespace) -> None:
    """Run a chaos experiment: `chaos -- --list` or `chaos -- kill-processor`."""
    run([str(venv_python()), "-m", "tools.chaos", *args.rest], check=False)


def task_durability(args: argparse.Namespace) -> None:
    """Compare write throughput against fsync=off and fsync=on Postgres."""
    run([str(venv_python()), "-m", "tools.durability", *args.rest], check=False)


def task_screenshots(args: argparse.Namespace) -> None:
    """Capture the dashboard panels as verified PNGs under docs/dashboards/."""
    run([str(venv_python()), "-m", "tools.screenshots", *args.rest], check=False)


def task_rules(_: argparse.Namespace) -> None:
    """Validate the Prometheus rules and run their promtool unit tests.

    Runs inside the Prometheus container because promtool ships with it and is
    not installed on the host. Both halves matter: `check` catches a rule file
    that will not load, and `test` catches one that loads and then evaluates to
    the wrong thing -- which is how a paging alert once fired while the system
    it watched was healthy.
    """
    require_docker()
    rules = "/etc/prometheus/rules"
    run(
        [
            *COMPOSE,
            "exec",
            "-T",
            "prometheus",
            "promtool",
            "check",
            "config",
            "/etc/prometheus/prometheus.yml",
        ]
    )
    run(
        [
            *COMPOSE,
            "exec",
            "-T",
            "prometheus",
            "promtool",
            "test",
            "rules",
            "/etc/prometheus/rule_tests/alerts_test.yml",
        ]
    )
    print(f"rules in {rules} are valid and their unit tests pass")


def task_lint(_: argparse.Namespace) -> None:
    py = str(venv_python())
    run([py, "-m", "ruff", "check", "."])
    run([py, "-m", "ruff", "format", "--check", "."])


def task_fmt(_: argparse.Namespace) -> None:
    py = str(venv_python())
    run([py, "-m", "ruff", "format", "."])
    run([py, "-m", "ruff", "check", "--fix", "."])


def task_typecheck(_: argparse.Namespace) -> None:
    run([str(venv_python()), "-m", "mypy", "libs", "services", "tools"])


def task_test(_: argparse.Namespace) -> None:
    """Unit tests only -- no Docker required."""
    run(
        [
            str(venv_python()),
            "-m",
            "pytest",
            "-m",
            "not integration",
            "--cov",
            "--cov-report=term-missing:skip-covered",
        ]
    )


def task_test_integration(_: argparse.Namespace) -> None:
    """Integration tests against the running stack."""
    require_docker()
    run([str(venv_python()), "-m", "pytest", "-m", "integration", "-v"])


def task_check(args: argparse.Namespace) -> None:
    """Everything CI runs on a pull request."""
    task_lint(args)
    task_typecheck(args)
    task_test(args)
    print("\n\033[32mall checks passed\033[0m")


TASKS = {
    "bootstrap": task_bootstrap,
    "up": task_up,
    "down": task_down,
    "nuke": task_nuke,
    "ps": task_ps,
    "logs": task_logs,
    "migrate": task_migrate,
    "topology": task_topology,
    "capture": task_capture,
    "replay": task_replay,
    "bridge": task_bridge,
    "smoke": task_smoke,
    "partitions": task_partitions,
    "replay-dlq": task_replay_dlq,
    "chaos": task_chaos,
    "durability": task_durability,
    "screenshots": task_screenshots,
    "rules": task_rules,
    "lint": task_lint,
    "fmt": task_fmt,
    "typecheck": task_typecheck,
    "test": task_test,
    "test-integration": task_test_integration,
    "check": task_check,
}


def main() -> None:
    parser = argparse.ArgumentParser(
        prog="tasks.py",
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("task", choices=[*TASKS, "help"], help="task to run")
    parser.add_argument("rest", nargs=argparse.REMAINDER, help="extra arguments")
    args = parser.parse_args()

    # Before any task runs, so every subprocess sees the local stack's
    # addresses. bootstrap is exempt: it is what creates .env in the first place.
    if args.task not in {"help", "bootstrap"}:
        load_dotenv()

    if args.task == "help":
        width = max(len(name) for name in TASKS)
        print("Available tasks:\n")
        for name, fn in TASKS.items():
            summary = (fn.__doc__ or "").strip().splitlines()[0] if fn.__doc__ else ""
            print(f"  {name.ljust(width)}  {summary}")
        return

    TASKS[args.task](args)


if __name__ == "__main__":
    main()
