"""Entry point for ``python -m tools.replay``.

The ``replay`` console script is the documented way in; this exists so the
harness also runs straight from a checkout, before the project is installed.
"""

from __future__ import annotations

from tools.replay.cli import main

if __name__ == "__main__":
    raise SystemExit(main())
