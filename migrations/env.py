"""Alembic environment.

The DSN comes from :class:`transitpulse_core.config.Settings`, not from
``alembic.ini``, so migrations, services and tests can never point at
different databases -- a mismatch that is very easy to miss and very annoying
to debug. ``target_metadata`` is ``None`` on purpose: the schema is declared in
hand-written SQL (see ``versions/0001_initial_schema.py``) because autogenerate
cannot reflect declarative partitioning or partial indexes.
"""

from __future__ import annotations

from alembic import context
from sqlalchemy import engine_from_config, pool

from transitpulse_core.config import get_settings

config = context.config
target_metadata = None

# psycopg3 needs the explicit dialect marker; pydantic hands us the bare scheme.
_dsn = get_settings().pg_dsn_str.replace("postgresql://", "postgresql+psycopg://", 1)
config.set_main_option("sqlalchemy.url", _dsn)


def run_migrations_offline() -> None:
    """Emit SQL to stdout without connecting -- used by the CI schema-diff check."""
    context.configure(
        url=config.get_main_option("sqlalchemy.url"),
        target_metadata=target_metadata,
        literal_binds=True,
        dialect_opts={"paramstyle": "named"},
    )
    with context.begin_transaction():
        context.run_migrations()


def run_migrations_online() -> None:
    connectable = engine_from_config(
        config.get_section(config.config_ini_section, {}),
        prefix="sqlalchemy.",
        poolclass=pool.NullPool,
    )
    with connectable.connect() as connection:
        context.configure(connection=connection, target_metadata=target_metadata)
        with context.begin_transaction():
            context.run_migrations()


if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()
