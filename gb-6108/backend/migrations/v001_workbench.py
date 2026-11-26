from src.models import audit, cases, reviews, run_pages, runs, users  # Register the tables.
from src.models.base import metadata
from migrations import v002_worker_failover


def upgrade(connection):
    # Version 1 is additive and fully declarative.
    metadata.create_all(connection)
    # Version 2 is a no-op on a fresh schema (columns/table already declared above)
    # and applies ALTER/CREATE steps idempotently on older databases.
    v002_worker_failover.upgrade(connection)
