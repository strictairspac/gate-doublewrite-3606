from sqlalchemy import JSON, BigInteger, Column, ForeignKey, Integer, String, Table, UniqueConstraint
from .base import metadata


# One durable row per inspected page. (run_id, page_key) is the idempotency key:
# after a crash and handoff, the next worker skips pages already present here,
# so a retry never writes a landed page twice.
run_pages = Table(
    "run_pages", metadata,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("run_id", ForeignKey("runs.id"), nullable=False, index=True),
    Column("page_key", String(32), nullable=False),
    Column("position", Integer, nullable=False),
    # The lease holder that actually landed this page; kept for the handoff trail.
    Column("owner_id", String(48), nullable=False),
    Column("owner_epoch", Integer, nullable=False),
    Column("result", JSON, nullable=False),
    Column("created_at", BigInteger, nullable=False),
    UniqueConstraint("run_id", "page_key", name="uq_run_page"),
)
