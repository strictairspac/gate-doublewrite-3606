"""Concurrency check at the queue level: two worker identities drain one queue.

The workers interleave on the same durable queue, each claiming the next available
batch with SKIP LOCKED semantics. Cross-worker handoff fencing and page-level
idempotency are covered directly in test_failover.py; here we prove that two
distinct owners can coexist, every batch finishes exactly once, and no page is
written twice. Interleaving (rather than true threads) keeps the in-memory SQLite
single-writer lock out of the assertion — the same service code runs on MySQL 8,
where row locks make concurrent claims safe.
"""
import os
import sys

BACKEND = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, BACKEND)
os.environ.update(
    DATABASE_URL="sqlite://", JWT_SECRET="x", DEMO_PASSWORD="x",
    REDIS_URL="redis://127.0.0.1:6399/0",
)

from sqlalchemy import create_engine, func, select  # noqa: E402
from sqlalchemy.pool import StaticPool  # noqa: E402

from src.config import resources  # noqa: E402

te = create_engine("sqlite://", connect_args={"check_same_thread": False}, poolclass=StaticPool)
resources.engine = te

from src.constants.job_state import JobState  # noqa: E402
from src.domain.fixtures import FIXTURES  # noqa: E402
from src.models.base import metadata  # noqa: E402
from src.models import audit, cases, reviews, run_pages, runs, users  # noqa: E402,F401
from src.models.cases import cases as ct  # noqa: E402
from src.models.run_pages import run_pages as pt  # noqa: E402
from src.models.runs import runs as rt  # noqa: E402
from src.services import execution  # noqa: E402

metadata.create_all(te)

with te.begin() as db:
    db.execute(ct.insert().values(code="C", title="t", description="d",
                                  payload=FIXTURES[0]["payload"], created_by="op"))
    cid = db.scalar(select(ct.c.id))
    for _ in range(3):
        db.execute(rt.insert().values(case_id=cid, status=JobState.QUEUED, parameter=6,
                                      actor="op", created_at=1, owner_epoch=0, attempts=0))
    run_ids = list(db.execute(select(rt.c.id)).scalars())

# Two workers take turns pulling from the shared durable queue until it drains.
claimed_owners = set()
turn = 0
for _ in range(20):
    wid = f"w{turn % 2}"
    if execution.service_one(wid):
        claimed_owners.add(wid)
        turn += 1
        continue
    break

with te.connect() as db:
    final = {r: dict(db.execute(select(rt).where(rt.c.id == r)).mappings().one()) for r in run_ids}
    page_counts = {r: db.scalar(select(func.count())
                                .select_from(pt).where(pt.c.run_id == r)) for r in run_ids}

assert claimed_owners == {"w0", "w1"}, claimed_owners  # both workers genuinely participated
assert all(row["status"] == JobState.DONE for row in final.values()), final
assert all(row["lease_expires_at"] is None for row in final.values())
assert all(count == 5 for count in page_counts.values()), page_counts
with te.connect() as db:
    for rid in run_ids:
        keys = db.execute(select(pt.c.page_key).where(pt.c.run_id == rid)).scalars().all()
        assert len(keys) == len(set(keys)) == 5  # no duplicate page writes
owners = {row["owner_id"] for row in final.values()}
print("participating workers:", sorted(claimed_owners), "| final owners:", sorted(owners))
print("page counts:", page_counts)
print("two-worker shared-queue drain passed")
