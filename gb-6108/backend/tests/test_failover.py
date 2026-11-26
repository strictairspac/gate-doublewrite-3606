"""Integration tests for worker failover, page-level idempotency and fencing.

Runs against in-memory SQLite through the real SQLAlchemy service code. The
SKIP LOCKED / lease semantics are exercised at the application level: SQLite
serializes writes, while the fencing conditions are plain WHERE clauses that
behave identically on MySQL 8.
"""
import os
import sys
import uuid
from uuid import uuid4

BACKEND = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, BACKEND)

os.environ.setdefault("DATABASE_URL", "sqlite://")
os.environ.setdefault("JWT_SECRET", "test-secret")
os.environ.setdefault("DEMO_PASSWORD", "x")
os.environ.setdefault("REDIS_URL", "redis://127.0.0.1:6399/0")

from sqlalchemy import create_engine, func, select  # noqa: E402
from sqlalchemy.pool import StaticPool  # noqa: E402

from src.config import resources  # noqa: E402

# Swap the engine before any service module binds it.
test_engine = create_engine(
    "sqlite://", connect_args={"check_same_thread": False}, poolclass=StaticPool
)
resources.engine = test_engine

from src.constants.job_state import JobState  # noqa: E402
from src.domain import scenario  # noqa: E402
from src.domain.fixtures import FIXTURES  # noqa: E402
from src.models import audit, cases, reviews, run_pages, runs, users  # noqa: E402,F401
from src.models.base import metadata  # noqa: E402
from src.models.cases import cases as cases_t  # noqa: E402
from src.models.run_pages import run_pages as pages_t  # noqa: E402
from src.models.runs import runs as runs_t  # noqa: E402
from src.services import execution  # noqa: E402
from src.services.execution import (  # noqa: E402
    LEASE_SECONDS,
    JobFailure,
    LeaseLost,
    claim_next,
    fail,
    heartbeat,
    persist_page,
    service_one,
)
from src.utils.serial import content_digest  # noqa: E402

metadata.create_all(test_engine)

# Deterministic, controllable clock for lease expiry scenarios.
clock = [1000.0]
execution.time = lambda: clock[0]  # type: ignore[assignment]


def reset_db():
    # Tests share one in-memory database; wipe business tables between cases so a
    # left-over running batch from one test cannot be claimed by the next, and
    # reset the controllable clock so lease timings start from a known point.
    with test_engine.begin() as db:
        for table in (pages_t, runs_t, cases_t, audit.audit):
            db.execute(table.delete())
    clock[0] = 1000.0


def seed_run(payload=None, parameter=6):
    payload = payload if payload is not None else FIXTURES[0]["payload"]
    with test_engine.begin() as db:
        db.execute(cases_t.insert().values(
            code=f"C-{uuid4().hex[:10]}", title="t", description="d",
            payload=payload, created_by="op",
        ))
        case_id = db.scalar(select(cases_t.c.id).order_by(cases_t.c.id.desc()))
        db.execute(runs_t.insert().values(
            case_id=case_id, status=JobState.QUEUED, parameter=parameter,
            actor="op", created_at=900, owner_epoch=0, attempts=0,
        ))
        return db.scalar(select(runs_t.c.id).order_by(runs_t.c.id.desc()))


def get_run(run_id):
    with test_engine.connect() as db:
        return dict(db.execute(select(runs_t).where(runs_t.c.id == run_id)).mappings().one())


def get_pages(run_id):
    with test_engine.connect() as db:
        return [dict(r) for r in db.execute(
            select(pages_t).where(pages_t.c.run_id == run_id).order_by(pages_t.c.position)
        ).mappings()]


def payload_of(run_id):
    with test_engine.connect() as db:
        return db.scalar(select(cases_t.c.payload).join(runs_t, runs_t.c.case_id == cases_t.c.id)
                         .where(runs_t.c.id == run_id))


def audit_actions(run_id):
    with test_engine.connect() as db:
        return db.execute(select(audit.audit.c.action).where(
            audit.audit.c.entity_id == run_id, audit.audit.c.entity == "run"
        )).scalars().all()


def test_happy_path_completes_and_evidence_matches_digest():
    run_id = seed_run()
    assert service_one("wA") is True
    run = get_run(run_id)
    assert run["status"] == JobState.DONE
    assert run["owner_id"] == "wA" and run["owner_epoch"] == 1
    assert run["total_pages"] == 5 and run["attempts"] == 1
    assert run["lease_expires_at"] is None and run["finished_at"] is not None
    assert run["result"] and run["digest"] == content_digest(run["result"])
    pages = get_pages(run_id)
    assert len(pages) == 5 and [p["position"] for p in pages] == [0, 1, 2, 3, 4]
    assert len({p["page_key"] for p in pages}) == 5
    # Nothing left for another worker.
    assert service_one("wB") is False


def test_crash_then_handoff_resumes_without_rewriting_pages():
    run_id = seed_run()
    pages = scenario.normalize(payload_of(run_id))
    with test_engine.begin() as db:
        task, epoch = claim_next(db, "wA", int(clock[0]))
    assert epoch == 1
    for position in (0, 1):  # A durably lands two pages, then the process dies.
        with test_engine.begin() as db:
            persist_page(db, "wA", run_id, 1, pages[position], position, 6, int(clock[0]))
    mid = get_run(run_id)
    assert mid["status"] == JobState.RUNNING and mid["result"] is None and mid["digest"] is None
    assert len(get_pages(run_id)) == 2

    clock[0] += LEASE_SECONDS + 5  # A stops heart-beating; lease goes stale.
    assert service_one("wB") is True  # B takes over and finishes the same batch.

    run = get_run(run_id)
    assert run["status"] == JobState.DONE
    assert run["owner_id"] == "wB" and run["owner_epoch"] == 2
    assert run["last_owner_id"] == "wA" and run["attempts"] == 2
    landed = get_pages(run_id)
    assert len(landed) == 5  # unique constraint: no page written twice
    assert len({p["page_key"] for p in landed}) == 5
    owners = {p["owner_id"] for p in landed}
    assert owners == {"wA", "wB"}  # A's pages kept, B only filled the gap
    assert "handoff" in audit_actions(run_id)
    assert run["digest"] == content_digest(run["result"])


def test_stale_worker_is_fenced_cannot_write_or_finish():
    run_id = seed_run()
    pages = scenario.normalize(payload_of(run_id))
    with test_engine.begin() as db:
        _, epoch_a = claim_next(db, "wA", int(clock[0]))
    with test_engine.begin() as db:
        persist_page(db, "wA", run_id, epoch_a, pages[0], 0, 6, int(clock[0]))

    clock[0] += LEASE_SECONDS + 5
    blocked = []

    def stall(_rid, epoch_b, position, renew):
        if position == 1:
            assert epoch_b == 2
            # Resurrected A still holds the old epoch: every write must be fenced.
            with test_engine.begin() as db:
                try:
                    persist_page(db, "wA", run_id, 1, pages[1], 1, 6, int(clock[0]))
                except LeaseLost:
                    blocked.append("page")
                else:
                    raise AssertionError("stale worker wrote a page")
                try:
                    heartbeat(db, "wA", run_id, 1, int(clock[0]))
                except LeaseLost:
                    blocked.append("heartbeat")
                else:
                    raise AssertionError("stale worker renewed a lost lease")

    assert service_one("wB", stall=stall) is True
    assert blocked == ["page", "heartbeat"]
    run = get_run(run_id)
    assert run["status"] == JobState.DONE and run["owner_id"] == "wB"
    # A's late failure report must not overwrite B's completed batch.
    fail(JobFailure(run_id, 1, ValueError("late error from dead attempt")), "wA")
    run_after = get_run(run_id)
    assert run_after["status"] == JobState.DONE and run_after["failure"] is None
    assert len(get_pages(run_id)) == 5


def test_slow_but_alive_worker_keeps_lease_via_heartbeat():
    run_id = seed_run()
    clock[0] = 2000.0

    def stall(_rid, epoch, position, renew):
        if position == 2:
            for _ in range(4):  # 32s stalled, longer than the 30s lease...
                clock[0] += 8
                renew()  # ...but heartbeats keep extending it.
            with test_engine.begin() as db:
                assert claim_next(db, "wB", int(clock[0])) is None  # not claimable

    assert service_one("wA", stall=stall) is True
    run = get_run(run_id)
    assert run["status"] == JobState.DONE and run["owner_id"] == "wA"
    assert run["owner_epoch"] == 1 and run["attempts"] == 1  # never handed off
    assert "handoff" not in audit_actions(run_id)


def test_other_batches_keep_running_normally():
    two_pages = {"pages": FIXTURES[0]["payload"]["pages"][:2]}
    r1 = seed_run(payload=two_pages)
    r2 = seed_run()  # 5 pages
    assert service_one("wA") is True
    assert service_one("wA") is True
    assert get_run(r1)["status"] == JobState.DONE
    assert get_run(r2)["status"] == JobState.DONE
    assert len(get_pages(r1)) == 2 and len(get_pages(r2)) == 5


def test_retry_within_same_owner_does_not_duplicate_page():
    run_id = seed_run()
    pages = scenario.normalize(payload_of(run_id))
    with test_engine.begin() as db:
        _, epoch = claim_next(db, "wA", int(clock[0]))
    with test_engine.begin() as db:
        first = persist_page(db, "wA", run_id, epoch, pages[0], 0, 6, int(clock[0]))
        second = persist_page(db, "wA", run_id, epoch, pages[0], 0, 6, int(clock[0]))
    assert first is not None and second is None  # second attempt skipped as durable
    assert len(get_pages(run_id)) == 1


def test_business_failure_is_persisted_and_not_reclaimed():
    run_id = seed_run(payload={"pages": "not-a-list"})
    try:
        service_one("wA")
    except JobFailure as exc:
        fail(exc, "wA")
    else:
        raise AssertionError("expected JobFailure for malformed payload")
    run = get_run(run_id)
    assert run["status"] == JobState.FAILED and run["failure"]
    with test_engine.begin() as db:
        assert claim_next(db, "wB", int(clock[0])) is None  # terminal, never requeued


def test_fail_after_handoff_is_a_noop():
    run_id = seed_run()
    with test_engine.begin() as db:
        _, epoch_a = claim_next(db, "wA", int(clock[0]))
    clock[0] += LEASE_SECONDS + 5
    assert service_one("wB") is True  # epoch 2, completes
    fail(JobFailure(run_id, epoch_a, RuntimeError("boom")), "wA")
    run = get_run(run_id)
    assert run["status"] == JobState.DONE and run["owner_id"] == "wB"


def test_new_owner_only_finalizes_when_all_pages_already_landed():
    run_id = seed_run()
    pages = scenario.normalize(payload_of(run_id))
    with test_engine.begin() as db:
        _, epoch_a = claim_next(db, "wA", int(clock[0]))
    for position, page in enumerate(pages):  # A lands every page but dies before finalize.
        with test_engine.begin() as db:
            persist_page(db, "wA", run_id, epoch_a, page, position, 6, int(clock[0]))
    assert get_run(run_id)["status"] == JobState.RUNNING  # no evidence yet
    clock[0] += LEASE_SECONDS + 5
    assert service_one("wB") is True  # B takes over; every page already durable.
    run = get_run(run_id)
    assert run["status"] == JobState.DONE and run["owner_id"] == "wB"
    assert len(get_pages(run_id)) == 5  # B wrote nothing, just generated evidence
    assert run["digest"] == content_digest(run["result"])


def test_same_worker_reclaim_is_not_a_handoff():
    run_id = seed_run()
    assert service_one("wA") is True
    clock[0] += LEASE_SECONDS + 5
    # Nothing is claimable once done; force a same-id reclaim path on a fresh run.
    second = seed_run()
    with test_engine.begin() as db:
        claim_next(db, "wA", int(clock[0]))
    clock[0] += LEASE_SECONDS + 5
    with test_engine.begin() as db:
        task, epoch = claim_next(db, "wA", int(clock[0]))  # same worker, restart
    run = get_run(second)
    assert run["owner_id"] == "wA" and run["last_owner_id"] == "wA"
    assert "handoff" not in audit_actions(second)


TESTS = [v for k, v in sorted(globals().items()) if k.startswith("test_") and callable(v)]

if __name__ == "__main__":
    for test in TESTS:
        reset_db()
        test()
        print(f"PASS {test.__name__}")
    print(f"\n{len(TESTS)} integration tests passed")
