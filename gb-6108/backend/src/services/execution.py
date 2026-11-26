import logging
from time import time

from sqlalchemy import func, select, update

from src.config.resources import engine
from src.constants.job_state import JobState
from src.domain import scenario
from src.models.cases import cases
from src.models.run_pages import run_pages
from src.models.runs import runs
from src.services.audit import record
from src.utils.serial import content_digest

logger = logging.getLogger(__name__)

# A lease must be renewed well within its lifetime. A worker that stops
# heart-beating loses the batch after LEASE_SECONDS; a slow but alive worker keeps
# renewing and therefore keeps the batch to itself.
LEASE_SECONDS = 30
HEARTBEAT_EVERY = 10


class LeaseLost(RuntimeError):
    """Raised when another worker has taken over the batch (fencing check failed)."""


class JobFailure(Exception):
    def __init__(self, run_id, epoch, cause):
        super().__init__(str(cause))
        self.run_id = run_id
        self.epoch = epoch
        self.cause = cause


def _claimable(now):
    # lease_expires_at is an absolute expiry timestamp, so a stale lease becomes
    # claimable once the current time has moved past it.
    return (runs.c.status == JobState.QUEUED) | (
        (runs.c.status == JobState.RUNNING)
        & (runs.c.lease_expires_at.is_not(None))
        & (runs.c.lease_expires_at < now)
    )


def claim_next(db, worker_id, now):
    """Atomically claim one queued batch, or take over an expired lease.

    Returns ``(run_mapping, epoch)`` or ``None``. SKIP LOCKED lets several workers
    scan the queue without blocking each other. The bumped ``owner_epoch`` is the
    fencing token every later write has to present.
    """
    statement = (
        select(runs)
        .where(_claimable(now))
        .order_by(runs.c.id)
        .limit(1)
        .with_for_update(skip_locked=True)
    )
    task = db.execute(statement).mappings().first()
    if task is None:
        return None
    run_id = task["id"]
    previous_owner = task["owner_id"]
    epoch = (task["owner_epoch"] or 0) + 1
    db.execute(
        update(runs)
        .where(runs.c.id == run_id)
        .values(
            status=JobState.RUNNING,
            owner_id=worker_id,
            owner_epoch=epoch,
            lease_expires_at=now + LEASE_SECONDS,
            attempts=task["attempts"] + 1,
            last_owner_id=previous_owner or task["last_owner_id"],
        )
    )
    if previous_owner and previous_owner != worker_id:
        logger.warning(
            "Run %s lease expired: %s handed off to %s (epoch %s)",
            run_id, previous_owner, worker_id, epoch,
        )
        record(db, "handoff", worker_id, "run", run_id)
    else:
        record(db, "claim", worker_id, "run", run_id)
    return dict(task), epoch


def heartbeat(db, worker_id, run_id, epoch, now):
    """Renew the lease. Zero rows updated means the lease was lost: stop working."""
    result = db.execute(
        update(runs)
        .where(
            runs.c.id == run_id,
            runs.c.owner_id == worker_id,
            runs.c.owner_epoch == epoch,
            runs.c.status == JobState.RUNNING,
        )
        .values(lease_expires_at=now + LEASE_SECONDS)
    )
    if result.rowcount != 1:
        raise LeaseLost(str(run_id))


def _owns(db, worker_id, run_id, epoch):
    return db.scalar(
        select(func.count())
        .select_from(runs)
        .where(
            runs.c.id == run_id,
            runs.c.owner_id == worker_id,
            runs.c.owner_epoch == epoch,
            runs.c.status == JobState.RUNNING,
        )
    ) == 1


def set_total_pages(db, worker_id, run_id, epoch, total, now):
    """Record the page count under the same fencing guard as page writes."""
    result = db.execute(
        update(runs)
        .where(
            runs.c.id == run_id,
            runs.c.owner_id == worker_id,
            runs.c.owner_epoch == epoch,
            runs.c.status == JobState.RUNNING,
        )
        .values(total_pages=total, lease_expires_at=now + LEASE_SECONDS)
    )
    if result.rowcount != 1:
        raise LeaseLost(str(run_id))


def persist_page(db, worker_id, run_id, epoch, page, position, parameter, now):
    """Idempotently persist one page.

    The unique ``(run_id, page_key)`` constraint guarantees a page already landed
    — by this worker before a retry, or by the previous owner before the handoff —
    is never written twice. Only the current lease holder (matching epoch) passes
    the fence, so a resurrected stale worker cannot write anything.
    """
    if not _owns(db, worker_id, run_id, epoch):
        raise LeaseLost(str(run_id))
    existing = db.scalar(
        select(func.count())
        .select_from(run_pages)
        .where(run_pages.c.run_id == run_id, run_pages.c.page_key == page["page"])
    )
    if existing:
        # Already durable from an earlier attempt; do not write it again.
        return None
    result = scenario.inspect_page(page, parameter)
    db.execute(
        run_pages.insert().values(
            run_id=run_id,
            page_key=page["page"],
            position=position,
            owner_id=worker_id,
            owner_epoch=epoch,
            result=result,
            created_at=now,
        )
    )
    return result


def finalize_if_complete(db, worker_id, run_id, epoch, pages, now):
    """Generate the batch evidence only after every page has landed.

    Result, digest and terminal status are written in one transaction so they can
    never diverge. Returns True when the run reached a terminal state.
    """
    if not _owns(db, worker_id, run_id, epoch):
        raise LeaseLost(str(run_id))
    landed = db.scalar(
        select(func.count()).select_from(run_pages).where(run_pages.c.run_id == run_id)
    )
    if landed != len(pages):
        return False
    page_rows = db.execute(
        select(run_pages.c.result)
        .where(run_pages.c.run_id == run_id)
        .order_by(run_pages.c.position)
    ).scalars().all()
    output = scenario.summarize(pages, list(page_rows))
    finish(
        db, run_id, epoch,
        {"status": JobState.DONE, "result": output, "digest": content_digest(output)},
        worker_id, now,
    )
    return True


def finish(db, run_id, epoch, output, worker_id, now):
    """Move the batch to a terminal state under the fencing token.

    A stale worker whose epoch no longer matches updates zero rows and therefore
    can neither finish nor fail a batch another worker has taken over.
    """
    result = db.execute(
        update(runs)
        .where(
            runs.c.id == run_id,
            runs.c.owner_id == worker_id,
            runs.c.owner_epoch == epoch,
            runs.c.status == JobState.RUNNING,
        )
        .values(**output, finished_at=now, lease_expires_at=None)
    )
    if result.rowcount != 1:
        raise LeaseLost(str(run_id))
    record(db, str(output["status"]), worker_id, "run", run_id)


def fail(job_failure, worker_id):
    """Persist a business/input failure without touching a handed-off batch."""
    with engine.begin() as db:
        run_id = job_failure.run_id
        task = db.execute(
            select(runs).where(runs.c.id == run_id).with_for_update()
        ).mappings().first()
        if task is None or task["status"] != JobState.RUNNING:
            return
        if task["owner_id"] != worker_id or task["owner_epoch"] != job_failure.epoch:
            # The lease moved on; this failure belongs to a dead attempt.
            return
        cause = job_failure.cause
        if isinstance(cause, (ValueError, TypeError, KeyError, OverflowError)):
            reason = str(cause) or "输入资料或批次参数不符合要求"
        else:
            reason = "处理失败，请检查 worker 日志"
        finish(
            db, run_id, job_failure.epoch,
            {"status": JobState.FAILED, "failure": reason[:320]},
            worker_id, int(time()),
        )


def service_one(worker_id, stall=None):
    """Drive one batch as far as possible. Returns True when a batch was claimed.

    Each page lands in its own transaction, so progress survives a worker crash and
    a handoff resumes from the first page not yet present in ``run_pages``.

    ``stall`` is an optional test hook ``stall(run_id, epoch, position, renew)``;
    ``renew`` performs a heartbeat transaction so a slow but alive worker can prove
    it is still responsive while stalled on a page.
    """
    with engine.begin() as db:
        claim = claim_next(db, worker_id, int(time()))
        if claim is None:
            return False
        task, epoch = claim
        run_id = task["id"]
        payload = db.scalar(select(cases.c.payload).where(cases.c.id == task["case_id"]))

    try:
        pages = scenario.normalize(payload)
    except Exception as exc:
        raise JobFailure(run_id, epoch, exc) from exc

    def renew():
        with engine.begin() as db:
            heartbeat(db, worker_id, run_id, epoch, int(time()))

    with engine.begin() as db:
        set_total_pages(db, worker_id, run_id, epoch, len(pages), int(time()))

    last_beat = time()
    for position, page in enumerate(pages):
        if time() - last_beat >= HEARTBEAT_EVERY:
            renew()
            last_beat = time()
        if stall is not None:
            stall(run_id, epoch, position, renew)
        with engine.begin() as db:
            # persist_page enforces the fence inside the write transaction: if the
            # lease was lost between heartbeats, this raises instead of writing.
            persist_page(db, worker_id, run_id, epoch, page, position, task["parameter"], int(time()))
            completed = finalize_if_complete(db, worker_id, run_id, epoch, pages, int(time()))
        # The previous owner may already have landed every page, in which case the
        # first iteration generated the evidence and moved the run to done. Stop
        # immediately so no further write is attempted against a cleared lease.
        if completed:
            break
    return True
