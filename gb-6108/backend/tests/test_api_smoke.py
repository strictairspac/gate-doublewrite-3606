"""Smoke test: service-layer list/get wiring and RunEntry serialization on SQLite."""
import os
import sys

BACKEND = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, BACKEND)

os.environ.setdefault("DATABASE_URL", "sqlite://")
os.environ.setdefault("JWT_SECRET", "test-secret")
os.environ.setdefault("DEMO_PASSWORD", "x")
os.environ.setdefault("REDIS_URL", "redis://127.0.0.1:6399/0")

from sqlalchemy import create_engine  # noqa: E402
from sqlalchemy.pool import StaticPool  # noqa: E402

from src.config import resources  # noqa: E402

resources.engine = create_engine(
    "sqlite://", connect_args={"check_same_thread": False}, poolclass=StaticPool
)

from src.constants.job_state import JobState  # noqa: E402
from src.domain.fixtures import FIXTURES  # noqa: E402
from src.models.base import metadata  # noqa: E402
from src.models import audit, cases, reviews, run_pages, runs, users  # noqa: E402,F401
from src.models.cases import cases as cases_t  # noqa: E402
from src.models.runs import runs as runs_t  # noqa: E402
from src.schemas.runs import RunEntry  # noqa: E402
from src.services import runs as run_service  # noqa: E402
from src.services import execution  # noqa: E402
from sqlalchemy import select  # noqa: E402

metadata.create_all(resources.engine)
execution.time = lambda: 5000.0  # type: ignore[assignment]

with resources.engine.begin() as db:
    db.execute(cases_t.insert().values(
        code="C-SMOKE", title="冒烟", description="d", payload=FIXTURES[0]["payload"], created_by="op"))
    case_id = db.scalar(select(cases_t.c.id))
    db.execute(runs_t.insert().values(
        case_id=case_id, status=JobState.QUEUED, parameter=6, actor="op",
        created_at=4900, owner_epoch=0, attempts=0))
    run_id = db.scalar(select(runs_t.c.id))

# Drive the batch through the real failover pipeline.
assert execution.service_one("w-smoke") is True

with resources.engine.connect() as db:
    detail = run_service.get(db, run_id)
    entries = run_service.list_entries(db)

entry = RunEntry.model_validate(detail)
assert entry.status is JobState.DONE
assert entry.total_pages == 5 and entry.landed_pages == 5
assert entry.owner_id == "w-smoke" and entry.owner_epoch == 1 and entry.attempts == 1
assert entry.handed_off is False and entry.last_owner_id is None
assert entry.digest and entry.result["metrics"] == {"已修复": 1, "隔离页": 2, "原页通过": 2}
assert len(entries) == 1 and entries[0]["landed_pages"] == 5
print("RunEntry fields:", entry.model_dump().keys() and "ok")
print("metrics:", entry.result["metrics"])

# A queued run must still serialize with the new nullable progress fields.
with resources.engine.begin() as db:
    db.execute(runs_t.insert().values(
        case_id=case_id, status=JobState.QUEUED, parameter=6, actor="op",
        created_at=4901, owner_epoch=0, attempts=0))
    queued_id = db.scalar(select(runs_t.c.id).order_by(runs_t.c.id.desc()))
with resources.engine.connect() as db:
    queued = RunEntry.model_validate(run_service.get(db, queued_id))
assert queued.status is JobState.QUEUED and queued.landed_pages == 0
assert queued.handed_off is False and queued.total_pages is None
print("queued entry serialization: ok")
print("\nAPI smoke test passed")
