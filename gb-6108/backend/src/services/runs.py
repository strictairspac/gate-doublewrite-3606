from time import time

from fastapi import HTTPException
from sqlalchemy import func, insert, select

from src.constants.job_state import JobState
from src.domain.scenario import CONFIG
from src.models.cases import cases
from src.models.run_pages import run_pages
from src.models.runs import runs
from . import audit, cases as case_service

# Pages already durable for each run, counted once for the whole list query.
landed = (
    select(run_pages.c.run_id, func.count().label("landed_pages"))
    .group_by(run_pages.c.run_id)
    .subquery()
)


def query():
    return (
        select(runs, cases.c.title.label("case_title"),
               func.coalesce(landed.c.landed_pages, 0).label("landed_pages"))
        .join(cases, cases.c.id == runs.c.case_id)
        .outerjoin(landed, landed.c.run_id == runs.c.id)
    )


def _decorate(row):
    entry = dict(row)
    # A handoff is only a real cross-process takeover: a previous owner exists and
    # differs from the current owner. The same worker re-claiming its own expired
    # lease (e.g. after a restart) is not presented as a takeover.
    entry["handed_off"] = bool(
        entry.get("last_owner_id")
        and entry.get("owner_id")
        and entry["last_owner_id"] != entry["owner_id"]
    )
    return entry


def get(db, run_id):
    run = db.execute(query().where(runs.c.id == run_id)).mappings().first()
    if run is None:
        raise HTTPException(404, "执行批次不存在")
    return _decorate(run)


def list_entries(db):
    rows = db.execute(query().order_by(runs.c.id.desc()).limit(100)).mappings()
    return [_decorate(row) for row in rows]


def enqueue(db, case_id, parameter, actor):
    if parameter > CONFIG["max_parameter"]:
        raise HTTPException(422, f"参数不能超过 {CONFIG['max_parameter']}")
    case_service.get(db, case_id)
    change = db.execute(insert(runs).values(
        case_id=case_id, status=JobState.QUEUED, parameter=parameter, actor=actor,
        created_at=int(time()), owner_epoch=0, attempts=0,
    ))
    run_id = change.inserted_primary_key[0]
    audit.record(db, "enqueue", actor, "run", run_id)
    return get(db, run_id)
