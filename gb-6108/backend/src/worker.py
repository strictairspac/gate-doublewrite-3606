import logging
import os
import time
import uuid

from redis.exceptions import RedisError
from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError

from src.config.resources import cache, engine
from src.services.execution import JobFailure, LeaseLost, fail, service_one

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("worker")

# Stable for the life of a worker process so lease rows and audit events can tell
# owners apart after a restart or a scale-out to several worker containers.
WORKER_ID = os.environ.get("WORKER_ID") or f"worker-{uuid.uuid4().hex[:12]}"


def check():
    with engine.connect() as connection:
        connection.execute(text("SELECT 1"))
    heartbeat = cache.get("workbench:heartbeat")
    return heartbeat is not None and time.time() - float(heartbeat) < 90


def work():
    logger.info("Worker %s started", WORKER_ID)
    while True:
        try:
            # Global process heartbeat, independent of any single batch lease.
            cache.set("workbench:heartbeat", time.time(), ex=90)
            try:
                if service_one(WORKER_ID):
                    continue
            except JobFailure as exc:
                logger.exception("Execution %s failed", exc.run_id)
                fail(exc, WORKER_ID)
                continue
            except LeaseLost:
                # Another worker took over the batch; drop the stale attempt and
                # re-enter the queue. Any pages this process already landed stay.
                logger.warning("Lease lost, another worker took over; rejoining queue")
                continue
            cache.blpop("workbench:wake", timeout=1)
        except (RedisError, SQLAlchemyError):
            logger.exception("Worker connection unavailable")
            time.sleep(2)


if __name__ == "__main__":
    work()
