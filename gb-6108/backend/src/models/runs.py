from sqlalchemy import JSON, BigInteger, Column, ForeignKey, Integer, String, Table
from .base import metadata


runs = Table(
    "runs", metadata,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("case_id", ForeignKey("cases.id"), nullable=False, index=True),
    Column("status", String(16), nullable=False, index=True),
    Column("parameter", Integer, nullable=False),
    Column("actor", String(48), nullable=False),
    Column("created_at", BigInteger, nullable=False),
    Column("finished_at", BigInteger, nullable=True),
    Column("result", JSON(none_as_null=True), nullable=True),
    Column("failure", String(320), nullable=True),
    Column("digest", String(64), nullable=True),
    # Lease / fencing ownership. owner_epoch is the fencing token bumped on every
    # claim or handoff; worker writes are conditional on still owning the epoch.
    Column("owner_id", String(48), nullable=True),
    Column("owner_epoch", Integer, nullable=False, server_default="0"),
    Column("lease_expires_at", BigInteger, nullable=True),
    Column("total_pages", Integer, nullable=True),
    Column("attempts", Integer, nullable=False, server_default="0"),
    Column("last_owner_id", String(48), nullable=True),
)
