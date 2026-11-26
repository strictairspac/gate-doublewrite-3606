# Version 2: worker failover — page-level durable progress and lease/fencing ownership.
from sqlalchemy import text


# (DDL statement, name of the column it adds) — column-level steps are skipped when
# the column already exists, so the migration is idempotent on older databases and
# a no-op on a fresh schema whose tables were created declaratively by v001.
COLUMN_STEPS = [
    ("ALTER TABLE runs ADD COLUMN owner_id VARCHAR(48) NULL", "owner_id"),
    ("ALTER TABLE runs ADD COLUMN owner_epoch INTEGER NOT NULL DEFAULT 0", "owner_epoch"),
    ("ALTER TABLE runs ADD COLUMN lease_expires_at BIGINT NULL", "lease_expires_at"),
    ("ALTER TABLE runs ADD COLUMN total_pages INTEGER NULL", "total_pages"),
    ("ALTER TABLE runs ADD COLUMN attempts INTEGER NOT NULL DEFAULT 0", "attempts"),
    ("ALTER TABLE runs ADD COLUMN last_owner_id VARCHAR(48) NULL", "last_owner_id"),
]

CREATE_PAGES = """
CREATE TABLE IF NOT EXISTS run_pages (
    id INTEGER NOT NULL AUTO_INCREMENT PRIMARY KEY,
    run_id INTEGER NOT NULL,
    page_key VARCHAR(32) NOT NULL,
    position INTEGER NOT NULL,
    owner_id VARCHAR(48) NOT NULL,
    owner_epoch INTEGER NOT NULL,
    result JSON NOT NULL,
    created_at BIGINT NOT NULL,
    UNIQUE KEY uq_run_page (run_id, page_key),
    KEY idx_run_pages_run (run_id),
    CONSTRAINT fk_run_pages_run FOREIGN KEY (run_id) REFERENCES runs (id)
)
"""

LEASE_INDEX = "idx_runs_lease"


def _existing_columns(connection):
    return {row[0] for row in connection.execute(text("SHOW COLUMNS FROM runs"))}


def _index_exists(connection, index_name):
    return connection.execute(
        text(
            "SELECT 1 FROM information_schema.statistics "
            "WHERE table_schema = DATABASE() AND table_name = 'runs' "
            "AND index_name = :name LIMIT 1"
        ),
        {"name": index_name},
    ).first() is not None


def upgrade(connection):
    if connection.dialect.name != "mysql":
        # Declarative metadata (v001) already creates the full current schema on
        # other dialects; nothing to patch there.
        return
    existing = _existing_columns(connection)
    for statement, column in COLUMN_STEPS:
        if column not in existing:
            connection.execute(text(statement))
    connection.execute(text(CREATE_PAGES))
    if not _index_exists(connection, LEASE_INDEX):
        connection.execute(
            text(f"CREATE INDEX {LEASE_INDEX} ON runs (status, lease_expires_at)")
        )
