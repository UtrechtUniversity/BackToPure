from __future__ import annotations

import sqlite3
from pathlib import Path

JOBS_SCHEMA = """
CREATE TABLE IF NOT EXISTS jobs (
    id TEXT PRIMARY KEY,
    job_type TEXT NOT NULL,
    status TEXT NOT NULL,
    params_json TEXT NOT NULL DEFAULT '{}',
    created_at TEXT NOT NULL,
    started_at TEXT,
    finished_at TEXT,
    exit_code INTEGER,
    log_path TEXT,
    artifact_dir TEXT,
    artifacts_json TEXT NOT NULL DEFAULT '{"canOpen": false, "canApply": false, "artifacts": {"directory": "", "csv": [], "json": []}}',
    results_json TEXT NOT NULL DEFAULT '{"entity_label": "", "found_label": "", "ready_label": "", "updated_label": "", "found_count": null, "ready_count": 0, "updated_count": 0}',
    apply_job_id TEXT,
    rollback_job_id TEXT,
    error_message TEXT
)
"""

JOB_CHANGE_SETS_SCHEMA = """
CREATE TABLE IF NOT EXISTS job_change_sets (
    id TEXT PRIMARY KEY,
    job_id TEXT NOT NULL,
    job_type TEXT NOT NULL,
    status TEXT NOT NULL,
    created_at TEXT NOT NULL,
    applied_at TEXT,
    rolled_back_at TEXT,
    item_count INTEGER NOT NULL DEFAULT 0,
    applied_item_count INTEGER NOT NULL DEFAULT 0
)
"""

JOB_CHANGE_SET_ITEMS_SCHEMA = """
CREATE TABLE IF NOT EXISTS job_change_set_items (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    change_set_id TEXT NOT NULL,
    item_key TEXT NOT NULL,
    entity_uuid TEXT NOT NULL,
    entity_label TEXT,
    field_name TEXT NOT NULL,
    identifier_type TEXT,
    old_value_json TEXT,
    new_value_json TEXT NOT NULL,
    apply_status TEXT NOT NULL DEFAULT 'pending',
    rollback_status TEXT NOT NULL DEFAULT 'pending',
    conflict_reason TEXT
)
"""


def _ensure_column(connection: sqlite3.Connection, table_name: str, column_name: str, column_sql: str) -> None:
    existing_columns = {
        row["name"]
        for row in connection.execute(f"PRAGMA table_info({table_name})").fetchall()
    }
    if column_name not in existing_columns:
        connection.execute(f"ALTER TABLE {table_name} ADD COLUMN {column_sql}")


def project_root() -> Path:
    return Path(__file__).resolve().parent.parent


def default_data_dir() -> Path:
    return project_root() / "data"


def connect_db(db_path: Path) -> sqlite3.Connection:
    connection = sqlite3.connect(db_path)
    connection.row_factory = sqlite3.Row
    return connection


def initialize_database(db_path: Path) -> None:
    with connect_db(db_path) as connection:
        connection.execute(JOBS_SCHEMA)
        connection.execute(JOB_CHANGE_SETS_SCHEMA)
        connection.execute(JOB_CHANGE_SET_ITEMS_SCHEMA)
        _ensure_column(
            connection,
            "jobs",
            "artifacts_json",
            """artifacts_json TEXT NOT NULL DEFAULT '{"canOpen": false, "canApply": false, "artifacts": {"directory": "", "csv": [], "json": []}}'""",
        )
        _ensure_column(
            connection,
            "jobs",
            "results_json",
            """results_json TEXT NOT NULL DEFAULT '{"entity_label": "", "found_label": "", "ready_label": "", "updated_label": "", "found_count": null, "ready_count": 0, "updated_count": 0}'""",
        )
        _ensure_column(
            connection,
            "jobs",
            "rollback_job_id",
            "rollback_job_id TEXT",
        )
        _ensure_column(
            connection,
            "job_change_sets",
            "rolled_back_at",
            "rolled_back_at TEXT",
        )
        _ensure_column(
            connection,
            "job_change_set_items",
            "rollback_status",
            "rollback_status TEXT NOT NULL DEFAULT 'pending'",
        )
        connection.execute(
            "CREATE INDEX IF NOT EXISTS idx_job_change_sets_job_id ON job_change_sets(job_id)"
        )
        connection.execute(
            "CREATE UNIQUE INDEX IF NOT EXISTS idx_job_change_set_items_key ON job_change_set_items(change_set_id, item_key)"
        )
        connection.commit()


def init_db(app) -> None:
    """Initialize shared backend storage locations for future API work."""
    data_dir = Path(app.config.get("BTP_DATA_DIR", default_data_dir()))
    data_dir.mkdir(parents=True, exist_ok=True)
    db_path = data_dir / "jobs.sqlite"
    initialize_database(db_path)

    app.extensions["btp_db"] = {
        "data_dir": data_dir,
        "db_path": db_path,
    }


init_db.default_data_dir = default_data_dir
