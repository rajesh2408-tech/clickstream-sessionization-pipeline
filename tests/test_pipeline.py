"""Integration tests for the clickstream processing pipeline."""

import csv
from pathlib import Path
import pytest
from src.config import Settings
from src.pipeline import run_pipeline
from src.reporting import export_reports
from src.storage import connect


def write_csv(path: Path, rows: list, fields: list = None):
    fields = fields or ["event_id", "user_id", "event_type", "event_timestamp", "ingestion_timestamp"]
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fields)
        w.writeheader()
        w.writerows(rows)


def test_pipeline_quarantines_and_is_idempotent(tmp_path):
    """
    Verifies that:
      1. Valid records are processed into sessions
      2. Missing timestamps and invalid foreign keys are quarantined
      3. Duplicates are detected
      4. Reruns on the same batch are idempotent (0 new events, no duplicate sessions)
    """
    inp = tmp_path / "events.csv"
    users = tmp_path / "users.csv"
    write_csv(users, [{"user_id": "u1"}], ["user_id"])

    write_csv(
        inp,
        [
            {"event_id": "e2", "user_id": "u1", "event_type": "view", "event_timestamp": "2026-11-10T10:20:00Z", "ingestion_timestamp": ""},
            {"event_id": "e1", "user_id": "u1", "event_type": "view", "event_timestamp": "2026-11-10T10:00:00Z", "ingestion_timestamp": ""},
            {"event_id": "e3", "user_id": "u1", "event_type": "view", "event_timestamp": "", "ingestion_timestamp": ""}, # missing ts -> reject
            {"event_id": "e4", "user_id": "unknown_user", "event_type": "view", "event_timestamp": "2026-11-10T10:25:00Z", "ingestion_timestamp": ""}, # bad ref -> reject
            {"event_id": "e1", "user_id": "u1", "event_type": "view", "event_timestamp": "2026-11-10T10:00:00Z", "ingestion_timestamp": ""}, # duplicate e1
        ],
    )

    db = tmp_path / "db.sqlite"
    settings = Settings(input_path=inp, db_type="sqlite", db_path=db, timeout_minutes=30, users_path=users)

    # 1. First execution
    first = run_pipeline(settings)
    assert first["input_count"] == 5
    assert first["new_valid_unique_count"] == 2
    assert first["rejected_count"] == 2
    assert first["duplicate_count"] == 1
    assert first["total_session_count"] == 1

    conn = connect(settings)
    cur = conn.execute("SELECT COUNT(*) FROM processed_events").fetchone()
    assert cur[0] == 2
    cur_sess = conn.execute("SELECT COUNT(*) FROM user_sessions").fetchone()
    assert cur_sess[0] == 1
    conn.close()

    # 2. Idempotent Second execution
    second = run_pipeline(settings)
    assert second["input_count"] == 5
    assert second["new_valid_unique_count"] == 0  # No new events accepted
    assert second["duplicate_count"] == 3  # e1, e2, e1 detected as database duplicates
    assert second["rejected_count"] == 2

    # Database count must NOT inflate
    conn = connect(settings)
    assert conn.execute("SELECT COUNT(*) FROM processed_events").fetchone()[0] == 2
    assert conn.execute("SELECT COUNT(*) FROM user_sessions").fetchone()[0] == 1
    conn.close()


def test_missing_required_column_fails(tmp_path):
    """Missing schema columns trigger schema validation error."""
    inp = tmp_path / "bad_schema.csv"
    write_csv(inp, [{"event_id": "e1"}], ["event_id", "user_id"])
    settings = Settings(input_path=inp, db_type="sqlite", db_path=tmp_path / "db.sqlite")

    with pytest.raises(ValueError, match="unexpected_schema_missing_columns"):
        run_pipeline(settings)


def test_empty_batch_is_successful(tmp_path):
    """Empty batch completes successfully with 0 counts."""
    inp = tmp_path / "empty.csv"
    write_csv(inp, [])
    settings = Settings(input_path=inp, db_type="sqlite", db_path=tmp_path / "db.sqlite")

    result = run_pipeline(settings)
    assert result["input_count"] == 0
    assert result["status"] == "SUCCESS"
    assert result["total_output_event_count"] == 0
    assert result["total_session_count"] == 0


def test_reporting_export_csv_and_reconciliation(tmp_path):
    """Export reports creates all 5 CSV files and the reconciliation JSON."""
    inp = tmp_path / "events.csv"
    write_csv(inp, [
        {"event_id": "e1", "user_id": "u1", "event_type": "view", "event_timestamp": "2026-11-10T10:00:00Z", "ingestion_timestamp": ""},
        {"event_id": "e2", "user_id": "u1", "event_type": "click", "event_timestamp": "2026-11-10T10:10:00Z", "ingestion_timestamp": ""},
    ])
    rep_dir = tmp_path / "reports"
    settings = Settings(input_path=inp, db_type="sqlite", db_path=tmp_path / "db.sqlite", output_dir=rep_dir, users_path=None)

    run_pipeline(settings)
    export_reports(settings, rep_dir)

    assert (rep_dir / "user_sessions.csv").exists()
    assert (rep_dir / "processed_events.csv").exists()
    assert (rep_dir / "rejected_events.csv").exists()
    assert (rep_dir / "duplicate_events.csv").exists()
    assert (rep_dir / "pipeline_runs.csv").exists()
    assert (rep_dir / "reconciliation_report.csv").exists()
