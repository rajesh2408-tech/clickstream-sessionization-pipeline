"""Central clickstream sessionization pipeline orchestrator."""

import csv
import json
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Set

from .config import Settings
from .sessionization import assign_sessions, parse_timestamp
from .storage import connect

REQUIRED_COLUMNS = {"event_id", "user_id", "event_type", "event_timestamp"}
OPTIONAL_COLUMNS = {"ingestion_timestamp"}


def utc_now() -> str:
    """Current UTC timestamp in ISO-8601 format."""
    return datetime.now(timezone.utc).isoformat()


def load_user_ids(users_path: Optional[Path]) -> Optional[Set[str]]:
    """Loads valid user IDs for referential integrity checking."""
    if users_path is None:
        return None
    if not users_path.exists():
        raise FileNotFoundError(f"Users reference file not found: {users_path}")

    with users_path.open(newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        if not reader.fieldnames or "user_id" not in reader.fieldnames:
            raise ValueError("users reference file must contain user_id column")
        return {r["user_id"].strip() for r in reader if r.get("user_id", "").strip()}


def read_input(path: Path) -> List[Dict[str, Any]]:
    """
    Reads the input CSV file.
    Validates required columns and preserves row numbers.
    """
    if not path.exists():
        raise FileNotFoundError(f"Input file not found: {path}")

    with path.open(newline="", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f, restkey="_extra_fields", restval=None)
        fields = set(reader.fieldnames or [])
        missing = REQUIRED_COLUMNS - fields
        if missing:
            raise ValueError(f"unexpected_schema_missing_columns:{','.join(sorted(missing))}")

        rows = []
        for line_number, row in enumerate(reader, start=2):
            row["_line_number"] = line_number
            rows.append(row)
        return rows


def canonical_event(row: Dict[str, Any], valid_users: Optional[Set[str]]) -> Dict[str, Any]:
    """Validates and normalizes a single event row against data contracts."""
    if row.get("_extra_fields"):
        raise ValueError("malformed_record_extra_fields")

    event_id = (row.get("event_id") or "").strip()
    user_id = (row.get("user_id") or "").strip()
    event_type = (row.get("event_type") or "").strip()

    if not event_id:
        raise ValueError("missing_event_id")
    if not user_id:
        raise ValueError("missing_user_id")
    if not event_type:
        raise ValueError("missing_event_type")

    # Referential integrity check
    if valid_users is not None and user_id not in valid_users:
        raise ValueError("unknown_user_reference")

    event_dt = parse_timestamp(row.get("event_timestamp") or "")

    ingestion = (row.get("ingestion_timestamp") or "").strip() or None
    if ingestion:
        try:
            ingestion = parse_timestamp(ingestion).isoformat()
        except Exception:
            ingestion = None

    return {
        "event_id": event_id,
        "user_id": user_id,
        "event_type": event_type,
        "event_timestamp": event_dt.isoformat(),
        "ingestion_timestamp": ingestion,
        "_event_dt": event_dt,
    }


def run_pipeline(settings: Settings) -> Dict[str, Any]:
    """
    Executes the ingestion, validation, sessionization, and storage pipeline.
    Safe and idempotent on repeated runs.
    """
    settings.validate()
    run_id = f"run_{uuid.uuid4().hex[:12]}"
    source_file = settings.input_path.name
    started = utc_now()
    start_clock = time.perf_counter()

    conn = connect(settings)
    input_count = valid_count = duplicate_count = rejected_count = 0
    status = "FAILED"
    error_message = None

    try:
        # Record pipeline run start
        conn.execute(
            """INSERT INTO pipeline_runs (run_id, source_file, start_time, status)
               VALUES (?, ?, ?, ?)""",
            (run_id, source_file, started, "RUNNING"),
        )
        conn.commit()

        # Read CSV input
        rows = read_input(settings.input_path)
        input_count = len(rows)

        # Optional users reference for foreign key check
        valid_users = load_user_ids(settings.users_path)

        accepted: List[Dict[str, Any]] = []
        rejects: List[Tuple[Optional[str], str, str]] = []
        duplicates: List[Tuple[str, str]] = []
        batch_seen: Dict[str, Tuple] = {}

        # Validate and separate valid, rejects, and duplicates
        for row in rows:
            raw_record = json.dumps(
                {k: v for k, v in row.items() if not k.startswith("_")}, ensure_ascii=False
            )
            try:
                event = canonical_event(row, valid_users)
                event_id = event["event_id"]
                signature = (
                    event["user_id"],
                    event["event_type"],
                    event["event_timestamp"],
                    event["ingestion_timestamp"],
                )

                # 1. Intra-batch duplicate check
                if event_id in batch_seen:
                    reason = (
                        "exact_duplicate_in_batch"
                        if batch_seen[event_id] == signature
                        else "conflicting_duplicate_in_batch"
                    )
                    duplicates.append((event_id, reason))
                    continue
                batch_seen[event_id] = signature

                # 2. Database duplicate check (idempotency check against prior runs)
                cursor = conn.execute(
                    """SELECT user_id, event_type, event_timestamp, ingestion_timestamp 
                       FROM processed_events WHERE event_id = ?""",
                    (event_id,),
                )
                existing = cursor.fetchone()
                if existing:
                    # In sqlite cursor.fetchone returns row-like, in mysql tuple or dict
                    old_sig = (
                        existing[0] if isinstance(existing, (tuple, list)) else existing["user_id"],
                        existing[1] if isinstance(existing, (tuple, list)) else existing["event_type"],
                        existing[2] if isinstance(existing, (tuple, list)) else existing["event_timestamp"],
                        existing[3] if isinstance(existing, (tuple, list)) else existing["ingestion_timestamp"],
                    )
                    reason = (
                        "exact_duplicate_in_database"
                        if old_sig == signature
                        else "conflicting_duplicate_in_database"
                    )
                    duplicates.append((event_id, reason))
                    continue

                accepted.append(event)

            except Exception as exc:
                eid = (row.get("event_id") or "").strip() or None
                reason_code = str(exc)
                if not reason_code:
                    reason_code = type(exc).__name__
                rejects.append((eid, raw_record, reason_code))

        affected_users = {e["user_id"] for e in accepted}

        # Transactionally insert rejected, duplicates, and new events
        with conn:
            # 1. Insert duplicates
            if duplicates:
                conn.executemany(
                    """INSERT INTO duplicate_events (run_id, source_file, event_id, duplicate_reason) 
                       VALUES (?, ?, ?, ?)""",
                    [(run_id, source_file, d[0], d[1]) for d in duplicates],
                )

            # 2. Insert rejected records
            if rejects:
                conn.executemany(
                    """INSERT INTO rejected_events (run_id, source_file, event_id, raw_record, reject_reason) 
                       VALUES (?, ?, ?, ?, ?)""",
                    [(run_id, source_file, r[0], r[1], r[2]) for r in rejects],
                )

            # 3. Insert newly accepted clean events
            if accepted:
                conn.executemany(
                    """INSERT INTO processed_events 
                       (event_id, user_id, event_type, event_timestamp, ingestion_timestamp, 
                        session_id, event_seq_num, time_since_prev_event_seconds, source_file, last_run_id) 
                       VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                    [
                        (
                            e["event_id"],
                            e["user_id"],
                            e["event_type"],
                            e["event_timestamp"],
                            e["ingestion_timestamp"],
                            "PENDING",
                            1,
                            None,
                            source_file,
                            run_id,
                        )
                        for e in accepted
                    ],
                )

            # 4. Recompute sessionization for all affected users across their history
            for user_id in affected_users:
                cur = conn.execute(
                    """SELECT event_id, user_id, event_type, event_timestamp, ingestion_timestamp, source_file 
                       FROM processed_events WHERE user_id = ?""",
                    (user_id,),
                )
                user_rows = cur.fetchall()
                events_history = []
                for item in user_rows:
                    if isinstance(item, (tuple, list)):
                        evt_dict = {
                            "event_id": item[0],
                            "user_id": item[1],
                            "event_type": item[2],
                            "event_timestamp": item[3],
                            "ingestion_timestamp": item[4],
                            "source_file": item[5],
                        }
                    else:
                        evt_dict = dict(item)
                    evt_dict["_event_dt"] = parse_timestamp(evt_dict["event_timestamp"])
                    events_history.append(evt_dict)

                assigned_events, user_sessions = assign_sessions(
                    events_history, timeout_minutes=settings.timeout_minutes
                )

                # Delete existing sessions for this user and insert recomputed sessions
                conn.execute("DELETE FROM user_sessions WHERE user_id = ?", (user_id,))

                # Update events with session_id, event_seq_num, time_since_prev_event_seconds
                for ev_row in assigned_events:
                    conn.execute(
                        """UPDATE processed_events 
                           SET session_id = ?, event_seq_num = ?, time_since_prev_event_seconds = ?, last_run_id = ? 
                           WHERE event_id = ?""",
                        (
                            ev_row["session_id"],
                            ev_row["event_seq_num"],
                            ev_row.get("time_since_prev_event_seconds"),
                            run_id,
                            ev_row["event_id"],
                        ),
                    )

                # Insert updated sessions
                for sess in user_sessions:
                    conn.execute(
                        """INSERT INTO user_sessions 
                           (session_id, user_id, session_start, session_end, event_count, duration_seconds, 
                            first_event_type, last_event_type, is_bounce, last_run_id) 
                           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                        (
                            sess["session_id"],
                            sess["user_id"],
                            sess["session_start"].isoformat(),
                            sess["session_end"].isoformat(),
                            sess["event_count"],
                            sess["duration_seconds"],
                            sess.get("first_event_type"),
                            sess.get("last_event_type"),
                            sess.get("is_bounce", 0),
                            run_id,
                        ),
                    )

        valid_count = len(accepted)
        duplicate_count = len(duplicates)
        rejected_count = len(rejects)

        cur_events = conn.execute("SELECT COUNT(*) FROM processed_events").fetchone()
        output_event_count = cur_events[0] if isinstance(cur_events, (tuple, list)) else cur_events["COUNT(*)"]

        cur_sess = conn.execute("SELECT COUNT(*) FROM user_sessions").fetchone()
        session_count = cur_sess[0] if isinstance(cur_sess, (tuple, list)) else cur_sess["COUNT(*)"]

        status = "PARTIAL_SUCCESS" if rejects else "SUCCESS"

        return {
            "run_id": run_id,
            "status": status,
            "input_count": input_count,
            "new_valid_unique_count": valid_count,
            "duplicate_count": duplicate_count,
            "rejected_count": rejected_count,
            "total_output_event_count": output_event_count,
            "total_session_count": session_count,
            "duration_seconds": round(time.perf_counter() - start_clock, 4),
        }

    except Exception as exc:
        error_message = f"{type(exc).__name__}: {exc}"
        raise

    finally:
        # Record final pipeline run status in database
        try:
            with conn:
                check_cur = conn.execute("SELECT 1 FROM pipeline_runs WHERE run_id = ?", (run_id,))
                exists = check_cur.fetchone()
                if exists:
                    cur_events = conn.execute("SELECT COUNT(*) FROM processed_events").fetchone()
                    out_cnt = cur_events[0] if isinstance(cur_events, (tuple, list)) else cur_events["COUNT(*)"]

                    cur_sess = conn.execute("SELECT COUNT(*) FROM user_sessions").fetchone()
                    sess_cnt = cur_sess[0] if isinstance(cur_sess, (tuple, list)) else cur_sess["COUNT(*)"]

                    final_status = "FAILED" if error_message else status
                    conn.execute(
                        """UPDATE pipeline_runs 
                           SET end_time = ?, input_count = ?, valid_unique_count = ?, duplicate_count = ?, 
                               rejected_count = ?, output_event_count = ?, session_count = ?, status = ?, 
                               error_message = ? 
                           WHERE run_id = ?""",
                        (
                            utc_now(),
                            input_count,
                            valid_count,
                            duplicate_count,
                            rejected_count,
                            out_cnt,
                            sess_cnt,
                            final_status,
                            error_message,
                            run_id,
                        ),
                    )
        finally:
            conn.close()
