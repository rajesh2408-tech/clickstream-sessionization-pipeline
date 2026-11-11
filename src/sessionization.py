"""Event-time sessionization logic and timestamp parsing."""

import hashlib
from datetime import datetime, timezone
from typing import Any, Dict, List, Tuple


def parse_timestamp(value: str) -> datetime:
    """Parse an ISO-8601 timestamp string and normalize to UTC."""
    if not value or not str(value).strip():
        raise ValueError("missing_event_timestamp")
    
    raw = str(value).strip()
    # Normalize Z to +00:00 for fromisoformat
    raw_clean = raw.replace("Z", "+00:00")
    try:
        parsed = datetime.fromisoformat(raw_clean)
    except ValueError as exc:
        raise ValueError("invalid_event_timestamp") from exc
    
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def deterministic_session_id(user_id: str, first_event_id: str) -> str:
    """Deterministic, collision-free session identifier."""
    digest = hashlib.sha256(f"{user_id}|{first_event_id}".encode()).hexdigest()[:16]
    return f"sess_{digest}"


def assign_sessions(
    events: List[Dict[str, Any]], timeout_minutes: int = 30
) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    """
    Groups events into user sessions based on event-time inactivity gaps.
    
    Boundary rules:
      - gap > timeout_minutes * 60: starts a new session
      - gap <= timeout_minutes * 60: stays in the same session
      - exact boundary (gap == timeout_minutes * 60): stays in the same session
    
    Returns:
      (assigned_events, session_summaries)
    """
    if not events:
        return [], []

    # Sort strictly by user_id, event datetime, and event_id (handles out-of-order events)
    ordered = sorted(events, key=lambda e: (e["user_id"], e["_event_dt"], e["event_id"]))

    assigned_events: List[Dict[str, Any]] = []
    current_session: Dict[str, Dict[str, Any]] = {}
    previous_event_time: Dict[str, datetime] = {}
    session_event_counters: Dict[str, int] = {}

    timeout_seconds = timeout_minutes * 60

    for event in ordered:
        user = event["user_id"]
        dt: datetime = event["_event_dt"]
        prior_dt = previous_event_time.get(user)

        time_since_prev = None
        if prior_dt is not None:
            time_since_prev = (dt - prior_dt).total_seconds()

        # Check boundary condition
        is_new_session = (prior_dt is None) or (time_since_prev > timeout_seconds)

        if is_new_session:
            sid = deterministic_session_id(user, event["event_id"])
            current_session[user] = {
                "session_id": sid,
                "user_id": user,
                "session_start": dt,
                "session_end": dt,
                "event_count": 0,
                "first_event_type": event.get("event_type", "unknown"),
                "last_event_type": event.get("event_type", "unknown"),
            }
            session_event_counters[sid] = 0
            time_since_prev_in_session = None
        else:
            sid = current_session[user]["session_id"]
            time_since_prev_in_session = time_since_prev

        session = current_session[user]
        session["session_end"] = dt
        session["event_count"] += 1
        session["last_event_type"] = event.get("event_type", "unknown")

        session_event_counters[sid] += 1

        row = dict(event)
        row["session_id"] = sid
        row["event_seq_num"] = session_event_counters[sid]
        row["time_since_prev_event_seconds"] = (
            round(time_since_prev_in_session, 1) if time_since_prev_in_session is not None else None
        )
        assigned_events.append(row)
        previous_event_time[user] = dt

    # Aggregate session summary records
    sessions_dict: Dict[str, Dict[str, Any]] = {}
    for row in assigned_events:
        sid = row["session_id"]
        if sid not in sessions_dict:
            sessions_dict[sid] = {
                "session_id": sid,
                "user_id": row["user_id"],
                "session_start": row["_event_dt"],
                "session_end": row["_event_dt"],
                "event_count": 0,
                "first_event_type": row.get("event_type", "unknown"),
                "last_event_type": row.get("event_type", "unknown"),
            }
        sessions_dict[sid]["session_end"] = row["_event_dt"]
        sessions_dict[sid]["event_count"] += 1
        sessions_dict[sid]["last_event_type"] = row.get("event_type", "unknown")

    session_summaries: List[Dict[str, Any]] = []
    for s in sessions_dict.values():
        duration = int((s["session_end"] - s["session_start"]).total_seconds())
        s["duration_seconds"] = duration
        s["is_bounce"] = 1 if (s["event_count"] == 1 or duration == 0) else 0
        session_summaries.append(s)

    return assigned_events, session_summaries
