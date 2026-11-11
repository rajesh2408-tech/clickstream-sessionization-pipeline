"""Unit tests for event-time sessionization and timestamp handling."""

from datetime import datetime, timezone
import pytest
from src.sessionization import assign_sessions, deterministic_session_id, parse_timestamp


def ev(eid: str, user: str, minute: int, event_type: str = "page_view") -> dict:
    dt = datetime(2026, 11, 10, 10, minute, tzinfo=timezone.utc)
    return {
        "event_id": eid,
        "user_id": user,
        "event_type": event_type,
        "event_timestamp": dt.isoformat(),
        "_event_dt": dt,
    }


def test_same_session_within_timeout():
    """Events within inactivity timeout belong to the same session."""
    assigned, sessions = assign_sessions([ev("e1", "u1", 0), ev("e2", "u1", 29)], timeout_minutes=30)
    assert len(sessions) == 1
    assert [x["session_id"] for x in assigned].count(sessions[0]["session_id"]) == 2
    assert sessions[0]["duration_seconds"] == 29 * 60
    assert sessions[0]["event_count"] == 2


def test_exact_timeout_stays_same_session():
    """Boundary condition: exactly 30 minutes gap remains in the same session."""
    assigned, sessions = assign_sessions([ev("e1", "u1", 0), ev("e2", "u1", 30)], timeout_minutes=30)
    assert len(sessions) == 1
    assert sessions[0]["duration_seconds"] == 30 * 60


def test_gap_over_timeout_starts_new_session():
    """Boundary condition: gap strictly greater than timeout starts a new session."""
    _, sessions = assign_sessions([ev("e1", "u1", 0), ev("e2", "u1", 31)], timeout_minutes=30)
    assert len(sessions) == 2


def test_users_are_isolated():
    """Events from different users never mix into the same session."""
    _, sessions = assign_sessions([ev("e1", "u1", 0), ev("e2", "u2", 1)], timeout_minutes=30)
    assert len(sessions) == 2
    user_ids = {s["user_id"] for s in sessions}
    assert user_ids == {"u1", "u2"}


def test_out_of_order_input_is_sorted():
    """Out-of-order events are chronologically sorted and assigned sequence numbers."""
    assigned, sessions = assign_sessions([ev("e2", "u1", 20), ev("e1", "u1", 0)], timeout_minutes=30)
    assert len(sessions) == 1
    assert [x["event_id"] for x in assigned] == ["e1", "e2"]
    assert assigned[0]["event_seq_num"] == 1
    assert assigned[1]["event_seq_num"] == 2
    assert assigned[1]["time_since_prev_event_seconds"] == 20 * 60


def test_timestamp_parse_missing_and_invalid():
    """Missing or malformed timestamps raise specific descriptive errors."""
    with pytest.raises(ValueError, match="missing_event_timestamp"):
        parse_timestamp("")
    with pytest.raises(ValueError, match="invalid_event_timestamp"):
        parse_timestamp("INVALID-TIMESTAMP-STRING")


def test_deterministic_session_id_stability():
    """Session ID generation is stable and deterministic."""
    id1 = deterministic_session_id("u1", "e1")
    id2 = deterministic_session_id("u1", "e1")
    assert id1 == id2
    assert id1.startswith("sess_")
