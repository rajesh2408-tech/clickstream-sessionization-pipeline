# Engineering Design Notes & Architectural Decisions

## 1. Problem Overview & Scope

The objective is to ingest raw clickstream interaction events from CSV, validate records against business rules and reference integrity, identify duplicates, and partition events into user visits/sessions using an **event-time inactivity gap** (default 30 minutes / 1,800 seconds).

The solution supports both **MySQL** (for production environments) and **SQLite** (for local development), outputs CSV reports and reconciliation metrics, and provides an interactive Streamlit UI featuring the **Session & Journey Explorer**.

---

## 2. Inactivity Gap & Boundary Conditions

### A. Event-Time vs. Processing-Time
Sessions are strictly bounded by true event occurrence timestamp (`event_timestamp`), not ingestion/arrival time. Mobile interactions buffered offline or delayed over network connections are properly reconstructed into cohesive user sessions.

### B. Out-of-Order Handling
Before computing session gaps, all historical events for affected users are sorted by:
$$\text{Sort Key} = (\text{user\_id}, \text{event\_timestamp}, \text{event\_id})$$
This guarantees deterministic ordering even if two events have identical timestamps.

### C. Boundary Rules
Let $\Delta t$ be the difference in seconds between consecutive events for a user:
1. $\Delta t \le 1,800\text{s}$ (30 minutes): Event continues the current session.
2. $\Delta t > 1,800\text{s}$ (e.g. 1,801s / 31 minutes): The prior session is closed, and a new session begins.
3. **Exact Boundary Value**: An event arriving at **exactly 30 minutes** ($\Delta t = 1,800.0\text{s}$) remains in the same session.
4. **Single-Event Sessions (Bounces)**: Handled cleanly with `duration_seconds = 0`, `event_count = 1`, and `is_bounce = 1`.

---

## 3. Pluggable Storage Architecture: MySQL & SQLite

To satisfy both local zero-configuration execution and production relational database requirements, the storage layer (`src/storage.py`) provides a unified database abstraction:

| Feature | Local SQLite Backend | MySQL Backend |
| :--- | :--- | :--- |
| **Driver** | Built-in `sqlite3` | `mysql-connector-python` |
| **Target Use** | Local development, unit tests, automated CI | Team environments, centralized warehouse |
| **Schema DDL** | `TEXT`, `INTEGER PRIMARY KEY AUTOINCREMENT` | `VARCHAR`, `INT AUTO_INCREMENT`, `InnoDB` |
| **Idempotency** | `PRAGMA foreign_keys = ON; INSERT OR REPLACE` | `REPLACE INTO` / `INSERT ON DUPLICATE KEY` |
| **Fallback** | Active default | Automatic fallback if MySQL is unreachable |

---

## 4. Referential Integrity & Data Quality Quarantine

1. **Foreign Key Reference**:
   - When `data/input/users.csv` is provided, every incoming event is checked against known `user_id` values.
   - Events with unknown users (e.g. `UNKNOWN_GUEST`) are quarantined with `reject_reason = 'unknown_user_reference'`.
2. **Schema & Field Checks**:
   - Missing `event_id`, `user_id`, or `event_type` are quarantined.
   - Missing or unparseable `event_timestamp` values are quarantined.
   - Corrupt width/extra fields are caught without failing the batch.
3. **Traceability**:
   - The verbatim JSON representation of the raw CSV record is preserved in `rejected_events.raw_record` for auditing.

---

## 5. Idempotency & Safe Reruns

To prevent duplicate sessions across repeated pipeline runs:
1. **Deterministic Session IDs**:
   $$\text{session\_id} = \text{"sess\_"} + \text{SHA256}(\text{user\_id} \mid \text{first\_event\_id})[0:16]$$
   Session IDs are derived from immutable natural attributes. Rerunning identical events produces the exact same session IDs.
2. **Database Duplicate Recognition**:
   - Events already present in `processed_events` are recognized during ingestion and flagged in `duplicate_events` rather than inserted twice.
   - User sessions are safely recomputed across the user's historical event log in a single transaction.

---

## 6. Mathematical Reconciliation Invariants

The pipeline enforces two mathematical equations in `reports/reconciliation_report.csv`:
1. **Source Balance**:
   $$\text{Input Rows} = \text{New Valid Events} + \text{Duplicate Occurrences} + \text{Rejected Occurrences}$$
2. **Session Conservation**:
   $$\text{Processed Events in Database} = \sum_{\text{sessions}} \text{event\_count}$$
If either delta $\neq 0$, the report flags `DISCREPANCY`.
