# Data Dictionary & Schema Specification

This document provides the formal data dictionary, schemas, grain, keys, nullability, and business rules for the **StreamSession Clickstream Pipeline**.

---

## 1. Input CSV Files

### A. Raw Events (`data/input/clickstream_events.csv`)
- **Grain**: One record per raw interaction event.

| Field Name | Type | Nullable | Key | Description |
| :--- | :--- | :--- | :--- | :--- |
| `event_id` | String | No | PK Candidate | Unique interaction ID. Missing values are quarantined (`missing_event_id`). |
| `user_id` | String | No | FK | User identifier. Missing values are quarantined (`missing_user_id`). |
| `event_type` | String | No | Dimension | Action name (e.g. `page_view`, `product_view`, `add_to_cart`, `purchase`, `login`). |
| `event_timestamp` | String | No | Dimension | ISO-8601 UTC timestamp. Missing or unparseable timestamps are quarantined. |
| `ingestion_timestamp` | String | Yes | Dimension | Ingestion occurrence timestamp. |

### B. Users Reference (`data/input/users.csv`)
- **Grain**: One record per registered/known user.

| Field Name | Type | Nullable | Key | Description |
| :--- | :--- | :--- | :--- | :--- |
| `user_id` | String | No | PK | Valid user identifier used for referential integrity checking. |

---

## 2. Target Database Tables (MySQL & SQLite)

### A. Processed Events (`processed_events`)
- **Grain**: One record per valid, deduplicated event assigned to a session.

| Column | MySQL Type | SQLite Type | Nullable | Key | Description |
| :--- | :--- | :--- | :--- | :--- | :--- |
| `event_id` | VARCHAR(64) | TEXT | No | PK | Unique event identifier. |
| `user_id` | VARCHAR(64) | TEXT | No | FK | User identifier. |
| `event_type` | VARCHAR(64) | TEXT | No | Dimension | Validated event action type. |
| `event_timestamp` | VARCHAR(32) | TEXT | No | Dimension | Normalized ISO-8601 UTC timestamp. |
| `ingestion_timestamp` | VARCHAR(32) | TEXT | Yes | Dimension | Ingestion timestamp. |
| `session_id` | VARCHAR(64) | TEXT | No | FK | Deterministic session ID (`sess_<hash>`). |
| `event_seq_num` | INT | INTEGER | No | Measure | 1-based chronological sequence within session. |
| `time_since_prev_event_seconds` | DOUBLE | REAL | Yes | Measure | Time delta ($\Delta t$) from preceding event in same session. |
| `source_file` | VARCHAR(255) | TEXT | No | Dimension | Input CSV file name. |
| `last_run_id` | VARCHAR(64) | TEXT | No | Audit FK | Pipeline run ID that processed/updated this event. |

### B. User Sessions (`user_sessions`)
- **Grain**: One record per computed user interaction session.

| Column | MySQL Type | SQLite Type | Nullable | Key | Description |
| :--- | :--- | :--- | :--- | :--- | :--- |
| `session_id` | VARCHAR(64) | TEXT | No | PK | Deterministic session ID. |
| `user_id` | VARCHAR(64) | TEXT | No | FK | User identifier. |
| `session_start` | VARCHAR(32) | TEXT | No | Dimension | Timestamp of first event in session. |
| `session_end` | VARCHAR(32) | TEXT | No | Dimension | Timestamp of last event in session. |
| `event_count` | INT | INTEGER | No | Measure | Total number of events in this session. |
| `duration_seconds` | INT | INTEGER | No | Measure | Total elapsed duration ($t_{\text{end}} - t_{\text{start}}$). |
| `first_event_type` | VARCHAR(64) | TEXT | Yes | Dimension | Event type of first interaction. |
| `last_event_type` | VARCHAR(64) | TEXT | Yes | Dimension | Event type of final interaction. |
| `is_bounce` | INT | INTEGER | No | Flag | 1 if single event or 0s duration, else 0. |
| `last_run_id` | VARCHAR(64) | TEXT | No | Audit FK | Pipeline run ID. |

### C. Quarantined Rejects (`rejected_events`)
- **Grain**: One record per rejected/invalid event row.

| Column | MySQL Type | SQLite Type | Nullable | Key | Description |
| :--- | :--- | :--- | :--- | :--- | :--- |
| `reject_id` | INT AUTO_INCREMENT | INTEGER PK AUTOINCREMENT | No | PK | Unique reject serial identifier. |
| `run_id` | VARCHAR(64) | TEXT | No | Audit FK | Pipeline run ID. |
| `source_file` | VARCHAR(255) | TEXT | No | Dimension | Source CSV filename. |
| `event_id` | VARCHAR(64) | TEXT | Yes | Dimension | Raw event ID (if present). |
| `raw_record` | TEXT | TEXT | No | Payload | Serialized raw JSON payload preserved verbatim. |
| `reject_reason` | VARCHAR(255) | TEXT | No | Dimension | Reason code (`missing_event_timestamp`, `unknown_user_reference`, etc.). |

### D. Duplicate Events (`duplicate_events`)
- **Grain**: One record per duplicate event occurrence.

| Column | MySQL Type | SQLite Type | Nullable | Key | Description |
| :--- | :--- | :--- | :--- | :--- | :--- |
| `duplicate_id` | INT AUTO_INCREMENT | INTEGER PK AUTOINCREMENT | No | PK | Unique duplicate serial identifier. |
| `run_id` | VARCHAR(64) | TEXT | No | Audit FK | Pipeline run ID. |
| `source_file` | VARCHAR(255) | TEXT | No | Dimension | Source CSV filename. |
| `event_id` | VARCHAR(64) | TEXT | No | Dimension | Duplicate event ID. |
| `duplicate_reason` | VARCHAR(255) | TEXT | No | Dimension | Duplicate classification (`exact_duplicate_in_batch`, `exact_duplicate_in_database`, etc.). |

### E. Pipeline Execution Runs (`pipeline_runs`)
- **Grain**: One record per pipeline execution run.

| Column | MySQL Type | SQLite Type | Nullable | Description |
| :--- | :--- | :--- | :--- | :--- |
| `run_id` | VARCHAR(64) | TEXT | No | Unique execution run identifier. |
| `source_file` | VARCHAR(255) | TEXT | No | Ingested file name. |
| `start_time` | VARCHAR(32) | TEXT | No | ISO-8601 UTC start time. |
| `end_time` | VARCHAR(32) | TEXT | Yes | ISO-8601 UTC completion time. |
| `input_count` | INT | INTEGER | No | Total raw records ingested. |
| `valid_unique_count` | INT | INTEGER | No | Newly accepted clean records. |
| `duplicate_count` | INT | INTEGER | No | Duplicate records detected. |
| `rejected_count` | INT | INTEGER | No | Quarantined records. |
| `output_event_count` | INT | INTEGER | No | Cumulative processed events in database. |
| `session_count` | INT | INTEGER | No | Cumulative user sessions in database. |
| `status` | VARCHAR(32) | TEXT | No | Execution status (`SUCCESS`, `PARTIAL_SUCCESS`, `FAILED`). |
| `error_message` | TEXT | TEXT | Yes | Failure stack trace if failed. |
