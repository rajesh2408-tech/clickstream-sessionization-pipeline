# StreamSession — Clickstream Event Sessionization Pipeline

[![Python](https://img.shields.io/badge/Python-3.10%2B-blue.svg)](https://www.python.org/)
[![Tests](https://img.shields.io/badge/Tests-11%20Passed-brightgreen.svg)]()
[![Input](https://img.shields.io/badge/Input-CSV-green.svg)]()
[![Database](https://img.shields.io/badge/Database-MySQL%20%7C%20SQLite-orange.svg)]()
[![Dashboard](https://img.shields.io/badge/UI-Streamlit-red.svg)](https://streamlit.io)

A production-grade, reproducible Data Engineering pipeline that transforms raw clickstream interaction events into user sessions using **event-time inactivity gaps**.

Featuring:
- **CSV Ingestion & Output**: Native CSV processing for events (`clickstream_events.csv`), user foreign keys (`users.csv`), and generated reports (`reports/*.csv`).
- **Pluggable Storage Engine (MySQL & SQLite)**: Supports MySQL server database storage (`mysql-connector-python`) with automatic local SQLite fallback for local development.
- **Session & Journey Explorer**: Interactive dashboard view allowing users to select a user, view all their sessions, drill down by session ID, and inspect all ordered interaction events, dwell times, and journey action flows.
- **Data Quality & Quarantine**: Dead-letter tracking for missing timestamps, invalid formats, malformed rows, and foreign key reference failures.
- **Idempotency & Safe Reruns**: Deterministic session ID hashing (`sess_<hash>`) preventing duplicate sessions or inflated metrics on repeated runs.
- **Automated Reconciliation**: Mathematical validation ensuring zero silent data loss.

---

## 1. Architecture Diagram & Workflow

```text
       CSV Sources: clickstream_events.csv + users.csv
                            │
                            ▼
                     [ Ingestion ]
                            │
                            ▼
          [ Schema & Row-Level Validation ]
                 │                    │
                 │ Valid              │ Invalid (missing ts, bad ref, malformed)
                 ▼                    ▼
       [ Duplicate Detection ]    rejected_events (Quarantine)
                 │
                 ▼
       [ Event History Storage ] (MySQL or SQLite)
                 │
                 ▼
       [ Event-Time Sessionization ]
       - Sort by (user_id, event_timestamp, event_id)
       - Inactivity gap > timeout (default 30 min)
       - Exact 30 min boundary remains in session
                 │
                 ▼
       [ Target Publishing ]
       - processed_events (with session_id, event_seq_num, dwell times)
       - user_sessions (session_start, session_end, duration, count, bounce)
                 │
                 ▼
       [ Operational Reporting & Reconciliation ]
       - reports/*.csv
       - reports/reconciliation_report.csv
       - Interactive Streamlit Dashboard
```

---

## 2. Quickstart & Setup

### Prerequisites
- Python 3.10 or newer (tested on Python 3.13)
- `pip` package manager

### Step 1: Create & Activate Virtual Environment

Create an isolated virtual environment in the project directory:

**On macOS / Linux:**
```bash
# Create the virtual environment
python3 -m venv .venv

# Activate the virtual environment
source .venv/bin/activate
```

**On Windows:**
```powershell
# Create the virtual environment
python -m venv .venv

# Activate the virtual environment (PowerShell)
.venv\Scripts\Activate.ps1
# Or in Command Prompt (cmd):
# .venv\Scripts\activate.bat
```

### Step 2: Install Dependencies
With the virtual environment activated, install all required dependencies:
```bash
pip install --upgrade pip
pip install -r requirements.txt
```

### Step 3: Run the Pipeline
Execute the clickstream sessionization pipeline on the sample CSV:
```bash
python3 main.py --input data/input/clickstream_events.csv --users data/input/users.csv
```

### Step 4: Run Automated Tests (11 Passed)
```bash
python3 -m pytest -v
```

### Step 5: Launch the Streamlit Interactive Dashboard
```bash
streamlit run app.py
```

---

## 3. Storage Options: MySQL & SQLite

The pipeline supports both **MySQL** (for multi-user/server deployments) and **SQLite** (for self-contained local runs):

### A. Local SQLite (Default)
Runs out of the box with zero configuration:
```bash
python3 main.py --db-type sqlite --db database/streamsession.db
```

### B. MySQL Database
To run against a MySQL server:
```bash
python3 main.py \
  --db-type mysql \
  --mysql-host localhost \
  --mysql-port 3306 \
  --mysql-user root \
  --mysql-password "your_password" \
  --mysql-database streamsession
```
*Note: In the Streamlit dashboard, you can toggle between SQLite and MySQL directly from the sidebar, configure connection parameters, and test the connection with one click.*

---

## 4. Key Features & Business Rules

### Event-Time Session Boundaries
- Events are sorted by `user_id`, `event_timestamp`, and `event_id` prior to window calculation to correctly order out-of-order events.
- An inactivity gap of $> 30\text{ minutes}$ closes the active session and begins a new session.
- **Boundary rule**: An event occurring at **exactly 30 minutes** ($1,800.0\text{s}$) remains in the same session.

### Referential Integrity
- If `users.csv` is provided, events with an unknown `user_id` are quarantined with reason `unknown_user_reference`.

### Deterministic Session IDs
- Session IDs are generated via:
  $$\text{session\_id} = \text{"sess\_"} + \text{SHA256}(\text{user\_id} \mid \text{first\_event\_id})[0:16]$$
- Rerunning the same input data produces identical session IDs, ensuring idempotency.

### Zero Silent Loss Reconciliation
Every run verifies the mathematical balance equation exported to `reports/reconciliation_report.csv`:
$$\text{Cumulative Run Inputs} = \text{Valid Accepted Events} + \text{Duplicate Occurrences} + \text{Rejected Occurrences}$$
$$\text{Processed Events in DB} = \sum_{\text{sessions}} \text{event\_count}$$

---

## 5. Streamlit Dashboard Highlights

Launch via `streamlit run app.py` to explore:
1. **🔍 Session & Journey Explorer**: Select any user, view their sessions, and click on any session ID to immediately return all events belonging to that session, sequence numbers, dwell times, and journey flows (`page_view ➔ product_view ➔ add_to_cart`).
2. **📈 Overview Analytics**: Duration histograms, activities by event type, and top users by session count.
3. **🧾 Processed Events**: Searchable table across event IDs, user IDs, and session IDs with one-click CSV export.
4. **🛡️ Data Quality & Quarantine**: Rejection reasons breakdown and verbatim raw record inspection.
5. **⚖️ Reconciliation & Audit**: Visual confirmation of data conservation equations with green status badges.
6. **🕒 Pipeline Runs**: Execution history with latency, status, and row count metrics.
7. **💻 SQL Analytics Console**: Execute custom SQL queries directly against MySQL or SQLite with preset templates and CSV downloads.

---

## 6. Project Directory Layout

```
├── app.py                         # Streamlit dashboard entrypoint
├── main.py                        # Pipeline CLI entrypoint
├── Makefile                       # Convenient workflow shortcuts (make run, test, dashboard)
├── requirements.txt               # Pinned dependencies
├── data/
│   └── input/
│       ├── clickstream_events.csv # Input events CSV
│       └── users.csv              # Reference users CSV
├── database/
│   └── streamsession.db           # Local SQLite database
├── reports/
│   ├── user_sessions.csv          # Exported session summaries CSV
│   ├── processed_events.csv       # Exported processed events CSV
│   ├── rejected_events.csv        # Quarantined rejects CSV
│   ├── duplicate_events.csv       # Duplicate records CSV
│   ├── pipeline_runs.csv          # Pipeline execution metrics CSV
│   └── reconciliation_report.csv  # Mathematical reconciliation audit CSV
├── scripts/
│   └── generate_data.py           # Scalable synthetic CSV generator
├── src/
│   ├── config.py                  # Settings (CSV paths, SQLite & MySQL configs)
│   ├── storage.py                 # Pluggable storage engine (MySQL & SQLite)
│   ├── sessionization.py          # Event-time sessionization logic
│   ├── pipeline.py                # CSV ingestion, validation, and session processing
│   └── reporting.py               # CSV reports and reconciliation exporter
└── tests/
    ├── conftest.py                # Pytest fixtures
    ├── test_sessionization.py     # Unit tests for boundary gaps and timestamps
    └── test_pipeline.py           # Integration tests for idempotency, quarantine, and schema
```
