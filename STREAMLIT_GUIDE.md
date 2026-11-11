# StreamSession Dashboard User Guide

This guide walks you through using the Streamlit interactive dashboard (`app.py`).

---

## 1. Starting the Dashboard

Ensure your virtual environment is activated:
```bash
# Activate virtual environment (if not already active)
source .venv/bin/activate       # On macOS / Linux
# .venv\Scripts\Activate.ps1   # On Windows PowerShell

# Start Streamlit
streamlit run app.py
```
Open your browser at `http://localhost:8501`.

---

## 2. Using the Session & Journey Explorer (Key Feature)

The **Session & Journey Explorer** allows analysts to drill down from high-level user metrics into individual session events:

```
[ User Selector ] ──► Filters Sessions Table
                            │
                            ▼
                    [ Session ID Selector ] ──► Returns All Session Events
                                                         │
                                                         ▼
                                       - Event sequence numbers (1, 2, 3...)
                                       - Occurrence timestamps & event types
                                       - Time elapsed since previous event
                                       - Visual action flow: page_view ➔ add_to_cart
```

### Steps:
1. Navigate to the **🔍 Session & Journey Explorer** tab.
2. Under **Step 1: Select User**, choose any `user_id` from the dropdown (e.g. `U101`).
3. View the table of all sessions associated with that user (showing start time, duration, event count, and bounce status).
4. Under **Step 2: Select Session to Inspect Events**, pick a `session_id` (e.g. `sess_e001...`).
5. The dashboard instantly filters and returns:
   - **Session Timeline**: Exact start and end timestamps and total duration.
   - **Session Events Table**: Chronologically ordered events with sequence numbers, event type, and dwell time from previous event.
   - **Action Flow**: Visual journey breadcrumb (`page_view` ➔ `product_view` ➔ `add_to_cart`).
   - **CSV Export**: Download buttons to save the session events or user sessions to CSV.

---

## 3. Storage Engine: Toggling Between MySQL & SQLite

In the sidebar under **💾 Database Engine**:
- **SQLite (Local File)**: Runs instantly with zero setup, storing data in `database/streamsession.db`.
- **MySQL (Server)**:
  1. Select **MySQL (Server)**.
  2. Expand the **MySQL Connection Settings** box.
  3. Enter Host (`localhost`), Port (`3306`), User (`root`), Password, and Database name (`streamsession`).
  4. Click **🔌 Test MySQL Connection** to verify server connectivity.
  5. When you click **▶ Run Sessionization Pipeline**, the pipeline will write to and read directly from MySQL!

---

## 4. Ingestion & Synthetic Data Generation

- **Use Bundled Sample CSV**: Runs on `data/input/clickstream_events.csv` and `data/input/users.csv`.
- **Upload Custom CSV**: Upload your own CSV file through the UI with optional users reference file.
- **Inactivity Timeout**: Adjust the timeout slider (default 30 minutes).
- **Synthetic CSV Generator**: Select volume (50, 1,000, 10,000, 100,000 events) and random seed to generate realistic test data with controlled edge cases.

---

## 5. Other Dashboard Tabs

- **📈 Overview Analytics**: Session duration histograms, activity counts by event type, and top users by session count.
- **🧾 Processed Events**: Filter and search through all valid processed events across all users.
- **🛡️ Data Quality & Quarantine**: Breakdown of rejection reasons (`missing_event_timestamp`, `unknown_user_reference`, etc.) and duplicate classifications.
- **⚖️ Reconciliation & Audit**: Visual mathematical verification equations proving zero silent data loss.
- **🕒 Pipeline Runs**: Historical audit ledger with execution durations, row counts, and status badges.
- **💻 SQL Analytics Console**: Run ad-hoc SQL queries directly against MySQL or SQLite with preset query templates and CSV downloads.
