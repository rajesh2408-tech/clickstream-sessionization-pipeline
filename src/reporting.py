"""Export reporting tables to CSV and reconciliation JSON summary."""

import csv
import json
from pathlib import Path
from typing import Union
import pandas as pd
from .config import Settings
from .storage import connect


def export_reports(settings_or_path: Union[Settings, Path], output_dir: Path):
    """
    Exports database tables into CSV reports and creates reconciliation_report.json.
    """
    output_dir.mkdir(parents=True, exist_ok=True)
    conn = connect(settings_or_path)

    try:
        tables = [
            ("processed_events", "processed_events.csv"),
            ("user_sessions", "user_sessions.csv"),
            ("rejected_events", "rejected_events.csv"),
            ("duplicate_events", "duplicate_events.csv"),
            ("pipeline_runs", "pipeline_runs.csv"),
        ]

        for table_name, file_name in tables:
            cursor = conn.execute(f"SELECT * FROM {table_name}")
            rows = cursor.fetchall()
            target_path = output_dir / file_name

            if rows:
                # Extract column names
                if hasattr(cursor, "description") and cursor.description:
                    col_names = [d[0] for d in cursor.description]
                elif hasattr(rows[0], "keys"):
                    col_names = list(rows[0].keys())
                else:
                    col_names = [f"col_{i}" for i in range(len(rows[0]))]

                with target_path.open("w", newline="", encoding="utf-8") as f:
                    writer = csv.writer(f)
                    writer.writerow(col_names)
                    for r in rows:
                        writer.writerow(list(r))
            else:
                target_path.write_text("", encoding="utf-8")

        # Mathematical reconciliation checks
        def get_scalar(sql):
            cur = conn.execute(sql)
            res = cur.fetchone()
            if res is None:
                return 0
            try:
                return res[0] or 0
            except Exception:
                return 0

        total_input = get_scalar("SELECT COALESCE(SUM(input_count), 0) FROM pipeline_runs")
        total_valid = get_scalar("SELECT COALESCE(SUM(valid_unique_count), 0) FROM pipeline_runs")
        total_duplicates = get_scalar("SELECT COUNT(*) FROM duplicate_events")
        total_rejects = get_scalar("SELECT COUNT(*) FROM rejected_events")
        total_processed_events = get_scalar("SELECT COUNT(*) FROM processed_events")
        total_sessions = get_scalar("SELECT COUNT(*) FROM user_sessions")
        sum_session_events = get_scalar("SELECT COALESCE(SUM(event_count), 0) FROM user_sessions")

        # Verification Invariants:
        # Invariant 1: Ingested = Accepted + Duplicates + Rejects
        source_delta = total_input - (total_valid + total_duplicates + total_rejects)
        is_source_balanced = (source_delta == 0)

        # Invariant 2: Processed Events in database == sum(event_count in user_sessions)
        session_events_delta = total_processed_events - sum_session_events
        is_session_balanced = (session_events_delta == 0)

        status = "BALANCED" if (is_source_balanced and is_session_balanced) else "DISCREPANCY"

        # Write reconciliation_report.csv
        recon_rows = [
            {"metric": "status", "value": status},
            {"metric": "cumulative_run_input_rows", "value": total_input},
            {"metric": "new_unique_events_accepted", "value": total_valid},
            {"metric": "duplicate_occurrences", "value": total_duplicates},
            {"metric": "rejected_occurrences", "value": total_rejects},
            {"metric": "total_processed_events", "value": total_processed_events},
            {"metric": "total_user_sessions", "value": total_sessions},
            {"metric": "sum_session_event_counts", "value": sum_session_events},
            {"metric": "source_equation_formula", "value": "input == valid_new + duplicates + rejects"},
            {"metric": "source_equation_balanced", "value": str(is_source_balanced)},
            {"metric": "source_equation_delta", "value": source_delta},
            {"metric": "session_equation_formula", "value": "processed_events == sum(event_count in user_sessions)"},
            {"metric": "session_equation_balanced", "value": str(is_session_balanced)},
            {"metric": "session_equation_delta", "value": session_events_delta},
        ]
        with (output_dir / "reconciliation_report.csv").open("w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=["metric", "value"])
            writer.writeheader()
            writer.writerows(recon_rows)

        # Remove any legacy json file if present
        legacy_json = output_dir / "reconciliation_report.json"
        if legacy_json.exists():
            legacy_json.unlink()

    finally:
        conn.close()
