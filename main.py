#!/usr/bin/env python3
"""Command-line entrypoint for StreamSession clickstream sessionization pipeline."""

import argparse
import json
import sys
from pathlib import Path

from src.config import Settings
from src.pipeline import run_pipeline
from src.reporting import export_reports


def main():
    parser = argparse.ArgumentParser(
        description="StreamSession Clickstream Event Sessionization Pipeline",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument("--input", default="data/input/clickstream_events.csv", help="Input events CSV file")
    parser.add_argument("--users", default="data/input/users.csv", help="Optional users reference CSV file")
    parser.add_argument("--no-user-reference", action="store_true", help="Disable foreign key check against users CSV")
    parser.add_argument("--timeout", type=int, default=30, help="Inactivity timeout gap in minutes")
    parser.add_argument("--db-type", choices=["sqlite", "mysql"], default="sqlite", help="Database backend engine")
    parser.add_argument("--db", default="database/streamsession.db", help="SQLite database path")
    parser.add_argument("--mysql-host", default="localhost", help="MySQL host")
    parser.add_argument("--mysql-port", type=int, default=3306, help="MySQL port")
    parser.add_argument("--mysql-user", default="root", help="MySQL username")
    parser.add_argument("--mysql-password", default="", help="MySQL password")
    parser.add_argument("--mysql-database", default="streamsession", help="MySQL database name")
    parser.add_argument("--output", default="reports", help="Directory to export CSV reports and reconciliation")

    args = parser.parse_args()

    users_p = None if args.no_user_reference else Path(args.users)
    settings = Settings(
        input_path=Path(args.input),
        db_type=args.db_type,
        db_path=Path(args.db),
        mysql_host=args.mysql_host,
        mysql_port=args.mysql_port,
        mysql_user=args.mysql_user,
        mysql_password=args.mysql_password,
        mysql_database=args.mysql_database,
        timeout_minutes=args.timeout,
        users_path=users_p,
        output_dir=Path(args.output),
    )

    try:
        result = run_pipeline(settings)
        export_reports(settings, settings.output_dir)
        print("\n" + "=" * 50)
        print("PIPELINE EXECUTION SUMMARY")
        print("=" * 50)
        print(json.dumps(result, indent=2))
        print(f"\nCSV reports and reconciliation exported to: {settings.output_dir.resolve()}")
    except Exception as exc:
        print(f"Pipeline execution failed: {type(exc).__name__}: {exc}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
