"""Synthetic clickstream CSV dataset generator with controlled anomalies."""

import argparse
import csv
import random
from datetime import datetime, timedelta, timezone
from pathlib import Path

EVENT_TYPES = [
    "page_view",
    "product_view",
    "search",
    "add_to_cart",
    "checkout_start",
    "purchase",
    "login",
    "logout",
]


def generate(rows: int, seed: int, output: Path):
    """Generates synthetic clickstream CSV events and users CSV."""
    random.seed(seed)
    output.parent.mkdir(parents=True, exist_ok=True)

    user_count = max(5, rows // 20)
    users = [f"U{100 + i}" for i in range(1, user_count + 1)]

    # 1. Write users reference CSV
    users_file = output.parent / "users.csv"
    with users_file.open("w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["user_id"])
        writer.writerows([[u] for u in users])

    start = datetime(2026, 11, 10, 10, 0, 0, tzinfo=timezone.utc)
    records = []

    # Generate normal user sessions
    for i in range(rows):
        user = random.choice(users)
        event_dt = start + timedelta(minutes=random.randint(0, 60 * 24 * 7), seconds=random.randint(0, 59))
        ingestion_dt = event_dt + timedelta(seconds=random.randint(1, 60))
        records.append({
            "event_id": f"E{i+1:04d}",
            "user_id": user,
            "event_type": random.choice(EVENT_TYPES),
            "event_timestamp": event_dt.strftime("%Y-%m-%dT%H:%M:%SZ"),
            "ingestion_timestamp": ingestion_dt.strftime("%Y-%m-%dT%H:%M:%SZ"),
        })

    # Inject controlled anomalies and edge cases
    if rows >= 10:
        # 1. Missing timestamp
        records[1]["event_timestamp"] = ""
        # 2. Unknown user reference (not in users.csv)
        records[3]["user_id"] = "UNKNOWN_GUEST"
        # 3. Duplicate event ID
        records[5] = dict(records[4])
        # 4. Out of order events (swap timestamps of adjacent events for same user)
        records[6]["user_id"] = records[7]["user_id"]
        records[6]["event_timestamp"] = (start + timedelta(minutes=25)).strftime("%Y-%m-%dT%H:%M:%SZ")
        records[7]["event_timestamp"] = (start + timedelta(minutes=10)).strftime("%Y-%m-%dT%H:%M:%SZ")

    with output.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=["event_id", "user_id", "event_type", "event_timestamp", "ingestion_timestamp"],
        )
        writer.writeheader()
        writer.writerows(records)

    print(f"Generated {rows} events at {output}")
    print(f"Generated user reference file at {users_file}")


def main():
    parser = argparse.ArgumentParser(description="Synthetic Clickstream Data Generator")
    parser.add_argument("--rows", type=int, default=1000, help="Number of events to generate")
    parser.add_argument("--seed", type=int, default=42, help="Random seed for reproducibility")
    parser.add_argument("--output", default="data/input/clickstream_events.csv", help="Output CSV path")
    args = parser.parse_args()

    generate(args.rows, args.seed, Path(args.output))


if __name__ == "__main__":
    main()
