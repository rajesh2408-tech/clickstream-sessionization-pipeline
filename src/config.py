"""Configuration settings for the StreamSession clickstream pipeline."""

import os
from dataclasses import dataclass
from pathlib import Path


@dataclass
class Settings:
    """Pipeline runtime configuration supporting CSV inputs, SQLite, and MySQL."""

    input_path: Path = Path("data/input/clickstream_events.csv")
    db_type: str = os.getenv("DB_TYPE", "sqlite").lower()  # "sqlite" or "mysql"
    db_path: Path = Path("database/streamsession.db")
    
    # MySQL connection options (defaults to standard local MySQL)
    mysql_host: str = os.getenv("MYSQL_HOST", "localhost")
    mysql_port: int = int(os.getenv("MYSQL_PORT", "3306"))
    mysql_user: str = os.getenv("MYSQL_USER", "root")
    mysql_password: str = os.getenv("MYSQL_PASSWORD", "")
    mysql_database: str = os.getenv("MYSQL_DATABASE", "streamsession")

    timeout_minutes: int = 30
    users_path: Path | None = Path("data/input/users.csv")
    output_dir: Path = Path("reports")

    def validate(self):
        if self.timeout_minutes < 0:
            raise ValueError("timeout_minutes must be zero or greater")
        if self.db_type not in ("sqlite", "mysql"):
            raise ValueError(f"Unsupported db_type: {self.db_type}. Use 'sqlite' or 'mysql'.")
