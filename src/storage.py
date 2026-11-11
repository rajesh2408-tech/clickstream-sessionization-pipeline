"""Pluggable Storage Layer supporting both SQLite and MySQL backends."""

import logging
import sqlite3
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple, Union
import pandas as pd

logger = logging.getLogger(__name__)

SQLITE_SCHEMA = """
PRAGMA foreign_keys = ON;
CREATE TABLE IF NOT EXISTS processed_events (
    event_id TEXT PRIMARY KEY,
    user_id TEXT NOT NULL,
    event_type TEXT NOT NULL,
    event_timestamp TEXT NOT NULL,
    ingestion_timestamp TEXT,
    session_id TEXT NOT NULL,
    event_seq_num INTEGER NOT NULL DEFAULT 1,
    time_since_prev_event_seconds REAL,
    source_file TEXT NOT NULL,
    last_run_id TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_events_user_time ON processed_events(user_id, event_timestamp);
CREATE INDEX IF NOT EXISTS idx_events_session ON processed_events(session_id);

CREATE TABLE IF NOT EXISTS user_sessions (
    session_id TEXT PRIMARY KEY,
    user_id TEXT NOT NULL,
    session_start TEXT NOT NULL,
    session_end TEXT NOT NULL,
    event_count INTEGER NOT NULL,
    duration_seconds INTEGER NOT NULL,
    first_event_type TEXT,
    last_event_type TEXT,
    is_bounce INTEGER NOT NULL DEFAULT 0,
    last_run_id TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_sessions_user_start ON user_sessions(user_id, session_start);

CREATE TABLE IF NOT EXISTS rejected_events (
    reject_id INTEGER PRIMARY KEY AUTOINCREMENT,
    run_id TEXT NOT NULL,
    source_file TEXT NOT NULL,
    event_id TEXT,
    raw_record TEXT NOT NULL,
    reject_reason TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_rejects_run ON rejected_events(run_id);

CREATE TABLE IF NOT EXISTS duplicate_events (
    duplicate_id INTEGER PRIMARY KEY AUTOINCREMENT,
    run_id TEXT NOT NULL,
    source_file TEXT NOT NULL,
    event_id TEXT NOT NULL,
    duplicate_reason TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_duplicates_run ON duplicate_events(run_id);

CREATE TABLE IF NOT EXISTS pipeline_runs (
    run_id TEXT PRIMARY KEY,
    source_file TEXT NOT NULL,
    start_time TEXT NOT NULL,
    end_time TEXT,
    input_count INTEGER NOT NULL DEFAULT 0,
    valid_unique_count INTEGER NOT NULL DEFAULT 0,
    duplicate_count INTEGER NOT NULL DEFAULT 0,
    rejected_count INTEGER NOT NULL DEFAULT 0,
    output_event_count INTEGER NOT NULL DEFAULT 0,
    session_count INTEGER NOT NULL DEFAULT 0,
    status TEXT NOT NULL,
    error_message TEXT
);
"""

MYSQL_SCHEMA = [
    """
    CREATE TABLE IF NOT EXISTS processed_events (
        event_id VARCHAR(64) PRIMARY KEY,
        user_id VARCHAR(64) NOT NULL,
        event_type VARCHAR(64) NOT NULL,
        event_timestamp VARCHAR(32) NOT NULL,
        ingestion_timestamp VARCHAR(32),
        session_id VARCHAR(64) NOT NULL,
        event_seq_num INT NOT NULL DEFAULT 1,
        time_since_prev_event_seconds DOUBLE,
        source_file VARCHAR(255) NOT NULL,
        last_run_id VARCHAR(64) NOT NULL,
        INDEX idx_events_user_time (user_id, event_timestamp),
        INDEX idx_events_session (session_id)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
    """,
    """
    CREATE TABLE IF NOT EXISTS user_sessions (
        session_id VARCHAR(64) PRIMARY KEY,
        user_id VARCHAR(64) NOT NULL,
        session_start VARCHAR(32) NOT NULL,
        session_end VARCHAR(32) NOT NULL,
        event_count INT NOT NULL,
        duration_seconds INT NOT NULL,
        first_event_type VARCHAR(64),
        last_event_type VARCHAR(64),
        is_bounce INT NOT NULL DEFAULT 0,
        last_run_id VARCHAR(64) NOT NULL,
        INDEX idx_sessions_user_start (user_id, session_start)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
    """,
    """
    CREATE TABLE IF NOT EXISTS rejected_events (
        reject_id INT AUTO_INCREMENT PRIMARY KEY,
        run_id VARCHAR(64) NOT NULL,
        source_file VARCHAR(255) NOT NULL,
        event_id VARCHAR(64),
        raw_record TEXT NOT NULL,
        reject_reason VARCHAR(255) NOT NULL,
        INDEX idx_rejects_run (run_id)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
    """,
    """
    CREATE TABLE IF NOT EXISTS duplicate_events (
        duplicate_id INT AUTO_INCREMENT PRIMARY KEY,
        run_id VARCHAR(64) NOT NULL,
        source_file VARCHAR(255) NOT NULL,
        event_id VARCHAR(64) NOT NULL,
        duplicate_reason VARCHAR(255) NOT NULL,
        INDEX idx_duplicates_run (run_id)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
    """,
    """
    CREATE TABLE IF NOT EXISTS pipeline_runs (
        run_id VARCHAR(64) PRIMARY KEY,
        source_file VARCHAR(255) NOT NULL,
        start_time VARCHAR(32) NOT NULL,
        end_time VARCHAR(32),
        input_count INT NOT NULL DEFAULT 0,
        valid_unique_count INT NOT NULL DEFAULT 0,
        duplicate_count INT NOT NULL DEFAULT 0,
        rejected_count INT NOT NULL DEFAULT 0,
        output_event_count INT NOT NULL DEFAULT 0,
        session_count INT NOT NULL DEFAULT 0,
        status VARCHAR(32) NOT NULL,
        error_message TEXT
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
    """,
]


class DatabaseConnection:
    """Unified database wrapper abstracting SQLite and MySQL differences."""

    def __init__(self, backend_type: str, raw_conn):
        self.backend_type = backend_type
        self.raw_conn = raw_conn

    def _prepare_sql(self, sql: str) -> str:
        """Adapts SQLite syntax to MySQL if necessary."""
        if self.backend_type == "mysql":
            # Convert SQLite placeholders '?' to MySQL '%s'
            # (unless inside literal strings, but simple replacement works for standard generated queries)
            converted = sql.replace("?", "%s")
            # Convert INSERT OR REPLACE to REPLACE INTO
            converted = converted.replace("INSERT OR REPLACE INTO", "REPLACE INTO")
            converted = converted.replace("INSERT OR IGNORE INTO", "INSERT IGNORE INTO")
            return converted
        return sql

    def execute(self, sql: str, params: Tuple = ()) -> Any:
        prepared = self._prepare_sql(sql)
        cursor = self.raw_conn.cursor()
        cursor.execute(prepared, params)
        return cursor

    def executemany(self, sql: str, params_list: List[Tuple]) -> Any:
        prepared = self._prepare_sql(sql)
        cursor = self.raw_conn.cursor()
        cursor.executemany(prepared, params_list)
        return cursor

    def commit(self):
        self.raw_conn.commit()

    def rollback(self):
        self.raw_conn.rollback()

    def close(self):
        try:
            self.raw_conn.close()
        except Exception:
            pass

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if exc_type:
            self.rollback()
        else:
            self.commit()


def connect_sqlite(db_path: Path) -> DatabaseConnection:
    """Initializes and returns a SQLite connection."""
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    conn.executescript(SQLITE_SCHEMA)
    conn.commit()
    return DatabaseConnection("sqlite", conn)


def connect_mysql(
    host: str = "localhost",
    port: int = 3306,
    user: str = "root",
    password: str = "",
    database: str = "streamsession",
) -> DatabaseConnection:
    """Initializes and returns a MySQL connection."""
    import mysql.connector

    # 1. Connect without database first to ensure database exists
    init_conn = mysql.connector.connect(
        host=host,
        port=port,
        user=user,
        password=password,
    )
    init_cursor = init_conn.cursor()
    init_cursor.execute(f"CREATE DATABASE IF NOT EXISTS `{database}` DEFAULT CHARACTER SET utf8mb4;")
    init_conn.commit()
    init_cursor.close()
    init_conn.close()

    # 2. Connect to the database
    conn = mysql.connector.connect(
        host=host,
        port=port,
        user=user,
        password=password,
        database=database,
        autocommit=False,
    )
    cursor = conn.cursor()
    for stmt in MYSQL_SCHEMA:
        cursor.execute(stmt)
    conn.commit()
    cursor.close()

    return DatabaseConnection("mysql", conn)


def connect(settings_or_path: Union[Any, Path]) -> DatabaseConnection:
    """
    Factory function returning an active database connection.
    If MySQL is requested but unavailable, falls back cleanly to SQLite.
    """
    from .config import Settings

    if isinstance(settings_or_path, Path):
        return connect_sqlite(settings_or_path)

    settings = settings_or_path
    if getattr(settings, "db_type", "sqlite") == "mysql":
        try:
            return connect_mysql(
                host=settings.mysql_host,
                port=settings.mysql_port,
                user=settings.mysql_user,
                password=settings.mysql_password,
                database=settings.mysql_database,
            )
        except Exception as e:
            logger.warning(
                f"MySQL connection to {settings.mysql_host}:{settings.mysql_port} failed ({e}). "
                f"Falling back to local SQLite at {settings.db_path}"
            )
            return connect_sqlite(settings.db_path)

    return connect_sqlite(settings.db_path)


def query_df(sql: str, settings_or_path: Union[Any, Path], params: Tuple = ()) -> pd.DataFrame:
    """Executes a SELECT query and returns a pandas DataFrame."""
    conn = connect(settings_or_path)
    try:
        prepared = conn._prepare_sql(sql)
        if conn.backend_type == "sqlite":
            return pd.read_sql_query(prepared, conn.raw_conn, params=params)
        else:
            return pd.read_sql(prepared, conn.raw_conn, params=params)
    except Exception as e:
        logger.error(f"query_df error: {e}")
        return pd.DataFrame()
    finally:
        conn.close()
