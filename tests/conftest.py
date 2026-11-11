"""Pytest fixtures and configuration."""

import shutil
import tempfile
from pathlib import Path
import pytest
from src.config import Settings


@pytest.fixture
def temp_workspace():
    """Create a temporary directory for test storage isolated from real data."""
    temp_dir = Path(tempfile.mkdtemp(prefix="test_pipeline_"))
    yield temp_dir
    shutil.rmtree(temp_dir, ignore_errors=True)


@pytest.fixture
def test_settings(temp_workspace):
    """Provides an isolated Settings instance pointing to the temp workspace."""
    return Settings(
        input_path=temp_workspace / "clickstream_events.csv",
        db_type="sqlite",
        db_path=temp_workspace / "streamsession.db",
        timeout_minutes=30,
        users_path=temp_workspace / "users.csv",
        output_dir=temp_workspace / "reports",
    )
