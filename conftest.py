import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import os
import tempfile

import pytest

os.environ.setdefault("REDIS_DB", "1")  # keep tests off the DB used by manual runs


@pytest.fixture
def temp_db_path():
    fd, path = tempfile.mkstemp(suffix=".db")
    os.close(fd)
    yield path
    for suffix in ("", "-wal", "-shm"):
        p = Path(path + suffix)
        if p.exists():
            p.unlink()
