from __future__ import annotations

import shutil
import tempfile
import contextlib


@contextlib.contextmanager
def temp_dir(suffix: str, dir: str):
    path = tempfile.mkdtemp(suffix=suffix, dir=dir)
    try:
        yield path
    finally:
        shutil.rmtree(path)
