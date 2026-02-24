"""
cleanup.py — Session artifact cleanup utilities.

Ensures debug screenshots, session-scoped caches, and other temporary
artifacts are wiped from disk after a pipeline run completes or fails.
"""
from __future__ import annotations

import asyncio
import os
import shutil

_BASE_DEBUG_DIR = ".debug"
_BASE_CACHE_DIR = ".cache"


def cleanup_session(session_id: str) -> None:
    """
    Synchronous cleanup of all artifacts for a session.

    Safe to call multiple times — uses ignore_errors so it never raises.
    """
    dirs_to_clean = [
        os.path.join(_BASE_DEBUG_DIR, session_id),
        os.path.join(_BASE_CACHE_DIR, session_id),
    ]
    for d in dirs_to_clean:
        if os.path.isdir(d):
            shutil.rmtree(d, ignore_errors=True)


async def async_cleanup_session(session_id: str) -> None:
    """Async wrapper for use in FastAPI background tasks or orchestrator finally blocks."""
    loop = asyncio.get_running_loop()
    await loop.run_in_executor(None, cleanup_session, session_id)
