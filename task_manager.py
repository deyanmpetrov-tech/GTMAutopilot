"""
task_manager.py — Thread-safe task registry with TTL garbage collection.

Replaces the bare global ``tasks = {}`` dict in api.py with a proper
TaskManager that provides:
  - asyncio.Lock for safe concurrent read/write
  - State-machine validation (illegal transitions rejected)
  - TTL-based GC loop (expired completed/errored tasks auto-deleted)
  - Max-concurrent-tasks capacity limit
"""
from __future__ import annotations

import asyncio
import time
import uuid
from dataclasses import dataclass, field
from enum import Enum
from typing import Any


# ── Task Status Enum ────────────────────────────────────────────────────────

class TaskStatus(str, Enum):
    PENDING = "pending"
    CRAWLING = "crawling"
    ANALYZING = "analyzing"
    REVIEW_REQUIRED = "review_required"
    COMPILING = "compiling"
    COMPLETED = "completed"
    ERROR = "error"


# Legal state transitions
_VALID_TRANSITIONS: dict[TaskStatus, set[TaskStatus]] = {
    TaskStatus.PENDING:          {TaskStatus.CRAWLING, TaskStatus.ERROR},
    TaskStatus.CRAWLING:         {TaskStatus.ANALYZING, TaskStatus.ERROR},
    TaskStatus.ANALYZING:        {TaskStatus.REVIEW_REQUIRED, TaskStatus.ERROR},
    TaskStatus.REVIEW_REQUIRED:  {TaskStatus.COMPILING, TaskStatus.ERROR},
    TaskStatus.COMPILING:        {TaskStatus.COMPLETED, TaskStatus.ERROR},
    TaskStatus.COMPLETED:        set(),   # terminal
    TaskStatus.ERROR:            {TaskStatus.PENDING},  # allow retry
}


# ── Task State Dataclass ────────────────────────────────────────────────────

@dataclass
class TaskState:
    task_id: str
    status: TaskStatus = TaskStatus.PENDING
    target_url: str = ""
    gtm_data: dict | None = None
    full_gtm: dict | None = None
    crawler_data: dict | None = None
    tracking_plan: dict | None = None
    compiled_gtm: dict | None = None
    validation_report: dict | None = None
    mechanical_ids: dict | None = None
    pipeline_warnings: dict | None = None
    orchestrator: Any = None          # PipelineOrchestrator reference
    error: str | None = None
    logs: list[str] = field(default_factory=list)
    created_at: float = field(default_factory=time.time)
    updated_at: float = field(default_factory=time.time)
    session_id: str = ""

    def add_log(self, msg: str) -> None:
        self.logs.append(msg)
        self.updated_at = time.time()

    def to_api_dict(self) -> dict:
        """Return JSON-serializable dict, excluding heavy/internal fields."""
        return {
            "task_id": self.task_id,
            "status": self.status.value,
            "target_url": self.target_url,
            "tracking_plan": self.tracking_plan,
            "crawler_data": self.crawler_data,
            "compiled_gtm": self.compiled_gtm,
            "validation_report": self.validation_report,
            "pipeline_warnings": self.pipeline_warnings,
            "error": self.error,
            "logs": self.logs,
            "created_at": self.created_at,
            "updated_at": self.updated_at,
        }


# ── Custom Exceptions ───────────────────────────────────────────────────────

class CapacityExceededError(Exception):
    """Raised when max_concurrent active tasks is reached."""


class TaskNotFoundError(Exception):
    """Raised when the requested task_id does not exist."""


class InvalidTransitionError(Exception):
    """Raised when a state transition violates the state machine."""


# ── Task Manager ────────────────────────────────────────────────────────────

class TaskManager:
    """
    Thread-safe async task registry with TTL garbage collection.

    Usage::

        manager = TaskManager(max_concurrent=5, ttl_seconds=3600)
        task = await manager.create_task(target_url="...", gtm_data={...})
        await manager.transition(task.task_id, TaskStatus.CRAWLING)

        # Run GC as a background asyncio task:
        asyncio.create_task(manager.gc_loop(interval=60))
    """

    def __init__(self, max_concurrent: int = 5, ttl_seconds: int = 3600):
        self._tasks: dict[str, TaskState] = {}
        self._lock = asyncio.Lock()
        self._max_concurrent = max_concurrent
        self._ttl_seconds = ttl_seconds

    # ── Create ──────────────────────────────────────────────────────────────

    async def create_task(self, target_url: str, gtm_data: dict) -> TaskState:
        """Create a new task if capacity allows.  Raises CapacityExceededError otherwise."""
        async with self._lock:
            active_count = sum(
                1 for t in self._tasks.values()
                if t.status not in (TaskStatus.COMPLETED, TaskStatus.ERROR)
            )
            if active_count >= self._max_concurrent:
                raise CapacityExceededError(
                    f"Maximum concurrent tasks ({self._max_concurrent}) reached. "
                    f"Try again later."
                )

            task_id = str(uuid.uuid4())
            task = TaskState(
                task_id=task_id,
                target_url=target_url,
                gtm_data=gtm_data,
                logs=["Task queued."],
            )
            self._tasks[task_id] = task
            return task

    # ── Read ────────────────────────────────────────────────────────────────

    async def get_task(self, task_id: str) -> TaskState | None:
        async with self._lock:
            return self._tasks.get(task_id)

    # ── State Transitions ───────────────────────────────────────────────────

    async def transition(self, task_id: str, new_status: TaskStatus) -> None:
        """Validate and execute a state transition."""
        async with self._lock:
            task = self._tasks.get(task_id)
            if not task:
                raise TaskNotFoundError(f"Task {task_id} not found")

            valid_next = _VALID_TRANSITIONS.get(task.status, set())
            if new_status not in valid_next:
                raise InvalidTransitionError(
                    f"Cannot transition from {task.status.value} to {new_status.value}. "
                    f"Valid: {[s.value for s in valid_next]}"
                )
            task.status = new_status
            task.updated_at = time.time()

    # ── Update Data Fields ──────────────────────────────────────────────────

    async def update_task(self, task_id: str, **kwargs) -> None:
        """Update arbitrary data fields on a task (not status — use transition())."""
        async with self._lock:
            task = self._tasks.get(task_id)
            if not task:
                raise TaskNotFoundError(f"Task {task_id} not found")
            for key, value in kwargs.items():
                if hasattr(task, key) and key != "status":
                    setattr(task, key, value)
            task.updated_at = time.time()

    # ── Garbage Collection ──────────────────────────────────────────────────

    async def gc_loop(self, interval: int = 60) -> None:
        """Background GC loop.  Run as ``asyncio.create_task(manager.gc_loop())``."""
        while True:
            await asyncio.sleep(interval)
            now = time.time()
            async with self._lock:
                expired = [
                    tid for tid, task in self._tasks.items()
                    if now - task.created_at > self._ttl_seconds
                    and task.status in (TaskStatus.COMPLETED, TaskStatus.ERROR)
                ]
                for tid in expired:
                    del self._tasks[tid]

    # ── Stats ───────────────────────────────────────────────────────────────

    async def stats(self) -> dict:
        async with self._lock:
            return {
                "total_tasks": len(self._tasks),
                "active_tasks": sum(
                    1 for t in self._tasks.values()
                    if t.status not in (TaskStatus.COMPLETED, TaskStatus.ERROR)
                ),
                "max_concurrent": self._max_concurrent,
            }
