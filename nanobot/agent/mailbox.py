"""Mailbox primitives for manager-worker task coordination."""

from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass, field
from typing import Any

TaskState = str  # running | completed | failed | cancelled
MailboxReadState = str  # ready | running | not_found | consumed | timeout


@dataclass(slots=True)
class TaskRequest:
    """Task request recorded when the manager dispatches a worker."""

    task_id: str
    session_key: str
    label: str
    task: str
    origin: dict[str, Any] = field(default_factory=dict)
    created_at: float = field(default_factory=time.time)


@dataclass(slots=True)
class TaskResult:
    """Worker result written to the manager mailbox."""

    task_id: str
    session_key: str
    label: str
    task: str
    status: str
    content: str
    sender: str = "subagent"
    completed_at: float = field(default_factory=time.time)
    dedupe_key: str | None = None
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass(slots=True)
class TaskSnapshot:
    """Read-only view of a task in the mailbox."""

    task_id: str
    session_key: str
    label: str
    task: str
    state: TaskState
    created_at: float
    completed_at: float | None = None
    consumed_at: float | None = None
    result_status: str | None = None
    error: str | None = None


@dataclass(slots=True)
class MailboxRead:
    """Result of a mailbox wait/consume operation."""

    state: MailboxReadState
    task: TaskSnapshot | None = None
    result: TaskResult | None = None


@dataclass(slots=True)
class _TaskRecord:
    request: TaskRequest
    state: TaskState = "running"
    result: TaskResult | None = None
    consumed_at: float | None = None
    completed_at: float | None = None
    error: str | None = None


class MailboxStore:
    """In-memory mailbox for worker task/result records.

    The store owns result deduplication and one-time result consumption. It is
    intentionally small; persistence can be added behind this protocol later
    without putting worker results back on the user pending queue.
    """

    def __init__(self) -> None:
        self._records: dict[str, _TaskRecord] = {}
        self._session_tasks: dict[str, set[str]] = {}
        self._dedupe_keys: set[str] = set()
        self._changed = asyncio.Condition()

    async def dispatch(self, request: TaskRequest) -> None:
        """Record that a task was dispatched."""
        async with self._changed:
            if request.task_id in self._records:
                return
            self._records[request.task_id] = _TaskRecord(request=request)
            self._session_tasks.setdefault(request.session_key, set()).add(request.task_id)
            self._changed.notify_all()

    async def record_result(self, result: TaskResult) -> bool:
        """Record a worker result.

        Returns ``True`` when this call writes a new result and ``False`` when
        the result is a duplicate or the task was already finalized.
        """
        async with self._changed:
            dedupe_key = result.dedupe_key or result.task_id
            if dedupe_key in self._dedupe_keys:
                return False

            record = self._records.get(result.task_id)
            if record is None:
                request = TaskRequest(
                    task_id=result.task_id,
                    session_key=result.session_key,
                    label=result.label,
                    task=result.task,
                    origin=dict(result.metadata),
                    created_at=result.completed_at,
                )
                record = _TaskRecord(request=request)
                self._records[result.task_id] = record
                self._session_tasks.setdefault(result.session_key, set()).add(result.task_id)
            elif record.result is not None:
                self._dedupe_keys.add(dedupe_key)
                return False

            record.result = result
            record.completed_at = result.completed_at
            record.state = self._state_for_result(result.status)
            record.error = result.content if result.status in {"error", "cancelled"} else None
            self._dedupe_keys.add(dedupe_key)
            self._changed.notify_all()
            return True

    async def mark_cancelled(
        self,
        task_id: str,
        *,
        session_key: str | None = None,
        reason: str = "Cancelled.",
    ) -> bool:
        """Mark a task cancelled and make the cancellation consumable once."""
        async with self._changed:
            record = self._records.get(task_id)
            if record is None:
                return False
            if session_key is not None and record.request.session_key != session_key:
                return False
            if record.result is not None:
                return False
            result = TaskResult(
                task_id=task_id,
                session_key=record.request.session_key,
                label=record.request.label,
                task=record.request.task,
                status="cancelled",
                content=reason,
                dedupe_key=task_id,
            )
            record.result = result
            record.completed_at = result.completed_at
            record.state = "cancelled"
            record.error = reason
            self._dedupe_keys.add(task_id)
            self._changed.notify_all()
            return True

    async def poll(
        self,
        session_key: str,
        *,
        task_id: str | None = None,
    ) -> list[TaskSnapshot]:
        """Return snapshots for one task or all tasks in a session."""
        async with self._changed:
            if task_id is not None:
                record = self._records.get(task_id)
                if record is None or record.request.session_key != session_key:
                    return []
                return [self._snapshot(record)]

            ids = self._session_tasks.get(session_key, set())
            snapshots = [
                self._snapshot(self._records[tid])
                for tid in ids
                if tid in self._records
            ]
            snapshots.sort(key=lambda item: (item.completed_at is None, item.created_at, item.task_id))
            return snapshots

    async def wait_for_result(
        self,
        session_key: str,
        *,
        task_id: str | None = None,
        timeout_seconds: float = 30.0,
    ) -> MailboxRead:
        """Wait for and consume a result once."""
        deadline = time.monotonic() + max(0.0, timeout_seconds)
        async with self._changed:
            while True:
                read = self._consume_ready_locked(session_key, task_id)
                if read.state != "running":
                    return read
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    return MailboxRead("timeout", task=read.task)
                try:
                    await asyncio.wait_for(self._changed.wait(), timeout=remaining)
                except asyncio.TimeoutError:
                    return MailboxRead("timeout", task=read.task)

    def _consume_ready_locked(
        self,
        session_key: str,
        task_id: str | None,
    ) -> MailboxRead:
        if task_id is not None:
            record = self._records.get(task_id)
            if record is None or record.request.session_key != session_key:
                return MailboxRead("not_found")
            snapshot = self._snapshot(record)
            if record.result is None:
                return MailboxRead("running", task=snapshot)
            if record.consumed_at is not None:
                return MailboxRead("consumed", task=snapshot, result=record.result)
            record.consumed_at = time.time()
            snapshot = self._snapshot(record)
            return MailboxRead("ready", task=snapshot, result=record.result)

        ids = self._session_tasks.get(session_key, set())
        records = [
            self._records[tid]
            for tid in ids
            if tid in self._records
        ]
        ready = [
            record
            for record in records
            if record.result is not None and record.consumed_at is None
        ]
        if ready:
            ready.sort(key=lambda record: (record.completed_at or record.request.created_at, record.request.task_id))
            record = ready[0]
            record.consumed_at = time.time()
            return MailboxRead("ready", task=self._snapshot(record), result=record.result)

        running = [record for record in records if record.result is None]
        if running:
            running.sort(key=lambda record: (record.request.created_at, record.request.task_id))
            return MailboxRead("running", task=self._snapshot(running[0]))
        if records:
            records.sort(key=lambda record: (record.completed_at or record.request.created_at, record.request.task_id))
            return MailboxRead("consumed", task=self._snapshot(records[-1]))
        return MailboxRead("not_found")

    @staticmethod
    def _state_for_result(status: str) -> TaskState:
        if status == "ok":
            return "completed"
        if status == "cancelled":
            return "cancelled"
        return "failed"

    @staticmethod
    def _snapshot(record: _TaskRecord) -> TaskSnapshot:
        result = record.result
        return TaskSnapshot(
            task_id=record.request.task_id,
            session_key=record.request.session_key,
            label=record.request.label,
            task=record.request.task,
            state=record.state,
            created_at=record.request.created_at,
            completed_at=record.completed_at,
            consumed_at=record.consumed_at,
            result_status=result.status if result is not None else None,
            error=record.error,
        )
