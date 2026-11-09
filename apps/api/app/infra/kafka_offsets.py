"""Own the offset requests shielded by the locked aiokafka consumer."""

from __future__ import annotations

import asyncio

from aiokafka import AIOKafkaConsumer, __version__
from aiokafka.errors import ConsumerStoppedError


class OffsetOwnedConsumer(AIOKafkaConsumer):  # type: ignore[misc]  # aiokafka is untyped
    """Manual-commit consumer whose inner offset requests can be joined on shutdown."""

    def __init__(self, *topics: str, group_id: str, **kwargs: object) -> None:
        if __version__ != "0.14.0":
            raise RuntimeError(
                "Kafka offset ownership requires verification against this aiokafka version"
            )
        if kwargs.pop("enable_auto_commit", False) is not False:
            raise ValueError("Kafka offset ownership requires manual commits")
        super().__init__(*topics, group_id=group_id, enable_auto_commit=False, **kwargs)
        self._offset_requests: set[asyncio.Task[object]] = set()
        self._offsets_stopping = False

    async def start(self) -> None:
        await super().start()
        coordinator = self._coordinator
        commit_offsets = coordinator._do_commit_offsets

        async def tracked_commit(assignment: object, offsets: object) -> None:
            # aiokafka schedules this coroutine in a shielded task. Check
            # before any I/O in case shutdown started before it ran.
            task = asyncio.current_task()
            assert task is not None
            task.add_done_callback(self._offset_request_done)
            if self._offsets_stopping:
                raise ConsumerStoppedError()
            self._offset_requests.add(task)
            await commit_offsets(assignment, offsets)

        coordinator._do_commit_offsets = tracked_commit

    def _offset_request_done(self, task: asyncio.Task[object]) -> None:
        self._offset_requests.discard(task)
        # The cancelled shield waiter may have detached before this request
        # failed. Observing the exception leaves it available to active callers.
        if not task.cancelled():
            task.exception()

    async def stop_offset_commits(self, remaining_seconds: float) -> None:
        """Gate, cancel and join offset I/O before stopping the Kafka client.

        Cancellation cannot withdraw a commit the broker already received.
        The worker calls commit only after its business transaction succeeds.
        """
        self._offsets_stopping = True
        requests = tuple(self._offset_requests)
        for task in requests:
            if not task.cancelling():
                task.cancel()
        if not requests:
            return

        _, pending = await asyncio.wait(requests, timeout=remaining_seconds)
        if pending:
            raise TimeoutError("Kafka offset requests exceeded the shutdown deadline")
