"""Shutdown adapters for the locked aiokafka 0.14.0 implementation."""

from __future__ import annotations

import asyncio
from typing import TYPE_CHECKING

from aiokafka import AIOKafkaProducer, __version__
from aiokafka.errors import ProducerClosed
from aiokafka.producer.sender import Sender

from app.core.config import get_settings
from app.core.logging import get_logger

if TYPE_CHECKING:
    from collections.abc import Mapping

    from aiokafka import AIOKafkaConsumer, TopicPartition
    from aiokafka.client import AIOKafkaClient
    from aiokafka.producer.message_accumulator import MessageBatch

logger = get_logger(__name__)


def _check_aiokafka_version() -> None:
    if __version__ != "0.14.0":
        raise RuntimeError(
            "Kafka shutdown adapters require verification against this aiokafka version"
        )


class _OwnedSender(Sender):  # type: ignore[misc]  # aiokafka ships no type information
    """Keep the produce requests that Sender creates and otherwise only waits on."""

    def __init__(self, *args: object, **kwargs: object) -> None:
        super().__init__(*args, **kwargs)
        self._requests: set[asyncio.Task[object]] = set()
        self._stopping = False

    async def _send_produce_req(
        self, node_id: int, batches: Mapping[TopicPartition, MessageBatch]
    ) -> None:
        if self._stopping:
            return
        task = asyncio.current_task()
        assert task is not None
        self._requests.add(task)
        try:
            await super()._send_produce_req(node_id, batches)
        finally:
            self._requests.discard(task)

    async def stop_requests(self) -> None:
        self._stopping = True
        tasks = set(self._requests)
        if self.sender_task is not None:
            tasks.add(self.sender_task)
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)


class ShutdownProducer(AIOKafkaProducer):  # type: ignore[misc]  # aiokafka is untyped
    """Bound graceful flush and own the sender's cancellation path."""

    _sender: _OwnedSender
    _closed: bool

    def __init__(self, **kwargs: object) -> None:
        _check_aiokafka_version()
        super().__init__(**kwargs)
        original = self._sender
        self._sender = _OwnedSender(
            self.client,
            acks=original._acks,
            txn_manager=self._txn_manager,
            retry_backoff_ms=original._retry_backoff * 1000,
            message_accumulator=self._message_accumulator,
            request_timeout_ms=self._request_timeout_ms,
        )

    async def stop(self) -> None:
        if self._closed:
            return
        self._closed = True
        self._message_accumulator._closed = True
        settings = get_settings().kafka
        try:
            async with asyncio.timeout(settings.shutdown_grace_seconds):
                await self.flush()
        except TimeoutError:
            logger.warning("kafka.producer.shutdown_deadline_exceeded")
        finally:
            # flush() owns no helper tasks. Cancel the sender and its network
            # requests before failing queued deliveries, then close the client.
            try:
                async with asyncio.timeout(settings.shutdown_cleanup_seconds):
                    await self._sender.stop_requests()
            finally:
                self._fail_pending_deliveries()
                await _close_client(self.client)

    def _fail_pending_deliveries(self) -> None:
        accumulator = self._message_accumulator
        batches = [batch for queue in accumulator._batches.values() for batch in queue]
        batches.extend(accumulator._pending_batches)
        accumulator.fail_all(ProducerClosed())
        # aiokafka also fails internal drain waiters, even without a caller.
        # Observe those exceptions without changing what any awaiter receives.
        for batch in batches:
            for future in (batch.future, batch._drain_waiter):
                if future.done() and not future.cancelled():
                    future.exception()


async def _close_client(client: AIOKafkaClient) -> None:
    try:
        async with asyncio.timeout(get_settings().kafka.shutdown_cleanup_seconds):
            await client.close()
    except TimeoutError:
        logger.warning("kafka.client.cleanup_deadline_exceeded")
        raise


async def stop_consumer(consumer: AIOKafkaConsumer) -> None:
    """Bound group leave and release local resources if the broker is unavailable."""
    settings = get_settings().kafka
    deadline = asyncio.timeout(settings.shutdown_cleanup_seconds)
    try:
        async with deadline:
            await consumer.stop()
    except TimeoutError:
        logger.warning("kafka.consumer.shutdown_deadline_exceeded")
        await _abort_consumer(consumer)
        if not deadline.expired():
            raise
    except asyncio.CancelledError:
        # aiokafka can propagate a child fetch task's cancellation even
        # though this shutdown task was never cancelled.
        task = asyncio.current_task()
        externally_cancelled = task is not None and task.cancelling() > 0
        try:
            await _abort_consumer(consumer)
        except Exception:
            if not externally_cancelled:
                raise
            logger.warning("kafka.consumer.cleanup_failed")
        if externally_cancelled:
            raise
    except Exception:
        try:
            await _abort_consumer(consumer)
        except Exception:
            logger.warning("kafka.consumer.cleanup_failed")
        raise


async def _abort_consumer(consumer: AIOKafkaConsumer) -> None:
    _check_aiokafka_version()
    # stop() sets _closed before waiting for its coordinator and fetcher.
    # Their tasks still need joining even if stop() was cancelled partway.
    coordinator = consumer._coordinator
    fetcher = consumer._fetcher
    tasks = {
        task
        for task in (
            getattr(coordinator, "_coordination_task", None),
            getattr(coordinator, "_heartbeat_task", None),
            getattr(coordinator, "_commit_refresh_task", None),
            getattr(fetcher, "_fetch_task", None),
        )
        if task is not None
    }
    if fetcher is not None:
        tasks.update(fetcher._pending_tasks)
    for task in tasks:
        task.cancel()
    try:
        async with asyncio.timeout(get_settings().kafka.shutdown_cleanup_seconds):
            await asyncio.gather(*tasks, return_exceptions=True)
            if fetcher is not None and not fetcher._closed:
                await fetcher.close()
    finally:
        await _close_client(consumer._client)
