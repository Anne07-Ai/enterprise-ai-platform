"""Verify broker-persisted offsets and replay with the owned consumer."""

from __future__ import annotations

import asyncio
from typing import TYPE_CHECKING
from uuid import uuid4

import pytest
from aiokafka import AIOKafkaConsumer, TopicPartition
from aiokafka.consumer.group_coordinator import GroupCoordinator
from aiokafka.errors import CommitFailedError, IllegalGenerationError
from app.core.config import get_settings
from app.infra import kafka
from app.infra.kafka_offsets import OffsetOwnedConsumer
from app.infra.kafka_shutdown import stop_consumer

if TYPE_CHECKING:
    from collections.abc import Mapping

    from aiokafka.structs import OffsetAndMetadata

pytestmark = pytest.mark.integration


@pytest.mark.parametrize("fault", ["cancelled", "broker_rejection", "commit_already_persisted"])
async def test_group_replays_record_after_interrupted_offset_request(
    monkeypatch: pytest.MonkeyPatch, fault: str
) -> None:
    topic = f"eaip-test-offset-{uuid4().hex}"
    servers = get_settings().kafka.bootstrap_servers
    group = f"{topic}-group"
    async with asyncio.timeout(20):
        for sequence in (1, 2, 3):
            await kafka.send(topic, {"sequence": sequence}, key="same-partition")

    started = asyncio.Event()
    send_commit = GroupCoordinator._do_commit_offsets

    async def inject_commit_fault(
        coordinator: GroupCoordinator,
        assignment: object,
        offsets: Mapping[TopicPartition, OffsetAndMetadata],
    ) -> None:
        if any(offset.offset == 2 for offset in offsets.values()):
            if fault == "broker_rejection":
                raise IllegalGenerationError()
            if fault == "commit_already_persisted":
                await send_commit(coordinator, assignment, offsets)
            started.set()
            await asyncio.Event().wait()
        await send_commit(coordinator, assignment, offsets)

    consumer = OffsetOwnedConsumer(
        topic,
        bootstrap_servers=servers,
        group_id=group,
        auto_offset_reset="earliest",
    )
    with monkeypatch.context() as patch:
        patch.setattr(GroupCoordinator, "_do_commit_offsets", inject_commit_fault)
        try:
            async with asyncio.timeout(20):
                await consumer.start()
                records = [await consumer.getone() for _ in range(3)]
                first, second, third = records
                partition = TopicPartition(topic, first.partition)
                assert [record.offset for record in records] == [0, 1, 2]
                assert all(record.partition == first.partition for record in records)
                await consumer.commit({partition: first.offset + 1})
                assert await consumer.committed(partition) == 1

                if fault == "broker_rejection":
                    with pytest.raises(CommitFailedError):
                        await consumer.commit({partition: second.offset + 1})
                else:
                    waiter = asyncio.create_task(consumer.commit({partition: second.offset + 1}))
                    try:
                        await started.wait()
                    finally:
                        waiter.cancel()
                        with pytest.raises(asyncio.CancelledError):
                            await waiter
                    assert consumer._offset_requests
                await consumer.stop_offset_commits(1.0)
                assert not consumer._offset_requests
                expected = third if fault == "commit_already_persisted" else second
                assert await consumer.committed(partition) == expected.offset
        finally:
            async with asyncio.timeout(10):
                await consumer.stop_offset_commits(1.0)
                await stop_consumer(consumer)

    restarted = AIOKafkaConsumer(
        topic,
        bootstrap_servers=servers,
        group_id=group,
        enable_auto_commit=False,
        auto_offset_reset="earliest",
    )
    try:
        async with asyncio.timeout(20):
            await restarted.start()
            replayed = await restarted.getone()
            assert replayed.offset == expected.offset
            assert replayed.partition == expected.partition
            assert replayed.value == expected.value
    finally:
        async with asyncio.timeout(10):
            await stop_consumer(restarted)
