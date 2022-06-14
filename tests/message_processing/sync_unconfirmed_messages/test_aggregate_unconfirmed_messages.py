import pytest

from aleph.jobs.sync_unconfirmed_messages import aggregate_unconfirmed_hashes
from aleph.model.messages import Message
from aleph.model.unconfirmed_messages import UnconfirmedMessage


@pytest.mark.asyncio
async def test_aggregate_unconfirmed_messages(test_db):
    await UnconfirmedMessage.collection.insert_many(
        [
            {"sender": "1", "hashes": ["123", "456", "789"], "reception_time": 1000000},
            {"sender": "2", "hashes": ["123", "789", "abc"], "reception_time": 1000000},
        ]
    )

    unconfirmed_message_sources = await aggregate_unconfirmed_hashes(from_time=0)
    expected_unconfirmed_sources = {
        "123": ["1", "2"],
        "456": ["1"],
        "789": ["1", "2"],
        "abc": ["2"],
    }

    assert unconfirmed_message_sources == expected_unconfirmed_sources


@pytest.mark.asyncio
async def test_aggregate_unconfirmed_messages_already_present(test_db):
    """
    Tests that messages already present on the local node are ignored when aggregating
    unconfirmed messages sent by the network.
    """

    await UnconfirmedMessage.collection.insert_many(
        [
            {"sender": "1", "hashes": ["123", "456", "789"], "reception_time": 1000000},
            {"sender": "2", "hashes": ["123", "789", "abc"], "reception_time": 1000000},
        ]
    )

    await Message.collection.insert_many([{"item_hash": "123"}, {"item_hash": "abc"}])

    unconfirmed_message_sources = await aggregate_unconfirmed_hashes(from_time=0)
    expected_unconfirmed_sources = {
        "456": ["1"],
        "789": ["1", "2"],
    }

    assert unconfirmed_message_sources == expected_unconfirmed_sources


@pytest.mark.asyncio
async def test_aggregate_unconfirmed_messages_ignore_old_data(test_db):
    """
    Tests that messages already present on the local node are ignored when aggregating
    unconfirmed messages sent by the network.
    """

    await UnconfirmedMessage.collection.insert_many(
        [
            {"sender": "1", "hashes": ["123", "456", "789"], "reception_time": 100000},
            {"sender": "2", "hashes": ["123", "789", "abc"], "reception_time": 1000000},
        ]
    )

    unconfirmed_message_sources = await aggregate_unconfirmed_hashes(from_time=1000000)
    expected_unconfirmed_sources = {
        "123": ["2"],
        "789": ["2"],
        "abc": ["2"],
    }

    assert unconfirmed_message_sources == expected_unconfirmed_sources
