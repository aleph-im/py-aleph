import pytest
from aleph.jobs.sync_unconfirmed_messages import list_unconfirmed_message_hashes
import datetime as dt


@pytest.mark.asyncio
async def test_list_unconfirmed_message_hashes(test_db, fixture_messages):
    # List all unconfirmed messages
    # TODO: update this by 3999, Dec 31st
    in_a_long_time = dt.datetime(4000, 1, 1)

    expected_hashes = {
        "9200cfab5950e5d173f07d7c61bb0524675d0305e808590e7d0a0752ce65f791",
        "4c33dd1ebf61bbb4342d8258b591fcd52cca73fd7c425542f78311d8f45ba274",
    }
    hashes = await list_unconfirmed_message_hashes(
        older_than=in_a_long_time.timestamp(), limit=1000
    )
    assert set(hashes) == expected_hashes

    # List only one message using older_than
    filtered_hashes = await list_unconfirmed_message_hashes(older_than=1652126600, limit=1000)
    assert filtered_hashes == ["9200cfab5950e5d173f07d7c61bb0524675d0305e808590e7d0a0752ce65f791"]

    # List 0 messages
    no_hashes = await list_unconfirmed_message_hashes(older_than=0, limit=1000)
    assert no_hashes == []
