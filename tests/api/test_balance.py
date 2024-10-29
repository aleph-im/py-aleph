import pytest

from aleph.db.models import AlephBalanceDb
from aleph.jobs.process_pending_messages import PendingMessageProcessor

MESSAGES_URI = "/api/v0/addresses/0x9319Ad3B7A8E0eE24f2E639c40D8eD124C5520Ba/balance"


@pytest.mark.asyncio
async def test_get_balance(
    ccn_api_client,
    message_processor: PendingMessageProcessor,
    instance_message_with_volumes_in_db,
    fixture_instance_message,
    user_balance: AlephBalanceDb,
):
    pipeline = message_processor.make_pipeline()
    # Exhaust the iterator
    _ = [message async for message in pipeline]

    assert fixture_instance_message.item_content

    response = await ccn_api_client.get(MESSAGES_URI)
    assert response.status == 200, await response.text()
    data = await response.json()
    assert data["balance"] == str(user_balance.balance)
    assert data["locked_amount"] == "2002.46666666666669698315672576427459716796875"
