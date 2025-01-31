import pytest
from aleph_message.models import Chain

from aleph.db.models import AlephBalanceDb
from aleph.jobs.process_pending_messages import PendingMessageProcessor

MESSAGES_URI = "/api/v0/addresses/0x9319Ad3B7A8E0eE24f2E639c40D8eD124C5520Ba/balance"


@pytest.mark.asyncio
async def test_get_balance(
    ccn_api_client,
    message_processor: PendingMessageProcessor,
    fixture_product_prices_aggregate_in_db,
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
    assert data["balance"] == user_balance.balance

    assert data["locked_amount"] == 1002.4666666666666

    details = data["details"]
    assert details["ETH"] == user_balance.balance


@pytest.mark.asyncio
async def test_get_balance_with_chain(
    ccn_api_client,
    message_processor: PendingMessageProcessor,
    fixture_product_prices_aggregate_in_db,
    instance_message_with_volumes_in_db,
    fixture_instance_message,
    user_balance_eth_avax: AlephBalanceDb,
):
    pipeline = message_processor.make_pipeline()
    _ = [message async for message in pipeline]

    assert fixture_instance_message.item_content
    expected_locked_amount = 1002.4666666666666
    chain = Chain.AVAX.value
    # Test Avax
    avax_response = await ccn_api_client.get(f"{MESSAGES_URI}?chain={chain}")

    assert avax_response.status == 200, await avax_response.text()
    avax_data = await avax_response.json()
    avax_expected_balance = user_balance_eth_avax.balance
    assert avax_data["balance"] == avax_expected_balance
    assert avax_data["locked_amount"] == expected_locked_amount

    # Verify ETH Value
    chain = Chain.ETH.value
    eth_response = await ccn_api_client.get(f"{MESSAGES_URI}?chain={chain}")
    assert eth_response.status == 200, await eth_response.text()
    eth_data = await eth_response.json()
    eth_expected_balance = user_balance_eth_avax.balance
    assert eth_data["balance"] == eth_expected_balance
    assert eth_data["locked_amount"] == expected_locked_amount

    # Verify All Chain
    total_response = await ccn_api_client.get(f"{MESSAGES_URI}")
    assert total_response.status == 200, await total_response.text()
    total_data = await total_response.json()
    total_expected_balance = user_balance_eth_avax.balance * 2
    assert total_data["balance"] == total_expected_balance
    assert total_data["locked_amount"] == expected_locked_amount

    details = total_data["details"]
    assert details is not None
    assert details["ETH"] == user_balance_eth_avax.balance
    assert details["AVAX"] == user_balance_eth_avax.balance


@pytest.mark.asyncio
async def test_get_balance_with_no_balance(
    ccn_api_client,
):
    response = await ccn_api_client.get(f"{MESSAGES_URI}")

    assert response.status == 200, await response.text()
    data = await response.json()
    assert data["balance"] == 0
    assert data["locked_amount"] == 0

    # Test Eth Case
    response = await ccn_api_client.get(f"{MESSAGES_URI}?chain{Chain.ETH.value}")

    assert response.status == 200, await response.text()
    data = await response.json()
    assert data["balance"] == 0
    assert data["locked_amount"] == 0
    details = data["details"]
    assert not details
