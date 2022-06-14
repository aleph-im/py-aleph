import itertools
from typing import Dict, Iterable, List

import pytest

MESSAGES_URI = "/api/v0/messages.json"


def get_messages_by_keys(messages: Iterable[Dict], **keys) -> List[Dict]:
    return [msg for msg in messages if all(msg[k] == v for k, v in keys.items())]


def assert_messages_equal(messages: Iterable[Dict], expected_messages: Iterable[Dict]):
    messages_by_hash = {msg["item_hash"]: msg for msg in messages}

    for expected_message in expected_messages:
        message = messages_by_hash[expected_message["item_hash"]]

        assert message["chain"] == expected_message["chain"]
        assert message["channel"] == expected_message["channel"]
        assert message["content"] == expected_message["content"]
        assert message["item_content"] == expected_message["item_content"]
        assert message["sender"] == expected_message["sender"]
        assert message["signature"] == expected_message["signature"]


@pytest.mark.asyncio
async def test_get_messages(fixture_messages, ccn_api_client):
    response = await ccn_api_client.get(MESSAGES_URI)
    assert response.status == 200, await response.text()

    data = await response.json()

    messages = data["messages"]
    assert len(messages) == len(fixture_messages)
    assert_messages_equal(messages, fixture_messages)

    assert data["pagination_total"] == len(messages)
    assert data["pagination_page"] == 1


@pytest.mark.asyncio
async def test_get_messages_filter_by_channel(fixture_messages, ccn_api_client):
    async def fetch_messages_by_channel(channel: str) -> Dict:
        response = await ccn_api_client.get(MESSAGES_URI, params={"channels": channel})
        assert response.status == 200, await response.text()
        return await response.json()

    data = await fetch_messages_by_channel("unit-tests")
    messages = data["messages"]

    unit_test_messages = get_messages_by_keys(fixture_messages, channel="unit-tests")

    assert len(messages) == len(unit_test_messages)
    assert_messages_equal(messages, unit_test_messages)

    data = await fetch_messages_by_channel("aggregates-tests")
    messages = data["messages"]

    aggregates_test_messages = get_messages_by_keys(
        fixture_messages, channel="aggregates-tests"
    )
    assert_messages_equal(messages, aggregates_test_messages)

    # Multiple channels
    data = await fetch_messages_by_channel("aggregates-tests,unit-tests")
    messages = data["messages"]

    assert_messages_equal(
        messages, itertools.chain(unit_test_messages, aggregates_test_messages)
    )

    # Nonexistent channel
    data = await fetch_messages_by_channel("none-pizza-with-left-beef")
    assert data["messages"] == []


@pytest.mark.asyncio
async def test_get_messages_filter_by_chain(fixture_messages, ccn_api_client):
    async def fetch_messages_by_chain(chain: str) -> Dict:
        response = await ccn_api_client.get(MESSAGES_URI, params={"chains": chain})
        assert response.status == 200, await response.text()
        return await response.json()

    eth_data = await fetch_messages_by_chain("ETH")
    eth_messages = eth_data["messages"]
    assert_messages_equal(
        eth_messages, get_messages_by_keys(fixture_messages, chain="ETH")
    )

    fake_chain_data = await fetch_messages_by_chain("2CHAINZ")
    fake_chain_messages = fake_chain_data["messages"]
    assert fake_chain_messages == []


@pytest.mark.asyncio
async def test_get_messages_filter_by_content_hash(fixture_messages, ccn_api_client):
    async def fetch_messages_by_content_hash(item_hash: str) -> Dict:
        response = await ccn_api_client.get(MESSAGES_URI, params={"contentHashes": item_hash})
        assert response.status == 200, await response.text()
        return await response.json()

    content_hash = "5ccdd7bccfbc5955e2e40166dd0cdea0b093154fd87bc2bea57e7c768cde2f21"
    data = await fetch_messages_by_content_hash(content_hash)
    messages = data["messages"]
    assert_messages_equal(
        messages, get_messages_by_keys(fixture_messages, item_hash="2953f0b52beb79fc0ed1bc455346fdcb530611605e16c636778a0d673d7184af")
    )

    fake_hash_data = await fetch_messages_by_content_hash("1234")
    fake_hash_messages = fake_hash_data["messages"]
    assert fake_hash_messages == []
