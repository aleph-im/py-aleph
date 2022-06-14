import pytest
from aleph.model.p2p import Peer
import datetime as dt


@pytest.mark.asyncio
async def test_get_peer_type_no_match(test_db):
    peer_address = await Peer.get_peer_address(peer_id="123", peer_type="HTTP")
    assert peer_address is None


@pytest.mark.asyncio
async def test_get_peer_type_match(test_db):
    peer_id = "123"
    http_address = "http://127.0.0.1:4024"

    await Peer.collection.insert_one(
        {
            "address": http_address,
            "type": "HTTP",
            "last_seen": dt.datetime.utcnow(),
            "sender": peer_id,
            "source": "p2p",
        }
    )

    peer_address = await Peer.get_peer_address(peer_id=peer_id, peer_type="HTTP")
    assert peer_address is not None
    assert peer_address == http_address
