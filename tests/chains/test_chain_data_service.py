import datetime as dt
import json

import pytest
from aleph_message.models import (
    Chain,
    ItemHash,
    ItemType,
    MessageType,
    PostContent,
    StoreContent,
)

from aleph.chains.chain_data_service import ChainDataService
from aleph.db.models import ChainTxDb, MessageDb
from aleph.schemas.chains.sync_events import OnChainSyncEventPayload
from aleph.schemas.chains.tezos_indexer_response import MessageEventPayload
from aleph.schemas.pending_messages import parse_message
from aleph.toolkit.timestamp import timestamp_to_datetime
from aleph.types.chain_sync import ChainSyncProtocol
from aleph.types.db_session import DbSession, DbSessionFactory


@pytest.mark.asyncio
async def test_prepare_sync_event_payload(mocker):
    archive_cid = "Qmsomething"

    messages = [
        MessageDb(
            item_hash=ItemHash(
                "abe22332402a5c45f20491b719b091fd0d7eab65ca1bcf4746840b787dee874b"
            ),
            type=MessageType.store,
            chain=Chain.ETH,
            sender="0x0dAd142fDD76A817CD52a700EaCA2D9D3491086B",
            signature="0x813f0be4ddd852e7f0c723ac95333be762d80690fe0fc0705ec0e1b7df7fa92d5cdbfba8ab0321aee8769c93f7bc5dc9d1268cb66e8cb453a6b8299ba3faac771b",
            item_type=ItemType.inline,
            item_content='{"address":"0x0dAd142fDD76A817CD52a700EaCA2D9D3491086B","time":1697718147.2695966,"item_type":"storage","item_hash":"ecbfcb9f92291b9385772c9b5cd094788f928ccb696ad1ecbf179a4e308e4350","mime_type":"application/octet-stream"}',
            time=dt.datetime(2023, 10, 19, 12, 22, 27, 269707, tzinfo=dt.timezone.utc),
            channel="TEST",
        )
    ]

    async def mock_add_file(
        session: DbSession, file_content: bytes, engine: ItemType = ItemType.ipfs
    ) -> str:
        content = file_content
        archive = OnChainSyncEventPayload.model_validate_json(content)

        assert archive.version == 1
        assert len(archive.content.messages) == len(messages)
        # Check that the time field was converted
        assert archive.content.messages[0].time == messages[0].time.timestamp()

        return archive_cid

    storage_service = mocker.AsyncMock()
    storage_service.add_file = mock_add_file
    chain_data_service = ChainDataService(
        session_factory=mocker.MagicMock(), storage_service=storage_service
    )

    sync_event_payload = await chain_data_service.prepare_sync_event_payload(
        session=mocker.MagicMock(), messages=messages
    )
    assert sync_event_payload.protocol == ChainSyncProtocol.OFF_CHAIN_SYNC
    assert sync_event_payload.version == 1
    assert sync_event_payload.content == archive_cid


@pytest.mark.asyncio
async def test_smart_contract_protocol_ipfs_store(
    mocker, session_factory: DbSessionFactory
):
    payload = MessageEventPayload(
        timestamp=1668611900,
        addr="KT1VBeLD7hzKpj17aRJ3Kc6QQFeikCEXi7W6",
        msgtype="STORE_IPFS",
        msgcontent="QmaMLRsvmDRCezZe2iebcKWtEzKNjBaQfwcu7mcpdm8eY2",
    )

    tx = ChainTxDb(
        hash="oorMNgusX6RxZ4NhzYriVDN8HDeMBNkjD3E8kx9a7j7dRRDGkzz",
        chain=Chain.TEZOS,
        height=584664,
        datetime=timestamp_to_datetime(1668611900),
        publisher="KT1BfL57oZfptdtMFZ9LNakEPvuPPA2urdSW",
        protocol=ChainSyncProtocol.SMART_CONTRACT,
        protocol_version=1,
        content=payload.model_dump(),
    )

    chain_data_service = ChainDataService(
        session_factory=session_factory, storage_service=mocker.AsyncMock()
    )

    pending_messages = await chain_data_service.get_tx_messages(tx)
    assert len(pending_messages) == 1
    pending_message_dict = pending_messages[0]

    # Check that the message is valid
    pending_message = parse_message(pending_message_dict)

    assert (
        pending_message.item_hash
        == "1eb440938336b13e7e4ad3f9ebea6de8dbf7fcbdba4c8861ea0a4f70e19e777d"
    )
    assert pending_message.sender == payload.addr
    assert pending_message.chain == Chain.TEZOS
    assert pending_message.signature is None
    assert pending_message.type == MessageType.store
    assert pending_message.item_type == ItemType.inline
    assert pending_message.channel is None

    assert pending_message.item_content
    message_content = StoreContent.model_validate_json(pending_message.item_content)
    assert message_content.item_hash == payload.message_content
    assert message_content.item_type == ItemType.ipfs
    assert message_content.address == payload.addr
    assert message_content.time == payload.timestamp


@pytest.mark.asyncio
async def test_smart_contract_protocol_regular_message(
    mocker, session_factory: DbSessionFactory
):
    content = PostContent(
        content={"body": "My first post on Tezos"},
        ref=None,
        type="my-type",
        address="KT1VBeLD7hzKpj17aRJ3Kc6QQFeikCEXi7W6",
        time=1000,
    )

    payload = MessageEventPayload(
        timestamp=1668611900,
        addr="KT1VBeLD7hzKpj17aRJ3Kc6QQFeikCEXi7W6",
        msgtype="POST",
        msgcontent=json.dumps(content.model_dump()),
    )

    tx = ChainTxDb(
        hash="oorMNgusX6RxZ4NhzYriVDN8HDeMBNkjD3E8kx9a7j7dRRDGkzz",
        chain=Chain.TEZOS,
        height=584664,
        datetime=timestamp_to_datetime(1668611900),
        publisher="KT1BfL57oZfptdtMFZ9LNakEPvuPPA2urdSW",
        protocol=ChainSyncProtocol.SMART_CONTRACT,
        protocol_version=1,
        content=payload.model_dump(mode="json"),
    )

    chain_data_service = ChainDataService(
        session_factory=session_factory, storage_service=mocker.AsyncMock()
    )

    pending_messages = await chain_data_service.get_tx_messages(tx)
    assert len(pending_messages) == 1
    pending_message_dict = pending_messages[0]

    # Check that the message is valid
    pending_message = parse_message(pending_message_dict)

    assert (
        pending_message.item_hash
        == "cbe9c48c7290d6e243c80247444c6d28c36a475c99286b6e921b5223dc2cba39"
    )
    assert pending_message.sender == payload.addr
    assert pending_message.chain == Chain.TEZOS
    assert pending_message.signature is None
    assert pending_message.type == MessageType.post
    assert pending_message.item_type == ItemType.inline
    assert pending_message.channel is None

    assert pending_message.item_content
    message_content = PostContent.model_validate_json(pending_message.item_content)
    assert message_content.address == content.address
    assert message_content.time == content.time
    assert message_content.ref == content.ref
    assert message_content.type == content.type
    assert message_content.content == content.content
