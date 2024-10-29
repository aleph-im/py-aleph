import datetime as dt
from typing import Dict, List, Set

import pytest
import pytz
from aleph_message.models import Chain, MessageType, PostContent
from configmanager import Config
from sqlalchemy import select

from aleph.chains.chain_data_service import ChainDataService
from aleph.db.models import MessageStatusDb, PendingMessageDb
from aleph.db.models.chains import ChainTxDb
from aleph.db.models.pending_txs import PendingTxDb
from aleph.handlers.message_handler import MessagePublisher
from aleph.jobs.process_pending_txs import PendingTxProcessor
from aleph.schemas.chains.tezos_indexer_response import MessageEventPayload
from aleph.storage import StorageService
from aleph.toolkit.timestamp import timestamp_to_datetime
from aleph.types.chain_sync import ChainSyncProtocol
from aleph.types.db_session import DbSessionFactory
from aleph.types.message_status import MessageStatus

from .load_fixtures import load_fixture_messages


# TODO: try to replace this fixture by a get_json fixture. Currently, the pinning
#       of the message content gets in the way in the real get_chaindata_messages function.
async def get_fixture_chaindata_messages(
    tx: ChainTxDb, seen_ids: List[str]
) -> List[Dict]:
    return load_fixture_messages(f"{tx.content}.json")


@pytest.mark.asyncio
async def test_process_pending_tx_on_chain_protocol(
    mocker,
    mock_config: Config,
    session_factory: DbSessionFactory,
    test_storage_service: StorageService,
):
    chain_data_service = mocker.AsyncMock()
    chain_data_service.get_tx_messages = get_fixture_chaindata_messages
    pending_tx_processor = PendingTxProcessor(
        session_factory=session_factory,
        message_publisher=MessagePublisher(
            session_factory=session_factory,
            storage_service=test_storage_service,
            config=mock_config,
            pending_message_exchange=mocker.AsyncMock(),
        ),
        chain_data_service=chain_data_service,
        pending_tx_queue=mocker.AsyncMock(),
    )
    pending_tx_processor.chain_data_service = chain_data_service

    chain_tx = ChainTxDb(
        hash="0xf49cb176c1ce4f6eb7b9721303994b05074f8fadc37b5f41ac6f78bdf4b14b6c",
        chain=Chain.ETH,
        datetime=pytz.utc.localize(dt.datetime.utcfromtimestamp(1632835747)),
        height=13314512,
        publisher="0x23eC28598DCeB2f7082Cc3a9D670592DfEd6e0dC",
        protocol=ChainSyncProtocol.ON_CHAIN_SYNC,
        protocol_version=1,
        content="test-data-pending-tx-messages",
    )

    pending_tx = PendingTxDb(tx=chain_tx)

    with session_factory() as session:
        session.add(pending_tx)
        session.commit()

    seen_ids: Set[str] = set()
    await pending_tx_processor.handle_pending_tx(
        pending_tx=pending_tx, seen_ids=seen_ids
    )

    fixture_messages = load_fixture_messages(f"{pending_tx.tx.content}.json")

    with session_factory() as session:
        pending_txs = session.execute(select(PendingTxDb)).scalars().all()
        assert not pending_txs

        for fixture_message in fixture_messages:
            item_hash = fixture_message["item_hash"]
            message_status_db = (
                session.execute(
                    select(MessageStatusDb).where(
                        MessageStatusDb.item_hash == item_hash
                    )
                )
            ).scalar_one()
            assert message_status_db.status == MessageStatus.PENDING

            pending_message_db = (
                session.execute(
                    select(PendingMessageDb).where(
                        PendingMessageDb.item_hash == item_hash
                    )
                )
            ).scalar_one()

            # TODO: need utils to compare message DB types to dictionaries / Pydantic classes / etc
            assert pending_message_db.sender == fixture_message["sender"]
            assert pending_message_db.item_content == fixture_message["item_content"]


async def _process_smart_contract_tx(
    mocker,
    mock_config: Config,
    session_factory: DbSessionFactory,
    test_storage_service: StorageService,
    payload: MessageEventPayload,
):
    chain_data_service = ChainDataService(
        session_factory=session_factory, storage_service=mocker.AsyncMock()
    )
    pending_tx_processor = PendingTxProcessor(
        session_factory=session_factory,
        message_publisher=MessagePublisher(
            session_factory=session_factory,
            storage_service=test_storage_service,
            config=mock_config,
            pending_message_exchange=mocker.AsyncMock(),
        ),
        chain_data_service=chain_data_service,
        pending_tx_queue=mocker.AsyncMock(),
    )
    pending_tx_processor.chain_data_service = chain_data_service

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

    pending_tx = PendingTxDb(tx=tx)

    with session_factory() as session:
        session.add(pending_tx)
        session.commit()

    await pending_tx_processor.handle_pending_tx(pending_tx=pending_tx)

    with session_factory() as session:
        pending_txs = session.execute(select(PendingTxDb)).scalars().all()
        assert not pending_txs

        pending_messages = list(session.execute(select(PendingMessageDb)).scalars())
        assert len(pending_messages) == 1
        pending_message_db = pending_messages[0]

        assert pending_message_db.signature is None
        assert not pending_message_db.check_message
        assert pending_message_db.sender == payload.addr

        if payload.message_type == "STORE_IPFS":
            assert pending_message_db.type == MessageType.store
        else:
            assert pending_message_db.type == MessageType(payload.message_type)

        message_status_db = (
            session.execute(
                select(MessageStatusDb).where(
                    MessageStatusDb.item_hash == pending_message_db.item_hash
                )
            )
        ).scalar_one()
        assert message_status_db.status == MessageStatus.PENDING


@pytest.mark.asyncio
async def test_process_pending_smart_contract_tx_store_ipfs(
    mocker,
    mock_config: Config,
    session_factory: DbSessionFactory,
    test_storage_service: StorageService,
):
    payload = MessageEventPayload(
        timestamp=1668611900,
        addr="KT1VBeLD7hzKpj17aRJ3Kc6QQFeikCEXi7W6",
        msgtype="STORE_IPFS",
        msgcontent="QmaMLRsvmDRCezZe2iebcKWtEzKNjBaQfwcu7mcpdm8eY2",
    )

    await _process_smart_contract_tx(
        mocker=mocker,
        mock_config=mock_config,
        session_factory=session_factory,
        test_storage_service=test_storage_service,
        payload=payload,
    )


@pytest.mark.asyncio
async def test_process_pending_smart_contract_tx_post(
    mocker,
    mock_config: Config,
    session_factory: DbSessionFactory,
    test_storage_service: StorageService,
):
    payload = MessageEventPayload(
        timestamp=1668611900,
        addr="KT1VBeLD7hzKpj17aRJ3Kc6QQFeikCEXi7W6",
        msgtype=MessageType.post.value,
        msgcontent=PostContent(
            content={"body": "My first post on Tezos"},
            ref=None,
            type="my-type",
            address="KT1VBeLD7hzKpj17aRJ3Kc6QQFeikCEXi7W6",
            time=1000,
        ).model_dump_json(),
    )

    await _process_smart_contract_tx(
        mocker=mocker,
        mock_config=mock_config,
        session_factory=session_factory,
        test_storage_service=test_storage_service,
        payload=payload,
    )
