import asyncio
import json
from typing import Any, Dict, List, Mapping, Optional, Self, Set, Type, Union, cast

import aio_pika.abc
from aleph_message.models import Chain, ItemHash, ItemType, MessageType, StoreContent
from configmanager import Config
from pydantic import ValidationError

from aleph.chains.common import LOGGER
from aleph.config import get_config
from aleph.db.accessors.chains import upsert_chain_tx
from aleph.db.accessors.files import upsert_file, upsert_tx_file_pin
from aleph.db.accessors.pending_txs import upsert_pending_tx
from aleph.db.models import ChainTxDb, MessageDb
from aleph.exceptions import (
    AlephStorageException,
    ContentCurrentlyUnavailable,
    InvalidContent,
)
from aleph.schemas.chains.indexer_response import GenericMessageEvent, MessageEvent
from aleph.schemas.chains.sync_events import (
    OffChainSyncEventPayload,
    OnChainContent,
    OnChainMessage,
    OnChainSyncEventPayload,
)
from aleph.schemas.chains.tezos_indexer_response import (
    MessageEventPayload as TezosMessageEventPayload,
)
from aleph.storage import StorageService
from aleph.toolkit.timestamp import utc_now
from aleph.types.chain_sync import ChainSyncProtocol
from aleph.types.db_session import DbSession, DbSessionFactory
from aleph.types.files import FileType
from aleph.utils import get_sha256


class ChainDataService:
    def __init__(
        self,
        session_factory: DbSessionFactory,
        storage_service: StorageService,
    ):
        self.session_factory = session_factory
        self.storage_service = storage_service

    async def prepare_sync_event_payload(
        self, session: DbSession, messages: List[MessageDb]
    ) -> OffChainSyncEventPayload:
        """
        Returns the payload of a sync event to be published on chain.

        We publish message archives on-chain at regular intervals. This function prepares the data
        before the node emits a transaction on-chain:
        1. Pack all messages as a JSON file
        2. Add this file to IPFS and get its CID
        3. Return the CID + some metadata.

        Note that the archive file is pinned on IPFS but not inserted in the `file_pins` table
        here. This is left upon the caller once the event is successfully emitted on chain to avoid
        persisting unused archives.
        """
        # In previous versions, it was envisioned to store messages on-chain. This proved to be
        # too expensive. The archive uses the same format as these "on-chain" data.
        archive = OnChainSyncEventPayload(
            protocol=ChainSyncProtocol.ON_CHAIN_SYNC,
            version=1,
            content=OnChainContent(
                messages=[OnChainMessage.from_orm(message) for message in messages]
            ),
        )
        archive_content: bytes = archive.model_dump_json().encode("utf-8")

        ipfs_cid = await self.storage_service.add_file(
            session=session, file_content=archive_content, engine=ItemType.ipfs
        )
        return OffChainSyncEventPayload(
            protocol=ChainSyncProtocol.OFF_CHAIN_SYNC, version=1, content=ipfs_cid
        )

    @staticmethod
    def _get_sync_messages(tx_content: Mapping[str, Any]):
        return tx_content["messages"]

    def _get_tx_messages_on_chain_protocol(self, tx: ChainTxDb):
        messages = self._get_sync_messages(tx.content)
        if not isinstance(messages, list):
            error_msg = f"Got bad data in tx {tx.chain}/{tx.hash}"
            raise InvalidContent(error_msg)
        return messages

    async def _get_tx_messages_off_chain_protocol(
        self, tx: ChainTxDb, seen_ids: Optional[Set[str]] = None
    ) -> List[Dict[str, Any]]:
        config = get_config()

        file_hash = tx.content
        assert isinstance(file_hash, str)

        if seen_ids is not None:
            if file_hash in seen_ids:
                # is it really what we want here?
                LOGGER.debug("Already seen")
                return []
            else:
                LOGGER.debug("Adding to seen_ids")
                seen_ids.add(file_hash)
        try:
            sync_file_content = await self.storage_service.get_json(
                content_hash=file_hash, timeout=60
            )
        except AlephStorageException:
            # Let the caller handle unavailable/invalid content
            raise
        except Exception as e:
            error_msg = f"Can't get content of offchain object {file_hash}"
            LOGGER.exception("%s", error_msg)
            raise ContentCurrentlyUnavailable(error_msg) from e

        try:
            messages = self._get_sync_messages(sync_file_content.value["content"])
        except AlephStorageException:
            LOGGER.debug("Got no message")
            raise

        LOGGER.info("Got bulk data with %d items" % len(messages))
        if config.ipfs.enabled.value:
            try:
                with self.session_factory() as session:
                    # Some chain data files are duplicated, and can be treated in parallel,
                    # hence the upsert.
                    upsert_file(
                        session=session,
                        file_hash=sync_file_content.hash,
                        file_type=FileType.FILE,
                        size=len(sync_file_content.raw_value),
                    )
                    upsert_tx_file_pin(
                        session=session,
                        file_hash=file_hash,
                        tx_hash=tx.hash,
                        created=utc_now(),
                    )
                    session.commit()

                # Some IPFS fetches can take a while, hence the large timeout.
                await asyncio.wait_for(
                    self.storage_service.pin_hash(file_hash), timeout=120
                )
            except asyncio.TimeoutError:
                LOGGER.warning(f"Can't pin hash {file_hash}")
        return messages

    @staticmethod
    def _get_tx_messages_smart_contract_protocol(tx: ChainTxDb) -> List[Dict[str, Any]]:
        """
        Parses a "smart contract" tx and returns the encapsulated Aleph message.

        This function may still be a bit specific to Tezos as this is the first and
        only supported chain, but it is meant to be generic. Update accordingly.
        Message validation should be left to the message processing pipeline.
        """

        payload_model: Union[Type[TezosMessageEventPayload], Type[MessageEvent]] = (
            TezosMessageEventPayload if tx.chain == Chain.TEZOS else MessageEvent
        )

        try:
            payload = cast(
                GenericMessageEvent, payload_model.model_validate(tx.content)
            )
        except ValidationError:
            raise InvalidContent(f"Incompatible tx content for {tx.chain}/{tx.hash}")

        message_type = payload.type

        message_dict = {
            "sender": payload.address,
            "chain": tx.chain.value,
            "signature": None,
            "item_type": ItemType.inline,
            "time": tx.datetime.timestamp(),
        }

        if message_type == "STORE_IPFS":
            message_type = MessageType.store.value
            content = StoreContent(
                address=payload.address,
                time=payload.timestamp_seconds,
                item_type=ItemType.ipfs,
                item_hash=ItemHash(payload.content),
                metadata=None,
            )
            item_content = json.dumps(content.model_dump(exclude_none=True))
        else:
            item_content = payload.content

        message_dict["item_hash"] = get_sha256(item_content)
        message_dict["type"] = message_type
        message_dict["item_content"] = item_content

        return [message_dict]

    async def get_tx_messages(
        self, tx: ChainTxDb, seen_ids: Optional[Set[str]] = None
    ) -> List[Dict[str, Any]]:
        match tx.protocol, tx.protocol_version:
            case ChainSyncProtocol.ON_CHAIN_SYNC, 1:
                return self._get_tx_messages_on_chain_protocol(tx)
            case ChainSyncProtocol.OFF_CHAIN_SYNC, 1:
                return await self._get_tx_messages_off_chain_protocol(
                    tx=tx, seen_ids=seen_ids
                )
            case ChainSyncProtocol.SMART_CONTRACT, 1:
                return self._get_tx_messages_smart_contract_protocol(tx)
            case _:
                error_msg = (
                    f"Unknown protocol/version object in tx {tx.chain}/{tx.hash}: "
                    f"{tx.protocol} v{tx.protocol_version}"
                )
                LOGGER.info("%s", error_msg)
                raise InvalidContent(error_msg)


async def make_pending_tx_exchange(config: Config) -> aio_pika.abc.AbstractExchange:
    mq_conn = await aio_pika.connect_robust(
        host=config.p2p.mq_host.value,
        port=config.rabbitmq.port.value,
        login=config.rabbitmq.username.value,
        password=config.rabbitmq.password.value,
    )
    channel = await mq_conn.channel()
    pending_tx_exchange = await channel.declare_exchange(
        name=config.rabbitmq.pending_tx_exchange.value,
        type=aio_pika.ExchangeType.TOPIC,
        auto_delete=False,
    )
    return pending_tx_exchange


class PendingTxPublisher:
    def __init__(self, pending_tx_exchange: aio_pika.abc.AbstractExchange):
        self.pending_tx_exchange = pending_tx_exchange

    @staticmethod
    def add_pending_tx(session: DbSession, tx: ChainTxDb):
        upsert_chain_tx(session=session, tx=tx)
        upsert_pending_tx(session=session, tx_hash=tx.hash)

    async def publish_pending_tx(self, tx: ChainTxDb):
        message = aio_pika.Message(body=tx.hash.encode("utf-8"))
        await self.pending_tx_exchange.publish(
            message=message, routing_key=f"{tx.chain.value}.{tx.publisher}.{tx.hash}"
        )

    async def add_and_publish_pending_tx(self, session: DbSession, tx: ChainTxDb):
        """
        Add an event published on one of the supported chains.
        Adds the tx to the database, creates a pending tx entry in the pending tx table
        and publishes a message on the pending tx exchange.

        Note that this function commits changes to the database for consistency
        between the DB and the message queue.
        """
        self.add_pending_tx(session=session, tx=tx)
        session.commit()
        await self.publish_pending_tx(tx)

    @classmethod
    async def new(cls, config: Config) -> Self:
        pending_tx_exchange = await make_pending_tx_exchange(config=config)
        return cls(
            pending_tx_exchange=pending_tx_exchange,
        )
