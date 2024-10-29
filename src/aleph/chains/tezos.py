import asyncio
import datetime as dt
import logging
from enum import Enum
from typing import List

import aiohttp
from aleph_message.models import Chain
from aleph_pytezos.crypto.key import Key
from configmanager import Config
from nacl.exceptions import BadSignatureError

import aleph.toolkit.json as aleph_json
from aleph.chains.abc import ChainReader, Verifier
from aleph.chains.chain_data_service import PendingTxPublisher
from aleph.chains.common import get_verification_buffer
from aleph.db.accessors.chains import get_last_height, upsert_chain_sync_status
from aleph.db.models import ChainTxDb
from aleph.schemas.chains.tezos_indexer_response import (
    IndexerMessageEvent,
    IndexerResponse,
    SyncStatus,
)
from aleph.schemas.pending_messages import BasePendingMessage
from aleph.toolkit.timestamp import utc_now
from aleph.types.chain_sync import ChainEventType, ChainSyncProtocol
from aleph.types.db_session import DbSession, DbSessionFactory

LOGGER = logging.getLogger(__name__)

# Default dApp URL for Micheline-style signatures
DEFAULT_DAPP_URL = "aleph.im"


class TezosSignatureType(str, Enum):
    RAW = "raw"
    MICHELINE = "micheline"


def datetime_to_iso_8601(datetime: dt.datetime) -> str:
    """
    Returns the timestamp formatted to ISO-8601, JS-style.

    Compared to the regular `isoformat()`, this function only provides precision down
    to milliseconds and prints a "Z" instead of +0000 for UTC.
    This format is typically used by JavaScript applications, like our TS SDK.

    Example: 2022-09-23T14:41:19.029Z

    :param datetime: The timestamp to format.
    :return: The formatted timestamp.
    """

    date_str = datetime.strftime("%Y-%m-%d")
    time_str = f"{datetime.hour:02d}:{datetime.minute:02d}:{datetime.second:02d}.{datetime.microsecond // 1000:03d}"
    return f"{date_str}T{time_str}Z"


def micheline_verification_buffer(
    verification_buffer: bytes,
    datetime: dt.datetime,
    dapp_url: str,
) -> bytes:
    """
    Computes the verification buffer for Micheline-type signatures.

    This verification buffer is used when signing data with a Tezos web wallet.
    See https://tezostaquito.io/docs/signing/#generating-a-signature-with-beacon-sdk.

    :param verification_buffer: The original (non-Tezos) verification buffer for the Aleph message.
    :param datetime: Timestamp of the message.
    :param dapp_url: The URL of the dApp, for use as part of the verification buffer.
    :return: The verification buffer used for the signature by the web wallet.
    """

    prefix = b"Tezos Signed Message:"
    timestamp = datetime_to_iso_8601(datetime).encode("utf-8")

    payload = b" ".join(
        (prefix, dapp_url.encode("utf-8"), timestamp, verification_buffer)
    )
    hex_encoded_payload = payload.hex()
    payload_size = str(len(hex_encoded_payload)).encode("utf-8")

    return b"\x05" + b"\x01\x00" + payload_size + payload


def get_tezos_verification_buffer(
    message: BasePendingMessage, signature_type: TezosSignatureType, dapp_url: str
) -> bytes:
    verification_buffer = get_verification_buffer(message)

    if signature_type == TezosSignatureType.RAW:
        return verification_buffer
    elif signature_type == TezosSignatureType.MICHELINE:
        return micheline_verification_buffer(
            verification_buffer, message.time, dapp_url
        )

    raise ValueError(f"Unsupported signature type: {signature_type}")


def make_graphql_status_query():
    return "{indexStatus {status}}"


def make_graphql_query(
    sync_contract_address: str, event_type: str, limit: int, skip: int
):
    return """
{
  indexStatus {
    oldestBlock
    recentBlock
    status
  }
  stats(address: "%s") {
    totalEvents
  }
  events(limit: %d, skip: %d, source: "%s", type: "%s") {
    _id
    source
    timestamp
    blockLevel
    operationHash
    type
    payload
  }
}
""" % (
        sync_contract_address,
        limit,
        skip,
        sync_contract_address,
        event_type,
    )


async def get_indexer_status(http_session: aiohttp.ClientSession) -> SyncStatus:
    response = await http_session.post("/", json={"query": make_graphql_status_query()})
    response.raise_for_status()
    response_json = await response.json()

    return SyncStatus(response_json["data"]["indexStatus"]["status"])


async def fetch_messages(
    http_session: aiohttp.ClientSession,
    sync_contract_address: str,
    event_type: str,
    limit: int,
    skip: int,
) -> IndexerResponse[IndexerMessageEvent]:
    query = make_graphql_query(
        limit=limit,
        skip=skip,
        sync_contract_address=sync_contract_address,
        event_type=event_type,
    )

    response = await http_session.post("/", json={"query": query})
    response.raise_for_status()
    response_json = await response.json()

    return IndexerResponse[IndexerMessageEvent].model_validate(response_json)


def indexer_event_to_chain_tx(
    indexer_event: IndexerMessageEvent,
) -> ChainTxDb:
    chain_tx = ChainTxDb(
        hash=indexer_event.operation_hash,
        chain=Chain.TEZOS,
        height=indexer_event.block_level,
        datetime=indexer_event.timestamp,
        publisher=indexer_event.source,
        protocol=ChainSyncProtocol.SMART_CONTRACT,
        protocol_version=1,
        content=indexer_event.payload.model_dump(),
    )

    return chain_tx


async def extract_aleph_messages_from_indexer_response(
    indexer_response: IndexerResponse[IndexerMessageEvent],
) -> List[ChainTxDb]:
    events = indexer_response.data.events
    return [indexer_event_to_chain_tx(event) for event in events]


class TezosVerifier(Verifier):
    async def verify_signature(self, message: BasePendingMessage) -> bool:
        """
        Verifies the cryptographic signature of a message signed with a Tezos key.
        """

        if message.signature is None:
            LOGGER.warning("'%s': missing signature.", message.item_hash)
            return False

        try:
            signature_dict = aleph_json.loads(message.signature)
        except aleph_json.DecodeError:
            LOGGER.warning(
                "Signature field for Tezos message is not JSON deserializable."
            )
            return False

        try:
            signature = signature_dict["signature"]
            public_key = signature_dict["publicKey"]
        except KeyError as e:
            LOGGER.warning(
                "'%s' key missing from Tezos signature dictionary.", e.args[0]
            )
            return False

        signature_type = TezosSignatureType(signature_dict.get("signingType", "raw"))
        dapp_url = signature_dict.get("dAppUrl", DEFAULT_DAPP_URL)

        key = Key.from_encoded_key(public_key)
        # Check that the sender ID is equal to the public key hash
        public_key_hash = key.public_key_hash()

        if message.sender != public_key_hash:
            LOGGER.warning(
                "Sender ID (%s) does not match public key hash (%s)",
                message.sender,
                public_key_hash,
            )

        verification_buffer = get_tezos_verification_buffer(
            message, signature_type, dapp_url
        )

        # Check the signature
        try:
            key.verify(signature, verification_buffer)
        except (ValueError, BadSignatureError):
            LOGGER.warning(
                "Received message with bad signature from %s" % message.sender
            )
            return False

        return True


class TezosConnector(ChainReader):
    def __init__(
        self,
        session_factory: DbSessionFactory,
        pending_tx_publisher: PendingTxPublisher,
    ):
        self.session_factory = session_factory
        self.pending_tx_publisher = pending_tx_publisher

    async def get_last_height(self, sync_type: ChainEventType) -> int:
        """Returns the last height for which we already have the ethereum data."""
        with self.session_factory() as session:
            last_height = get_last_height(
                session=session, chain=Chain.TEZOS, sync_type=sync_type
            )

        # Keep the same behavior as Ethereum for now
        if last_height is None:
            last_height = -1

        return last_height

    async def fetch_incoming_messages(
        self, session: DbSession, indexer_url: str, sync_contract_address: str
    ) -> None:
        """
        Fetch the latest message events from the Aleph sync smart contract.
        :param session: DB session.
        :param indexer_url: URL of the Tezos indexer.
        :param sync_contract_address: Tezos address of the Aleph sync smart contract.
        """

        async with aiohttp.ClientSession(indexer_url) as http_session:
            status = await get_indexer_status(http_session)

            if status != SyncStatus.SYNCED:
                LOGGER.warning("Tezos indexer is not yet synced, waiting until it is")
                return

            last_stored_height = await self.get_last_height(
                sync_type=ChainEventType.MESSAGE
            )
            # TODO: maybe get_last_height() should not return a negative number on startup?
            # Avoid an off-by-one error at startup
            if last_stored_height == -1:
                last_stored_height = 0

            limit = 100

            try:
                while True:
                    indexer_response_data = await fetch_messages(
                        http_session,
                        sync_contract_address=sync_contract_address,
                        event_type="MessageEvent",
                        limit=limit,
                        skip=last_stored_height,
                    )
                    txs = await extract_aleph_messages_from_indexer_response(
                        indexer_response_data
                    )
                    LOGGER.info("%d new txs", len(txs))
                    for tx in txs:
                        await self.pending_tx_publisher.add_and_publish_pending_tx(
                            session=session, tx=tx
                        )

                    last_stored_height += limit
                    if (
                        last_stored_height
                        >= indexer_response_data.data.stats.total_events
                    ):
                        last_stored_height = (
                            indexer_response_data.data.stats.total_events
                        )
                        break

            finally:
                upsert_chain_sync_status(
                    session=session,
                    chain=Chain.TEZOS,
                    sync_type=ChainEventType.MESSAGE,
                    height=last_stored_height,
                    update_datetime=utc_now(),
                )

    async def fetcher(self, config: Config):
        while True:
            try:
                with self.session_factory() as session:
                    await self.fetch_incoming_messages(
                        session=session,
                        indexer_url=config.tezos.indexer_url.value,
                        sync_contract_address=config.tezos.sync_contract.value,
                    )
                    session.commit()
            except Exception:
                LOGGER.exception(
                    "An unexpected exception occurred, "
                    "relaunching Tezos message sync in 10 seconds"
                )
            else:
                LOGGER.info("Processed all transactions, waiting 10 seconds.")
            await asyncio.sleep(10)
