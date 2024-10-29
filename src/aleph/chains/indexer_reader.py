import asyncio
import datetime as dt
import itertools
import logging
from dataclasses import dataclass
from typing import (
    Any,
    Dict,
    Iterable,
    List,
    Mapping,
    Optional,
    Tuple,
    Type,
    TypeVar,
    Union,
)

import aiohttp
from aleph_message.models import Chain
from pydantic import BaseModel

import aleph.toolkit.json as aleph_json
from aleph.chains.chain_data_service import PendingTxPublisher
from aleph.db.accessors.chains import (
    add_indexer_range,
    get_missing_indexer_datetime_multirange,
)
from aleph.db.models import ChainTxDb
from aleph.schemas.chains.indexer_response import (
    EntityType,
    IndexerAccountStateResponse,
    IndexerBlockchain,
    IndexerEventResponse,
    MessageEvent,
    SyncEvent,
)
from aleph.toolkit.range import MultiRange, Range
from aleph.toolkit.timestamp import timestamp_to_datetime
from aleph.types.chain_sync import ChainEventType, ChainSyncProtocol
from aleph.types.db_session import DbSession, DbSessionFactory

LOGGER = logging.getLogger(__name__)


def make_account_state_query(
    blockchain: IndexerBlockchain, accounts: List[str], type_: EntityType
):
    accounts_str = "[" + ", ".join(f'"{account}"' for account in accounts) + "]"

    return """
{
  state: accountState(
    blockchain: "%s"
    account: %s
    type: %s
  ) {
    blockchain
    type
    indexer
    account
    accurate
    progress
    pending
    processed
  }
}
    """ % (
        blockchain.value,
        accounts_str,
        type_.value,
    )


def make_events_query(
    event_type: ChainEventType,
    blockchain: IndexerBlockchain,
    datetime_range: Optional[Tuple[dt.datetime, dt.datetime]] = None,
    block_range: Optional[Tuple[int, int]] = None,
    limit: int = 1000,
):
    if datetime_range and block_range:
        raise ValueError("Only one range of datetimes or blocks can be specified.")
    if not datetime_range and not block_range:
        raise ValueError("A range of datetimes or blocks must be specified.")

    model: Union[Type[MessageEvent], Type[SyncEvent]]

    if event_type == ChainEventType.MESSAGE:
        model = MessageEvent
        event_type_str = "messageEvents"
    else:
        model = SyncEvent
        event_type_str = "syncEvents"

    fields = "\n".join(model.model_fields.keys())
    params: Dict[str, Any] = {
        "blockchain": f'"{blockchain.value}"',
        "limit": limit,
        "reverse": "false",
    }

    if block_range is not None:
        params["startHeight"] = block_range[0]
        params["endHeight"] = block_range[1]

    if datetime_range is not None:
        # The timestamp must be expressed in milliseconds
        params["startDate"] = datetime_range[0].timestamp() * 1000
        params["endDate"] = datetime_range[1].timestamp() * 1000

    return """
{
  %s(%s) {
    %s
  }
}
""" % (
        event_type_str,
        ", ".join(f"{k}: {v}" for k, v in params.items()),
        fields,
    )


T = TypeVar("T", bound=BaseModel)


class AlephIndexerClient:
    def __init__(self, indexer_url: str):
        self.indexer_url = indexer_url
        self.http_session: Optional[aiohttp.ClientSession] = None

    async def __aenter__(self) -> "AlephIndexerClient":
        self.http_session = aiohttp.ClientSession(self.indexer_url)
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self.http_session:
            await self.http_session.close()

    async def _query(self, query: str, model: Type[T]) -> T:
        if self.http_session is None:
            raise ValueError(
                "HTTP session not opened. Use `async with AlephIndexerClient(...):`."
            )

        response = await self.http_session.post("/", json={"query": query})
        response.raise_for_status()
        response_json = await response.json()
        return model.model_validate(response_json)

    async def fetch_account_state(
        self,
        blockchain: IndexerBlockchain,
        accounts: List[str],
    ) -> IndexerAccountStateResponse:
        query = make_account_state_query(
            blockchain=blockchain, accounts=accounts, type_=EntityType.LOG
        )

        return await self._query(
            query=query,
            model=IndexerAccountStateResponse,
        )

    async def fetch_events(
        self,
        blockchain: IndexerBlockchain,
        event_type: ChainEventType,
        datetime_range: Optional[Tuple[dt.datetime, dt.datetime]] = None,
        block_range: Optional[Tuple[int, int]] = None,
        limit: int = 1000,
    ) -> IndexerEventResponse:
        query = make_events_query(
            event_type=event_type,
            blockchain=blockchain,
            block_range=block_range,
            datetime_range=datetime_range,
            limit=limit,
        )

        return await self._query(query=query, model=IndexerEventResponse)


@dataclass
class IndexerSyncState:
    nb_events: int
    last_block: int
    last_block_datetime: dt.datetime


def indexer_event_to_chain_tx(
    chain: Chain,
    indexer_event: Union[MessageEvent, SyncEvent],
) -> ChainTxDb:
    if isinstance(indexer_event, MessageEvent):
        protocol = ChainSyncProtocol.SMART_CONTRACT
        protocol_version = 1
        content = indexer_event.model_dump()
    else:
        sync_message = aleph_json.loads(indexer_event.message)

        protocol = sync_message["protocol"]
        protocol_version = sync_message["version"]
        content = sync_message["content"]

    chain_tx = ChainTxDb(
        hash=indexer_event.transaction,
        chain=chain,
        height=indexer_event.height,
        # Indexer timestamps are expressed in milliseconds
        datetime=timestamp_to_datetime(indexer_event.timestamp / 1000),
        publisher=indexer_event.address,
        protocol=protocol,
        protocol_version=protocol_version,
        content=content,
    )

    return chain_tx


async def extract_aleph_messages_from_indexer_response(
    chain: Chain,
    indexer_response: IndexerEventResponse,
) -> List[ChainTxDb]:
    message_events = indexer_response.data.message_events
    sync_events = indexer_response.data.sync_events

    all_events: Iterable[Union[MessageEvent, SyncEvent]] = itertools.chain(
        message_events, sync_events
    )

    return [
        indexer_event_to_chain_tx(chain=chain, indexer_event=indexer_event)
        for indexer_event in all_events
    ]


class AlephIndexerReader:
    BLOCKCHAIN_MAP: Mapping[Chain, IndexerBlockchain] = {
        Chain.BSC: IndexerBlockchain.BSC,
        Chain.ETH: IndexerBlockchain.ETHEREUM,
        Chain.SOL: IndexerBlockchain.SOLANA,
    }

    def __init__(
        self,
        chain: Chain,
        session_factory: DbSessionFactory,
        pending_tx_publisher: PendingTxPublisher,
    ):
        self.chain = chain
        self.session_factory = session_factory
        self.pending_tx_publisher = pending_tx_publisher

        self.blockchain = self.BLOCKCHAIN_MAP[chain]

    async def fetch_range(
        self,
        session: DbSession,
        indexer_client: AlephIndexerClient,
        chain: Chain,
        event_type: ChainEventType,
        datetime_range: Range[dt.datetime],
    ):
        start_datetime = datetime_range.lower
        end_datetime = datetime_range.upper

        limit = 1000

        while True:
            events_response = await indexer_client.fetch_events(
                blockchain=self.blockchain,
                event_type=event_type,
                datetime_range=(start_datetime, end_datetime),
                limit=limit,
            )

            nb_events_fetched = (
                len(events_response.data.message_events)
                if event_type == ChainEventType.MESSAGE
                else len(events_response.data.sync_events)
            )
            LOGGER.info(
                "%s %s event indexer: fetched %d events.",
                chain,
                event_type,
                nb_events_fetched,
            )

            if nb_events_fetched:
                txs = await extract_aleph_messages_from_indexer_response(
                    chain=chain, indexer_response=events_response
                )
                LOGGER.info("%d new txs", len(txs))
                # Events are listed in reverse order in the indexer response
                for tx in txs:
                    self.pending_tx_publisher.add_pending_tx(session=session, tx=tx)

                if nb_events_fetched >= limit:
                    last_event_datetime = txs[-1].datetime
                    upper_inc = False
                else:
                    last_event_datetime = end_datetime
                    upper_inc = True

                synced_range = Range(
                    start_datetime,
                    last_event_datetime,
                    upper_inc=upper_inc,
                )
            else:
                synced_range = Range(start_datetime, end_datetime, upper_inc=True)
                txs = []

            LOGGER.info(
                "%s %s indexer: fetched %s",
                chain.value,
                event_type.value,
                str(synced_range),
            )

            add_indexer_range(
                session=session,
                chain=chain,
                event_type=event_type,
                datetime_range=synced_range,
            )

            # Committing periodically reduces the size of DB transactions for large numbers
            # of events.
            session.commit()

            # Now that the txs are committed to the DB, add them to the pending tx message queue
            for tx in txs:
                await self.pending_tx_publisher.publish_pending_tx(tx)

            if nb_events_fetched < limit:
                LOGGER.info(
                    "%s %s event indexer: done fetching events.",
                    chain.value,
                    event_type.value,
                )
                break

            start_datetime = synced_range.upper

    async def fetch_new_events(
        self,
        session: DbSession,
        indexer_url: str,
        smart_contract_address: str,
        event_type: ChainEventType,
    ) -> None:
        async with AlephIndexerClient(indexer_url=indexer_url) as indexer_client:
            account_state = await indexer_client.fetch_account_state(
                blockchain=self.blockchain,
                accounts=[smart_contract_address],
            )

            if not account_state.data.state:
                LOGGER.warning(
                    "No account data found for %s. Is the indexer up to date?",
                    smart_contract_address,
                )
                return

            indexer_multirange = MultiRange(
                *[
                    Range(rng[0], rng[1], upper_inc=True)
                    for rng in account_state.data.state[0].processed
                ]
            )

            multirange_to_sync = get_missing_indexer_datetime_multirange(
                session=session,
                chain=self.chain,
                event_type=event_type,
                indexer_multirange=indexer_multirange,
            )

            for range_to_sync in multirange_to_sync:
                LOGGER.info(
                    "%s %s event indexer: fetching %s",
                    self.chain.value,
                    event_type.value,
                    str(range_to_sync),
                )
                await self.fetch_range(
                    session=session,
                    indexer_client=indexer_client,
                    chain=self.chain,
                    event_type=event_type,
                    datetime_range=range_to_sync,
                )

    async def fetcher(
        self, indexer_url: str, smart_contract_address: str, event_type: ChainEventType
    ):
        while True:
            try:
                with self.session_factory() as session:
                    await self.fetch_new_events(
                        session=session,
                        indexer_url=indexer_url,
                        smart_contract_address=smart_contract_address,
                        event_type=event_type,
                    )
                    session.commit()
            except Exception:
                LOGGER.exception(
                    "An unexpected exception occurred, "
                    "relaunching %s %s indexer sync in 10 seconds",
                    self.chain.value,
                    event_type.value,
                )
            else:
                LOGGER.info(
                    "%s %s indexer: processed all transactions, waiting 10 seconds.",
                    self.chain.value,
                    event_type.value,
                )
            await asyncio.sleep(10)
