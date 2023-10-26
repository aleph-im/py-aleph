"""
Job in charge of loading messages stored on-chain and put them in the pending message queue.
"""

import asyncio
import logging
from typing import Dict, Optional, Set

import aio_pika.abc
from configmanager import Config
from setproctitle import setproctitle

from aleph.chains.chain_data_service import ChainDataService
from aleph.db.accessors.pending_txs import (
    get_pending_tx,
    delete_pending_tx,
)
from aleph.db.connection import make_engine, make_session_factory
from aleph.handlers.message_handler import MessagePublisher
from aleph.services.cache.node_cache import NodeCache
from aleph.services.ipfs.common import make_ipfs_client
from aleph.services.ipfs.service import IpfsService
from aleph.services.storage.fileystem_engine import FileSystemStorageEngine
from aleph.storage import StorageService
from aleph.toolkit.logging import setup_logging
from aleph.toolkit.monitoring import setup_sentry
from aleph.toolkit.timestamp import utc_now
from aleph.types.chain_sync import ChainSyncProtocol
from aleph.types.db_session import DbSessionFactory
from .job_utils import prepare_loop

LOGGER = logging.getLogger(__name__)


class PendingTxProcessor:
    def __init__(
        self,
        session_factory: DbSessionFactory,
        message_publisher: MessagePublisher,
        chain_data_service: ChainDataService,
        pending_tx_queue: aio_pika.abc.AbstractQueue,
    ):
        self.session_factory = session_factory
        self.message_publisher = message_publisher
        self.chain_data_service = chain_data_service
        self.pending_tx_queue = pending_tx_queue

    async def handle_pending_tx(
        self, pending_tx_hash: str, seen_ids: Optional[Set[str]] = None
    ) -> None:
        with self.session_factory() as session:
            pending_tx = get_pending_tx(session=session, tx_hash=pending_tx_hash)

            if pending_tx is None:
                LOGGER.warning("TX %s is not pending anymore", pending_tx_hash)
                return

            tx = pending_tx.tx
            LOGGER.info("%s Handling TX in block %s", tx.chain, tx.height)

            # If the chain data file is unavailable, we leave it to the pending tx
            # processor to log the content unavailable exception and retry later.
            messages = await self.chain_data_service.get_tx_messages(
                tx=pending_tx.tx, seen_ids=seen_ids
            )

            if messages:
                for i, message_dict in enumerate(messages):
                    await self.message_publisher.add_pending_message(
                        message_dict=message_dict,
                        reception_time=utc_now(),
                        tx_hash=tx.hash,
                        check_message=tx.protocol != ChainSyncProtocol.SMART_CONTRACT,
                    )

            else:
                LOGGER.debug("TX contains no message")

            if messages is not None:
                # bogus or handled, we remove it.
                delete_pending_tx(session=session, tx_hash=pending_tx_hash)
                session.commit()

    async def process_pending_txs(self) -> None:
        """
        Process chain transactions in the Pending TX queue.
        """

        seen_ids: Set[str] = set()
        LOGGER.info("handling TXs")
        async with self.pending_tx_queue.iterator() as queue_iter:
            async for pending_tx_message in queue_iter:
                async with pending_tx_message.process():
                    pending_tx_hash = pending_tx_message.body.decode("utf-8")
                    await self.handle_pending_tx(
                        pending_tx_hash=pending_tx_hash, seen_ids=seen_ids
                    )


async def make_pending_tx_queue(config: Config) -> aio_pika.abc.AbstractQueue:
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
    pending_tx_queue = await channel.declare_queue(
        name="pending-tx-queue", durable=True, auto_delete=False
    )
    await pending_tx_queue.bind(pending_tx_exchange, routing_key="#")
    return pending_tx_queue


async def handle_txs_task(config: Config):
    engine = make_engine(config=config, application_name="aleph-txs")
    session_factory = make_session_factory(engine)

    node_cache = NodeCache(
        redis_host=config.redis.host.value, redis_port=config.redis.port.value
    )
    ipfs_client = make_ipfs_client(config)
    ipfs_service = IpfsService(ipfs_client=ipfs_client)
    storage_service = StorageService(
        storage_engine=FileSystemStorageEngine(folder=config.storage.folder.value),
        ipfs_service=ipfs_service,
        node_cache=node_cache,
    )
    message_publisher = MessagePublisher(
        session_factory=session_factory,
        storage_service=storage_service,
        config=config,
    )
    chain_data_service = ChainDataService(
        session_factory=session_factory, storage_service=storage_service
    )
    pending_tx_queue = await make_pending_tx_queue(config=config)
    pending_tx_processor = PendingTxProcessor(
        session_factory=session_factory,
        message_publisher=message_publisher,
        chain_data_service=chain_data_service,
        pending_tx_queue=pending_tx_queue,
    )

    while True:
        try:
            await pending_tx_processor.process_pending_txs()
            await asyncio.sleep(5)
        except Exception:
            LOGGER.exception("Error in pending txs job")

        await asyncio.sleep(0.01)


def pending_txs_subprocess(config_values: Dict):
    setproctitle("aleph.jobs.txs_task_loop")
    loop, config = prepare_loop(config_values)

    setup_sentry(config)
    setup_logging(
        loglevel=config.logging.level.value,
        filename="/tmp/txs_task_loop.log",
        max_log_file_size=config.logging.max_log_file_size.value,
    )

    loop.run_until_complete(handle_txs_task(config))
