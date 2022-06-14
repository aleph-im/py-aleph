"""
Job that shares unconfirmed messages on the network periodically.

Messages are shared in real-time on the network using P2P and IPFS topics. However,
some nodes may be offline at the time and miss these messages. While this is not an
issue usually for regular nodes (they will just receive the message later on from
on-chain data), this can become problematic if a node in charge of pushing messages
on-chain is down for a set amount of time. if this happens, the messages shared during
the downtime of this/these node(s) would simply not be published on-chain, causing
synchronisation issues on the network.

This job provides a solution to this issue. It works in three parts:
* the publisher task periodically sends the list of all the messages older than
  the last TX block that have yet to be confirmed by on-chain data.
* the receiver task stores the list of unconfirmed messages for each peer detected
  on the network.
* the sync/aggregator task aggregates the confirmation data from all the nodes
  and fetches messages using the HTTP API. These messages are added to the pending
  message queue.

Note: currently, we use an IPFS topic given the unreliability of the P2P daemon.
TODO: use a P2P topic once we have a solution (ex: Rust bindings).
"""

import asyncio
import json
import logging
import random
import time
from typing import List, Dict, Optional

import pytz
import sentry_sdk
from configmanager import Config
from setproctitle import setproctitle

from aleph.exceptions import InvalidMessageError
from aleph.logging import setup_logging
from aleph.model.messages import Message
from aleph.model.pending import PendingMessage, PendingTX
from aleph.network import check_message, INCOMING_MESSAGE_AUTHORIZED_FIELDS
from aleph.services.ipfs.pubsub import pub as pub_ipfs, sub as sub_ipfs
from aleph.services.p2p import singleton
from aleph.services.p2p.http import get_messages_from_peer
from .job_utils import prepare_loop
from ..model.chains import Chain
from ..model.p2p import Peer
from ..model.unconfirmed_messages import UnconfirmedMessage
from ..toolkit.split import split_iterable
from ..toolkit.string import truncate_log

PUB_LOGGER = logging.getLogger(f"{__name__}.publish")
SYNC_LOGGER = logging.getLogger(f"{__name__}.sync")


async def list_unconfirmed_message_hashes(older_than: float, limit: int) -> List[str]:
    """
    Returns the list of the hashes of unconfirmed messages, up to `limit` messages.
    :param older_than: Epoch timestamp. The function will only return unconfirmed
                       messages older than this value.
    :param limit: Maximum number of messages to return.
    """

    unconfirmed_hashes = [
        msg["item_hash"]
        async for msg in (
            Message.collection.find(
                filter={"confirmed": False, "time": {"$lt": older_than}},
                projection={"_id": 0, "item_hash": 1},
            )
            .sort([("time", 1)])
            .limit(limit)
        )
    ]

    return unconfirmed_hashes


async def publish_unconfirmed_messages(
    topic: str, older_than: float, limit: int = 10000
):
    unconfirmed_messages = await list_unconfirmed_message_hashes(
        older_than=older_than, limit=limit
    )
    PUB_LOGGER.info("Publishing %d unconfirmed messages", len(unconfirmed_messages))
    data = json.dumps(unconfirmed_messages).encode("utf-8")
    await pub_ipfs(topic, data)


async def receive_unconfirmed_messages(topic: str):
    """
    Receives unconfirmed messages sync data from the network and stores it in the DB.
    :param topic: The IPFS topic where unconfirmed messages sync data is published.
    """

    restart_wait_time = 2

    while True:
        try:
            async for mvalue in sub_ipfs(topic):

                sender = mvalue["from"]
                data = mvalue["data"].decode("utf-8")
                try:
                    unconfirmed_hashes = json.loads(data)
                except json.JSONDecodeError:
                    SYNC_LOGGER.warning(
                        "Could not parse sync data from %s: %s",
                        sender,
                        truncate_log(data, 100),
                    )
                    continue

                SYNC_LOGGER.info(
                    "Peer %s notified %d unconfirmed messages",
                    sender,
                    len(unconfirmed_hashes),
                )
                await UnconfirmedMessage.collection.update(
                    {
                        "$set": {
                            "peer_id": sender,
                            "hashes": unconfirmed_hashes,
                            "reception_time": time.time(),
                        },
                    },
                    upsert=True,
                )
        except Exception:
            SYNC_LOGGER.exception(
                "Unexpected exception while syncing unconfirmed messages."
                "Restarting in %d seconds...",
                restart_wait_time,
            )
            await asyncio.sleep(restart_wait_time)


async def aggregate_unconfirmed_hashes(from_time: float) -> Dict[str, List[str]]:
    """
    Returns a dictionary of item_hash -> providers based on unconfirmed messages
    sent by the other nodes.

    :param from_time: Minimum reception time. Documents received before this epoch time
                      will be ignored from the query.
    :return: A dictionary of item_hash -> providers of all unconfirmed messages
             notified by the nodes on the network.
    """

    unconfirmed_message_sources: Dict[str, List[str]] = {}

    async for result in UnconfirmedMessage.collection.aggregate(
        [
            # Filter out old data
            {"$match": {"reception_time": {"$gte": from_time}}},
            # List the senders by message item_hash
            {"$unwind": "$hashes"},
            {"$group": {"_id": "$hashes", "providers": {"$push": "$sender"}}},
            # Only return messages not already present on the node
            {
                "$lookup": {
                    "from": "messages",
                    "localField": "_id",
                    "foreignField": "item_hash",
                    "as": "existing_messages",
                },
            },
            {"$match": {"existing_messages": {"$eq": []}}},
        ]
    ):
        unconfirmed_message_sources[result["_id"]] = result["providers"]

    return unconfirmed_message_sources


async def fetch_and_verify_message(
    item_hash: str, providers: List[str]
) -> Optional[Dict]:
    """
    Fetches a message from the HTTP API of a peer ID randomly selected in the list
    of providers.

    Validates the data sent by the node and discards corrupted data.

    :param item_hash: Item hash of the message to retrieve.
    :param providers: List of peer IDs that announced they have the message. Each provider
                      is expected to be able to provide the message.
    :return: The message, or None if it could not be fetched from any of the providers.
    """

    randomized_providers = random.sample(providers, len(providers))
    for provider in randomized_providers:
        provider_uri = await Peer.get_peer_address(peer_id=provider, peer_type="HTTP")
        if provider_uri is None:
            SYNC_LOGGER.warning("Could not determine HTTP address for %s", provider)
            continue

        messages = await get_messages_from_peer(
            peer_uri=provider_uri, item_hash=item_hash, timeout=15
        )
        if messages is None:
            SYNC_LOGGER.warning(
                "Message %s could not be fetched from %s", item_hash, provider
            )
            continue

        message = messages[0]
        try:
            # Check the message immediately, signature included, in order to
            # ignore any node that tampers with the data. Calling the function
            # this way also filters out fields added by the API and allows
            # to add the message to the pending queue as is.
            return await check_message(message, from_network=True, trusted=False)
        except InvalidMessageError as e:
            SYNC_LOGGER.warning(
                "Message fetched from %s is invalid: %s", provider, str(e)
            )

    return None


async def fetch_and_verify_message_task(
    item_hash: str, providers: List[str], task_semaphore: asyncio.Semaphore
) -> Optional[Dict]:
    async with task_semaphore:
        return await fetch_and_verify_message(item_hash, providers)


async def fetch_missing_messages(last_run_time: float):
    unconfirmed_message_sources = await aggregate_unconfirmed_hashes(
        from_time=last_run_time
    )

    if not unconfirmed_message_sources:
        SYNC_LOGGER.info(
            "No unconfirmed messages notified by the network, nothing to do."
        )
        return

    SYNC_LOGGER.info(
        "Found %d messages to fetch from the network", len(unconfirmed_message_sources)
    )

    message_semaphore = asyncio.BoundedSemaphore(100)

    tasks = [
        asyncio.create_task(
            fetch_and_verify_message_task(
                item_hash, providers, task_semaphore=message_semaphore
            )
        )
        for item_hash, providers in unconfirmed_message_sources.items()
    ]

    results = await asyncio.gather(*tasks, return_exceptions=True)
    errors, messages = split_iterable(
        results, lambda result: isinstance(result, BaseException)
    )

    for error in errors:
        SYNC_LOGGER.exception(
            "unexpected error while fetching unconfirmed messages",
            exc_info=(type(error), error, error.__traceback__),
        )

    pending_messages = [
        {
            # TODO: this filtering is redundant as long as we use check_message(trusted=True).
            #       update this to use the PendingMessage models added in #273.
            "message": {
                k: v
                for k, v in message.items()
                if k in INCOMING_MESSAGE_AUTHORIZED_FIELDS
            },
            "source": {
                "chain_name": None,
                "tx_hash": None,
                "height": None,
                "check_message": True,
            },
        }
        for message in messages
    ]
    await PendingMessage.collection.bulk_write(pending_messages)


async def get_last_processed_tx_time() -> float:
    """
    Returns the epoch timestamp of the last processed TX in the chains collection.
    """

    # TODO: check if this is correct in a multi-chain context
    last_processed_tx = await Chain.collection.find(
        {"_id": 0, "last_update": 1},
        sort={"last_update": -1},
        limit=1,
    )
    if last_processed_tx is None:
        raise ValueError("Could not find last processed TX.")

    localized_update_datetime = pytz.utc.localize(last_processed_tx["last_update"])
    return localized_update_datetime.timestamp()


async def publish_unconfirmed_messages_loop(topic: str, job_period: float):
    while True:
        try:
            # Avoid publishing data if the node is currently syncing
            nb_pending_txs = await PendingTX.collection.count_documents({})

            if nb_pending_txs:
                PUB_LOGGER.info(
                    "Node currently syncing (%d pending txs). Unconfirmed messages not published.",
                    nb_pending_txs,
                )

            else:
                last_processed_tx = await Chain.collection.find_one(
                    {"chain": "ETH"}, {"_id": 0, "last_update": 1}
                )
                await publish_unconfirmed_messages(
                    topic=topic,
                    older_than=last_processed_tx["last_update"].epoch(),
                    limit=10000,
                )
        except Exception:
            PUB_LOGGER.exception("Could not publish unconfirmed messages")

        await asyncio.sleep(job_period)


async def sync_unconfirmed_messages_loop(job_period: float):
    await asyncio.sleep(4)
    last_run_time = time.time()

    SYNC_LOGGER.info("Running sync aggregate task every %d seconds", job_period)

    while True:
        try:
            await fetch_missing_messages(last_run_time=last_run_time)
        except Exception:
            SYNC_LOGGER.exception("Error in sync unconfirmed messages job")

        await asyncio.sleep(job_period)


async def run_sync_message_tasks(config: Config):
    topic = config.ipfs.sync_topic.value
    job_period = 300

    await asyncio.gather(
        publish_unconfirmed_messages_loop(topic=topic, job_period=job_period),
        receive_unconfirmed_messages(topic=topic),
        sync_unconfirmed_messages_loop(job_period=job_period),
    )


def sync_unconfirmed_messages_subprocess(config_values: Dict, api_servers: List):
    proctitle = "aleph.jobs.sync_unconfirmed_messages"
    setproctitle(proctitle)
    loop, config = prepare_loop(config_values)

    sentry_sdk.init(
        dsn=config.sentry.dsn.value,
        traces_sample_rate=config.sentry.traces_sample_rate.value,
        ignore_errors=[KeyboardInterrupt],
    )
    setup_logging(
        loglevel=config.logging.level.value,
        filename=f"/tmp/{proctitle.replace('.', '-')}.log",
        max_log_file_size=config.logging.max_log_file_size.value,
    )
    singleton.api_servers = api_servers

    loop.run_until_complete(run_sync_message_tasks(config))
