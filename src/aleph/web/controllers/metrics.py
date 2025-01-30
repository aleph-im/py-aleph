import asyncio
import json
import platform
from dataclasses import asdict, dataclass
from logging import getLogger
from typing import Dict, Optional
from urllib.parse import urljoin

import aiohttp
from aiocache import cached
from aleph_message.models import Chain
from dataclasses_json import DataClassJsonMixin
from requests import HTTPError
from web3 import Web3

from aleph import __version__
from aleph.config import get_config
from aleph.db.accessors.chains import get_last_height
from aleph.db.models import FilePinDb, MessageDb, PeerDb, PendingMessageDb, PendingTxDb
from aleph.services.cache.node_cache import NodeCache
from aleph.types.chain_sync import ChainEventType
from aleph.types.db_session import DbSession

LOGGER = getLogger("WEB.metrics")

config = get_config()


def format_dict_for_prometheus(values: Dict) -> str:
    """Format a dict to a Prometheus tags string"""
    values = (
        f"{key}={json.dumps(value)}"
        for key, value in values.items()
        if value is not None
    )
    return "{" + ",".join(values) + "}"


def format_dataclass_for_prometheus(instance) -> str:
    """Turn a dataclass into Prometheus text format"""

    result = []
    for key, value in asdict(instance).items():
        if value is None:
            # prometheus don't like null value
            continue
        if isinstance(value, dict):
            # Use a constant value of 1 for version, as Prometheus does
            result.append(f"{key}{format_dict_for_prometheus(value)} 1")
        else:
            result.append(f"{key} {json.dumps(value)}")
    return "\n".join(result)


@dataclass
class BuildInfo:
    """Dataclass used to export aleph node build info."""

    python_version: str
    version: str
    # branch: str
    # revision: str


@dataclass
class Metrics(DataClassJsonMixin):
    """Dataclass used to expose aleph node metrics.

    Naming convention: https://prometheus.io/docs/practices/naming/
    """

    pyaleph_build_info: BuildInfo

    pyaleph_status_peers_total: int

    pyaleph_status_sync_messages_total: int
    pyaleph_status_sync_permanent_files_total: int

    pyaleph_status_sync_pending_messages_total: int
    pyaleph_status_sync_pending_txs_total: int

    pyaleph_status_chain_eth_last_committed_height: Optional[int]

    pyaleph_processing_pending_messages_seen_ids_total: Optional[int] = None
    pyaleph_processing_pending_messages_tasks_total: Optional[int] = None
    pyaleph_processing_pending_messages_aggregate_tasks: int = 0
    pyaleph_processing_pending_messages_forget_tasks: int = 0
    pyaleph_processing_pending_messages_post_tasks: int = 0
    pyaleph_processing_pending_messages_program_tasks: int = 0
    pyaleph_processing_pending_messages_store_tasks: int = 0

    pyaleph_processing_pending_messages_action_total: Optional[int] = None

    pyaleph_status_sync_messages_reference_total: Optional[int] = None
    pyaleph_status_sync_messages_remaining_total: Optional[int] = None
    pyaleph_status_chain_eth_height_reference_total: Optional[int] = None
    pyaleph_status_chain_eth_height_remaining_total: Optional[int] = None


pyaleph_build_info = BuildInfo(
    python_version=platform.python_version(),
    version=__version__,
)


# Cache Aleph messages count for 2 minutes by default
@cached(ttl=config.aleph.cache.ttl.total_aleph_messages)
async def fetch_reference_total_messages() -> Optional[int]:
    """Obtain the total number of Aleph messages from another node."""
    LOGGER.debug("Fetching Aleph messages count")

    config = get_config()
    url = config.aleph.reference_node_url.value
    if url is None:
        return None

    async with aiohttp.ClientSession() as session:
        try:
            async with session.get(
                urljoin(url, "metrics.json"), raise_for_status=True
            ) as resp:
                data = await resp.json()
                return int(data["pyaleph_status_sync_messages_total"])
        except aiohttp.ClientResponseError:
            LOGGER.warning("ETH height could not be obtained")
            return None


# Cache ETH height for 10 minutes by default
@cached(ttl=config.aleph.cache.ttl.eth_height)
async def fetch_eth_height() -> Optional[int]:
    """Obtain the height of the Ethereum blockchain."""
    LOGGER.debug("Fetching ETH height")
    config = get_config()

    try:
        if config.ethereum.enabled.value:
            w3 = Web3(Web3.HTTPProvider(config.ethereum.api_url.value))
            return await asyncio.get_event_loop().run_in_executor(
                None, getattr, w3.eth, "block_number"
            )
        else:
            return None
    except HTTPError:
        return -1  # We got a boggus value!


# Cache metrics for 10 seconds by default
@cached(ttl=config.aleph.cache.ttl.metrics)
async def get_metrics(session: DbSession, node_cache: NodeCache) -> Metrics:
    sync_messages_reference_total = await fetch_reference_total_messages()
    eth_reference_height = await fetch_eth_height()

    # select count(*) can be slow but estimates are not refreshed often enough to give
    # a meaningful sense of progress for the node main page on /.
    sync_messages_total: int = MessageDb.count(session=session)
    peers_count = PeerDb.count(session=session)

    eth_last_committed_height = get_last_height(
        session=session, chain=Chain.ETH, sync_type=ChainEventType.SYNC
    )

    if not (sync_messages_reference_total is None or sync_messages_total is None):
        sync_messages_remaining_total = (
            sync_messages_reference_total - sync_messages_total
        )
    else:
        sync_messages_remaining_total = None

    if eth_reference_height is not None and eth_last_committed_height is not None:
        # Some blocks may not contain Aleph messages, and therefore the last committed height
        # may be higher than the height of the last block containing Aleph messages.
        eth_remaining_height = max(eth_reference_height - eth_last_committed_height, 0)
    else:
        eth_remaining_height = None

    retry_message_job_tasks = await node_cache.get("retry_messages_job_tasks")
    nb_message_jobs = int(retry_message_job_tasks) if retry_message_job_tasks else 0

    return Metrics(
        pyaleph_build_info=pyaleph_build_info,
        pyaleph_status_peers_total=peers_count,
        pyaleph_processing_pending_messages_tasks_total=nb_message_jobs,
        pyaleph_status_sync_messages_total=sync_messages_total,
        pyaleph_status_sync_permanent_files_total=FilePinDb.count(session=session),
        pyaleph_status_sync_messages_reference_total=sync_messages_reference_total,
        pyaleph_status_sync_messages_remaining_total=sync_messages_remaining_total,
        pyaleph_status_sync_pending_messages_total=PendingMessageDb.count(
            session=session
        ),
        pyaleph_status_sync_pending_txs_total=PendingTxDb.count(session=session),
        pyaleph_status_chain_eth_last_committed_height=eth_last_committed_height,
        pyaleph_status_chain_eth_height_reference_total=eth_reference_height,
        pyaleph_status_chain_eth_height_remaining_total=eth_remaining_height,
    )
