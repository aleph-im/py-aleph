""" While our own streamer libp2p protocol is still unstable, use direct
HTTP connection to standard rest API.
"""

import asyncio
import base64
import logging
from random import sample
from typing import List, Optional, Sequence

import aiohttp

LOGGER = logging.getLogger("P2P.HTTP")

SESSIONS = dict()


async def api_get_request(base_uri, method, timeout=1):
    if timeout not in SESSIONS:
        connector = aiohttp.TCPConnector(limit_per_host=5)
        SESSIONS[timeout] = aiohttp.ClientSession(
            read_timeout=timeout, connector=connector
        )

    uri = f"{base_uri}/api/v0/{method}"
    try:
        async with SESSIONS[timeout].get(uri) as resp:
            if resp.status != 200:
                result = None
            else:
                result = await resp.model_dump_json()
    except (
        TimeoutError,
        asyncio.TimeoutError,
        ConnectionRefusedError,
        aiohttp.ClientError,
        OSError,
    ):
        result = None
    except Exception:
        LOGGER.exception("Error in retrieval")
        result = None
    return result


async def get_peer_hash_content(
    base_uri: str, item_hash: str, timeout: int = 1
) -> Optional[bytes]:
    result = None
    item = await api_get_request(base_uri, f"storage/{item_hash}", timeout=timeout)
    if item is not None and item["status"] == "success" and item["content"] is not None:
        # TODO: IMPORTANT /!\ verify the hash of received data!
        return base64.decodebytes(item["content"].encode("utf-8"))
    else:
        LOGGER.debug(f"can't get hash {item_hash}")

    return result


async def request_hash(
    api_servers: Sequence[str], item_hash: str, timeout: int = 1
) -> Optional[bytes]:
    uris: List[str] = sample(api_servers, k=len(api_servers))

    for uri in uris:
        content = await get_peer_hash_content(uri, item_hash, timeout=timeout)
        if content is not None:
            return content

    return None  # Nothing found...
