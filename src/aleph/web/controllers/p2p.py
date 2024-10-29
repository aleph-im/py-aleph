import asyncio
import json
import logging
from typing import Any, Dict, List, Optional, Union, cast

from aiohttp import web
from aleph_p2p_client import AlephP2PServiceClient
from configmanager import Config
from pydantic import BaseModel, Field, ValidationError

from aleph.services.ipfs import IpfsService
from aleph.services.p2p.pubsub import publish as pub_p2p
from aleph.toolkit.shield import shielded
from aleph.types.protocol import Protocol
from aleph.web.controllers.app_state_getters import (
    get_config_from_request,
    get_ipfs_service_from_request,
    get_p2p_client_from_request,
)
from aleph.web.controllers.utils import (
    PublicationStatus,
    broadcast_and_process_message,
    broadcast_status_to_http_status,
    validate_message_dict,
)

LOGGER = logging.getLogger(__name__)


def _validate_request_data(config: Config, request_data: Dict) -> None:
    """
    Validates the content of a JSON pubsub message depending on the channel
    and raises a 422 error if the data does not match the expected format.

    :param config: Application configuration.
    :param request_data: Request JSON data, as a dictionary.
    """

    topic = request_data.get("topic")

    # Only accept publishing on the message topic.
    message_topic = config.aleph.queue_topic.value
    if topic != message_topic:
        raise web.HTTPForbidden(
            reason=f"Unauthorized P2P topic: {topic}. Use {message_topic}."
        )

    data = request_data.get("data")
    if not isinstance(data, str):
        raise web.HTTPUnprocessableEntity(
            reason="'data': expected a serialized JSON string."
        )

    try:
        message_dict = json.loads(cast(str, request_data.get("data")))
    except ValueError:
        raise web.HTTPUnprocessableEntity(
            reason="'data': must be deserializable as JSON."
        )

    validate_message_dict(message_dict)


async def _pub_on_p2p_topics(
    p2p_client: AlephP2PServiceClient,
    ipfs_service: Optional[IpfsService],
    topic: str,
    payload: Union[str, bytes],
) -> List[Protocol]:
    failed_publications = []

    if ipfs_service:
        try:
            await asyncio.wait_for(ipfs_service.pub(topic, payload), 10)
        except Exception:
            LOGGER.exception("Can't publish on ipfs")
            failed_publications.append(Protocol.IPFS)

    try:
        await asyncio.wait_for(
            pub_p2p(
                p2p_client,
                topic,
                payload,
                loopback=True,
            ),
            10,
        )
    except Exception:
        LOGGER.exception("Can't publish on p2p")
        failed_publications.append(Protocol.P2P)

    return failed_publications


async def pub_json(request: web.Request):
    """Forward the message to P2P host and IPFS server as a pubsub message"""
    request_data = await request.json()
    _validate_request_data(
        config=get_config_from_request(request), request_data=request_data
    )

    ipfs_service = get_ipfs_service_from_request(request)
    p2p_client = get_p2p_client_from_request(request)

    failed_publications = await _pub_on_p2p_topics(
        p2p_client=p2p_client,
        ipfs_service=ipfs_service,
        topic=request_data.get("topic"),
        payload=request_data.get("data"),
    )
    pub_status = PublicationStatus.from_failures(failed_publications)

    return web.json_response(
        text=pub_status.model_dump_json(),
        status=500 if pub_status == "error" else 200,
    )


class PubMessageRequest(BaseModel):
    sync: bool = False
    message_dict: Dict[str, Any] = Field(alias="message")


@shielded
async def pub_message(request: web.Request):
    try:
        request_data = PubMessageRequest.model_validate(await request.json())
    except ValidationError as e:
        raise web.HTTPUnprocessableEntity(text=e.json(indent=4))
    except ValueError:
        # Body must be valid JSON
        raise web.HTTPUnprocessableEntity()

    pending_message = validate_message_dict(request_data.message_dict)
    broadcast_status = await broadcast_and_process_message(
        pending_message=pending_message,
        message_dict=request_data.message_dict,
        sync=request_data.sync,
        request=request,
        logger=LOGGER,
    )

    status_code = broadcast_status_to_http_status(broadcast_status)
    return web.json_response(
        text=broadcast_status.model_dump_json(), status=status_code
    )
