import datetime as dt
import logging
from dataclasses import dataclass
from decimal import Decimal
from typing import Any, Dict, Optional

from aiohttp import web
from aiohttp.web_exceptions import HTTPException
from aleph_message.models import ExecutableContent, ItemHash, MessageType
from dataclasses_json import DataClassJsonMixin
from pydantic import BaseModel, Field

from aleph.db.accessors.messages import get_message_by_item_hash, get_message_status
from aleph.db.models import MessageDb
from aleph.db.models.pending_messages import PendingMessageDb
from aleph.services.cost import compute_cost, compute_flow_cost
from aleph.types.db_session import DbSession
from aleph.types.message_status import MessageStatus
from aleph.web.controllers.app_state_getters import (
    get_session_factory_from_request,
    get_storage_service_from_request,
)

LOGGER = logging.getLogger(__name__)


# This is not defined in aiohttp.web_exceptions
class HTTPProcessing(HTTPException):
    status_code = 102


# Mapping between message statuses to their corresponding exceptions and messages
MESSAGE_STATUS_EXCEPTIONS = {
    MessageStatus.PENDING: (HTTPProcessing, "Message still pending"),
    MessageStatus.REJECTED: (web.HTTPNotFound, "This message was rejected"),
    MessageStatus.FORGOTTEN: (
        web.HTTPGone,
        "This message has been forgotten",
    ),
}


@dataclass
class MessagePrice(DataClassJsonMixin):
    """Dataclass used to expose message required tokens."""

    required_tokens: Optional[Decimal] = None


async def get_executable_message(session: DbSession, item_hash_str: str) -> MessageDb:
    """Attempt to get an executable message from the database.
    Raises an HTTP exception if the message is not found, not processed or is not an executable message.
    """

    # Parse the item_hash_str into an ItemHash object
    try:
        item_hash = ItemHash(item_hash_str)
    except ValueError:
        raise web.HTTPBadRequest(body=f"Invalid message hash: {item_hash_str}")

    # Get the message status from the database
    message_status_db = get_message_status(session=session, item_hash=item_hash)
    if not message_status_db:
        raise web.HTTPNotFound(body=f"Message not found with hash: {item_hash}")
    # Loop through the status_exceptions to find a match and raise the corresponding exception
    if message_status_db.status in MESSAGE_STATUS_EXCEPTIONS:
        exception, error_message = MESSAGE_STATUS_EXCEPTIONS[message_status_db.status]
        raise exception(body=f"{error_message}: {item_hash_str}")
    assert message_status_db.status == MessageStatus.PROCESSED

    # Get the message from the database
    message: Optional[MessageDb] = get_message_by_item_hash(session, item_hash)
    if not message:
        raise web.HTTPNotFound(body="Message not found, despite appearing as processed")
    if message.type not in (MessageType.instance, MessageType.program):
        raise web.HTTPBadRequest(
            body=f"Message is not an executable message: {item_hash_str}"
        )

    return message


async def message_price(request: web.Request):
    """Returns the price of an executable message."""

    session_factory = get_session_factory_from_request(request)
    with session_factory() as session:
        message = await get_executable_message(session, request.match_info["item_hash"])

        content: ExecutableContent = message.parsed_content
        try:
            if content.payment and content.payment.is_stream:
                required_tokens = compute_flow_cost(session=session, content=content)
            else:
                required_tokens = compute_cost(session=session, content=content)
        except RuntimeError as e:
            raise web.HTTPNotFound(reason=str(e))

    return web.json_response(
        {
            "required_tokens": float(required_tokens),
            "payment_type": content.payment.type if content.payment else None,
        }
    )


class PubMessageRequest(BaseModel):
    sync: bool = False
    message_dict: Dict[str, Any] = Field(alias="message")


async def message_price_estimate(request: web.Request):
    """Returns the estimated price of an executable message passed on the body."""

    session_factory = get_session_factory_from_request(request)
    storage_service = get_storage_service_from_request(request)

    with session_factory() as session:
        parsed_body = PubMessageRequest.parse_obj(await request.json())
        pending_message = PendingMessageDb.from_message_dict(
            parsed_body.message_dict,
            reception_time=dt.datetime.now(),
            fetched=False,
        )

        content_dict = await storage_service.get_message_content(pending_message)

        message = MessageDb.from_pending_message(
            pending_message,
            content_dict=content_dict.value,
            content_size=len(content_dict.raw_value),
        )

        content: ExecutableContent = message.parsed_content

        try:
            if content.payment and content.payment.is_stream:
                required_tokens = compute_flow_cost(session=session, content=content)
            else:
                required_tokens = compute_cost(session=session, content=content)
        except RuntimeError as e:
            raise web.HTTPNotFound(reason=str(e))

    return web.json_response(
        {
            "required_tokens": float(required_tokens),
            "payment_type": content.payment.type if content.payment else None,
        }
    )
