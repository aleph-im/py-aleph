import json
import logging
from typing import Any, Dict, Iterable, List, Optional

import aio_pika.abc
import aiohttp.web_ws
from aiohttp import WSMsgType, web
from aleph_message.models import Chain, ItemHash, MessageType
from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    ValidationError,
    field_validator,
    model_validator,
)

import aleph.toolkit.json as aleph_json
from aleph.db.accessors.messages import (
    count_matching_messages,
    get_forgotten_message,
    get_matching_messages,
    get_message_by_item_hash,
    get_message_status,
    get_rejected_message,
)
from aleph.db.accessors.pending_messages import get_pending_messages
from aleph.db.models import MessageDb, MessageStatusDb
from aleph.schemas.api.messages import (
    AlephMessage,
    ForgottenMessage,
    ForgottenMessageStatus,
    MessageWithStatus,
    PendingMessage,
    PendingMessageStatus,
    PostMessage,
    ProcessedMessageStatus,
    RejectedMessageStatus,
    format_message,
    format_message_dict,
)
from aleph.toolkit.shield import shielded
from aleph.types.db_session import DbSession, DbSessionFactory
from aleph.types.message_status import MessageStatus
from aleph.types.sort_order import SortBy, SortOrder
from aleph.web.controllers.app_state_getters import (
    get_config_from_request,
    get_mq_ws_channel_from_request,
    get_session_factory_from_request,
)
from aleph.web.controllers.utils import (
    DEFAULT_MESSAGES_PER_PAGE,
    DEFAULT_PAGE,
    LIST_FIELD_SEPARATOR,
    mq_make_aleph_message_topic_queue,
)

LOGGER = logging.getLogger(__name__)


DEFAULT_WS_HISTORY = 10


class BaseMessageQueryParams(BaseModel):
    sort_by: SortBy = Field(
        default=SortBy.TIME,
        alias="sortBy",
        description="Key to use to sort the messages. "
        "'time' uses the message time field. "
        "'tx-time' uses the first on-chain confirmation time.",
    )
    sort_order: SortOrder = Field(
        default=SortOrder.DESCENDING,
        alias="sortOrder",
        description="Order in which messages should be listed: "
        "-1 means most recent messages first, 1 means older messages first.",
    )
    message_type: Optional[MessageType] = Field(
        default=None,
        alias="msgType",
        description="Message type. Deprecated: use msgTypes instead",
    )
    message_types: Optional[List[MessageType]] = Field(
        default=None, alias="msgTypes", description="Accepted message types."
    )
    addresses: Optional[List[str]] = Field(
        default=None, description="Accepted values for the 'sender' field."
    )
    refs: Optional[List[str]] = Field(
        default=None, description="Accepted values for the 'content.ref' field."
    )
    content_hashes: Optional[List[ItemHash]] = Field(
        default=None,
        alias="contentHashes",
        description="Accepted values for the 'content.item_hash' field.",
    )
    content_keys: Optional[List[ItemHash]] = Field(
        default=None,
        alias="contentKeys",
        description="Accepted values for the 'content.keys' field.",
    )
    content_types: Optional[List[str]] = Field(
        default=None,
        alias="contentTypes",
        description="Accepted values for the 'content.type' field.",
    )
    chains: Optional[List[Chain]] = Field(
        default=None, description="Accepted values for the 'chain' field."
    )
    channels: Optional[List[str]] = Field(
        default=None, description="Accepted values for the 'channel' field."
    )
    tags: Optional[List[str]] = Field(
        default=None, description="Accepted values for the 'content.content.tag' field."
    )
    hashes: Optional[List[ItemHash]] = Field(
        default=None, description="Accepted values for the 'item_hash' field."
    )

    @model_validator(mode="after")
    def validate_field_dependencies(self):
        start_date = self.start_date
        end_date = self.end_date
        if start_date and end_date and (end_date < start_date):
            raise ValueError("end date cannot be lower than start date.")
        start_block = self.start_block
        end_block = self.end_block
        if start_block and end_block and (end_block < start_block):
            raise ValueError("end block cannot be lower than start block.")

        return self

    @field_validator(
        "hashes",
        "addresses",
        "refs",
        "content_hashes",
        "content_keys",
        "content_types",
        "chains",
        "channels",
        "message_types",
        "tags",
        mode="before",
    )
    @classmethod
    def split_str(cls, v):
        if isinstance(v, str):
            return v.split(LIST_FIELD_SEPARATOR)
        return v

    model_config = ConfigDict(populate_by_name=True)


class MessageQueryParams(BaseMessageQueryParams):
    pagination: int = Field(
        default=DEFAULT_MESSAGES_PER_PAGE,
        ge=0,
        description="Maximum number of messages to return. Specifying 0 removes this limit.",
    )
    page: int = Field(
        default=DEFAULT_PAGE, ge=1, description="Offset in pages. Starts at 1."
    )

    start_date: float = Field(
        default=0,
        ge=0,
        alias="startDate",
        description="Start date timestamp. If specified, only messages with "
        "a time field greater or equal to this value will be returned.",
    )
    end_date: float = Field(
        default=0,
        ge=0,
        alias="endDate",
        description="End date timestamp. If specified, only messages with "
        "a time field lower than this value will be returned.",
    )

    start_block: int = Field(
        default=0,
        ge=0,
        alias="startBlock",
        description="Start block number. If specified, only messages with "
        "a block number greater or equal to this value will be returned.",
    )
    end_block: int = Field(
        default=0,
        ge=0,
        alias="endBlock",
        description="End block number. If specified, only messages with "
        "a block number lower than this value will be returned.",
    )


class WsMessageQueryParams(BaseMessageQueryParams):
    history: Optional[int] = Field(
        DEFAULT_WS_HISTORY,
        ge=0,
        lt=200,
        description="Historical elements to send through the websocket.",
    )


def message_to_dict(message: MessageDb) -> Dict[str, Any]:
    message_dict = message.to_dict()
    message_dict["time"] = message.time.timestamp()
    confirmations = [
        {"chain": c.chain, "hash": c.hash, "height": c.height}
        for c in message.confirmations
    ]
    message_dict["confirmations"] = confirmations
    message_dict["confirmed"] = bool(confirmations)
    return message_dict


def format_response_dict(
    messages: List[Dict[str, Any]], pagination: int, page: int, total_messages: int
) -> Dict[str, Any]:
    return {
        "messages": messages,
        "pagination_per_page": pagination,
        "pagination_page": page,
        "pagination_total": total_messages,
        "pagination_item": "messages",
    }


def format_response(
    messages: Iterable[MessageDb], pagination: int, page: int, total_messages: int
) -> web.Response:
    formatted_messages = [message_to_dict(message) for message in messages]

    response = format_response_dict(
        messages=formatted_messages,
        pagination=pagination,
        page=page,
        total_messages=total_messages,
    )

    return web.json_response(text=aleph_json.dumps(response).decode("utf-8"))


async def view_messages_list(request: web.Request) -> web.Response:
    """Messages list view with filters"""

    try:
        query_params = MessageQueryParams.model_validate(request.query)
    except ValidationError as e:
        raise web.HTTPUnprocessableEntity(text=e.json(indent=4))

    # If called from the messages/page/{page}.json endpoint, override the page
    # parameters with the URL one
    if url_page_param := request.match_info.get("page"):
        query_params.page = int(url_page_param)

    find_filters = query_params.model_dump(exclude_none=True)

    pagination_page = query_params.page
    pagination_per_page = query_params.pagination

    session_factory = get_session_factory_from_request(request)
    with session_factory() as session:
        messages = get_matching_messages(
            session, include_confirmations=True, **find_filters
        )
        total_msgs = count_matching_messages(session, **find_filters)

        return format_response(
            messages,
            pagination=pagination_per_page,
            page=pagination_page,
            total_messages=total_msgs,
        )


async def _send_history_to_ws(
    ws: aiohttp.web_ws.WebSocketResponse,
    session_factory: DbSessionFactory,
    history: int,
    query_params: WsMessageQueryParams,
) -> None:
    with session_factory() as session:
        messages = get_matching_messages(
            session=session,
            pagination=history,
            include_confirmations=True,
            **query_params.model_dump(exclude_none=True),
        )
        for message in messages:
            await ws.send_str(format_message(message).model_dump_json())


def message_matches_filters(
    message: AlephMessage, query_params: WsMessageQueryParams
) -> bool:
    if message_type := query_params.message_type:
        if message.type != message_type:
            return False

    # For simple filters, this reduces the amount of boilerplate
    filters_by_message_field = {
        "sender": "addresses",
        "type": "message_type",
        "item_hash": "hashes",
        "chain": "chains",
        "channel": "channels",
    }

    for message_field, query_field in filters_by_message_field.items():
        if user_filters := getattr(query_params, query_field):
            if not isinstance(user_filters, list):
                user_filters = [user_filters]
            if getattr(message, message_field) not in user_filters:
                return False

    # Process filters on content and content.content
    message_content = message.content
    if refs := query_params.refs:
        ref = getattr(message_content, "ref", None)
        if ref not in refs:
            return False

    if content_types := query_params.content_types:
        content_type = getattr(message_content, "type", None)
        if content_type not in content_types:
            return False

    if content_hashes := query_params.content_hashes:
        content_hash = getattr(message_content, "item_hash", None)
        if content_hash not in content_hashes:
            return False

    # For tags, we only need to match one filter
    if query_tags := query_params.tags:
        nested_content = getattr(message.content, "content")
        if not nested_content:
            return False

        content_tags = set(getattr(nested_content, "tags", []))
        if (content_tags & set(query_tags)) == set():
            return False

    return True


async def _start_mq_consumer(
    ws: aiohttp.web_ws.WebSocketResponse,
    mq_queue: aio_pika.abc.AbstractQueue,
    query_params: WsMessageQueryParams,
) -> aio_pika.abc.ConsumerTag:
    """
    Starts the consumer task responsible for forwarding new aleph.im messages from
    the processing pipeline to a websocket.

    :param ws: Websocket.
    :param mq_queue: Message queue object.
    :param query_params: Message filters specified by the caller.
    """

    async def _process_message(mq_message: aio_pika.abc.AbstractMessage):
        payload_bytes = mq_message.body
        payload_dict = aleph_json.loads(payload_bytes)
        message = format_message_dict(payload_dict["message"])

        if message_matches_filters(message=message, query_params=query_params):
            try:
                await ws.send_str(message.model_dump_json())
            except ConnectionResetError:
                # We can detect the WS closing in this task in addition to the main one.
                # The main task will also detect the close event.
                # We just ignore this exception to avoid the "task exception was never retrieved"
                # warning.
                LOGGER.info("Cannot send messages because the websocket is closed")

    # Note that we use the consume pattern here instead of using the `queue.iterator()`
    # pattern because cancelling the iterator attempts to close the queue and channel.
    # See discussion here: https://github.com/mosquito/aio-pika/issues/358
    consumer_tag = await mq_queue.consume(_process_message, no_ack=True)
    return consumer_tag


@shielded
async def messages_ws(request: web.Request) -> web.WebSocketResponse:
    ws = web.WebSocketResponse()
    await ws.prepare(request)

    config = get_config_from_request(request)
    session_factory = get_session_factory_from_request(request)
    mq_channel = await get_mq_ws_channel_from_request(request=request, logger=LOGGER)

    try:
        query_params = WsMessageQueryParams.model_validate(request.query)
    except ValidationError as e:
        raise web.HTTPUnprocessableEntity(text=e.json(indent=4))

    history = query_params.history

    if history:
        try:
            await _send_history_to_ws(
                ws=ws,
                session_factory=session_factory,
                history=history,
                query_params=query_params,
            )
        except ConnectionResetError:
            LOGGER.info("Could not send history, aborting message websocket")
            return ws

    mq_queue = await mq_make_aleph_message_topic_queue(
        channel=mq_channel, config=config, routing_key="processed.*"
    )
    consumer_tag = None

    try:
        # Start a task to handle outgoing traffic to the websocket.
        consumer_tag = await _start_mq_consumer(
            ws=ws,
            mq_queue=mq_queue,
            query_params=query_params,
        )
        LOGGER.debug(
            "Started consuming mq %s for websocket. Consumer tag: %s",
            mq_queue.name,
            consumer_tag,
        )

        # Wait for the websocket to close.
        while not ws.closed:
            # Users can potentially send anything to the websocket. Ignore these messages
            # and only handle "close" messages.
            ws_msg = await ws.receive()
            LOGGER.debug("rx ws msg: %s", str(ws_msg))
            if ws_msg.type == WSMsgType.CLOSE:
                LOGGER.debug("ws close received")
                break

    finally:
        # In theory, we should cancel the consumer with `mq_queue.cancel()` before deleting the queue.
        # In practice, this sometimes leads to an RPC timeout that closes the channel.
        # To avoid this situation, we just delete the queue directly.
        # Note that even if the queue is in auto-delete mode, it will only be deleted automatically
        # once the channel closes. We delete it manually to avoid keeping queues around.
        if consumer_tag:
            LOGGER.info("Deleting consumer %s (queue: %s)", consumer_tag, mq_queue.name)
            await mq_queue.cancel(consumer_tag=consumer_tag)

        LOGGER.info("Deleting queue: %s", mq_queue.name)
        await mq_queue.delete(if_unused=False, if_empty=False)

    return ws


def _get_message_with_status(
    session: DbSession, status_db: MessageStatusDb
) -> MessageWithStatus:
    status = status_db.status
    item_hash = status_db.item_hash
    reception_time = status_db.reception_time
    if status == MessageStatus.PENDING:
        # There may be several instances of the same pending message, return the first.
        pending_messages_db = get_pending_messages(session=session, item_hash=item_hash)
        pending_messages = [PendingMessage.from_orm(m) for m in pending_messages_db]
        return PendingMessageStatus(
            status=MessageStatus.PENDING,
            item_hash=item_hash,
            reception_time=reception_time,
            messages=pending_messages,
        )

    if status == MessageStatus.PROCESSED:
        message_db = get_message_by_item_hash(
            session=session, item_hash=ItemHash(item_hash)
        )
        if not message_db:
            raise web.HTTPNotFound()

        message = format_message(message_db)
        return ProcessedMessageStatus(
            item_hash=item_hash,
            reception_time=reception_time,
            message=message,
        )

    if status == MessageStatus.FORGOTTEN:
        forgotten_message_db = get_forgotten_message(
            session=session, item_hash=item_hash
        )
        if not forgotten_message_db:
            raise web.HTTPNotFound()

        return ForgottenMessageStatus(
            item_hash=item_hash,
            reception_time=reception_time,
            message=ForgottenMessage.from_orm(forgotten_message_db),
            forgotten_by=forgotten_message_db.forgotten_by,
        )

    if status == MessageStatus.REJECTED:
        rejected_message_db = get_rejected_message(session=session, item_hash=item_hash)
        if not rejected_message_db:
            raise web.HTTPNotFound()

        return RejectedMessageStatus(
            item_hash=item_hash,
            reception_time=reception_time,
            error_code=rejected_message_db.error_code,
            details=rejected_message_db.details,
            message=rejected_message_db.message,
        )

    raise NotImplementedError(f"Unknown message status: {status}")


async def view_message(request: web.Request):
    item_hash_str = request.match_info.get("item_hash")
    if not item_hash_str:
        raise web.HTTPUnprocessableEntity(text=f"Invalid message hash: {item_hash_str}")

    item_hash = ItemHash(item_hash_str)

    session_factory: DbSessionFactory = request.app["session_factory"]
    with session_factory() as session:
        message_status_db = get_message_status(session=session, item_hash=item_hash)
        if message_status_db is None:
            raise web.HTTPNotFound()
        message_with_status = _get_message_with_status(
            session=session, status_db=message_status_db
        )

    return web.json_response(text=message_with_status.model_dump_json())


async def view_message_content(request: web.Request):
    item_hash_str = request.match_info.get("item_hash")
    if not item_hash_str:
        raise web.HTTPUnprocessableEntity(text=f"Invalid message hash: {item_hash_str}")

    item_hash = ItemHash(item_hash_str)

    session_factory: DbSessionFactory = request.app["session_factory"]
    with session_factory() as session:
        message_status_db = get_message_status(session=session, item_hash=item_hash)
        if message_status_db is None:
            raise web.HTTPNotFound()
        message_with_status = _get_message_with_status(
            session=session, status_db=message_status_db
        )

    status = message_with_status.status
    if (
        status != MessageStatus.PROCESSED
        or not hasattr(message_with_status, "message")
        or not isinstance(message_with_status.message, PostMessage)
    ):
        raise web.HTTPUnprocessableEntity(
            text=f"Invalid message hash status {status} for hash {item_hash_str}"
        )

    message_type = message_with_status.message.type
    if message_type != MessageType.post:
        raise web.HTTPUnprocessableEntity(
            text=f"Invalid message hash type {message_type} for hash {item_hash_str}"
        )

    content = message_with_status.message.content.content
    return web.json_response(text=json.dumps(content))
