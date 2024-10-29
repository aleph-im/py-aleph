import asyncio
import datetime as dt
import json
import logging
from io import BytesIO, StringIO
from math import ceil
from typing import Any, Dict, List, Mapping, Optional, Union, overload

import aio_pika
import aio_pika.abc
import aiohttp_jinja2
from aiohttp import web
from aiohttp.web_request import FileField
from aleph_p2p_client import AlephP2PServiceClient
from configmanager import Config
from pydantic import BaseModel

import aleph.toolkit.json as aleph_json
from aleph.db.accessors.files import insert_grace_period_file_pin
from aleph.schemas.pending_messages import BasePendingMessage, parse_message
from aleph.services.ipfs import IpfsService
from aleph.services.p2p.pubsub import publish as pub_p2p
from aleph.toolkit.shield import shielded
from aleph.toolkit.timestamp import utc_now
from aleph.types.db_session import DbSession
from aleph.types.message_status import (
    InvalidMessageException,
    MessageProcessingStatus,
    MessageStatus,
)
from aleph.types.protocol import Protocol
from aleph.web.controllers.app_state_getters import (
    get_config_from_request,
    get_ipfs_service_from_request,
    get_mq_channel_from_request,
    get_p2p_client_from_request,
)

DEFAULT_MESSAGES_PER_PAGE = 20
DEFAULT_PAGE = 1
LIST_FIELD_SEPARATOR = ","


@overload
def file_field_to_io(multi_dict: bytes) -> BytesIO: ...


@overload
def file_field_to_io(multi_dict: str) -> StringIO: ...  # type: ignore[misc]


@overload
def file_field_to_io(multi_dict: FileField) -> BytesIO: ...


def file_field_to_io(file_field):
    if isinstance(file_field, bytes):
        return BytesIO(file_field)
    elif isinstance(file_field, str):
        return StringIO(file_field)

    return file_field.file


def get_path_page(request: web.Request) -> Optional[int]:
    page_str = request.match_info.get("page")
    if page_str is None:
        return None

    try:
        page = int(page_str)
    except ValueError:
        raise web.HTTPBadRequest(text=f"Invalid page value in path: {page_str}")

    if page < 1:
        raise web.HTTPUnprocessableEntity(text="Page number must be greater than 1.")

    return page


class Pagination(object):
    @staticmethod
    def get_pagination_params(request):
        pagination_page = int(request.match_info.get("page", "1"))
        pagination_page = int(request.query.get("page", pagination_page))
        pagination_param = int(
            request.query.get("pagination", DEFAULT_MESSAGES_PER_PAGE)
        )
        with_pagination = pagination_param != 0

        if pagination_page < 1:
            raise web.HTTPBadRequest(
                text=f"Query field 'page' must be ≥ 1, not {pagination_page}"
            )

        if not with_pagination:
            pagination_per_page = None
            pagination_skip = None
        else:
            pagination_per_page = pagination_param
            pagination_skip = (pagination_page - 1) * pagination_param

        return (pagination_page, pagination_per_page, pagination_skip)

    def __init__(self, page, per_page, total_count, url_base=None, query_string=None):
        self.page = page
        self.per_page = per_page
        self.total_count = total_count
        self.url_base = url_base
        self.query_string = query_string

    @property
    def pages(self):
        return int(ceil(self.total_count / float(self.per_page)))

    @property
    def has_prev(self):
        return self.page > 1

    @property
    def has_next(self):
        return self.page < self.pages

    def iter_pages(self, left_edge=2, left_current=2, right_current=5, right_edge=2):
        last = 0
        for num in range(1, self.pages + 1):
            if (
                num <= left_edge
                or (
                    num > self.page - left_current - 1
                    and num < self.page + right_current
                )
                or num > self.pages - right_edge
            ):
                if last + 1 != num:
                    yield None
                yield num
                last = num


def prepare_date_filters(request, filter_key):
    date_filters = None

    start_date = float(request.query.get("startDate", 0))
    end_date = float(request.query.get("endDate", 0))

    if start_date < 0:
        raise ValueError("startDate field may not be negative")
    if end_date < 0:
        raise ValueError("endDate field may not be negative")

    if start_date:
        date_filters = {}
        date_filters[filter_key] = {"$gte": start_date}

    if end_date:
        new_filter = {}
        new_filter[filter_key] = {"$lte": end_date}
        if date_filters is not None:
            date_filters = {"$and": [date_filters, new_filter]}
        else:
            date_filters = new_filter

    return date_filters


def cond_output(request, context, template):
    if request.rel_url.path.endswith(".json"):
        if "pagination" in context:
            context.pop("pagination")
        response = web.json_response(context, dumps=lambda v: json.dumps(v))
    else:
        response = aiohttp_jinja2.render_template(template, request, context)

    response.enable_compression()

    return response


async def mq_make_aleph_message_topic_queue(
    channel: aio_pika.abc.AbstractChannel,
    config: Config,
    routing_key: Optional[str] = None,
) -> aio_pika.abc.AbstractQueue:
    mq_message_exchange = await channel.declare_exchange(
        name=config.rabbitmq.message_exchange.value,
        type=aio_pika.ExchangeType.TOPIC,
        auto_delete=False,
    )
    mq_queue = await channel.declare_queue(
        auto_delete=True,
        exclusive=True,
        # Auto-delete the queue after 30 seconds. This guarantees that queues are deleted even
        # if a bug makes the consumer crash before cleanup.
        arguments={"x-expires": 30000},
    )
    await mq_queue.bind(mq_message_exchange, routing_key=routing_key)
    return mq_queue


def processing_status_to_http_status(status: MessageProcessingStatus) -> int:
    mapping = {
        MessageProcessingStatus.PROCESSED_NEW_MESSAGE: 200,
        MessageProcessingStatus.PROCESSED_CONFIRMATION: 200,
        MessageProcessingStatus.FAILED_WILL_RETRY: 202,
        MessageProcessingStatus.FAILED_REJECTED: 422,
    }
    return mapping[status]


def message_status_to_http_status(status: MessageStatus) -> int:
    mapping = {
        MessageStatus.PENDING: 202,
        MessageStatus.PROCESSED: 200,
        MessageStatus.REJECTED: 422,
    }
    return mapping[status]


async def mq_read_one_message(
    mq_queue: aio_pika.abc.AbstractQueue, timeout: float
) -> Optional[aio_pika.abc.AbstractIncomingMessage]:
    """
    Consume one element from a message queue and then return.
    """

    queue: asyncio.Queue = asyncio.Queue()

    async def _process_message(message: aio_pika.abc.AbstractMessage):
        await queue.put(message)

    consumer_tag = await mq_queue.consume(_process_message, no_ack=True)

    try:
        return await asyncio.wait_for(queue.get(), timeout)
    except asyncio.TimeoutError:
        return None
    finally:
        await mq_queue.cancel(consumer_tag)


def validate_message_dict(message_dict: Mapping[str, Any]) -> BasePendingMessage:
    try:
        return parse_message(message_dict)
    except InvalidMessageException as e:
        raise web.HTTPUnprocessableEntity(text=str(e))


class PublicationStatus(BaseModel):
    status: str
    failed: List[Protocol]

    @classmethod
    def from_failures(cls, failed_publications: List[Protocol]):
        status = {
            0: "success",
            1: "warning",
            2: "error",
        }[len(failed_publications)]
        return cls(status=status, failed=failed_publications)


async def pub_on_p2p_topics(
    p2p_client: AlephP2PServiceClient,
    ipfs_service: Optional[IpfsService],
    topic: str,
    payload: Union[str, bytes],
    logger: logging.Logger,
) -> List[Protocol]:

    failed_publications = []

    if ipfs_service:
        try:
            await asyncio.wait_for(ipfs_service.pub(topic, payload), 10)
        except Exception:
            logger.exception("Can't publish on ipfs")
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
        logger.exception("Can't publish on p2p")
        failed_publications.append(Protocol.P2P)

    return failed_publications


class BroadcastStatus(BaseModel):
    publication_status: PublicationStatus
    message_status: Optional[MessageStatus] = None


def broadcast_status_to_http_status(broadcast_status: BroadcastStatus) -> int:
    if broadcast_status.publication_status.status == "error":
        return 500

    message_status = broadcast_status.message_status
    # Message status should always be set if the publication succeeded
    # TODO: improve typing to make this check useless
    assert message_status is not None
    return message_status_to_http_status(message_status)


def format_pending_message_dict(pending_message: BasePendingMessage) -> Dict[str, Any]:
    pending_message_dict = pending_message.model_dump(exclude_none=True)
    pending_message_dict["time"] = pending_message_dict["time"].timestamp()
    return pending_message_dict


@shielded
async def broadcast_and_process_message(
    pending_message: BasePendingMessage,
    sync: bool,
    request: web.Request,
    logger: logging.Logger,
    message_dict: Optional[Dict[str, Any]] = None,
) -> BroadcastStatus:
    """
    Broadcast a message to the network and process the message on the local node.
    This utility function enables endpoints to publish messages on the network and wait until they
    are processed locally.

    :param pending_message: Message to broadcast + process.
    :param sync: Whether the function should wait until the message is processed before returning.
    :param request: The web request object, used to extract global state.
    :param logger: Logger.
    :param message_dict: The message as a dictionary, if already available. Used for optimization purposes;
                         if not provided, the function will call format_pending_message_dict().
    """

    # In sync mode, wait for a message processing event. We need to create the queue
    # before publishing the message on P2P topics in order to guarantee that the event
    # will be picked up.
    config = get_config_from_request(request)

    if sync:
        mq_channel = await get_mq_channel_from_request(request=request, logger=logger)
        mq_queue = await mq_make_aleph_message_topic_queue(
            channel=mq_channel,
            config=config,
            routing_key=f"*.{pending_message.item_hash}",
        )
    else:
        mq_queue = None

    # We publish the message on P2P topics early, for 3 reasons:
    # 1. Just because this node is unable to process the message does not
    #    necessarily mean the message is incorrect (ex: bug in a new version).
    # 2. If the publication fails after the processing, we end up in a situation where
    #    a message exists without being propagated to the other nodes, ultimately
    #    causing sync issues on the network.
    # 3. The message is currently fed to this node using the P2P service client
    #    loopback mechanism.
    ipfs_service = get_ipfs_service_from_request(request)
    p2p_client = get_p2p_client_from_request(request)

    message_topic = config.aleph.queue_topic.value
    message_dict = message_dict or format_pending_message_dict(pending_message)

    failed_publications = await pub_on_p2p_topics(
        p2p_client=p2p_client,
        ipfs_service=ipfs_service,
        topic=message_topic,
        payload=aleph_json.dumps(message_dict),
        logger=logger,
    )
    pub_status = PublicationStatus.from_failures(failed_publications)
    if pub_status.status == "error":
        return BroadcastStatus(publication_status=pub_status, message_status=None)

    status = BroadcastStatus(
        publication_status=pub_status, message_status=MessageStatus.PENDING
    )

    # When publishing in async mode, just respond with 202 (Accepted).
    if not sync:
        return status

    # Ignore type checking here, we know that mq_queue is set at this point
    assert mq_queue is not None
    response = await mq_read_one_message(mq_queue, timeout=30)

    # Delete the queue immediately
    await mq_queue.delete(if_empty=False)

    # If the message was not processed before the timeout, return a 202.
    if response is None:
        return status

    routing_key = response.routing_key
    assert routing_key is not None  # again, for type checking
    status_str, _item_hash = routing_key.split(".")
    processing_status = MessageProcessingStatus(status_str)

    status.message_status = processing_status.to_message_status()
    return status


def add_grace_period_for_file(session: DbSession, file_hash: str, hours: int):
    current_datetime = utc_now()
    delete_by = current_datetime + dt.timedelta(hours=hours)
    insert_grace_period_file_pin(
        session=session,
        file_hash=file_hash,
        created=utc_now(),
        delete_by=delete_by,
    )
