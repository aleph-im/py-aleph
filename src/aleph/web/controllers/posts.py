from typing import Any, Dict, List, Optional

from aiohttp import web
from aleph_message.models import ItemHash
from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    ValidationError,
    field_validator,
    model_validator,
)
from sqlalchemy import select

from aleph.db.accessors.posts import (
    MergedPost,
    MergedPostV0,
    count_matching_posts,
    get_matching_posts,
    get_matching_posts_legacy,
)
from aleph.db.models import ChainTxDb, message_confirmations
from aleph.types.db_session import DbSession, DbSessionFactory
from aleph.types.sort_order import SortBy, SortOrder
from aleph.web.controllers.utils import (
    DEFAULT_MESSAGES_PER_PAGE,
    DEFAULT_PAGE,
    LIST_FIELD_SEPARATOR,
    Pagination,
    cond_output,
    get_path_page,
)


class PostQueryParams(BaseModel):
    addresses: Optional[List[str]] = Field(
        default=None, description="Accepted values for the 'sender' field."
    )
    hashes: Optional[List[ItemHash]] = Field(
        default=None, description="Accepted values for the 'item_hash' field."
    )
    refs: Optional[List[str]] = Field(
        default=None, description="Accepted values for the 'content.ref' field."
    )
    post_types: Optional[List[str]] = Field(
        default=None,
        alias="types",
        description="Accepted values for the 'content.type' field.",
    )
    tags: Optional[List[str]] = Field(
        default=None, description="Accepted values for the 'content.content.tag' field."
    )
    channels: Optional[List[str]] = Field(
        default=None, description="Accepted values for the 'channel' field."
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
    pagination: int = Field(
        default=DEFAULT_MESSAGES_PER_PAGE,
        ge=0,
        description="Maximum number of messages to return. Specifying 0 removes this limit.",
    )
    page: int = Field(
        default=DEFAULT_PAGE, ge=1, description="Offset in pages. Starts at 1."
    )
    sort_by: SortBy = Field(
        default=SortBy.TIME,
        alias="sortBy",
        description="Key to use to sort the posts. "
        "'time' uses the post creation time field. "
        "'tx-time' uses the first on-chain confirmation time.",
    )
    sort_order: SortOrder = Field(
        default=SortOrder.DESCENDING,
        alias="sortOrder",
        description="Order in which messages should be listed: "
        "-1 means most recent messages first, 1 means older messages first.",
    )

    @model_validator(mode="after")
    def validate_field_dependencies(self):
        start_date = self.start_date
        end_date = self.end_date
        if start_date and end_date and (end_date < start_date):
            raise ValueError("end date cannot be lower than start date.")

        return self

    @field_validator(
        "addresses", "hashes", "refs", "post_types", "channels", "tags", mode="before"
    )
    @classmethod
    def split_str(cls, v) -> List[str]:
        if isinstance(v, str):
            return v.split(LIST_FIELD_SEPARATOR)
        return v

    model_config = ConfigDict(populate_by_name=True)


def merged_post_to_dict(merged_post: MergedPost) -> Dict[str, Any]:
    return {
        "item_hash": merged_post.item_hash,
        "content": merged_post.content,
        "original_item_hash": merged_post.original_item_hash,
        "original_type": merged_post.original_type,
        "address": merged_post.owner,
        "ref": merged_post.ref,
        "channel": merged_post.channel,
        "created": merged_post.created.isoformat(),
        "last_updated": merged_post.last_updated.isoformat(),
    }


def get_post_confirmations(
    session: DbSession, post: MergedPostV0
) -> List[Dict[str, Any]]:
    select_stmt = (
        select(
            ChainTxDb.chain,
            ChainTxDb.hash,
            ChainTxDb.height,
        )
        .select_from(message_confirmations)
        .join(ChainTxDb, message_confirmations.c.tx_hash == ChainTxDb.hash)
        .where(message_confirmations.c.item_hash == post.item_hash)
    )

    results = session.execute(select_stmt).all()
    return [
        {"chain": row.chain, "hash": row.hash, "height": row.height} for row in results
    ]


def merged_post_v0_to_dict(
    session: DbSession, merged_post: MergedPostV0
) -> Dict[str, Any]:

    confirmations = get_post_confirmations(session, merged_post)

    return {
        "chain": merged_post.chain,
        "item_hash": merged_post.item_hash,
        "sender": merged_post.owner,
        "type": merged_post.type,
        "channel": merged_post.channel,
        "confirmed": bool(confirmations),
        "content": merged_post.content,
        "item_content": merged_post.item_content,
        "item_type": merged_post.item_type,
        "signature": merged_post.signature,
        "size": merged_post.size,
        "time": merged_post.time,
        "confirmations": confirmations,
        "original_item_hash": merged_post.original_item_hash,
        "original_signature": merged_post.original_signature,
        "original_type": merged_post.original_type,
        "hash": merged_post.original_item_hash,
        "address": merged_post.owner,
        "ref": merged_post.ref,
    }


def get_query_params(request: web.Request) -> PostQueryParams:
    try:
        query_params = PostQueryParams.model_validate(request.query)
    except ValidationError as e:
        raise web.HTTPUnprocessableEntity(text=e.json(indent=4))

    path_page = get_path_page(request)
    if path_page:
        query_params.page = path_page

    return query_params


async def view_posts_list_v0(request: web.Request) -> web.Response:
    query_string = request.query_string
    query_params = get_query_params(request)

    find_filters = query_params.model_dump(exclude_none=True)

    pagination_page = query_params.page
    pagination_per_page = query_params.pagination

    session_factory: DbSessionFactory = request.app["session_factory"]

    with session_factory() as session:
        total_posts = count_matching_posts(session=session, **find_filters)
        results = get_matching_posts_legacy(session=session, **find_filters)
        posts = [merged_post_v0_to_dict(session, post) for post in results]

    context: Dict[str, Any] = {"posts": posts}

    if pagination_per_page is not None:

        pagination = Pagination(
            pagination_page,
            pagination_per_page,
            total_posts,
            url_base="/messages/posts/page/",
            query_string=query_string,
        )

        context.update(
            {
                "pagination": pagination,
                "pagination_page": pagination_page,
                "pagination_total": total_posts,
                "pagination_per_page": pagination_per_page,
                "pagination_item": "posts",
            }
        )

    return cond_output(request, context, "TODO.html")


async def view_posts_list_v1(request) -> web.Response:
    """Posts list view with filters"""

    query_string = request.query_string

    try:
        query_params = PostQueryParams.model_validate(request.query)
    except ValidationError as e:
        raise web.HTTPUnprocessableEntity(text=e.json(indent=4))

    path_page = get_path_page(request)
    if path_page:
        query_params.page = path_page

    find_filters = query_params.model_dump(exclude_none=True)

    pagination_page = query_params.page
    pagination_per_page = query_params.pagination

    session_factory: DbSessionFactory = request.app["session_factory"]
    with session_factory() as session:
        total_posts = count_matching_posts(session=session, **find_filters)
        results = get_matching_posts(session=session, **find_filters)
        posts = [merged_post_to_dict(post) for post in results]

    context: Dict[str, Any] = {"posts": posts}

    if pagination_per_page is not None:

        pagination = Pagination(
            pagination_page,
            pagination_per_page,
            total_posts,
            url_base="/messages/posts/page/",
            query_string=query_string,
        )

        context.update(
            {
                "pagination": pagination,
                "pagination_page": pagination_page,
                "pagination_total": total_posts,
                "pagination_per_page": pagination_per_page,
                "pagination_item": "posts",
            }
        )

    return cond_output(request, context, "TODO.html")
