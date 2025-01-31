import datetime as dt
import logging
from typing import (
    Any,
    Dict,
    Iterable,
    Literal,
    Optional,
    Sequence,
    Tuple,
    Union,
    overload,
)

from sqlalchemy import delete, event, func, literal_column, select, update
from sqlalchemy.dialects.postgresql import aggregate_order_by, insert
from sqlalchemy.orm import defer, selectinload

from aleph.cache import cache
from aleph.db.models import AggregateDb, AggregateElementDb
from aleph.types.db_session import DbSession

logger = logging.getLogger(__name__)


@event.listens_for(AggregateDb, "after_update", propagate=True)
@event.listens_for(AggregateDb, "after_delete", propagate=True)
def prune_cache_for_updated_aggregates(mapper, connection, target):
    cache.delete_namespace(f"aggregates_by_owner:{target.owner}")


def aggregate_exists(session: DbSession, key: str, owner: str) -> bool:
    return AggregateDb.exists(
        session=session,
        where=(AggregateDb.key == key) & (AggregateDb.owner == owner),
    )


AggregateContent = Iterable[Tuple[str, Dict[str, Any]]]
AggregateContentWithInfo = Iterable[Tuple[str, dt.datetime, dt.datetime, str, str]]


@overload
def get_aggregates_by_owner(
    session: Any,
    owner: str,
    with_info: Literal[False],
    keys: Optional[Sequence[str]] = None,
) -> AggregateContent: ...


@overload
def get_aggregates_by_owner(
    session: Any,
    owner: str,
    with_info: Literal[True],
    keys: Optional[Sequence[str]] = None,
) -> AggregateContentWithInfo: ...


@overload
def get_aggregates_by_owner(
    session, owner: str, with_info: bool, keys: Optional[Sequence[str]] = None
) -> Union[AggregateContent, AggregateContentWithInfo]: ...


def get_aggregates_by_owner(session, owner, with_info, keys=None):
    cache_key = f"{with_info} {keys}"

    if (
        aggregates := cache.get(cache_key, namespace=f"aggregates_by_owner:{owner}")
    ) is not None:
        logging.debug(f"cache hit for aggregates_by_owner on cache key {cache_key}")
        return aggregates

    where_clause = AggregateDb.owner == owner
    if keys:
        where_clause = where_clause & AggregateDb.key.in_(keys)
    if with_info:
        query = (
            session.query(
                AggregateDb.key,
                AggregateDb.content,
                AggregateDb.creation_datetime.label("created"),
                AggregateElementDb.creation_datetime.label("last_updated"),
                AggregateDb.last_revision_hash.label("last_update_item_hash"),
                AggregateElementDb.item_hash.label("original_item_hash"),
            )
            .join(
                AggregateElementDb,
                AggregateDb.last_revision_hash == AggregateElementDb.item_hash,
            )
            .filter(AggregateDb.owner == owner)
        )
    else:
        query = (
            session.query(AggregateDb.key, AggregateDb.content)
            .filter(where_clause)
            .order_by(AggregateDb.key)
        )
    result = query.all()
    cache.set(cache_key, result, namespace="aggregates_by_owner:{owner}")
    return result


def get_aggregate_by_key(
    session: DbSession,
    owner: str,
    key: str,
    with_content: bool = True,
) -> Optional[AggregateDb]:
    options = []

    if not with_content:
        options.append(defer(AggregateDb.content))

    select_stmt = select(AggregateDb).where(
        (AggregateDb.owner == owner) & (AggregateDb.key == key)
    )
    return (
        session.execute(
            select_stmt.options(
                *options,
                selectinload(AggregateDb.last_revision),
            )
        )
    ).scalar()


def get_aggregate_content_keys(
    session: DbSession, owner: str, key: str
) -> Iterable[str]:
    return AggregateDb.jsonb_keys(
        session=session,
        column=AggregateDb.content,
        where=(AggregateDb.key == key) & (AggregateDb.owner == owner),
    )


def get_aggregate_elements(
    session: DbSession, owner: str, key: str
) -> Iterable[AggregateElementDb]:
    select_stmt = (
        select(AggregateElementDb)
        .where((AggregateElementDb.key == key) & (AggregateElementDb.owner == owner))
        .order_by(AggregateElementDb.creation_datetime)
    )
    return (session.execute(select_stmt)).scalars()


def insert_aggregate(
    session: DbSession,
    key: str,
    owner: str,
    content: Dict[str, Any],
    creation_datetime: dt.datetime,
    last_revision_hash: str,
) -> None:
    insert_stmt = insert(AggregateDb).values(
        key=key,
        owner=owner,
        content=content,
        creation_datetime=creation_datetime,
        last_revision_hash=last_revision_hash,
        dirty=False,
    )
    session.execute(insert_stmt)


def update_aggregate(
    session: DbSession,
    key: str,
    owner: str,
    content: Dict[str, Any],
    creation_datetime: dt.datetime,
    last_revision_hash: str,
    prepend: bool = False,
) -> None:
    merged_content = (
        content + AggregateDb.content if prepend else AggregateDb.content + content
    )

    update_stmt = (
        update(AggregateDb)
        .values(
            content=merged_content,
            creation_datetime=creation_datetime,
            last_revision_hash=last_revision_hash,
        )
        .where((AggregateDb.key == key) & (AggregateDb.owner == owner))
    )
    session.execute(update_stmt)


def insert_aggregate_element(
    session: DbSession,
    item_hash: str,
    key: str,
    owner: str,
    content: Dict[str, Any],
    creation_datetime: dt.datetime,
) -> None:
    insert_stmt = insert(AggregateElementDb).values(
        item_hash=item_hash,
        key=key,
        owner=owner,
        content=content,
        creation_datetime=creation_datetime,
    )
    session.execute(insert_stmt)


def count_aggregate_elements(session: DbSession, owner: str, key: str) -> int:
    select_stmt = select(AggregateElementDb).where(
        (AggregateElementDb.key == key) & (AggregateElementDb.owner == owner)
    )
    return session.execute(select(func.count()).select_from(select_stmt)).scalar_one()


def merge_aggregate_elements(elements: Iterable[AggregateElementDb]) -> Dict:
    content = {}
    for element in elements:
        content.update(element.content)
    return content


def mark_aggregate_as_dirty(session: DbSession, owner: str, key: str) -> None:
    update_stmt = (
        update(AggregateDb)
        .values(dirty=True)
        .where((AggregateDb.key == key) & (AggregateDb.owner == owner))
    )
    session.execute(update_stmt)


def refresh_aggregate(session: DbSession, owner: str, key: str) -> None:
    # Step 1: use a group by to retrieve the aggregate content. This uses a custom
    # aggregate function (see 78dd67881db4_jsonb_merge_aggregate.py).
    select_merged_aggregate_subquery = (
        select(
            AggregateElementDb.key,
            AggregateElementDb.owner,
            func.min(AggregateElementDb.creation_datetime).label("creation_datetime"),
            func.max(AggregateElementDb.creation_datetime).label(
                "last_revision_datetime"
            ),
            func.jsonb_merge(
                aggregate_order_by(
                    AggregateElementDb.content, AggregateElementDb.creation_datetime
                )
            ).label("content"),
        )
        .group_by(AggregateElementDb.key, AggregateElementDb.owner)
        .where((AggregateElementDb.key == key) & (AggregateElementDb.owner == owner))
    ).subquery()

    # Step 2: we miss the last revision hash, so we retrieve it through an additional
    # join.
    # TODO: is this really necessary? Could we just store the last revision datetime
    #       instead and avoid the join? Consider the case where two aggregate elements
    #       have the same timestamp.
    select_stmt = select(
        select_merged_aggregate_subquery.c.key,
        select_merged_aggregate_subquery.c.owner,
        select_merged_aggregate_subquery.c.creation_datetime,
        select_merged_aggregate_subquery.c.content,
        AggregateElementDb.item_hash,
        literal_column("false").label("dirty"),
    ).join(
        AggregateElementDb,
        (select_merged_aggregate_subquery.c.key == AggregateElementDb.key)
        & (select_merged_aggregate_subquery.c.owner == AggregateElementDb.owner)
        & (
            select_merged_aggregate_subquery.c.last_revision_datetime
            == AggregateElementDb.creation_datetime
        ),
    )

    # Step 3: insert/update the aggregate.
    insert_stmt = insert(AggregateDb).from_select(
        ["key", "owner", "creation_datetime", "content", "last_revision_hash", "dirty"],
        select_stmt,
    )
    upsert_aggregate_stmt = insert_stmt.on_conflict_do_update(
        constraint="aggregates_pkey",
        set_={
            "content": insert_stmt.excluded.content,
            "creation_datetime": insert_stmt.excluded.creation_datetime,
            "last_revision_hash": insert_stmt.excluded.last_revision_hash,
            "dirty": insert_stmt.excluded.dirty,
        },
    )

    session.execute(upsert_aggregate_stmt)


def delete_aggregate(session: DbSession, owner: str, key: str) -> None:
    delete_aggregate_stmt = delete(AggregateDb).where(
        (AggregateDb.key == key) & (AggregateDb.owner == owner)
    )
    session.execute(delete_aggregate_stmt)


def delete_aggregate_element(session: DbSession, item_hash: str) -> None:
    delete_element_stmt = delete(AggregateElementDb).where(
        AggregateElementDb.item_hash == item_hash
    )
    session.execute(delete_element_stmt)
