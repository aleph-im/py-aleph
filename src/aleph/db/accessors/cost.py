from decimal import Decimal
from typing import Iterable, List, Optional

from aleph_message.models import PaymentType
from sqlalchemy import func, select
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.sql import Insert

from aleph.db.models.account_costs import AccountCostsDb
from aleph.toolkit.costs import format_cost
from aleph.types.db_session import DbSession


def get_total_cost_for_address(
    session: DbSession,
    address: str,
    payment_type: Optional[PaymentType] = PaymentType.hold,
) -> Decimal:
    select_stmt = (
        select(func.sum(AccountCostsDb.cost_hold))
        .select_from(AccountCostsDb)
        .where(
            (AccountCostsDb.owner == address)
            & (AccountCostsDb.payment_type == payment_type)
        )
    )

    total_cost = session.execute(select_stmt).scalar()
    return format_cost(Decimal(total_cost or 0))


def get_message_costs(session: DbSession, item_hash: str) -> Iterable[AccountCostsDb]:
    select_stmt = select(AccountCostsDb).where(AccountCostsDb.item_hash == item_hash)
    return (session.execute(select_stmt)).scalars().all()


def make_costs_upsert_query(costs: List[AccountCostsDb]) -> Insert:
    costs_dict = [
        cost.to_dict(
            exclude={
                "id",
            }
        )
        for cost in costs
    ]

    upsert_stmt = insert(AccountCostsDb).values(costs_dict)

    return upsert_stmt.on_conflict_do_update(
        constraint="account_costs_owner_item_hash_type_name_key",
        set_={
            "cost_hold": upsert_stmt.excluded.cost_hold,
            "cost_stream": upsert_stmt.excluded.cost_stream,
        },
    )
