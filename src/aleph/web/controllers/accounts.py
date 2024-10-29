import json
from itertools import groupby
from typing import Any, Dict, List

from aiohttp import web
from aleph_message.models import MessageType
from pydantic import ValidationError, parse_obj_as

from aleph.db.accessors.balances import get_total_balance
from aleph.db.accessors.cost import get_total_cost_for_address
from aleph.db.accessors.files import get_address_files_for_api, get_address_files_stats
from aleph.db.accessors.messages import get_message_stats_by_address
from aleph.schemas.api.accounts import (
    GetAccountBalanceResponse,
    GetAccountFilesQueryParams,
    GetAccountFilesResponse,
    GetAccountFilesResponseItem,
)
from aleph.types.db_session import DbSessionFactory
from aleph.web.controllers.app_state_getters import get_session_factory_from_request


def make_stats_dict(stats) -> Dict[str, Any]:
    stats_dict = {}

    sorted_stats = sorted(stats, key=lambda s: s.address)
    for address, address_stats in groupby(sorted_stats, key=lambda s: s.address):
        nb_messages_by_type = {s.type: s.nb_messages for s in address_stats}
        stats_dict[address] = {
            "messages": sum(val for val in nb_messages_by_type.values()),
            "aggregates": nb_messages_by_type.get(MessageType.aggregate, 0),
            "posts": nb_messages_by_type.get(MessageType.post, 0),
            "programs": nb_messages_by_type.get(MessageType.program, 0),
            "stores": nb_messages_by_type.get(MessageType.store, 0),
        }

    return stats_dict


async def addresses_stats_view(request: web.Request):
    """Returns the stats of some addresses."""

    addresses: List[str] = request.query.getall("addresses[]", [])
    session_factory: DbSessionFactory = request.app["session_factory"]

    with session_factory() as session:
        stats = get_message_stats_by_address(session=session, addresses=addresses)

    stats_dict = make_stats_dict(stats)

    output = {"data": stats_dict}
    return web.json_response(output, dumps=lambda v: json.dumps(v))


def _get_address_from_request(request: web.Request) -> str:
    address = request.match_info.get("address")
    if address is None:
        raise web.HTTPUnprocessableEntity(text="Address must be specified.")
    return address


async def get_account_balance(request: web.Request):
    address = _get_address_from_request(request)

    session_factory: DbSessionFactory = get_session_factory_from_request(request)
    with session_factory() as session:
        balance = get_total_balance(
            session=session, address=address, include_dapps=False
        )
        total_cost = get_total_cost_for_address(session=session, address=address)

    if balance is None:
        raise web.HTTPNotFound()

    return web.json_response(
        text=GetAccountBalanceResponse(
            address=address, balance=balance, locked_amount=total_cost
        ).model_dump_json()
    )


async def get_account_files(request: web.Request) -> web.Response:
    address = _get_address_from_request(request)

    try:
        query_params = GetAccountFilesQueryParams.model_validate(request.query)
    except ValidationError as e:
        raise web.HTTPUnprocessableEntity(text=e.json(indent=4))

    session_factory: DbSessionFactory = get_session_factory_from_request(request)

    with session_factory() as session:
        file_pins = list(
            get_address_files_for_api(
                session=session,
                owner=address,
                pagination=query_params.pagination,
                page=query_params.page,
                sort_order=query_params.sort_order,
            )
        )
        nb_files, total_size = get_address_files_stats(session=session, owner=address)

        if not file_pins:
            raise web.HTTPNotFound()

        response = GetAccountFilesResponse(
            address=address,
            total_size=total_size,
            files=parse_obj_as(List[GetAccountFilesResponseItem], file_pins),
            pagination_page=query_params.page,
            pagination_total=nb_files,
            pagination_per_page=query_params.pagination,
        )
        return web.json_response(text=response.model_dump_json())
