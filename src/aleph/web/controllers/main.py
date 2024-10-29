import asyncio
import logging
from dataclasses import asdict
from typing import Dict, Optional

import aiohttp_jinja2
from aiohttp import web
from pydantic import BaseModel

from aleph.db.accessors.metrics import query_metric_ccn, query_metric_crn
from aleph.types.db_session import DbSessionFactory
from aleph.web.controllers.app_state_getters import (
    get_node_cache_from_request,
    get_session_factory_from_request,
)
from aleph.web.controllers.metrics import format_dataclass_for_prometheus, get_metrics

logger = logging.getLogger(__name__)


@aiohttp_jinja2.template("index.html")
async def index(request: web.Request) -> Dict:
    """Index of aleph."""

    session_factory: DbSessionFactory = get_session_factory_from_request(request)
    node_cache = get_node_cache_from_request(request)
    with session_factory() as session:
        return asdict(await get_metrics(session=session, node_cache=node_cache))


async def status_ws(request: web.Request) -> web.WebSocketResponse:
    ws = web.WebSocketResponse()
    await ws.prepare(request)

    session_factory: DbSessionFactory = get_session_factory_from_request(request)
    node_cache = get_node_cache_from_request(request)

    previous_status = None
    while True:
        with session_factory() as session:
            status = await get_metrics(session=session, node_cache=node_cache)

        if status != previous_status:
            try:
                await ws.send_json(asdict(status))
            except ConnectionResetError:
                logger.warning("Websocket connection reset")
                await ws.close()
                return ws
            previous_status = status

        await asyncio.sleep(2)


async def metrics(request: web.Request) -> web.Response:
    """Prometheus compatible metrics.

    Naming convention:
    https://prometheus.io/docs/practices/naming/
    """
    session_factory = get_session_factory_from_request(request)
    node_cache = get_node_cache_from_request(request)

    with session_factory() as session:
        return web.Response(
            text=format_dataclass_for_prometheus(
                await get_metrics(session=session, node_cache=node_cache)
            )
        )


async def metrics_json(request: web.Request) -> web.Response:
    """JSON version of the Prometheus metrics."""
    session_factory: DbSessionFactory = get_session_factory_from_request(request)
    node_cache = get_node_cache_from_request(request)

    with session_factory() as session:
        return web.Response(
            text=(await get_metrics(session=session, node_cache=node_cache)).to_json(),
            content_type="application/json",
        )


class Metrics(BaseModel):
    start_date: Optional[float] = None
    end_date: Optional[float] = None
    sort: Optional[str] = None


def _get_node_id_from_request(request: web.Request) -> str:
    address = request.match_info.get("node_id")
    if address is None:
        raise web.HTTPUnprocessableEntity(body="node_id must be specified.")
    return address


async def ccn_metric(request: web.Request) -> web.Response:
    """Fetch metrics for CCN node id"""

    session_factory: DbSessionFactory = get_session_factory_from_request(request)
    query_params = Metrics.model_validate(request.query)

    node_id = _get_node_id_from_request(request)

    with session_factory() as session:
        ccn = query_metric_ccn(
            session,
            node_id=node_id,
            start_date=query_params.start_date,
            end_date=query_params.end_date,
            sort_order=query_params.sort,
        )
        if not ccn:
            raise web.HTTPNotFound()

        if not ccn["item_hash"]:
            raise web.HTTPNotFound()

        result = {"metrics": ccn}
        return web.json_response(result)


async def crn_metric(request: web.Request) -> web.Response:
    """Fetch Metric for crn."""

    session_factory: DbSessionFactory = get_session_factory_from_request(request)
    query_params = Metrics.model_validate(request.query)

    node_id = _get_node_id_from_request(request)

    with session_factory() as session:
        crn = query_metric_crn(
            session,
            node_id=node_id,
            start_date=query_params.start_date,
            end_date=query_params.end_date,
            sort_order=query_params.sort,
        )

        if not crn:
            raise web.HTTPNotFound()

        if not crn["item_hash"]:
            raise web.HTTPNotFound()

        result = {"metrics": crn}
        return web.json_response(result)
