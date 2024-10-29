from aiohttp import web
from pydantic import BaseModel, ConfigDict, ValidationError

from aleph.db.accessors.messages import get_programs_triggered_by_messages
from aleph.types.db_session import DbSessionFactory
from aleph.types.sort_order import SortOrder


class GetProgramQueryFields(BaseModel):
    sort_order: SortOrder = SortOrder.DESCENDING
    model_config = ConfigDict(extra="forbid")


async def get_programs_on_message(request: web.Request) -> web.Response:
    try:
        query = GetProgramQueryFields(**request.query)
    except ValidationError as error:
        return web.json_response(
            data=error.json(), status=web.HTTPBadRequest.status_code
        )

    session_factory: DbSessionFactory = request.app["session_factory"]

    with session_factory() as session:
        messages = [
            {
                "item_hash": result.item_hash,
                "content": {
                    "on": {"message": result.message_subscriptions},
                },
            }
            for result in get_programs_triggered_by_messages(
                session=session, sort_order=query.sort_order
            )
        ]

    response = web.json_response(data=messages)
    response.enable_compression()
    return response
