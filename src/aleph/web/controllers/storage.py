import base64
import logging

from aiohttp import web

from aleph.exceptions import AlephStorageException, UnknownHashError
from aleph.handlers.forget import count_file_references
from aleph.storage import add_json, get_hash_content, add_file
from aleph.types import ItemType
from aleph.utils import run_in_executor
from aleph.web import app

logger = logging.getLogger(__name__)


async def add_ipfs_json_controller(request):
    """Forward the json content to IPFS server and return an hash"""
    data = await request.json()

    output = {"status": "success", "hash": await add_json(data, engine=ItemType.IPFS)}
    return web.json_response(output)


app.router.add_post("/api/v0/ipfs/add_json", add_ipfs_json_controller)


async def add_storage_json_controller(request):
    """Forward the json content to IPFS server and return an hash"""
    data = await request.json()

    output = {"status": "success", "hash": await add_json(data, engine=ItemType.Storage)}
    return web.json_response(output)


app.router.add_post("/api/v0/storage/add_json", add_storage_json_controller)


async def storage_add_file(request):
    # No need to pin it here anymore.
    # TODO: find a way to specify linked ipfs hashes in posts/aggr.
    post = await request.post()
    file_hash = await add_file(
        post["file"].file, filename=post["file"].filename, engine=ItemType.Storage
    )

    output = {"status": "success", "hash": file_hash}
    return web.json_response(output)


app.router.add_post("/api/v0/storage/add_file", storage_add_file)


def prepare_content(content):
    return base64.encodebytes(content).decode("utf-8")


async def get_hash(request):
    item_hash = request.match_info.get("hash", None)
    if item_hash is None:
        return web.HTTPBadRequest(text="No hash provided")
    try:
        engine = ItemType.from_hash(item_hash)
    except UnknownHashError as e:
        logger.warning(e.args[0])
        return web.HTTPBadRequest(text="Invalid hash provided")

    try:
        hash_content = await get_hash_content(
            item_hash,
            use_network=False,
            use_ipfs=True,
            engine=engine,
            store_value=False,
        )
    except AlephStorageException:
        return web.HTTPNotFound(text=f"No file found for hash {item_hash}")

    content = await run_in_executor(None, prepare_content, hash_content.value)
    result = {
        "status": "success",
        "hash": item_hash,
        "engine": engine,
        "content": content,
    }

    response = await run_in_executor(None, web.json_response, result)
    response.enable_compression()
    return response


app.router.add_get("/api/v0/storage/{hash}", get_hash)


async def get_raw_hash(request):
    item_hash = request.match_info.get("hash", None)

    if item_hash is None:
        raise web.HTTPBadRequest(text="No hash provided")

    try:
        engine = ItemType.from_hash(item_hash)
    except UnknownHashError:
        raise web.HTTPBadRequest(text="Invalid hash")

    try:
        content = await get_hash_content(
            item_hash,
            use_network=False,
            use_ipfs=True,
            engine=engine,
            store_value=False,
        )
    except AlephStorageException as e:
        raise web.HTTPNotFound(text="Not found") from e

    response = web.Response(body=content.value)
    response.enable_compression()
    return response


app.router.add_get("/api/v0/storage/raw/{hash}", get_raw_hash)


async def get_file_references_count(request):
    item_hash = request.match_info.get("hash", None)
    count = await count_file_references(storage_hash=item_hash)
    return web.json_response(data=count)


app.router.add_get("/api/v0/storage/count/{hash}", get_file_references_count)
