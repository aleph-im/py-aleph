import base64
import hashlib
import logging
import os
import tempfile
from decimal import Decimal
from typing import Optional

import aio_pika
import aiofiles
import pydantic
from aiohttp import BodyPartReader, web
from aiohttp.web_request import FileField
from aleph_message.models import ItemType, StoreContent
from mypy.dmypy_server import MiB
from pydantic import ValidationError

from aleph.chains.signature_verifier import SignatureVerifier
from aleph.db.accessors.balances import get_total_balance
from aleph.db.accessors.cost import get_total_cost_for_address
from aleph.db.accessors.files import count_file_pins, get_file
from aleph.exceptions import AlephStorageException, UnknownHashError
from aleph.schemas.pending_messages import (
    BasePendingMessage,
    PendingInlineStoreMessage,
    PendingStoreMessage,
)
from aleph.storage import StorageService
from aleph.types.db_session import DbSession
from aleph.types.message_status import InvalidSignature
from aleph.utils import item_type_from_hash, run_in_executor
from aleph.web.controllers.app_state_getters import (
    get_config_from_request,
    get_mq_channel_from_request,
    get_session_factory_from_request,
    get_signature_verifier_from_request,
    get_storage_service_from_request,
)
from aleph.web.controllers.utils import (
    add_grace_period_for_file,
    broadcast_and_process_message,
    broadcast_status_to_http_status,
    mq_make_aleph_message_topic_queue,
)

logger = logging.getLogger(__name__)

MAX_FILE_SIZE = 100 * MiB
MAX_UNAUTHENTICATED_UPLOAD_FILE_SIZE = 25 * MiB
MAX_UPLOAD_FILE_SIZE = 1000 * MiB


async def add_ipfs_json_controller(request: web.Request):
    """Forward the json content to IPFS server and return an hash"""
    storage_service = get_storage_service_from_request(request)
    session_factory = get_session_factory_from_request(request)
    config = get_config_from_request(request)
    grace_period = config.storage.grace_period.value

    data = await request.json()
    with session_factory() as session:
        output = {
            "status": "success",
            "hash": await storage_service.add_json(
                session=session, value=data, engine=ItemType.ipfs
            ),
        }
        add_grace_period_for_file(
            session=session, file_hash=output["hash"], hours=grace_period
        )
        session.commit()

    return web.json_response(output)


async def add_storage_json_controller(request: web.Request):
    """Forward the json content to IPFS server and return an hash"""
    storage_service = get_storage_service_from_request(request)
    session_factory = get_session_factory_from_request(request)
    config = get_config_from_request(request)
    grace_period = config.storage.grace_period.value

    data = await request.json()
    with session_factory() as session:
        output = {
            "status": "success",
            "hash": await storage_service.add_json(
                session=session, value=data, engine=ItemType.storage
            ),
        }
        add_grace_period_for_file(
            session=session, file_hash=output["hash"], hours=grace_period
        )
        session.commit()

    return web.json_response(output)


async def _verify_message_signature(
    pending_message: BasePendingMessage, signature_verifier: SignatureVerifier
) -> None:
    try:
        await signature_verifier.verify_signature(pending_message)
    except InvalidSignature:
        raise web.HTTPForbidden()


async def _verify_user_balance(session: DbSession, address: str, size: int) -> None:
    current_balance = get_total_balance(session=session, address=address) or Decimal(0)
    required_balance = (size / MiB) / 3
    current_cost_for_user = get_total_cost_for_address(session=session, address=address)
    if size > 25 * MiB:
        if current_balance < (Decimal(required_balance) + current_cost_for_user):
            raise web.HTTPPaymentRequired()


class StorageMetadata(pydantic.BaseModel):
    message: PendingInlineStoreMessage
    sync: bool


class UploadedFile:
    def __init__(self, max_size: int):
        self.max_size = max_size
        self.hash = ""
        self.size = 0
        self._hasher = hashlib.sha256()
        self._temp_file_path = None
        self._temp_file = None

    async def open_temp_file(self):
        if not self._temp_file_path:
            raise ValueError("File content has not been validated and read yet.")
        self._temp_file = await aiofiles.open(self._temp_file_path, "rb")
        return self._temp_file

    async def close_temp_file(self):
        if self._temp_file is not None:
            await self._temp_file.close()
            self._temp_file = None

    async def cleanup(self):
        await self.close_temp_file()
        if self._temp_file_path and os.path.exists(self._temp_file_path):
            os.remove(self._temp_file_path)
            self._temp_file_path = None

    async def read_and_validate(self):
        total_read = 0
        chunk_size = 8192

        # From aiofiles changelog:
        # On Python 3.12, aiofiles.tempfile.NamedTemporaryFile now accepts a
        # delete_on_close argument, just like the stdlib version.
        # On Python 3.12, aiofiles.tempfile.NamedTemporaryFile no longer
        # exposes a delete attribute, just like the stdlib version.
        #
        # so we might need to modify this code for python 3.12 at some point

        # it would be ideal to uses aiofiles.tempfile.NamedTemporaryFile but it
        # doesn't seems to be able to support our current workflow
        temp_file = tempfile.NamedTemporaryFile("w+b", delete=False)
        self._temp_file_path = temp_file.name
        temp_file.close()

        async with aiofiles.open(self._temp_file_path, "w+b") as f:
            async for chunk in self._read_chunks(chunk_size):
                total_read += len(chunk)
                if total_read > self.max_size:
                    raise web.HTTPRequestEntityTooLarge(
                        reason="File size exceeds the maximum limit.",
                        max_size=self.max_size,
                        actual_size=total_read,
                    )
                self._hasher.update(chunk)  # Update file hash while reading the file
                await f.write(chunk)

            self.hash = self._hasher.hexdigest()
            self.size = total_read
            await f.seek(0)

    async def _read_chunks(self, chunk_size):
        raise NotImplementedError("Subclasses must implement this method")

    def get_hash(self) -> str:
        return self._hasher.hexdigest()


class MultipartUploadedFile(UploadedFile):
    def __init__(self, file_field: BodyPartReader, max_size: int):
        super().__init__(max_size)
        self.file_field = file_field

    async def _read_chunks(self, chunk_size):
        async for chunk in self.file_field.__aiter__():
            yield chunk


class RawUploadedFile(UploadedFile):
    def __init__(self, request: web.Request, max_size: int):
        super().__init__(max_size)
        self.request = request

    async def _read_chunks(self, chunk_size):
        async for chunk in self.request.content.iter_chunked(chunk_size):
            yield chunk


async def _check_and_add_file(
    session: DbSession,
    signature_verifier: SignatureVerifier,
    storage_service: StorageService,
    message: Optional[PendingStoreMessage],
    uploaded_file: UploadedFile,
    grace_period: int,
) -> str:
    file_hash = uploaded_file.get_hash()
    # Perform authentication and balance checks
    if message:
        await _verify_message_signature(
            pending_message=message, signature_verifier=signature_verifier
        )
        if not message.item_content:
            raise web.HTTPUnprocessableEntity(reason="Store message content needed")

        try:
            message_content = StoreContent.model_validate_json(message.item_content)
            if message_content.item_hash != file_hash:
                raise web.HTTPUnprocessableEntity(
                    reason=f"File hash does not match ({file_hash} != {message_content.item_hash})"
                )
        except ValidationError as e:
            raise web.HTTPUnprocessableEntity(
                reason=f"Invalid store message content: {e.json()}"
            )

        await _verify_user_balance(
            session=session,
            address=message_content.address,
            size=uploaded_file.size,
        )
    else:
        message_content = None

    temp_file = await uploaded_file.open_temp_file()
    file_content = await temp_file.read()

    if isinstance(file_content, bytes):
        file_bytes = file_content
    elif isinstance(file_content, str):
        file_bytes = file_content.encode("utf-8")
    else:
        raise web.HTTPUnprocessableEntity(
            reason=f"Invalid file content type, got {type(file_content)}"
        )

    await storage_service.add_file_content_to_local_storage(
        session=session, file_content=file_bytes, file_hash=file_hash
    )
    await uploaded_file.cleanup()

    # For files uploaded without authenticated upload, add a grace period of 1 day.
    if message_content is None:
        add_grace_period_for_file(
            session=session, file_hash=file_hash, hours=grace_period
        )
    return file_hash


async def _make_mq_queue(
    request: web.Request,
    sync: bool,
    routing_key: Optional[str] = None,
) -> Optional[aio_pika.abc.AbstractQueue]:
    if not sync:
        return None

    mq_channel = await get_mq_channel_from_request(request, logger)
    config = get_config_from_request(request)
    return await mq_make_aleph_message_topic_queue(
        channel=mq_channel, config=config, routing_key=routing_key
    )


async def storage_add_file(request: web.Request):
    storage_service = get_storage_service_from_request(request)
    session_factory = get_session_factory_from_request(request)
    signature_verifier = get_signature_verifier_from_request(request)
    config = get_config_from_request(request)
    grace_period = config.storage.grace_period.value
    metadata = None
    uploaded_file: Optional[UploadedFile] = None

    try:
        if request.content_type == "multipart/form-data":
            reader = await request.multipart()
            async for part in reader:
                if part.name == "file":
                    uploaded_file = MultipartUploadedFile(part, MAX_FILE_SIZE)
                    await uploaded_file.read_and_validate()
                elif part.name == "metadata":
                    metadata = await part.read(decode=True)
        else:
            uploaded_file = RawUploadedFile(
                request=request, max_size=MAX_UNAUTHENTICATED_UPLOAD_FILE_SIZE
            )
            await uploaded_file.read_and_validate()

        if uploaded_file is None:
            raise web.HTTPBadRequest(
                reason="File should be sent as FormData or Raw Upload"
            )

        max_upload_size = (
            MAX_UNAUTHENTICATED_UPLOAD_FILE_SIZE if not metadata else MAX_FILE_SIZE
        )
        if uploaded_file.size > max_upload_size:
            raise web.HTTPRequestEntityTooLarge(
                actual_size=uploaded_file.size, max_size=max_upload_size
            )

        uploaded_file.max_size = max_upload_size

        status_code = 200

        if metadata:
            metadata_bytes = (
                metadata.file.read() if isinstance(metadata, FileField) else metadata
            )
            try:
                storage_metadata = StorageMetadata.model_validate_json(metadata_bytes)
            except ValidationError as e:
                raise web.HTTPUnprocessableEntity(
                    reason=f"Could not decode metadata: {e.json()}"
                )

            message = storage_metadata.message
            sync = storage_metadata.sync
        else:
            message = None
            sync = False

        with session_factory() as session:
            file_hash = await _check_and_add_file(
                session=session,
                signature_verifier=signature_verifier,
                storage_service=storage_service,
                message=message,
                uploaded_file=uploaded_file,
                grace_period=grace_period,
            )
            session.commit()
        if message:
            broadcast_status = await broadcast_and_process_message(
                pending_message=message, sync=sync, request=request, logger=logger
            )
            status_code = broadcast_status_to_http_status(broadcast_status)

        output = {"status": "success", "hash": file_hash}
        return web.json_response(data=output, status=status_code)

    finally:
        if uploaded_file is not None:
            await uploaded_file.cleanup()


def assert_file_is_downloadable(session: DbSession, file_hash: str) -> None:
    """
    Check if the file is on the aleph.im network and can be downloaded from the API.
    This filters out requests for files outside the network / nonexistent files.
    """
    file_metadata = get_file(session=session, file_hash=file_hash)
    if not file_metadata:
        raise web.HTTPNotFound(text="Not found")

    if file_metadata.size > MAX_FILE_SIZE:
        raise web.HTTPRequestEntityTooLarge(
            max_size=MAX_FILE_SIZE, actual_size=file_metadata.size
        )


def prepare_content(content):
    return base64.encodebytes(content).decode("utf-8")


async def get_hash(request):
    item_hash = request.match_info.get("hash", None)
    if item_hash is None:
        return web.HTTPBadRequest(text="No hash provided")
    try:
        engine = item_type_from_hash(item_hash)
    except UnknownHashError as e:
        logger.warning(e.args[0])
        return web.HTTPBadRequest(text="Invalid hash provided")

    session_factory = get_session_factory_from_request(request)
    with session_factory() as session:
        assert_file_is_downloadable(session=session, file_hash=item_hash)

    storage_service = get_storage_service_from_request(request)

    try:
        hash_content = await storage_service.get_hash_content(
            item_hash,
            use_network=False,
            use_ipfs=True,
            engine=engine,
            store_value=False,
            timeout=30,
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


async def get_raw_hash(request):
    item_hash = request.match_info.get("hash", None)

    if item_hash is None:
        raise web.HTTPBadRequest(text="No hash provided")

    try:
        engine = item_type_from_hash(item_hash)
    except UnknownHashError:
        raise web.HTTPBadRequest(text="Invalid hash")

    session_factory = get_session_factory_from_request(request)
    with session_factory() as session:
        assert_file_is_downloadable(session=session, file_hash=item_hash)

    storage_service = get_storage_service_from_request(request)

    try:
        content = await storage_service.get_hash_content(
            item_hash,
            use_network=False,
            use_ipfs=True,
            engine=engine,
            store_value=False,
            timeout=30,
        )
    except AlephStorageException as e:
        raise web.HTTPNotFound(text="Not found") from e

    response = web.Response(body=content.value)
    response.enable_compression()
    return response


async def get_file_pins_count(request: web.Request) -> web.Response:
    item_hash = request.match_info.get("hash", None)

    if item_hash is None:
        raise web.HTTPBadRequest(text="No hash provided")

    session_factory = get_session_factory_from_request(request)
    with session_factory() as session:
        count = count_file_pins(session=session, file_hash=item_hash)
    return web.json_response(data=count)
