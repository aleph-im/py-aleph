import datetime as dt
import logging
from typing import Any, Dict, Mapping, Optional

import aio_pika.abc
import psycopg2
import sqlalchemy.exc
from aleph_message.models import ItemHash, ItemType, MessageType
from configmanager import Config
from pydantic import ValidationError
from sqlalchemy.dialects.postgresql import insert

from aleph.chains.signature_verifier import SignatureVerifier
from aleph.db.accessors.files import insert_content_file_pin, upsert_file
from aleph.db.accessors.messages import (
    get_forgotten_message,
    get_message_by_item_hash,
    make_confirmation_upsert_query,
    make_message_status_upsert_query,
    make_message_upsert_query,
    reject_new_pending_message,
)
from aleph.db.accessors.pending_messages import delete_pending_message
from aleph.db.models import MessageDb, MessageStatusDb, PendingMessageDb
from aleph.db.models.messages import ForgottenMessageDb
from aleph.exceptions import (
    ContentCurrentlyUnavailable,
    InvalidContent,
    UnknownHashError,
)
from aleph.handlers.content.aggregate import AggregateMessageHandler
from aleph.handlers.content.content_handler import ContentHandler
from aleph.handlers.content.forget import ForgetMessageHandler
from aleph.handlers.content.post import PostMessageHandler
from aleph.handlers.content.store import StoreMessageHandler
from aleph.handlers.content.vm import VmMessageHandler
from aleph.schemas.pending_messages import parse_message
from aleph.storage import StorageService
from aleph.toolkit.timestamp import timestamp_to_datetime
from aleph.types.db_session import DbSession, DbSessionFactory
from aleph.types.files import FileType
from aleph.types.message_processing_result import ProcessedMessage, RejectedMessage
from aleph.types.message_status import (
    ErrorCode,
    InvalidMessageException,
    InvalidMessageFormat,
    InvalidSignature,
    MessageContentUnavailable,
    MessageOrigin,
    MessageStatus,
)

LOGGER = logging.getLogger(__name__)


class BaseMessageHandler:
    content_handlers: Dict[MessageType, ContentHandler]

    def __init__(
        self,
        storage_service: StorageService,
        config: Config,
    ):
        self.storage_service = storage_service

        vm_handler = VmMessageHandler()

        self.content_handlers = {
            MessageType.aggregate: AggregateMessageHandler(),
            MessageType.instance: vm_handler,
            MessageType.post: PostMessageHandler(
                balances_addresses=config.aleph.balances.addresses.value,
                balances_post_type=config.aleph.balances.post_type.value,
            ),
            MessageType.program: vm_handler,
            MessageType.store: StoreMessageHandler(
                storage_service=storage_service,
                grace_period=config.storage.grace_period.value,
            ),
        }

        self.content_handlers[MessageType.forget] = ForgetMessageHandler(
            content_handlers=self.content_handlers,
        )

    # TODO typing: make this function generic on message type
    def get_content_handler(self, message_type: MessageType) -> ContentHandler:
        return self.content_handlers[message_type]

    async def verify_signature(self, pending_message: PendingMessageDb):
        if pending_message.check_message:
            # TODO: remove type: ignore by deciding the pending message type
            await self._signature_verifier.verify_signature(pending_message)  # type: ignore

    async def fetch_pending_message(
        self, pending_message: PendingMessageDb
    ) -> MessageDb:
        item_hash = pending_message.item_hash

        try:
            content = await self.storage_service.get_message_content(pending_message)
        except InvalidContent as e:
            error_msg = f"Invalid message content for {item_hash}: {str(e)}"
            LOGGER.warning(error_msg)
            raise InvalidMessageFormat(error_msg)

        except (ContentCurrentlyUnavailable, Exception) as e:
            if not isinstance(e, ContentCurrentlyUnavailable):
                LOGGER.exception("Can't get content of object %s" % item_hash)
            raise MessageContentUnavailable(f"Could not fetch content for {item_hash}")

        try:
            validated_message = MessageDb.from_pending_message(
                pending_message=pending_message,
                content_dict=content.value,
                content_size=len(content.raw_value),
            )
        except ValidationError as e:
            raise InvalidMessageFormat(errors=e.errors()) from e

        return validated_message

    async def fetch_related_content(self, session: DbSession, message: MessageDb):
        content_handler = self.get_content_handler(message.type)

        try:
            await content_handler.fetch_related_content(
                session=session, message=message
            )
        except UnknownHashError as e:
            raise InvalidMessageFormat(
                f"Invalid IPFS hash for message {message.item_hash}"
            ) from e

    async def load_fetched_content(
        self, session: DbSession, pending_message: PendingMessageDb
    ) -> PendingMessageDb:
        if pending_message.item_type != ItemType.inline:
            pending_message.fetched = False
            return pending_message

        # We reuse fetch_pending_messages to load the inline content. The check
        # above ensures we will not load content from the network here.
        message = await self.fetch_pending_message(pending_message=pending_message)
        content_handler = self.get_content_handler(message.type)
        is_fetched = await content_handler.is_related_content_fetched(
            session=session, message=message
        )

        pending_message.fetched = is_fetched
        pending_message.content = message.content
        return pending_message


class MessagePublisher(BaseMessageHandler):
    """
    Class in charge of adding pending messages to the node.
    """

    def __init__(
        self,
        session_factory: DbSessionFactory,
        storage_service: StorageService,
        config: Config,
        pending_message_exchange: aio_pika.abc.AbstractExchange,
    ):
        super().__init__(
            storage_service=storage_service,
            config=config,
        )
        self.session_factory = session_factory
        self.pending_message_exchange = pending_message_exchange

    async def _publish_pending_message(self, pending_message: PendingMessageDb) -> None:
        mq_message = aio_pika.Message(body=f"{pending_message.id}".encode("utf-8"))
        process_or_fetch = "process" if pending_message.fetched else "fetch"
        if pending_message.origin != MessageOrigin.ONCHAIN:
            await self.pending_message_exchange.publish(
                mq_message,
                routing_key=f"{process_or_fetch}.{pending_message.item_hash}",
            )

    async def add_pending_message(
        self,
        message_dict: Mapping[str, Any],
        reception_time: dt.datetime,
        tx_hash: Optional[str] = None,
        check_message: bool = True,
        origin: Optional[MessageOrigin] = MessageOrigin.P2P,
    ) -> Optional[PendingMessageDb]:
        # TODO: this implementation is just messy, improve it.
        with self.session_factory() as session:
            try:
                # we don't check signatures yet.
                message = parse_message(message_dict)
            except InvalidMessageException as e:
                LOGGER.warning(e)
                reject_new_pending_message(
                    session=session,
                    pending_message=message_dict,
                    exception=e,
                    tx_hash=tx_hash,
                )
                session.commit()
                return None

            pending_message = PendingMessageDb.from_obj(
                message,
                reception_time=reception_time,
                tx_hash=tx_hash,
                check_message=check_message,
                origin=origin,
            )

            try:
                pending_message = await self.load_fetched_content(
                    session, pending_message
                )
            except InvalidMessageException as e:
                LOGGER.warning("Invalid message: %s - %s", message.item_hash, str(e))
                reject_new_pending_message(
                    session=session,
                    pending_message=message_dict,
                    exception=e,
                    tx_hash=tx_hash,
                )
                session.commit()
                return None

            # Check if there are an already existing record
            existing_message = (
                session.query(PendingMessageDb)
                .filter_by(
                    sender=pending_message.sender,
                    item_hash=pending_message.item_hash,
                    signature=pending_message.signature,
                )
                .one_or_none()
            )
            if existing_message:
                return existing_message

            upsert_message_status_stmt = make_message_status_upsert_query(
                item_hash=pending_message.item_hash,
                new_status=MessageStatus.PENDING,
                reception_time=reception_time,
                where=MessageStatusDb.status == MessageStatus.REJECTED,
            )
            insert_pending_message_stmt = (
                insert(PendingMessageDb)
                .values(pending_message.to_dict(exclude={"id"}))
                .on_conflict_do_nothing("uq_pending_message")
            )

            try:
                session.execute(upsert_message_status_stmt)
                session.execute(insert_pending_message_stmt)
                session.commit()
            except sqlalchemy.exc.IntegrityError:
                # Handle the unique constraint violation.
                LOGGER.warning("Duplicate pending message detected trying to save it.")
                return None

            except (psycopg2.Error, sqlalchemy.exc.SQLAlchemyError) as e:
                LOGGER.warning(
                    "Failed to add new pending message %s - DB error: %s",
                    pending_message.item_hash,
                    str(e),
                )
                session.rollback()
                reject_new_pending_message(
                    session=session,
                    pending_message=message_dict,
                    exception=e,
                    tx_hash=tx_hash,
                )
                session.commit()
                return None

            await self._publish_pending_message(pending_message)
            return pending_message


class MessageHandler(BaseMessageHandler):
    """
    Class in charge of processing pending messages, i.e. validate their correctness, fetch related
    content and insert data in the proper DB tables.
    """

    content_handlers: Dict[MessageType, ContentHandler]

    def __init__(
        self,
        signature_verifier: SignatureVerifier,
        storage_service: StorageService,
        config: Config,
    ):
        super().__init__(storage_service=storage_service, config=config)
        self._signature_verifier = signature_verifier

    async def verify_signature(self, pending_message: PendingMessageDb):
        if pending_message.check_message:
            # TODO: remove type: ignore by deciding the pending message type
            await self._signature_verifier.verify_signature(pending_message)  # type: ignore

    @staticmethod
    async def confirm_existing_message(
        session: DbSession,
        existing_message: MessageDb,
        pending_message: PendingMessageDb,
    ):
        if pending_message.signature != existing_message.signature:
            raise InvalidSignature(f"Invalid signature for {pending_message.item_hash}")

        delete_pending_message(session=session, pending_message=pending_message)
        if tx_hash := pending_message.tx_hash:
            session.execute(
                make_confirmation_upsert_query(
                    item_hash=pending_message.item_hash, tx_hash=tx_hash
                )
            )

    @staticmethod
    async def confirm_existing_forgotten_message(
        session: DbSession,
        forgotten_message: ForgottenMessageDb,
        pending_message: PendingMessageDb,
    ):
        if pending_message.signature != forgotten_message.signature:
            raise InvalidSignature(f"Invalid signature for {pending_message.item_hash}")

        delete_pending_message(session=session, pending_message=pending_message)

    async def insert_message(
        self, session: DbSession, pending_message: PendingMessageDb, message: MessageDb
    ):
        session.execute(make_message_upsert_query(message))
        if message.item_type != ItemType.inline:
            upsert_file(
                session=session,
                file_hash=message.item_hash,
                size=message.size,
                file_type=FileType.FILE,
            )
            insert_content_file_pin(
                session=session,
                file_hash=message.item_hash,
                owner=message.sender,
                item_hash=message.item_hash,
                created=timestamp_to_datetime(message.content["time"]),
            )

        delete_pending_message(session=session, pending_message=pending_message)
        session.execute(
            make_message_status_upsert_query(
                item_hash=message.item_hash,
                new_status=MessageStatus.PROCESSED,
                reception_time=pending_message.reception_time,
                where=(MessageStatusDb.status == MessageStatus.PENDING),
            )
        )

        if tx_hash := pending_message.tx_hash:
            session.execute(
                make_confirmation_upsert_query(
                    item_hash=message.item_hash, tx_hash=tx_hash
                )
            )

    async def verify_and_fetch(
        self, session: DbSession, pending_message: PendingMessageDb
    ) -> MessageDb:
        await self.verify_signature(pending_message=pending_message)
        validated_message = await self.fetch_pending_message(
            pending_message=pending_message
        )
        await self.fetch_related_content(session=session, message=validated_message)
        return validated_message

    async def process(
        self, session: DbSession, pending_message: PendingMessageDb
    ) -> ProcessedMessage | RejectedMessage:
        """
        Process a pending message.

        If the message is successfully processed, returns a handled message object
        representing the processed message and some additional metadata.
        Throws a MessageProcessingException if the message cannot be processed.

        :param session: DB session.
        :param pending_message: Pending message to process.
        :return: The processed message with some metadata indicating whether the message
                 is a new one or a confirmation.
        """

        # Note: Check if message already exists (and confirm it)
        existing_message = get_message_by_item_hash(
            session=session, item_hash=ItemHash(pending_message.item_hash)
        )
        if existing_message:
            await self.confirm_existing_message(
                session=session,
                existing_message=existing_message,
                pending_message=pending_message,
            )
            return ProcessedMessage(message=existing_message, is_confirmation=True)

        # Note: Check if message is already forgotten (and confirm it)
        # this is to avoid race conditions when a confirmation arrives after the FORGET message has been preocessed
        forgotten_message = get_forgotten_message(
            session=session, item_hash=ItemHash(pending_message.item_hash)
        )
        if forgotten_message:
            await self.confirm_existing_forgotten_message(
                session=session,
                forgotten_message=forgotten_message,
                pending_message=pending_message,
            )
            return RejectedMessage(
                pending_message=pending_message,
                error_code=ErrorCode.FORGET_TARGET_NOT_FOUND,
            )

        message = await self.verify_and_fetch(
            session=session, pending_message=pending_message
        )
        content_handler = self.get_content_handler(message.type)
        await content_handler.check_dependencies(session=session, message=message)
        await self.check_permissions(session=session, message=message)
        await content_handler.check_balance(session=session, message=message)
        await self.insert_message(
            session=session, pending_message=pending_message, message=message
        )
        await content_handler.process(session=session, messages=[message])
        return ProcessedMessage(
            message=message,
            origin=(
                MessageOrigin(pending_message.origin)
                if pending_message.origin
                else None
            ),
            is_confirmation=False,
        )

    async def check_permissions(self, session: DbSession, message: MessageDb):
        content_handler = self.get_content_handler(message.type)
        await content_handler.check_permissions(session=session, message=message)
