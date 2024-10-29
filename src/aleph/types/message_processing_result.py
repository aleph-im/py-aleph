from typing import Any, Dict, Protocol

from aleph.db.models import MessageDb, PendingMessageDb
from aleph.schemas.api.messages import format_message
from aleph.types.message_status import ErrorCode, MessageProcessingStatus


class MessageProcessingResult(Protocol):
    status: MessageProcessingStatus

    @property
    def item_hash(self) -> str:
        pass

    def to_dict(self) -> Dict[str, Any]:
        pass


class ProcessedMessage(MessageProcessingResult):
    def __init__(self, message: MessageDb, is_confirmation: bool = False):
        self.message = message
        self.status = (
            MessageProcessingStatus.PROCESSED_CONFIRMATION
            if is_confirmation
            else MessageProcessingStatus.PROCESSED_NEW_MESSAGE
        )

    @property
    def item_hash(self) -> str:
        return self.message.item_hash

    def to_dict(self) -> Dict[str, Any]:
        return {
            "status": self.status.value,
            "message": format_message(self.message).model_dump(),
        }


class FailedMessage(MessageProcessingResult):
    def __init__(
        self, pending_message: PendingMessageDb, error_code: ErrorCode, will_retry: bool
    ):
        self.pending_message = pending_message
        self.error_code = error_code

        self.status = (
            MessageProcessingStatus.FAILED_WILL_RETRY
            if will_retry
            else MessageProcessingStatus.FAILED_REJECTED
        )

    @property
    def item_hash(self) -> str:
        return self.pending_message.item_hash

    def to_dict(self) -> Dict[str, Any]:
        return {
            "status": self.status.value,
            "item_hash": self.item_hash,
        }


class WillRetryMessage(FailedMessage):
    def __init__(self, pending_message: PendingMessageDb, error_code: ErrorCode):
        super().__init__(pending_message, error_code, will_retry=True)


class RejectedMessage(FailedMessage):
    def __init__(self, pending_message: PendingMessageDb, error_code: ErrorCode):
        super().__init__(pending_message, error_code, will_retry=False)
