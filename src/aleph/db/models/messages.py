import datetime as dt
from typing import Any, Dict, List, Mapping, Optional, Type

from aleph_message.models import (
    AggregateContent,
    BaseContent,
    Chain,
    ForgetContent,
    InstanceContent,
    ItemType,
    MessageType,
    PostContent,
    ProgramContent,
    StoreContent,
)
from pydantic import ValidationError
from sqlalchemy import (
    ARRAY,
    TIMESTAMP,
    Column,
    ForeignKey,
    Integer,
    String,
    Table,
    UniqueConstraint,
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import relationship
from sqlalchemy_utils.types.choice import ChoiceType

from aleph.toolkit.timestamp import timestamp_to_datetime
from aleph.types.channel import Channel
from aleph.types.message_status import ErrorCode, MessageStatus

from .base import Base
from .chains import ChainTxDb
from .pending_messages import PendingMessageDb

CONTENT_TYPE_MAP: Dict[MessageType, Type[BaseContent]] = {
    MessageType.aggregate: AggregateContent,
    MessageType.forget: ForgetContent,
    MessageType.instance: InstanceContent,
    MessageType.post: PostContent,
    MessageType.program: ProgramContent,
    MessageType.store: StoreContent,
}


message_confirmations = Table(
    "message_confirmations",
    Base.metadata,
    Column("id", Integer, primary_key=True),
    Column("item_hash", ForeignKey("messages.item_hash"), nullable=False, index=True),
    Column("tx_hash", ForeignKey("chain_txs.hash", ondelete="CASCADE"), nullable=False),
    UniqueConstraint("item_hash", "tx_hash"),
)


def validate_message_content(
    message_type: MessageType,
    content_dict: Dict[str, Any],
) -> BaseContent:
    content_type = CONTENT_TYPE_MAP[message_type]
    content = content_type.model_validate(content_dict)
    # Validate that the content time can be converted to datetime. This will
    # raise a ValueError and be caught
    # TODO: move this validation in aleph-message
    try:
        _ = dt.datetime.fromtimestamp(content_dict["time"])
    except ValueError as e:
        raise ValidationError(str(e)) from e

    return content


class MessageStatusDb(Base):
    __tablename__ = "message_status"

    item_hash: str = Column(String, primary_key=True)
    status: MessageStatus = Column(ChoiceType(MessageStatus), nullable=False)
    reception_time: dt.datetime = Column(TIMESTAMP(timezone=True), nullable=False)


class MessageDb(Base):
    """
    A message that was processed and validated by the CCN.
    """

    __tablename__ = "messages"

    item_hash: str = Column(String, primary_key=True)
    type: MessageType = Column(ChoiceType(MessageType), nullable=False)
    chain: Chain = Column(ChoiceType(Chain), nullable=False)
    sender: str = Column(String, nullable=False, index=True)
    signature: Optional[str] = Column(String, nullable=True)
    item_type: ItemType = Column(ChoiceType(ItemType), nullable=False)
    item_content: Optional[str] = Column(String, nullable=True)
    content: Any = Column(JSONB, nullable=False)
    time: dt.datetime = Column(TIMESTAMP(timezone=True), nullable=False, index=True)
    channel: Optional[Channel] = Column(String, nullable=True, index=True)
    size: int = Column(Integer, nullable=False)

    confirmations: "List[ChainTxDb]" = relationship(
        "ChainTxDb", secondary=message_confirmations
    )

    _parsed_content: Optional[BaseContent] = None

    @property
    def confirmed(self) -> bool:
        return bool(self.confirmations)

    @property
    def parsed_content(self):
        if self._parsed_content is None:
            self._parsed_content = validate_message_content(self.type, self.content)
        return self._parsed_content

    @staticmethod
    def _coerce_content(
        pending_message: PendingMessageDb, content_dict: Dict[str, Any]
    ) -> Dict[str, Any]:
        if content_dict.get("address") is None:
            content_dict["address"] = pending_message.sender
        if content_dict.get("time") is None:
            content_dict["time"] = pending_message.time.timestamp()
        return content_dict

    @classmethod
    def from_pending_message(
        cls,
        pending_message: PendingMessageDb,
        content_dict: Dict[str, Any],
        content_size: int,
    ) -> "MessageDb":
        content_dict = cls._coerce_content(pending_message, content_dict)
        parsed_content = validate_message_content(pending_message.type, content_dict)

        message = cls(
            item_hash=pending_message.item_hash,
            type=pending_message.type,
            chain=pending_message.chain,
            sender=pending_message.sender,
            signature=pending_message.signature,
            item_type=pending_message.item_type,
            item_content=pending_message.item_content,
            content=content_dict,
            time=pending_message.time,
            channel=pending_message.channel,
            size=content_size,
        )
        message._parsed_content = parsed_content
        return message

    @classmethod
    def from_message_dict(cls, message_dict: Dict[str, Any]) -> "MessageDb":
        """
        Utility function to translate Aleph message dictionaries, such as those returned by the API,
        in the corresponding DB object.
        """

        item_hash = message_dict["item_hash"]

        return cls(
            item_hash=item_hash,
            type=message_dict["type"],
            chain=Chain(message_dict["chain"]),
            sender=message_dict["sender"],
            signature=message_dict["signature"],
            item_type=ItemType(message_dict.get("item_type", ItemType.inline)),
            item_content=message_dict.get("item_content"),
            content=message_dict["content"],
            time=timestamp_to_datetime(message_dict["time"]),
            channel=message_dict.get("channel"),
            size=message_dict.get("size", 0),
        )


# TODO: move these to their own files?
class ForgottenMessageDb(Base):
    __tablename__ = "forgotten_messages"

    item_hash: str = Column(String, primary_key=True)
    type: MessageType = Column(ChoiceType(MessageType), nullable=False)
    chain: Chain = Column(ChoiceType(Chain), nullable=False)
    sender: str = Column(String, nullable=False, index=True)
    signature: Optional[str] = Column(String, nullable=True)
    item_type: ItemType = Column(ChoiceType(ItemType), nullable=False)
    time: dt.datetime = Column(TIMESTAMP(timezone=True), nullable=False, index=True)
    channel: Optional[Channel] = Column(String, nullable=True, index=True)
    forgotten_by: List[str] = Column(ARRAY(String), nullable=False)  # type: ignore


class ErrorCodeDb(Base):
    __tablename__ = "error_codes"

    code: int = Column(Integer, primary_key=True)
    description: str = Column(String, nullable=False)


class RejectedMessageDb(Base):
    __tablename__ = "rejected_messages"

    item_hash: str = Column(String, primary_key=True)
    message: Mapping[str, Any] = Column(JSONB, nullable=False)
    error_code: ErrorCode = Column(
        ChoiceType(ErrorCode, impl=Integer()), nullable=False
    )
    details: Optional[Dict[str, Any]] = Column(JSONB, nullable=True)
    traceback: Optional[str] = Column(String, nullable=True)
    tx_hash: Optional[str] = Column(ForeignKey("chain_txs.hash"), nullable=True)
