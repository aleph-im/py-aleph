import datetime as dt
from typing import (
    Annotated,
    Any,
    Dict,
    Generic,
    List,
    Literal,
    Mapping,
    Optional,
    Type,
    TypeVar,
    Union,
)

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
from pydantic import BaseModel, ConfigDict, Field

from aleph.db.models import MessageDb
from aleph.types.message_status import ErrorCode, MessageStatus

MType = TypeVar("MType", bound=MessageType)
ContentType = TypeVar("ContentType", bound=BaseContent)


class MessageConfirmation(BaseModel):
    """Format of the result when a message has been confirmed on a blockchain"""

    model_config = ConfigDict(
        from_attributes=True, json_encoders={dt.datetime: lambda d: d.timestamp()}
    )

    chain: Chain
    height: int
    hash: str


class BaseMessage(BaseModel, Generic[MType, ContentType]):
    model_config = ConfigDict(
        from_attributes=True,
        json_encoders={dt.datetime: lambda d: d.timestamp()},
    )

    sender: str
    chain: Chain
    signature: Optional[str] = None
    type: MType
    item_content: Optional[str] = None
    item_type: ItemType
    item_hash: str
    time: dt.datetime
    channel: Optional[str] = None
    content: ContentType
    confirmed: bool
    confirmations: List[MessageConfirmation]


class AggregateMessage(
    BaseMessage[Literal[MessageType.aggregate], AggregateContent]  # type: ignore
): ...


class ForgetMessage(
    BaseMessage[Literal[MessageType.forget], ForgetContent]  # type: ignore
): ...


class InstanceMessage(BaseMessage[Literal[MessageType.instance], InstanceContent]): ...  # type: ignore


class PostMessage(BaseMessage[Literal[MessageType.post], PostContent]): ...  # type: ignore


class ProgramMessage(BaseMessage[Literal[MessageType.program], ProgramContent]): ...  # type: ignore


class StoreMessage(BaseMessage[Literal[MessageType.store], StoreContent]): ...  # type: ignore


MESSAGE_CLS_DICT: Dict[
    Any,
    Type[
        AggregateMessage
        | ForgetMessage
        | InstanceMessage
        | PostMessage
        | ProgramMessage
        | StoreMessage
    ],
] = {
    MessageType.aggregate: AggregateMessage,
    MessageType.forget: ForgetMessage,
    MessageType.instance: InstanceMessage,
    MessageType.post: PostMessage,
    MessageType.program: ProgramMessage,
    MessageType.store: StoreMessage,
}


AlephMessage = Annotated[
    Union[
        AggregateMessage,
        ForgetMessage,
        InstanceMessage,
        PostMessage,
        ProgramMessage,
        StoreMessage,
    ],
    Field(discriminator="type"),
]


def format_message(message: MessageDb) -> AlephMessage:
    message_type = message.type

    message_cls = MESSAGE_CLS_DICT[message_type]
    return message_cls.from_orm(message)


def format_message_dict(message: Dict[str, Any]) -> AlephMessage:
    message_type = message.get("type")
    message_cls = MESSAGE_CLS_DICT[message_type]
    return message_cls.model_validate(message)


class BaseMessageStatus(BaseModel):
    status: MessageStatus
    item_hash: str
    reception_time: dt.datetime


# We already have a model for the validation of pending messages, but this one
# is only used for formatting and does not try to be smart.
class PendingMessage(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    sender: str
    chain: Chain
    signature: Optional[str] = None
    type: MessageType
    item_content: Optional[str] = None
    item_type: ItemType
    item_hash: str
    time: dt.datetime
    channel: Optional[str] = None
    content: Optional[Dict[str, Any]] = None
    reception_time: dt.datetime


class PendingMessageStatus(BaseMessageStatus):
    model_config = ConfigDict(from_attributes=True)

    status: MessageStatus = MessageStatus.PENDING
    messages: List[PendingMessage]


class ProcessedMessageStatus(BaseMessageStatus):
    model_config = ConfigDict(from_attributes=True)

    status: MessageStatus = MessageStatus.PROCESSED
    message: AlephMessage


class ForgottenMessage(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    sender: str
    chain: Chain
    signature: Optional[str] = None
    type: MessageType
    item_type: ItemType
    item_hash: str
    time: dt.datetime
    channel: Optional[str] = None


class ForgottenMessageStatus(BaseMessageStatus):
    status: MessageStatus = MessageStatus.FORGOTTEN
    message: ForgottenMessage
    forgotten_by: List[str]


class RejectedMessageStatus(BaseMessageStatus):
    status: MessageStatus = MessageStatus.REJECTED
    message: Mapping[str, Any]
    error_code: ErrorCode
    details: Any = None


MessageWithStatus = Union[
    PendingMessageStatus,
    ProcessedMessageStatus,
    ForgottenMessageStatus,
    RejectedMessageStatus,
]


class MessageListResponse(BaseModel):
    model_config = ConfigDict(
        json_encoders={dt.datetime: lambda d: d.timestamp()},
    )

    messages: List[AlephMessage]
    pagination_page: int
    pagination_total: int
    pagination_per_page: int
    pagination_item: Literal["messages"] = "messages"
