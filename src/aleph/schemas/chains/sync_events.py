import datetime as dt
from typing import Annotated, List, Literal, Optional, Union

from aleph_message.models import Chain, ItemHash, ItemType, MessageType
from pydantic import BaseModel, ConfigDict, Field, field_validator

from aleph.types.chain_sync import ChainSyncProtocol
from aleph.types.channel import Channel


class OnChainMessage(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    sender: str
    chain: Chain
    signature: Optional[str] = None
    type: MessageType
    item_content: Optional[str] = None
    item_type: ItemType
    item_hash: ItemHash
    time: float
    channel: Optional[Channel] = None

    @field_validator("time", mode="before")
    def check_time(cls, v, values):
        if isinstance(v, dt.datetime):
            return v.timestamp()

        return v


class OnChainContent(BaseModel):
    messages: List[OnChainMessage]


class OnChainSyncEventPayload(BaseModel):
    protocol: Literal[ChainSyncProtocol.ON_CHAIN_SYNC]
    version: int
    content: OnChainContent


class OffChainSyncEventPayload(BaseModel):
    protocol: Literal[ChainSyncProtocol.OFF_CHAIN_SYNC]
    version: int
    content: str


SyncEventPayload = Annotated[
    Union[OnChainSyncEventPayload, OffChainSyncEventPayload],
    Field(discriminator="protocol"),
]
