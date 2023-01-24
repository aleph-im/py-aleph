"""
Base (abstract) class for messages.
"""

import datetime as dt
from hashlib import sha256
from typing import Optional, Generic, TypeVar

from aleph_message.models import BaseContent, Chain
from aleph_message.models import MessageType, ItemType
from pydantic import root_validator, validator
from pydantic.generics import GenericModel

from aleph.toolkit.timestamp import timestamp_to_datetime
from aleph.utils import item_type_from_hash

MType = TypeVar("MType", bound=MessageType)
ContentType = TypeVar("ContentType", bound=BaseContent)


class AlephBaseMessage(GenericModel, Generic[MType, ContentType]):
    """
    The base structure of an Aleph message.
    All the fields of this class appear in all the representations
    of messages on the Aleph network.
    """

    sender: str
    chain: Chain
    signature: Optional[str]
    type: MType
    item_content: Optional[str]
    item_type: ItemType
    item_hash: str
    time: dt.datetime
    channel: Optional[str] = None
    content: Optional[ContentType] = None

    @root_validator()
    def check_item_type(cls, values):
        """
        Checks that the item hash of the message matches the one inferred from the hash.
        Only applicable to storage/ipfs item types.
        """
        item_type_value = values.get("item_type")
        if item_type_value is None:
            raise ValueError("Could not determine item type")

        item_type = ItemType(item_type_value)
        if item_type == ItemType.inline:
            return values

        item_hash = values.get("item_hash")
        if item_hash is None:
            raise ValueError("Could not determine item hash")

        expected_item_type = item_type_from_hash(item_hash)
        if item_type != expected_item_type:
            raise ValueError(
                f"Expected {expected_item_type} based on hash but item type is {item_type}."
            )
        return values

    @validator("item_hash")
    def check_item_hash(cls, v, values):
        """
        For inline item types, check that the item hash is equal to
        the hash of the item content.
        """

        item_type = values.get("item_type")
        if item_type is None:
            raise ValueError("Could not determine item type")

        if item_type == ItemType.inline:
            item_content: str = values.get("item_content")
            if item_content is None:
                raise ValueError("Could not find inline item content")

            computed_hash: str = sha256(item_content.encode()).hexdigest()
            if v != computed_hash:
                raise ValueError(
                    "'item_hash' does not match 'sha256(item_content)'"
                    f", expecting {computed_hash}"
                )
        elif item_type == ItemType.ipfs:
            # TODO: CHeck that the hash looks like an IPFS multihash
            pass
        else:
            if item_type != ItemType.storage:
                raise ValueError(f"Unknown item type: '{item_type}'")
        return v

    @validator("time", pre=True)
    def check_time(cls, v, values):
        """
        Parses the time field as a UTC datetime. Contrary to the default datetime
        validator, this implementation raises an exception if the time field is
        too far in the future.
        """

        d = timestamp_to_datetime(v)
        return d
