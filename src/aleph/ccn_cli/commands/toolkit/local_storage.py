from typing import Any, Dict, FrozenSet, List, Optional

from aleph_message.models import MessageType

from aleph.model.filepin import PermanentPin
from aleph.model.messages import Message


async def get_hashes(
    item_type_field: str, item_hash_field: str, msg_type: Optional[MessageType] = None
) -> FrozenSet[str]:
    def rgetitem(dictionary: Any, fields: List[str]) -> Any:
        value = dictionary[fields[0]]
        if len(fields) > 1:
            return rgetitem(value, fields[1:])
        return value

    filters = {
        # Check if the hash field exists in case the message was forgotten
        item_hash_field: {"$exists": 1},
        item_type_field: {"$in": ["ipfs", "storage"]},
    }
    if msg_type:
        filters["type"] = msg_type

    hashes = [
        rgetitem(msg, item_hash_field.split("."))
        async for msg in Message.collection.find(
            filters,
            {item_hash_field: 1},
            batch_size=1000,
        )
    ]

    # Temporary fix for api2. A message has a list of dicts as item hash.
    hashes = [h for h in hashes if isinstance(h, str)]

    return frozenset(hashes)


async def list_expected_local_files() -> Dict[str, FrozenSet[str]]:
    """
    Lists the files that are expected to be found on the local storage of the CCN.
    This includes:
    * the content of any message with item_type in ["storage", "ipfs"]
    * the stored content of any STORE message with content.item_type in ["storage", "ipfs"]
    * file pins
    """

    expected_files = {}

    expected_files["non-inline messages"] = await get_hashes(
        item_type_field="item_type",
        item_hash_field="item_hash",
    )
    expected_files["stores"] = await get_hashes(
        item_type_field="content.item_type",
        item_hash_field="content.item_hash",
        msg_type=MessageType.store,
    )

    # We also keep permanent pins, even if they are also stored on IPFS
    expected_files["file pins"] = frozenset(
        [
            pin["multihash"]
            async for pin in PermanentPin.collection.find({}, {"multihash": 1})
        ]
    )

    return expected_files
