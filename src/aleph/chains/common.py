import asyncio
import json
import logging
from dataclasses import asdict
from enum import IntEnum
from typing import Dict, Optional, Tuple, List

from aleph_message.models import MessageType, ItemType
from bson import ObjectId
from pymongo import UpdateOne

from aleph.config import get_config
from aleph.exceptions import (
    AlephStorageException,
    InvalidContent,
    ContentCurrentlyUnavailable,
    UnknownHashError,
    InvalidMessageError,
)
from aleph.handlers.forget import handle_forget_message
from aleph.handlers.storage import handle_new_storage
from aleph.model.db_bulk_operation import DbBulkOperation
from aleph.model.filepin import PermanentPin
from aleph.model.messages import CappedMessage, Message
from aleph.model.pending import PendingMessage, PendingTX
from aleph.network import check_message as check_message_fn
from aleph.permissions import check_sender_authorization
from aleph.storage import get_json, pin_hash, add_json, get_message_content
from .tx_context import TxContext
from ..schemas.pending_messages import BasePendingMessage
from ..utils import item_type_from_hash

LOGGER = logging.getLogger("chains.common")


async def get_verification_buffer(message: BasePendingMessage) -> bytes:
    """Returns a serialized string to verify the message integrity
    (this is was it signed)
    """
    buffer = f"{message.chain}\n{message.sender}\n{message.type}\n{message.item_hash}"
    return buffer.encode("utf-8")


async def mark_confirmed_data(chain_name, tx_hash, height):
    """Returns data required to mark a particular hash as confirmed
    in underlying chain.
    """
    return {
        "confirmed": True,
        "confirmations": [  # TODO: we should add the current one there
            # and not replace it.
            {"chain": chain_name, "height": height, "hash": tx_hash}
        ],
    }


async def delayed_incoming(message, chain_name=None, tx_hash=None, height=None):
    if message is None:
        return
    await PendingMessage.collection.insert_one(
        {
            "message": message,
            "source": dict(
                chain_name=chain_name,
                tx_hash=tx_hash,
                height=height,
                check_message=True,  # should we store this?
            ),
        }
    )


class IncomingStatus(IntEnum):
    FAILED_PERMANENTLY = -1
    RETRYING_LATER = 0
    MESSAGE_HANDLED = 1


async def mark_message_for_retry(
    message: Dict,
    chain_name: Optional[str],
    tx_hash: Optional[str],
    height: Optional[int],
    check_message: bool,
    retrying: bool,
    existing_id,
):
    if not retrying:
        await PendingMessage.collection.insert_one(
            {
                "message": message,
                "source": dict(
                    chain_name=chain_name,
                    tx_hash=tx_hash,
                    height=height,
                    check_message=check_message,  # should we store this?
                ),
            }
        )
    else:
        LOGGER.debug(f"Incrementing for {existing_id}")
        result = await PendingMessage.collection.update_one(
            filter={"_id": ObjectId(existing_id)}, update={"$inc": {"retries": 1}}
        )
        LOGGER.debug(f"Update result {result}")


def update_message_item_type(message_dict: Dict) -> Dict:
    """
    Ensures that the item_type field of a message is present.
    Sets it to the default value if the field is not specified.
    """
    if "item_type" in message_dict:
        return message_dict

    if "item_content" in message_dict:
        item_type = ItemType.inline
    else:
        item_type = item_type_from_hash(message_dict["item_hash"])

    message_dict["item_type"] = item_type
    return message_dict


async def incoming(
    message: Dict,
    chain_name: Optional[str] = None,
    tx_hash: Optional[str] = None,
    height: Optional[int] = None,
    seen_ids: Optional[Dict[Tuple, int]] = None,
    check_message: bool = False,
    retrying: bool = False,
    existing_id: Optional[ObjectId] = None,
) -> Tuple[IncomingStatus, List[DbBulkOperation]]:
    """New incoming message from underlying chain.

    For regular messages it will be marked as confirmed
    if existing in database, created if not.
    """

    # TODO: this is a temporary fix to set the item_type of the message to the correct
    #       value. This should be replaced by a full use of Pydantic models.
    message = update_message_item_type(message)

    item_hash = message["item_hash"]
    sender = message["sender"]
    ids_key = (item_hash, sender, chain_name)

    if chain_name and tx_hash and height and seen_ids is not None:
        if ids_key in seen_ids.keys():
            if height > seen_ids[ids_key]:
                return IncomingStatus.MESSAGE_HANDLED, []

    filters = {
        "item_hash": item_hash,
        "chain": message["chain"],
        "sender": message["sender"],
        "type": message["type"],
    }
    existing = await Message.collection.find_one(
        filters,
        projection={"confirmed": 1, "confirmations": 1, "time": 1, "signature": 1},
    )

    if check_message:
        if existing is None or (existing["signature"] != message["signature"]):
            # check/sanitize the message if needed
            try:
                message = await check_message_fn(
                    message, from_chain=(chain_name is not None)
                )
            except InvalidMessageError:
                return IncomingStatus.FAILED_PERMANENTLY, []

    if message is None:
        return IncomingStatus.MESSAGE_HANDLED, []

    if retrying:
        LOGGER.debug("(Re)trying %s." % item_hash)
    else:
        LOGGER.info("Incoming %s." % item_hash)

    # we set the incoming chain as default for signature
    message["chain"] = message.get("chain", chain_name)

    # if existing is None:
    #     # TODO: verify if search key is ok. do we need an unique key for messages?
    #     existing = await Message.collection.find_one(
    #         filters, projection={'confirmed': 1, 'confirmations': 1, 'time': 1})

    if chain_name and tx_hash and height:
        # We are getting a confirmation here
        new_values = await mark_confirmed_data(chain_name, tx_hash, height)

        updates = {
            "$set": {
                "confirmed": True,
            },
            "$min": {"time": message["time"]},
            "$addToSet": {"confirmations": new_values["confirmations"][0]},
        }
    else:
        updates = {
            "$max": {
                "confirmed": False,
            },
            "$min": {"time": message["time"]},
        }

    # new_values = {'confirmed': False}  # this should be our default.
    should_commit = False
    if existing:
        if seen_ids is not None and height is not None:
            if ids_key in seen_ids.keys():
                if height > seen_ids[ids_key]:
                    return IncomingStatus.MESSAGE_HANDLED, []
                else:
                    seen_ids[ids_key] = height
            else:
                seen_ids[ids_key] = height

        # THIS CODE SHOULD BE HERE...
        # But, if a race condition appeared, we might have the message twice.
        # if (existing['confirmed'] and
        #         chain_name in [c['chain'] for c in existing['confirmations']]):
        #     return

        LOGGER.debug("Updating %s." % item_hash)

        if chain_name and tx_hash and height:
            # we need to update messages adding the confirmation
            # await Message.collection.update_many(filters, updates)
            should_commit = True

    else:
        # if not (chain_name and tx_hash and height):
        #     new_values = {'confirmed': False}  # this should be our default.

        try:
            content = await get_message_content(message)

        except InvalidContent:
            LOGGER.warning("Can't get content of object %r, won't retry." % item_hash)
            return IncomingStatus.FAILED_PERMANENTLY, []

        except (ContentCurrentlyUnavailable, Exception) as e:
            if not isinstance(e, ContentCurrentlyUnavailable):
                LOGGER.exception("Can't get content of object %r" % item_hash)
            await mark_message_for_retry(
                message=message,
                chain_name=chain_name,
                tx_hash=tx_hash,
                height=height,
                check_message=check_message,
                retrying=retrying,
                existing_id=existing_id,
            )
            return IncomingStatus.RETRYING_LATER, []

        json_content = content.value
        if json_content.get("address", None) is None:
            json_content["address"] = message["sender"]

        if json_content.get("time", None) is None:
            json_content["time"] = message["time"]

        # warning: those handlers can modify message and content in place
        # and return a status. None has to be retried, -1 is discarded, True is
        # handled and kept.
        # TODO: change this, it's messy.
        try:
            if message["type"] == MessageType.store:
                handling_result = await handle_new_storage(message, json_content)
            elif message["type"] == MessageType.forget:
                # Handling it here means that there we ensure that the message
                # has been forgotten before it is saved on the node.
                # We may want the opposite instead: ensure that the message has
                # been saved before it is forgotten.
                handling_result = await handle_forget_message(message, json_content)
            else:
                handling_result = True
        except UnknownHashError:
            LOGGER.warning(f"Invalid IPFS hash for message {item_hash}, won't retry.")
            return IncomingStatus.FAILED_PERMANENTLY, []
        except Exception:
            LOGGER.exception("Error using the message type handler")
            handling_result = None

        if handling_result is None:
            LOGGER.debug("Message type handler has failed, retrying later.")
            await mark_message_for_retry(
                message=message,
                chain_name=chain_name,
                tx_hash=tx_hash,
                height=height,
                check_message=check_message,
                retrying=retrying,
                existing_id=existing_id,
            )
            return IncomingStatus.RETRYING_LATER, []

        if not handling_result:
            LOGGER.warning(
                "Message type handler has failed permanently for "
                "%r, won't retry." % item_hash
            )
            return IncomingStatus.FAILED_PERMANENTLY, []

        if not await check_sender_authorization(message, json_content):
            LOGGER.warning("Invalid sender for %s" % item_hash)
            return IncomingStatus.MESSAGE_HANDLED, []

        if seen_ids is not None and height is not None:
            if ids_key in seen_ids.keys():
                if height > seen_ids[ids_key]:
                    return IncomingStatus.MESSAGE_HANDLED, []
                else:
                    seen_ids[ids_key] = height
            else:
                seen_ids[ids_key] = height

        LOGGER.debug("New message to store for %s." % item_hash)
        # message.update(new_values)
        updates["$set"] = {
            "content": json_content,
            "size": len(content.raw_value),
            "item_content": message.get("item_content"),
            "item_type": message.get("item_type"),
            "channel": message.get("channel"),
            "signature": message.get("signature"),
            **updates.get("$set", {}),
        }
        should_commit = True

    if should_commit:
        update_op = UpdateOne(filters, updates, upsert=True)
        bulk_ops = [DbBulkOperation(Message, update_op)]

        # Capped collections do not accept updates that increase the size, so
        # we must ignore confirmations.
        if existing is None:
            bulk_ops.append(DbBulkOperation(CappedMessage, update_op))

        return IncomingStatus.MESSAGE_HANDLED, bulk_ops

    return IncomingStatus.MESSAGE_HANDLED, []


async def process_one_message(message: Dict, *args, **kwargs):
    """
    Helper function to process a message on the spot.
    """
    status, ops = await incoming(message, *args, **kwargs)
    for op in ops:
        await op.collection.collection.bulk_write([op.operation])


async def get_chaindata(messages, bulk_threshold=2000):
    """Returns content ready to be broadcasted on-chain (aka chaindata).

    If message length is over bulk_threshold (default 2000 chars), store list
    in IPFS and store the object hash instead of raw list.
    """
    chaindata = {"protocol": "aleph", "version": 1, "content": {"messages": messages}}
    content = json.dumps(chaindata)
    if len(content) > bulk_threshold:
        ipfs_id = await add_json(chaindata)
        return json.dumps(
            {"protocol": "aleph-offchain", "version": 1, "content": ipfs_id}
        )
    else:
        return content


async def get_chaindata_messages(
    chaindata: Dict, context: TxContext, seen_ids: Optional[List] = None
):
    config = get_config()

    protocol = chaindata.get("protocol", None)
    version = chaindata.get("version", None)
    if protocol == "aleph" and version == 1:
        messages = chaindata["content"]["messages"]
        if not isinstance(messages, list):
            error_msg = f"Got bad data in tx {context!r}"
            raise InvalidContent(error_msg)
        return messages

    if protocol == "aleph-offchain" and version == 1:
        assert isinstance(chaindata.get("content"), str)
        if seen_ids is not None:
            if chaindata["content"] in seen_ids:
                # is it really what we want here?
                LOGGER.debug("Already seen")
                return None
            else:
                LOGGER.debug("Adding to seen_ids")
                seen_ids.append(chaindata["content"])
        try:
            content = await get_json(chaindata["content"], timeout=10)
        except AlephStorageException:
            # Let the caller handle unavailable/invalid content
            raise
        except Exception as e:
            error_msg = f"Can't get content of offchain object {chaindata['content']!r}"
            LOGGER.exception("%s", error_msg)
            raise ContentCurrentlyUnavailable(error_msg) from e

        try:
            messages = await get_chaindata_messages(content.value, context)
        except AlephStorageException:
            LOGGER.debug("Got no message")
            raise

        LOGGER.info("Got bulk data with %d items" % len(messages))
        if config.ipfs.enabled.value:
            try:
                LOGGER.info(f"chaindata {chaindata}")
                await PermanentPin.register(
                    multihash=chaindata["content"],
                    reason={
                        "source": "chaindata",
                        "protocol": chaindata["protocol"],
                        "version": chaindata["version"],
                    },
                )
                # Some IPFS fetches can take a while, hence the large timeout.
                await asyncio.wait_for(pin_hash(chaindata["content"]), timeout=120)
            except asyncio.TimeoutError:
                LOGGER.warning(f"Can't pin hash {chaindata['content']}")
        return messages
    else:
        error_msg = f"Got unknown protocol/version object in tx {context!r}"
        LOGGER.info("%s", error_msg)
        raise InvalidContent(error_msg)


async def incoming_chaindata(content: Dict, context: TxContext):
    """Incoming data from a chain.
    Content can be inline of "offchain" through an ipfs hash.
    For now we only add it to the database, it will be processed later.
    """
    await PendingTX.collection.insert_one(
        {"content": content, "context": asdict(context)}
    )
