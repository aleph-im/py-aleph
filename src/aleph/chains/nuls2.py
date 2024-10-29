import asyncio
import base64
import functools
import json
import logging
import struct
import time
from operator import itemgetter
from typing import AsyncIterator, Dict, Tuple

import aiohttp
from aleph_message.models import Chain
from coincurve.keys import PrivateKey
from configmanager import Config
from nuls2.api.server import get_server
from nuls2.model.data import (
    CHEAP_UNIT_FEE,
    get_address,
    hash_from_address,
    recover_message_address,
)
from nuls2.model.transaction import Transaction

from aleph.chains.common import get_verification_buffer
from aleph.db.accessors.chains import get_last_height, upsert_chain_sync_status
from aleph.db.accessors.messages import get_unconfirmed_messages
from aleph.db.accessors.pending_messages import count_pending_messages
from aleph.db.accessors.pending_txs import count_pending_txs
from aleph.schemas.chains.tx_context import TxContext
from aleph.schemas.pending_messages import BasePendingMessage
from aleph.toolkit.timestamp import utc_now
from aleph.types.db_session import DbSessionFactory
from aleph.utils import run_in_executor

from ..db.models import ChainTxDb
from ..types.chain_sync import ChainEventType
from .abc import ChainWriter, Verifier
from .chain_data_service import ChainDataService, PendingTxPublisher

LOGGER = logging.getLogger("chains.nuls2")
CHAIN_NAME = "NULS2"


class Nuls2Verifier(Verifier):
    async def verify_signature(self, message: BasePendingMessage) -> bool:
        """Verifies a signature of a message, return True if verified, false if not"""

        if message.signature is None:
            LOGGER.warning("'%s': missing signature.", message.item_hash)
            return False

        sig_raw = base64.b64decode(message.signature)

        sender_hash = hash_from_address(message.sender)
        (sender_chain_id,) = struct.unpack("h", sender_hash[:2])
        verification = get_verification_buffer(message)
        try:
            address = await run_in_executor(
                None,
                functools.partial(
                    recover_message_address,
                    sig_raw,
                    verification,
                    chain_id=sender_chain_id,
                ),
            )
        except Exception:
            LOGGER.exception("NULS Signature verification error")
            return False

        if address != message.sender:
            LOGGER.warning(
                "Received bad signature from %s for %s" % (address, message.sender)
            )
            return False
        else:
            return True


class Nuls2Connector(ChainWriter):
    def __init__(
        self,
        session_factory: DbSessionFactory,
        pending_tx_publisher: PendingTxPublisher,
        chain_data_service: ChainDataService,
    ):
        self.session_factory = session_factory
        self.pending_tx_publisher = pending_tx_publisher
        self.chain_data_service = chain_data_service

    async def get_last_height(self, sync_type: ChainEventType) -> int:
        """Returns the last height for which we already have the nuls data."""
        with self.session_factory() as session:
            last_height = get_last_height(
                session=session, chain=Chain.NULS2, sync_type=sync_type
            )

        if last_height is None:
            last_height = -1

        return last_height

    async def _request_transactions(
        self, config, session, start_height
    ) -> AsyncIterator[Tuple[Dict, TxContext]]:
        """Continuously request data from the NULS blockchain."""
        target_addr = config.nuls2.sync_address.value
        remark = config.nuls2.remark.value
        chain_id = config.nuls2.chain_id.value

        last_height = None
        async for tx in get_transactions(
            config, session, chain_id, target_addr, start_height, remark=remark
        ):
            ldata = tx["txDataHex"]
            LOGGER.info("Handling TX in block %s" % tx["height"])
            try:
                ddata = bytes.fromhex(ldata).decode("utf-8")
                last_height = tx["height"]
                jdata = json.loads(ddata)

                context = TxContext(
                    chain=Chain(CHAIN_NAME),
                    hash=tx["hash"],
                    height=tx["height"],
                    time=tx["createTime"],
                    publisher=tx["coinFroms"][0]["address"],
                )
                yield jdata, context

            except json.JSONDecodeError:
                # if it's not valid json, just ignore it...
                LOGGER.info("Incoming logic data is not JSON, ignoring. %r" % ldata)

        if last_height:
            with self.session_factory() as session:
                upsert_chain_sync_status(
                    session=session,
                    chain=Chain.NULS2,
                    sync_type=ChainEventType.SYNC,
                    height=last_height,
                    update_datetime=utc_now(),
                )
                session.commit()

    async def fetcher(self, config: Config):
        last_stored_height = await self.get_last_height(sync_type=ChainEventType.SYNC)

        LOGGER.info("Last block is #%d" % last_stored_height)
        async with aiohttp.ClientSession() as http_session:
            while True:
                last_stored_height = await self.get_last_height(
                    sync_type=ChainEventType.SYNC
                )
                async for jdata, context in self._request_transactions(
                    config, http_session, last_stored_height + 1
                ):
                    tx = ChainTxDb.from_sync_tx_context(
                        tx_context=context, tx_data=jdata
                    )
                    with self.session_factory() as db_session:
                        await self.pending_tx_publisher.add_and_publish_pending_tx(
                            session=db_session, tx=tx
                        )
                        db_session.commit()

                await asyncio.sleep(10)

    async def packer(self, config: Config):
        server = get_server(config.nuls2.api_url.value)
        target_addr = config.nuls2.sync_address.value
        remark = config.nuls2.remark.value.encode("utf-8")

        pri_key = bytes.fromhex(config.nuls2.private_key.value)
        privkey = PrivateKey(pri_key)
        pub_key = privkey.public_key.format()
        chain_id = config.nuls2.chain_id.value
        address = get_address(pub_key, config.nuls2.chain_id.value)

        LOGGER.info("NULS2 Connector set up with address %s" % address)
        i = 0
        nonce = await get_nonce(server, address, chain_id)

        while True:
            with self.session_factory() as session:
                if (count_pending_txs(session=session, chain=Chain.NULS2)) or (
                    count_pending_messages(session=session, chain=Chain.NULS2)
                ):
                    await asyncio.sleep(30)
                    continue

                if i >= 100:
                    await asyncio.sleep(30)  # wait three (!!) blocks
                    nonce = await get_nonce(server, address, chain_id)
                    i = 0

                messages = list(
                    get_unconfirmed_messages(
                        session=session, limit=10000, chain=Chain.ETH
                    )
                )

            if len(messages):
                # This function prepares a chain data file and makes it downloadable from the node.
                sync_event_payload = (
                    await self.chain_data_service.prepare_sync_event_payload(
                        session=session, messages=messages
                    )
                )
                # Required to apply update to the files table in get_chaindata
                session.commit()

                content = sync_event_payload.model_dump_json()
                tx = await prepare_transfer_tx(
                    address,
                    [(target_addr, CHEAP_UNIT_FEE)],
                    nonce,
                    chain_id=chain_id,
                    asset_id=1,
                    raw_tx_data=content.encode("utf-8"),
                    remark=remark,
                )
                await tx.sign_tx(pri_key)
                tx_hex = (await tx.serialize(update_data=False)).hex()
                ret = await broadcast(server, tx_hex, chain_id=chain_id)
                LOGGER.info("Broadcasted %r on %s" % (ret["hash"], CHAIN_NAME))
                nonce = ret["hash"][-16:]

            await asyncio.sleep(config.nuls2.commit_delay.value)
            i += 1


async def get_base_url(config):
    return config.nuls2.explorer_url.value


async def get_transactions(
    config, session, chain_id, target_addr, start_height, end_height=None, remark=None
):
    check_url = "{}transactions.json".format(await get_base_url(config))

    async with session.get(
        check_url,
        params={
            "address": target_addr,
            "sort_order": 1,
            "startHeight": start_height + 1,
            "pagination": 500,
        },
    ) as resp:
        jres = await resp.model_dump_json()
        for tx in sorted(jres["transactions"], key=itemgetter("height")):
            if remark is not None and tx["remark"] != remark:
                continue

            yield tx


async def broadcast(server, tx_hex, chain_id=1):
    return await server.broadcastTx(chain_id, tx_hex)


async def get_balance(server, address, chain_id, asset_id):
    return await server.getAccountBalance(chain_id, chain_id, asset_id, address)


async def prepare_transfer_tx(
    address, targets, nonce, chain_id=1, asset_id=1, remark=b"", raw_tx_data=None
):
    """Targets are tuples: address and value."""
    outputs = [
        {
            "address": add,
            "amount": val,
            "lockTime": 0,
            "assetsChainId": chain_id,
            "assetsId": asset_id,
        }
        for add, val in targets
    ]

    tx = await Transaction.from_dict(
        {
            "type": 2,
            "time": int(time.time()),
            "remark": remark,
            "coinFroms": [
                {
                    "address": address,
                    "assetsChainId": chain_id,
                    "assetsId": asset_id,
                    "amount": 0,
                    "nonce": nonce,
                    "locked": 0,
                }
            ],
            "coinTos": outputs,
        }
    )
    tx.inputs[0]["amount"] = (await tx.calculate_fee()) + sum(
        [o["amount"] for o in outputs]
    )

    if raw_tx_data is not None:
        tx.raw_tx_data = raw_tx_data

    return tx


async def get_nonce(server, account_address, chain_id, asset_id=1):
    balance_info = await get_balance(server, account_address, chain_id, asset_id)
    return balance_info["nonce"]
