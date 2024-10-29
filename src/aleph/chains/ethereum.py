import asyncio
import functools
import importlib.resources
import json
import logging
from typing import AsyncIterator, Dict, Tuple

from aleph_message.models import Chain
from configmanager import Config
from eth_account import Account
from eth_account.messages import encode_defunct
from hexbytes import HexBytes
from web3 import Web3
from web3._utils.events import get_event_data
from web3.exceptions import MismatchedABI
from web3.gas_strategies.rpc import rpc_gas_price_strategy
from web3.middleware.filter import local_filter_middleware
from web3.middleware.geth_poa import geth_poa_middleware

from aleph.chains.common import get_verification_buffer
from aleph.db.accessors.chains import get_last_height, upsert_chain_sync_status
from aleph.db.accessors.messages import get_unconfirmed_messages
from aleph.db.accessors.pending_messages import count_pending_messages
from aleph.db.accessors.pending_txs import count_pending_txs
from aleph.db.models.chains import ChainTxDb
from aleph.schemas.chains.tx_context import TxContext
from aleph.schemas.pending_messages import BasePendingMessage
from aleph.toolkit.timestamp import utc_now
from aleph.types.chain_sync import ChainEventType
from aleph.types.db_session import DbSessionFactory
from aleph.utils import run_in_executor

from .abc import ChainWriter, Verifier
from .chain_data_service import ChainDataService, PendingTxPublisher
from .indexer_reader import AlephIndexerReader

LOGGER = logging.getLogger("chains.ethereum")
CHAIN_NAME = "ETH"


def get_web3(config) -> Web3:
    web3 = Web3(Web3.HTTPProvider(config.ethereum.api_url.value))
    if config.ethereum.chain_id.value == 4:  # rinkeby
        web3.middleware_onion.inject(geth_poa_middleware, layer=0)
    web3.middleware_onion.add(local_filter_middleware)
    web3.eth.set_gas_price_strategy(rpc_gas_price_strategy)

    return web3


async def get_contract_abi():
    contract_abi_resource = (
        importlib.resources.files("aleph.chains.assets") / "ethereum_sc_abi.json"
    )
    with contract_abi_resource.open("r", encoding="utf-8") as f:
        return json.load(f)


async def get_contract(config, web3: Web3):
    return web3.eth.contract(
        address=config.ethereum.sync_contract.value, abi=await get_contract_abi()
    )


def get_logs_query(web3: Web3, contract, start_height, end_height):
    return web3.eth.get_logs(
        {"address": contract.address, "fromBlock": start_height, "toBlock": end_height}
    )


class EthereumVerifier(Verifier):
    async def verify_signature(self, message: BasePendingMessage) -> bool:
        """Verifies a signature of a message, return True if verified, false if not"""

        verification = get_verification_buffer(message)

        message_hash = await run_in_executor(
            None, functools.partial(encode_defunct, text=verification.decode("utf-8"))
        )

        verified = False
        try:
            # we assume the signature is a valid string
            address = await run_in_executor(
                None,
                functools.partial(
                    Account.recover_message, message_hash, signature=message.signature
                ),
            )
            if address == message.sender:
                verified = True
            else:
                LOGGER.warning(
                    "Received bad signature from %s for %s" % (address, message.sender)
                )
                return False

        except Exception:
            LOGGER.exception("Error processing signature for %s" % message.sender)
            verified = False

        return verified


class EthereumConnector(ChainWriter):
    def __init__(
        self,
        session_factory: DbSessionFactory,
        pending_tx_publisher: PendingTxPublisher,
        chain_data_service: ChainDataService,
    ):
        self.session_factory = session_factory
        self.pending_tx_publisher = pending_tx_publisher
        self.chain_data_service = chain_data_service

        self.indexer_reader = AlephIndexerReader(
            chain=Chain.ETH,
            session_factory=session_factory,
            pending_tx_publisher=pending_tx_publisher,
        )

    async def get_last_height(self, sync_type: ChainEventType) -> int:
        """Returns the last height for which we already have the ethereum data."""
        with self.session_factory() as session:
            last_height = get_last_height(
                session=session, chain=Chain.ETH, sync_type=sync_type
            )

        if last_height is None:
            last_height = -1

        return last_height

    @staticmethod
    async def _get_logs(config, web3: Web3, contract, start_height):
        try:
            logs = get_logs_query(web3, contract, start_height + 1, "latest")
            for log in logs:
                yield log

            if not logs:
                LOGGER.info("No recent transactions, waiting 10 seconds.")
                await asyncio.sleep(10)

        except ValueError as e:
            # we got an error, let's try the pagination aware version.
            if e.args[0]["code"] != -32005:
                return

            last_block = await asyncio.get_event_loop().run_in_executor(
                None, web3.eth.get_block_number
            )
            if start_height < config.ethereum.start_height.value:
                start_height = config.ethereum.start_height.value

            end_height = start_height + 1000

            while True:
                try:
                    logs = get_logs_query(web3, contract, start_height, end_height)

                    for log in logs:
                        yield log

                    if not logs:
                        LOGGER.info("Processed all transactions, waiting 10 seconds.")
                        await asyncio.sleep(10)

                    start_height = end_height + 1
                    end_height = start_height + 1000

                    if start_height > last_block:
                        LOGGER.info("Ending big batch sync")
                        break

                except ValueError as e:
                    if e.args[0]["code"] == -32005:
                        end_height = start_height + 100
                    else:
                        raise

    async def _request_transactions(
        self, config, web3: Web3, contract, abi, start_height
    ) -> AsyncIterator[Tuple[Dict, TxContext]]:
        """Continuously request data from the Ethereum blockchain.
        TODO: support websocket API.
        """

        logs = self._get_logs(config, web3, contract, start_height + 1)

        async for log in logs:
            try:
                event_data = await run_in_executor(
                    None, get_event_data, web3.codec, abi, log
                )
            except MismatchedABI:
                # Ignore message events, they're handled by the indexer reader.
                continue
            LOGGER.info("Handling TX in block %s" % event_data.blockNumber)
            publisher = event_data.args.addr
            timestamp = event_data.args.timestamp

            if publisher in config.ethereum.authorized_emitters.value:
                message = event_data.args.message
                try:
                    jdata = json.loads(message)
                    context = TxContext(
                        chain=Chain(CHAIN_NAME),
                        hash=event_data.transactionHash.hex(),
                        time=timestamp,
                        height=event_data.blockNumber,
                        publisher=publisher,
                    )
                    yield jdata, context

                except json.JSONDecodeError:
                    # if it's not valid json, just ignore it...
                    LOGGER.info(
                        "Incoming logic data is not JSON, ignoring. %r" % message
                    )

                except Exception:
                    LOGGER.exception("Can't decode incoming logic data %r" % message)

            else:
                LOGGER.info(
                    "TX with unauthorized emitter %s in block %s",
                    publisher,
                    event_data.blockNumber,
                )

            # Since we got no critical exception, save last received object
            # block height to do next requests from there.
            last_height = event_data.blockNumber
            if last_height:
                with self.session_factory() as session:
                    upsert_chain_sync_status(
                        session=session,
                        chain=Chain.ETH,
                        sync_type=ChainEventType.SYNC,
                        height=last_height,
                        update_datetime=utc_now(),
                    )
                    session.commit()

    async def fetch_ethereum_sync_events(self, config: Config):
        last_stored_height = await self.get_last_height(sync_type=ChainEventType.SYNC)

        LOGGER.info("Last block is #%d" % last_stored_height)

        web3 = await run_in_executor(None, get_web3, config)
        contract = await get_contract(config, web3)
        abi = contract.events.SyncEvent._get_event_abi()

        while True:
            last_stored_height = await self.get_last_height(
                sync_type=ChainEventType.SYNC
            )
            async for jdata, context in self._request_transactions(
                config, web3, contract, abi, last_stored_height
            ):
                tx = ChainTxDb.from_sync_tx_context(tx_context=context, tx_data=jdata)
                with self.session_factory() as session:
                    await self.pending_tx_publisher.add_and_publish_pending_tx(
                        session=session, tx=tx
                    )
                    session.commit()

    async def fetch_sync_events_task(self, config: Config):
        while True:
            try:
                await self.fetch_ethereum_sync_events(config)
            except Exception:
                LOGGER.exception(
                    "An unexpected exception occurred, "
                    "relaunching Ethereum message sync in 10 seconds"
                )
            else:
                LOGGER.info("Processed all transactions, waiting 10 seconds.")
            await asyncio.sleep(10)

    async def fetcher(self, config: Config):
        message_event_task = self.indexer_reader.fetcher(
            indexer_url=config.aleph.indexer_url.value,
            # The indexer requires the address to be in the same format as the address it was
            # configured with. This appears to be lowercase for now.
            smart_contract_address=config.ethereum.sync_contract.value.lower(),
            event_type=ChainEventType.MESSAGE,
        )
        sync_event_task = self.fetch_sync_events_task(config)

        await asyncio.gather(message_event_task, sync_event_task)

    @staticmethod
    def _broadcast_content(
        config, contract, web3: Web3, account, gas_price, nonce, content
    ):
        tx = contract.functions.doEmit(content).build_transaction(
            {
                "chainId": config.ethereum.chain_id.value,
                "gasPrice": gas_price,
                "nonce": nonce,
            }
        )
        signed_tx = account.sign_transaction(tx)
        return web3.eth.send_raw_transaction(signed_tx.rawTransaction)

    async def packer(self, config: Config):
        web3 = await run_in_executor(None, get_web3, config)
        contract = await get_contract(config, web3)

        pri_key = HexBytes(config.ethereum.private_key.value)
        account = Account.from_key(pri_key)
        address = account.address

        LOGGER.info("Ethereum Connector set up with address %s" % address)
        i = 0
        gas_price = web3.eth.generate_gas_price()
        while True:
            with self.session_factory() as session:
                # Wait for sync operations to complete
                if (count_pending_txs(session=session, chain=Chain.ETH)) or (
                    count_pending_messages(session=session, chain=Chain.ETH)
                ) > 1000:
                    await asyncio.sleep(30)
                    continue
                gas_price = web3.eth.generate_gas_price()

                if i >= 100:
                    await asyncio.sleep(30)  # wait three (!!) blocks
                    gas_price = web3.eth.generate_gas_price()
                    i = 0

                if gas_price > config.ethereum.max_gas_price.value:
                    # gas price too high, wait a bit and retry.
                    await asyncio.sleep(60)
                    continue

                nonce = web3.eth.get_transaction_count(account.address)

                messages = list(
                    get_unconfirmed_messages(
                        session=session, limit=10000, chain=Chain.ETH
                    )
                )

            if messages:
                LOGGER.info("Chain sync: %d unconfirmed messages")

                # This function prepares a chain data file and makes it downloadable from the node.
                sync_event_payload = (
                    await self.chain_data_service.prepare_sync_event_payload(
                        session=session, messages=messages
                    )
                )
                # Required to apply update to the files table in get_chaindata
                session.commit()
                response = await run_in_executor(
                    None,
                    self._broadcast_content,
                    config,
                    contract,
                    web3,
                    account,
                    int(gas_price * 1.1),
                    nonce,
                    sync_event_payload.model_dump_json(),
                )
                LOGGER.info("Broadcast %r on %s" % (response, CHAIN_NAME))

            await asyncio.sleep(config.ethereum.commit_delay.value)
            i += 1
