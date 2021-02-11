import logging
import sys
from typing import Dict

import sentry_sdk
from configmanager import Config

LOGGER = logging.getLogger(__name__)


def get_defaults():
    return {
        'aleph': {
            'queue_topic': 'ALEPH-QUEUE',
            'host': '0.0.0.0',
            'port': 8000,
            'reference_node_url': None,
        },
        'p2p': {
            'port': 4025,
            'http_port': 4024,
            'host': '0.0.0.0',
            'key': None,
            'reconnect_delay': 60,
            'clients': ['http'],
            'peers': [
                '/ip4/51.159.57.71/tcp/4025/p2p/QmZkurbY2G2hWay59yiTgQNaQxHSNzKZFt2jbnwJhQcKgV',
                '/ip4/95.216.100.234/tcp/4025/p2p/Qmaxufiqdyt5uVWcy1Xh2nh3Rs3382ArnSP2umjCiNG2Vs',
                '/ip4/62.210.93.220/tcp/4025/p2p/QmXdci5feFmA2pxTg8p3FCyWmSKnWYAAmr7Uys1YCTFD8U'
            ]
        },
        'storage': {
            'folder': './data/',
            'store_files': False,
            'engine': 'rocksdb'
        },
        'nuls': {
            'chain_id': 8964,
            'enabled': False,
            'packing_node': False,
            'private_key': None,
            'commit_delay': 14
        },
        'nuls2': {
            'chain_id': 1,
            'enabled': False,
            'packing_node': False,
            'api_url': 'https://apiserver.nuls.io/',
            'explorer_url': 'https://nuls.world',
            'private_key': None,
            'sync_address': None,
            'commit_delay': 14,
            'remark': 'ALEPH-SYNC',
            'token_contract': None
        },
        'ethereum': {
            'enabled': False,
            'api_url': 'http://127.0.0.1:8545',
            'packing_node': False,
            'chain_id': 1,
            'private_key': None,
            'sync_contract': None,
            'start_height': 11400000,
            'commit_delay': 35,
            'token_contract': None,
            'token_start_height': 10900000,
            'max_gas_price': 150000000000,
            'authorized_emitters': [
                '0x23eC28598DCeB2f7082Cc3a9D670592DfEd6e0dC'
            ]
        },
        'mongodb': {
            'uri': 'mongodb://127.0.0.1:27017',
            'database': 'aleph'
        },
        'mail': {
            'email_sender': 'aleph@localhost.localdomain',
            'smtp_url': 'smtp://localhost'
        },
        'ipfs': {
            'enabled': True,        
            'host': '127.0.0.1',
            'port': 5001,
            'gateway_port': 8080,
            'id': None,
            'reconnect_delay': 60,
            'peers': [
              '/dnsaddr/api1.aleph.im/ipfs/12D3KooWFVKNb19Fk9ceoRiSNjdu5rDW3FsAa9DUCG7BkA8vkFsg',
              '/ip4/51.159.57.71/tcp/4001/p2p/QmeqShhZnPZgNSAwPy3iKJcHVLSc4hBJfPv5vTNi784R75'
            ]
        },
        'sentry': {
            'dsn': None,
            'traces_sample_rate': None,
        }
    }


def load_config(args) -> Config:
    LOGGER.info("Loading configuration")
    config = Config(schema=get_defaults())

    if args.config_file is not None:
        LOGGER.debug("Loading config file '%s'", args.config_file)
        config.yaml.load(args.config_file)

    if (not config.p2p.key.value) and args.key_path:
        LOGGER.debug("Loading key pair from file")
        with open(args.key_path, 'r') as key_file:
            config.p2p.key.value = key_file.read()

    if not config.p2p.key.value:
        LOGGER.critical("Node key cannot be empty")
        sys.exit(1)

    if args.port:
        config.aleph.port.value = args.port
    if args.host:
        config.aleph.host.value = args.host

    if args.sentry_disabled:
        LOGGER.info("Sentry disabled by CLI arguments")
        config.sentry.dns.value = None

    return config


def unpack_config(config_serialized: Dict) -> Config:
    config = Config(schema=get_defaults())
    config.load_values(config_serialized)
    return config


def initialize_sentry(config: Config):
    """Initialize Sentry in the current process.

    This should be done in every process if multiple processes are created.
    """
    if config.sentry.dsn.value:
        sentry_sdk.init(
            dsn=config.sentry.dsn.value,
            traces_sample_rate=config.sentry.traces_sample_rate.value,
            ignore_errors=[KeyboardInterrupt],
        )
        LOGGER.info("Sentry enabled")
    else:
        LOGGER.info("Sentry disabled")