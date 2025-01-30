import logging

from configmanager import Config


def get_defaults():
    return {
        "logging": {
            # Logging level.
            "level": logging.WARNING,
            # Max log file size for each process.
            "max_log_file_size": 50_000_000,  # 50MB
        },
        "aleph": {
            # Name of the P2P pubsub topic used to distribute pending messages across the aleph.im network.
            "queue_topic": "ALEPH-TEST",
            # URL of another Core Channel Node to compare the synchronization status.
            "reference_node_url": None,
            # URL of the aleph.im cross-chain indexer.
            "indexer_url": "https://multichain.api.aleph.cloud",
            "balances": {
                # Addresses allowed to publish balance updates.
                "addresses": [
                    "0xB34f25f2c935bCA437C061547eA12851d719dEFb",
                    "0xa1B3bb7d2332383D96b7796B908fB7f7F3c2Be10",
                ],
                # POST message type for balance updates.
                "post_type": "balances-update",
            },
            "jobs": {
                "pending_messages": {
                    # Maximum number of retries for a message.
                    "max_retries": 10,
                    # Maximum number of messages/files fetched at the same time.
                    "max_concurrency": 10,
                },
                "pending_txs": {
                    # Maximum number of chain/sync events processed at the same time.
                    "max_concurrency": 20,
                },
            },
            "cache": {
                "ttl": {
                    "total_aleph_messages": 120,
                    "eth_height": 600,
                    "metrics": 10,
                },
            },
        },
        "p2p": {
            # Port used for HTTP communication between nodes.
            "http_port": 4024,
            # Port used for P2P communication between nodes.
            "port": 4025,
            # Port used to communicate with the local P2P service.
            "control_port": 4030,
            # Hostname of the P2P service.
            "daemon_host": "p2p-service",
            # Hostname of the RabbitMQ service, as viewed by the Core Channel Node code.
            "mq_host": "rabbitmq",
            # Delay between connection attempts to other nodes on the network.
            "reconnect_delay": 60,
            # P2P pubsub topic used for liveness checks.
            "alive_topic": "ALIVE",
            # Enabled P2P clients (HTTP and/or P2P).
            "clients": ["http"],
            # Bootstrap peers for the P2P service.
            "peers": [
                "/dns/api1.aleph.im/tcp/4025/p2p/Qmaxufiqdyt5uVWcy1Xh2nh3Rs3382ArnSP2umjCiNG2Vs",
                "/dns/api2.aleph.im/tcp/4025/p2p/QmZkurbY2G2hWay59yiTgQNaQxHSNzKZFt2jbnwJhQcKgV",
            ],
            # Topics to listen to by default on the P2P service.
            "topics": ["ALIVE", "ALEPH-TEST"],
        },
        "storage": {
            # Folder used to store files on the node.
            "folder": "/var/lib/pyaleph",
            # Whether to store files on the node.
            "store_files": True,
            # Interval between garbage collector runs, expressed in hours.
            "garbage_collector_period": 24,
            # Grapce period for files, expressed in hours.
            "grace_period": 24,
        },
        "nuls2": {
            # NULS2 chain ID.
            "chain_id": 1,
            # Whether to fetch transactions from NULS2.
            "enabled": False,
            # Whether to enable publishing of messages on NULS2 from this node.
            "packing_node": False,
            # NULS2 RPC node URL.
            "api_url": "https://apiserver.nuls.io/",
            # NULS2 explorer URL.
            "explorer_url": "https://nuls.world",
            # NULS2 private key. Only required if packing_node is set to true.
            "private_key": None,
            # Address of the aleph.im smart contract on NULS2.
            "sync_address": None,
            # Delay in seconds between publication attempts.
            "commit_delay": 14,
            # Remark filter for transactions.
            "remark": "ALEPH-SYNC",
        },
        "bsc": {
            # Whether to fetch transactions from the BSC chain.
            "enabled": True,
            # Address of the aleph.im smart contract on the BSC chain.
            "sync_contract": "0xdF270752C8C71D08acbae4372687DA65AECe2D5D",
        },
        "ethereum": {
            # Whether to fetch transactions from Ethereum.
            "enabled": False,
            # Ethereum RPC node URL.
            "api_url": "http://127.0.0.1:8545",
            # Whether to enable publishing of messages on Ethereum from this node.
            "packing_node": False,
            # Ethereum chain ID.
            "chain_id": 1,
            # Ethereum private key. Only required if packing_node is set to true.
            "private_key": None,
            # Address of the aleph.im smart contract on Ethereum.
            "sync_contract": None,
            # Ethereum block height to start from when fetching sync events.
            "start_height": 11400000,
            # Delay in seconds between publication attempts.
            "commit_delay": 35,
            # Maximum gas price accepted when publishing to Ethereum.
            "max_gas_price": 150000000000,
            # Authorized publishers for sync events.
            "authorized_emitters": ["0x23eC28598DCeB2f7082Cc3a9D670592DfEd6e0dC"],
            # Delay in seconds between archive checks.
            "archive_delay": 30,
            # Delay in seconds between blockchain message checks.
            "message_delay": 30,
            # http client timeout, default 60s
            "client_timeout": 60,
        },
        "tezos": {
            # Whether to fetch transactions from Tezos.
            "enabled": True,
            # URL of the aleph.im indexer for Tezos.
            "indexer_url": "https://tezos-mainnet.api.aleph.cloud",
            # Address of the aleph.im smart contract on Tezos.
            "sync_contract": "KT1FfEoaNvooDfYrP61Ykct6L8z7w7e2pgnT",
        },
        "postgres": {
            # Hostname of the local PostgreSQL database.
            "host": "postgres",
            # Port of the local PostgreSQL database.
            "port": 5432,
            # Name of the database.
            "database": "aleph",
            # Username for the local PostgreSQL database.
            "user": "aleph",
            # Password for the local PostgreSQL database.
            "password": "decentralize-everything",
            # Maximum number of concurrent connections to the local PostgreSQL database.
            "pool_size": 50,
        },
        "ipfs": {
            # Whether to enable storage and communication on IPFS.
            "enabled": True,
            # Hostname of the IPFS service.
            "host": "ipfs",
            # Port of the IPFS service.
            "port": 5001,
            # IPFS pubsub topic used for liveness checks.
            "alive_topic": "ALEPH_ALIVE",
            # Delay between connection attempts to other nodes on the network.
            "reconnect_delay": 60,
            # Bootstrap peers for IPFS.
            "peers": [
                "/dnsaddr/api1.aleph.im/ipfs/12D3KooWNgogVS6o8fVsPdzh2FJpCdJJLVSgJT38XGE1BJoCerHx",
                "/ip4/51.159.57.71/tcp/4001/p2p/12D3KooWBH3JVSBwHLNzxv7EzniBP3tDmjJaoa3EJBF9wyhZtHt2",
                "/ip4/62.210.93.220/tcp/4001/p2p/12D3KooWLcmvqojHzUnR7rr8YhFKGDD8z7fmsPyBfAm2rT3sFGAF",
            ],
        },
        "rabbitmq": {
            # Hostname of the RabbitMQ service.
            "host": "rabbitmq",
            # Port of the RabbitMQ service.
            "port": 5672,
            # Username of the RabbitMQ service.
            "username": "aleph-p2p",
            # Password of the RabbitMQ service.
            "password": "change-me!",
            # Name of the exchange used to publish messages from the node to the P2P network.
            "pub_exchange": "p2p-publish",
            # Name of the exchange used to publish messages from the P2P network to the node.
            "sub_exchange": "p2p-subscribe",
            # Name of the exchange used to publish processed messages (output of the message processor).
            "message_exchange": "aleph-messages",
            # Name of the RabbitMQ exchange used for pending messages (input of the message processor).
            "pending_message_exchange": "aleph-pending-messages",
            # Name of the RabbitMQ exchange used for sync/message events (input of the TX processor).
            "pending_tx_exchange": "aleph-pending-txs",
        },
        "redis": {
            # Hostname of the Redis service.
            "host": "redis",
            # Port of the Redis service.
            "port": 6379,
        },
        "sentry": {
            # Sentry DSN.
            "dsn": None,
            # Sentry trace sample rate.
            "traces_sample_rate": None,
        },
    }


app_config = Config(schema=get_defaults())


def get_config() -> Config:
    return app_config
