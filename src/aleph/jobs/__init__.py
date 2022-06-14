import logging
from multiprocessing import Process
from typing import Dict, List, Coroutine

from aleph.jobs.process_pending_messages import pending_messages_subprocess, retry_messages_task
from aleph.jobs.process_pending_txs import pending_txs_subprocess, handle_txs_task
from aleph.jobs.sync_unconfirmed_messages import sync_unconfirmed_messages_subprocess
from aleph.jobs.reconnect_ipfs import reconnect_ipfs_job

LOGGER = logging.getLogger("jobs")


def start_jobs(
    config,
    shared_stats: Dict,
    api_servers: List[str],
    use_processes=True,
) -> List[Coroutine]:
    LOGGER.info("starting jobs")
    tasks: List[Coroutine] = []

    if use_processes:
        config_values = config.dump_values()
        p1 = Process(
            target=pending_messages_subprocess,
            args=(
                config_values,
                shared_stats,
                api_servers,
            ),
        )
        p2 = Process(
            target=pending_txs_subprocess,
            args=(config_values, api_servers),
        )
        sync_unconfirmed_messages_process = Process(
            target=sync_unconfirmed_messages_subprocess,
            args=(config_values, api_servers),
        )
        p1.start()
        p2.start()
        sync_unconfirmed_messages_process.start()
    else:
        tasks.append(retry_messages_task(config=config, shared_stats=shared_stats))
        tasks.append(handle_txs_task(config))

    if config.ipfs.enabled.value:
        tasks.append(reconnect_ipfs_job(config))

    return tasks
