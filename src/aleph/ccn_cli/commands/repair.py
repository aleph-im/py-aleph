"""
Repairs the local CCN by checking the following
"""
import asyncio
import itertools
from typing import Dict, FrozenSet, Set, Tuple
from typing import cast

import typer
from aleph_message.models import ItemHash
from configmanager import Config

import aleph.model
import aleph.services.p2p.singleton as singleton
from aleph import config as aleph_config
from aleph.ccn_cli.cli_config import CliConfig
from aleph.config import get_defaults
from aleph.exceptions import ContentCurrentlyUnavailable
from aleph.model import init_db_globals
from aleph.services.p2p import http
from aleph.storage import get_hash_content
from .toolkit.local_storage import list_expected_local_files

repair_ns = typer.Typer()


async def init_api_servers():
    peers = [peer async for peer in aleph.model.db["peers"].find({"type": "HTTP"})]
    singleton.api_servers = [peer["address"] for peer in peers]


async def list_missing_files() -> FrozenSet[str]:
    if aleph.model.db is None:  # for mypy
        raise ValueError("DB not initialized as expected.")

    # Get a set of all the files currently in GridFS
    gridfs_files = frozenset(
        [
            file["filename"]
            async for file in aleph.model.db["fs.files"].find(
                projection={"_id": 0, "filename": 1},
                batch_size=1000,
            )
        ]
    )

    typer.echo(f"Found {len(gridfs_files)} files in local storage.")

    expected_local_files_dict = await list_expected_local_files()
    expected_local_files = frozenset(
        itertools.chain.from_iterable(expected_local_files_dict.values())
    )

    missing_files = expected_local_files - gridfs_files
    return missing_files


async def fetch_and_store_file(filename: str):
    item_hash = ItemHash(filename)
    _ = await get_hash_content(
        content_hash=filename,
        engine=item_hash.item_type,
        use_network=True,
        use_ipfs=True,
        store_value=True,
        timeout=15,
    )


def process_results(
    finished_tasks: Set[asyncio.Task], task_dict: Dict[asyncio.Task, str]
) -> Tuple[Set[str], Set[str]]:
    fetched_files = set()
    failed_files = set()

    for task in finished_tasks:
        filename = task_dict.pop(task)
        exception = task.exception()

        if exception is None:
            fetched_files.add(filename)

        else:
            failed_files.add(filename)
            if isinstance(exception, ContentCurrentlyUnavailable):
                typer.echo(
                    f"WARNING: Could not fetch {filename}: currently unavailable."
                )
            else:
                typer.echo(
                    f"ERROR: Could not fetch {filename}: unexpected error: {exception}"
                )

    return fetched_files, failed_files


async def fetch_files(missing_files: FrozenSet[str], batch_size: int):
    tasks = set()
    task_dict = {}

    fetched_files = set()
    failed_files = set()

    for i, filename in enumerate(missing_files, start=1):
        typer.echo(f"Fetching {filename} ({i}/{len(missing_files)})...")
        fetch_task = asyncio.create_task(fetch_and_store_file(filename))
        tasks.add(fetch_task)
        task_dict[fetch_task] = filename

        if len(tasks) == batch_size:
            done, tasks = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
            fetched, failed = process_results(done, task_dict)
            fetched_files |= fetched
            failed_files |= failed

    # Finish
    if tasks:
        done, _ = await asyncio.wait(tasks, return_when=asyncio.ALL_COMPLETED)
        fetched, failed = process_results(done, task_dict)
        fetched_files |= fetched
        failed_files |= failed

    typer.echo(f"Successfully fetched {len(fetched_files)} files.")
    if failed_files:
        typer.echo(f"WARNING: Failed to fetch {len(failed_files)} files.")


async def fetch_missing_files():
    missing_files = await list_missing_files()
    typer.echo(f"Found {len(missing_files)} missing files.")

    await fetch_files(missing_files, 2000)


async def run(ctx: typer.Context):
    config = Config(schema=get_defaults())
    cli_config = cast(CliConfig, ctx.obj)
    config.yaml.load(str(cli_config.config_file_path))

    # Set the config global variable, otherwise the IPFS client will not be initialized properly
    aleph_config.app_config = config

    init_db_globals(config=config)
    # To be able to fetch data from the network
    await init_api_servers()
    if aleph.model.db is None:  # for mypy
        raise ValueError("DB not initialized as expected.")

    await fetch_missing_files()

    # Clean up aiohttp client sessions to avoid a warning
    for client_session in http.SESSIONS.values():
        await client_session.close()

    typer.echo("Done.")


@repair_ns.command(name="run")
def run_repair(ctx: typer.Context):
    asyncio.run(run(ctx))
