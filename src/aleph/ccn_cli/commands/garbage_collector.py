"""
This migration checks all the files stored in local storage (=GridFS) and compares them to the list
of messages already on the node. The files that are not linked to any message are scheduled for
deletion.
"""
import asyncio
import datetime as dt
from typing import Any, Dict, FrozenSet, List, Optional
from typing import cast

import pytz
import typer
from aleph_message.models import MessageType
from configmanager import Config

import aleph.model
from aleph.ccn_cli.cli_config import CliConfig
from aleph.config import get_defaults
from aleph.model import init_db_globals
from aleph.model.filepin import PermanentPin
from aleph.model.hashes import delete_value as delete_gridfs_file
from aleph.model.messages import Message

gc_ns = typer.Typer()


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


def print_files_to_preserve(files_to_preserve: Dict[str, FrozenSet[str]]) -> None:
    typer.echo("The following files will be preserved:")
    for file_type, files in files_to_preserve.items():
        typer.echo(f"* {len(files)} {file_type}")


async def list_files_to_preserve(
    gridfs_files_dict: Dict[str, Dict],
    temporary_files_ttl: int,
) -> Dict[str, FrozenSet[str]]:
    files_to_preserve_dict = {}

    # Preserve any file that was uploaded less than an hour ago
    current_datetime = pytz.utc.localize(dt.datetime.utcnow())
    files_to_preserve_dict["temporary files"] = frozenset(
        [
            file["filename"]
            for file in gridfs_files_dict.values()
            if file["uploadDate"]
            > current_datetime - dt.timedelta(seconds=temporary_files_ttl)
        ]
    )

    # Get all the messages that potentially store data in local storage:
    # * any message with item_type in ["storage", "ipfs"]
    # * STOREs with content.item_type in ["storage", "ipfs"]
    files_to_preserve_dict["non-inline messages"] = await get_hashes(
        item_type_field="item_type",
        item_hash_field="item_hash",
    )
    files_to_preserve_dict["stores"] = await get_hashes(
        item_type_field="content.item_type",
        item_hash_field="content.item_hash",
        msg_type=MessageType.store,
    )

    # We also keep permanent pins, even if they are also stored on IPFS
    files_to_preserve_dict["file pins"] = frozenset(
        [
            pin["multihash"]
            async for pin in PermanentPin.collection.find({}, {"multihash": 1})
        ]
    )

    return files_to_preserve_dict


async def run(ctx: typer.Context, dry_run: bool):
    config = Config(schema=get_defaults())
    cli_config = cast(CliConfig, ctx.obj)
    config.yaml.load(str(cli_config.config_file_path))

    init_db_globals(config=config)
    if aleph.model.db is None:  # for mypy
        raise ValueError("DB not initialized as expected.")

    # Get a set of all the files currently in GridFS
    gridfs_files_dict = {
        file["filename"]: file
        async for file in aleph.model.db["fs.files"].find(
            projection={"_id": 0, "filename": 1, "length": 1, "uploadDate": 1},
            batch_size=1000,
        )
    }
    gridfs_files = frozenset(gridfs_files_dict.keys())

    typer.echo(f"Found {len(gridfs_files_dict)} files in local storage.")

    files_to_preserve_dict = await list_files_to_preserve(
        gridfs_files_dict=gridfs_files_dict,
        temporary_files_ttl=config.storage.temporary_files_ttl.value,
    )
    files_to_preserve = frozenset().union(*files_to_preserve_dict.values())
    files_to_delete = gridfs_files - files_to_preserve

    if cli_config.verbose:
        print_files_to_preserve(files_to_preserve_dict)

    restored_memory = sum(
        gridfs_files_dict[filename]["length"] for filename in files_to_delete
    )
    typer.echo(
        f"{len(files_to_delete)} will be deleted, totaling {restored_memory} bytes."
    )

    if dry_run:
        if cli_config.verbose:
            if files_to_delete:
                typer.echo("The following files will be deleted:")
                for file_to_delete in files_to_delete:
                    typer.echo(f"* {file_to_delete}")

    else:
        for file_to_delete in files_to_delete:
            typer.echo(f"Deleting {file_to_delete}...")
            await delete_gridfs_file(file_to_delete)

    typer.echo("Done.")


@gc_ns.command(name="run")
def run_gc(
    ctx: typer.Context,
    dry_run: bool = typer.Option(
        False, help="If set, display files to delete without deleting them."
    ),
):
    asyncio.run(run(ctx, dry_run))
