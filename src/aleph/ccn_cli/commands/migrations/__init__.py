import asyncio
from pathlib import Path
from traceback import format_exc
from typing import Optional
from typing import cast

import typer

from aleph.ccn_cli.cli_config import CliConfig
from .migration_runner import run_migrations

migrations_ns = typer.Typer()


FILTER_SCRIPTS_HELP = (
    "A filter for migration scripts. If specified, only the files "
    "matching the provided glob expression will be run."
)

KEY_FILE_HELP = (
    "Path to the private key file, if any. "
    "Only used to upgrade the key to the latest format."
)


def run_migration_command(
    cli_config: CliConfig,
    command: str,
    filter_scripts: Optional[str],
    key_file: Optional[Path],
):
    try:
        asyncio.run(
            run_migrations(
                cli_config=cli_config,
                command=command,
                filter_scripts=filter_scripts,
                key_file=key_file,
            )
        )
    except Exception as e:
        typer.echo(f"{command} failed: {e}.", err=True)
        if cli_config.verbose:
            typer.echo(format_exc())
        raise typer.Exit(code=1)


@migrations_ns.command()
def upgrade(
    ctx: typer.Context,
    filter_scripts: Optional[str] = typer.Option(
        None,
        help=FILTER_SCRIPTS_HELP,
    ),
    key_file: Optional[Path] = typer.Option(
        None,
        help=KEY_FILE_HELP,
    ),
):
    cli_config = cast(CliConfig, ctx.obj)
    run_migration_command(
        cli_config=cli_config,
        command="upgrade",
        filter_scripts=filter_scripts,
        key_file=key_file,
    )


@migrations_ns.command()
def downgrade(
    ctx: typer.Context,
    filter_scripts: Optional[str] = typer.Option(
        None,
        help=FILTER_SCRIPTS_HELP,
    ),
    key_file: Optional[Path] = typer.Option(
        None,
        help=KEY_FILE_HELP,
    ),
):
    cli_config = cast(CliConfig, ctx.obj)
    run_migration_command(
        cli_config=cli_config,
        command="downgrade",
        filter_scripts=filter_scripts,
        key_file=key_file,
    )
