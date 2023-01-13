import asyncio
import importlib.util
import logging
from pathlib import Path
from types import ModuleType
from typing import Iterable, Optional

import typer
from configmanager import Config

from aleph.ccn_cli.cli_config import CliConfig
from aleph.config import get_defaults
from aleph.model import init_db_globals

migrations_ns = typer.Typer()

LOGGER = logging.getLogger()

SERIALIZED_KEY_FILE = "serialized-node-secret.key"


def init_config(config_file_path: Path) -> Config:
    config = Config(schema=get_defaults())
    config.yaml.load(str(config_file_path))
    return config


def import_module_from_path(path: str) -> ModuleType:
    spec = importlib.util.spec_from_file_location("migration_module", path)
    if spec is None:
        raise ImportError(f"Could not import migrations from {path}.")

    migration_module = importlib.util.module_from_spec(spec)
    if spec.loader is None:
        raise ImportError(f"Could not load module from spec for {path}.")

    spec.loader.exec_module(migration_module)
    return migration_module


def list_migration_scripts(
    migrations_dir: Path, glob_expression: Optional[str]
) -> Iterable[Path]:
    migration_scripts = set(migrations_dir.glob("*.py"))
    if glob_expression:
        migration_scripts = migration_scripts & set(
            migrations_dir.glob(glob_expression)
        )

    return migration_scripts


async def run_migrations(
    cli_config: CliConfig,
    command: str,
    filter_scripts: Optional[str],
    key_file: Optional[Path],
):
    # Initialize some basic config and global variables
    config = init_config(cli_config.config_file_path)
    init_db_globals(config=config)

    migration_scripts_dir = Path(__file__).parent / "scripts"
    migration_scripts = sorted(
        list_migration_scripts(migration_scripts_dir, filter_scripts)
    )

    for migration_script in migration_scripts:
        migration_script_path = migration_scripts_dir / migration_script
        migration_module = import_module_from_path(str(migration_script_path))

        typer.echo(f"\n> Running {command} for {migration_script}...")
        if cli_config.verbose:
            if migration_module.__doc__ is not None:
                typer.echo(migration_module.__doc__.lstrip())

        migration_func = getattr(migration_module, command)

        kwargs = {
            "config_file": cli_config.config_file_path,
            "key_dir": cli_config.key_dir,
            "key_file": key_file,
            "config": config,
        }

        if asyncio.iscoroutinefunction(migration_func):
            await migration_func(**kwargs)
        else:
            migration_func(**kwargs)

    LOGGER.info(
        f"Successfully ran %s. You can now start the Core Channel Node.", command
    )
