from pathlib import Path
from typing import Optional

import typer

from .cli_config import CliConfig
from .commands.garbage_collector import gc_ns
from .commands.keys import keys_ns
from .commands.migrations import migrations_ns
from .commands.repair import repair_ns

app = typer.Typer()


def validate_config_file_path(config: Optional[Path]) -> Optional[Path]:
    if config is not None:
        if not config.is_file():
            raise typer.BadParameter(f"'{config.absolute()}' does not exist")

    return config


def validate_key_dir(key_dir: Optional[Path]) -> Optional[Path]:
    if key_dir is not None:
        if key_dir.exists and not key_dir.is_dir():
            raise typer.BadParameter(
                f"'{key_dir.absolute()}' already exists and is not a directory"
            )

    return key_dir


@app.callback()
def main(
    ctx: typer.Context,
    config: Optional[Path] = typer.Option(
        None,
        help="Path to the configuration file of the CCN. Defaults to <cwd>/config.yml.",
        callback=validate_config_file_path,
    ),
    key_dir: Optional[Path] = typer.Option(
        None,
        help="Path to the key directory. Defaults to <cwd>/keys/.",
        callback=validate_key_dir,
    ),
    verbose: bool = typer.Option(False, help="Show more information."),
):
    """
    Aleph Core Channel Node CLI for operators.
    """

    cli_config = CliConfig(
        config_file_path=Path.cwd() / "config.yml",
        key_dir=Path.cwd() / "keys",
        verbose=False,
    )

    if config is not None:
        cli_config.config_file_path = config

    if key_dir is not None:
        cli_config.key_dir = key_dir

    cli_config.verbose = verbose

    ctx.obj = cli_config


app.add_typer(gc_ns, name="gc", help="Invoke the garbage collector.")
app.add_typer(keys_ns, name="keys", help="Operations on private keys.")
app.add_typer(migrations_ns, name="migrations", help="Run DB migrations.")
app.add_typer(
    repair_ns,
    name="repair",
    help="Performs checks on the local install and fixes issues like missing files.",
)


if __name__ == "__main__":
    app()
