import typer

from aleph.ccn_cli.cli_config import CliConfig
from typing import cast
from aleph.ccn_cli.services.keys import generate_keypair, save_keys

keys_ns = typer.Typer()


@keys_ns.command()
def generate(ctx: typer.Context):
    """
    Generates a new set of private/public keys for the Core Channel Node.
    The keys will be created in the key directory. You can modify the destination
    by using the --key-dir option.
    """
    cli_config = cast(CliConfig, ctx.obj)
    print(cli_config)

    typer.echo(f"Generating a key pair in {cli_config.key_dir.absolute()}...")
    key_pair = generate_keypair()
    save_keys(key_pair, str(cli_config.key_dir))
    typer.echo("Done.")


@keys_ns.command()
def show(ctx: typer.Context):
    """
    Prints the private key of the node.
    """
    cli_config = cast(CliConfig, ctx.obj)

    key_path = cli_config.key_dir / "node-secret.key"
    if not key_path.exists():
        typer.echo(
            f"'{key_path.absolute()}' does not exist. Did you run 'keys generate'?",
            err=True,
        )
        raise typer.Exit(code=1)

    if not key_path.is_file():
        typer.echo(f"'{key_path}' is not a file.", err=True)
        raise typer.Exit(code=1)

    with key_path.open() as f:
        typer.echo(f.read())
