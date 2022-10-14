"""
This migration moves the private key file to the key directory and generates two new keys:
a public key, and a serialized version of the private key for use by the P2P service.
"""

from pathlib import Path
from typing import Optional

import typer
import yaml
from Crypto.PublicKey import RSA
from p2pclient.libp2p_stubs.crypto.rsa import KeyPair, RSAPrivateKey

from aleph.exceptions import InvalidKeyDirException
from aleph.services.keys import save_keys

SERIALIZED_KEY_FILE = "serialized-node-secret.key"


def populate_key_dir(private_key_str: str, output_dir: Path) -> None:
    private_key = RSAPrivateKey(RSA.import_key(private_key_str))
    key_pair = KeyPair(private_key=private_key, public_key=private_key.get_public_key())
    save_keys(key_pair, str(output_dir))


def get_key_from_config(config_file: Path) -> Optional[str]:
    """
    In previous versions of the CCN, it was possible to set the key value directly
    in the config file. This function tries to find it in the config or returns None.

    :param config_file: Path to the CCN configuration file.
    :return: The private key used to identify the node on the P2P network, or None
             if the key is not provided in the config file.
    """
    with open(config_file) as f:
        config = yaml.safe_load(f)

    try:
        return config["p2p"]["key"]
    except KeyError:
        return None


def upgrade(**kwargs):
    config_file = Path(kwargs["config_file"])
    key_dir = Path(kwargs["key_dir"])
    key_file = Path(kwargs["key_file"]) if kwargs["key_file"] else None

    # Nothing to do if the serialized key file already exists
    serialized_key_file = key_dir / SERIALIZED_KEY_FILE
    if serialized_key_file.is_file():
        typer.echo(
            f"Serialized key file {serialized_key_file} already exists, nothing to do"
        )
        return

    if not key_dir.is_dir():
        raise InvalidKeyDirException(
            f"The specified key directory ('{key_dir}') is not a directory."
        )

    # We prioritize the key provided as a file. If a key is also provided in
    # the config file, it will be ignored.
    if key_file is not None:
        with open(key_file) as f:
            private_key = f.read()
    else:
        typer.echo("Key file not specified. Looking in the config file...")
        private_key = get_key_from_config(config_file)

    if private_key is None:
        raise ValueError("Key file path not specified and key not provided in config.")

    typer.echo(
        f"Migrating the private key in {key_dir} and using it to generate a public key "
        "and a serialized private key...",
    )
    populate_key_dir(private_key, key_dir)
    typer.echo(f"Migrated the private/public keys in {key_dir}.")


def downgrade(**kwargs):
    # Nothing to do, the key file is still present in the key directory
    pass
