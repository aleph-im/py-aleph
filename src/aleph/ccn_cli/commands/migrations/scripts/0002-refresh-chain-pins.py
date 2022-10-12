"""
This migration resets the chain height in order to ensure that a PermanentPin is present for every chaindata
committed on a chain. It has to be run after the introduction of PermanentPin in version 0.2.0.
"""

import typer
from configmanager import Config

from aleph.model.chains import Chain
from aleph.model.filepin import PermanentPin


async def upgrade(config: Config, **kwargs):
    # We measure over 5000 permanent pins on new nodes that did process all chaindata.
    # We therefore use this value to estimate if a node did process all chaindata already or not.
    expected_permanent_pins = 5000

    if (await PermanentPin.count(filter={})) < expected_permanent_pins:
        typer.echo("PermanentPin documents missing, fetching chaindata again")
        start_height = config.ethereum.start_height.value
        await Chain.set_last_height("ETH", start_height)
    else:
        typer.echo(
            "PermanentPin documents already present, no need to re-fetch chaindata"
        )


def downgrade(**kwargs):
    # Nothing to do, processing the chain data multiple times only adds some load on the node.
    pass
