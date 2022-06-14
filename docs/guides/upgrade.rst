************************
Upgrading a PyAleph node
************************

This tutorial explains how to upgrade your PyAleph node to a newer version.

Update the configuration and keys
=================================

Version 0.2.0 requires a new way to store the private key on disk.
We provide an automated tool to keep your configuration file and keys up to date.
You must stop the node before running the configuration updater.

Create the keys directory
-------------------------

The updater expects that you already created a keys/ directory to hold your private and public keys.
If you do not already have one, you need to create the directory, copy the key of your node in it
and adjust the ownership of the folder:

.. parsed-literal::

    mkdir keys
    cp node-secret.key keys/
    docker run --rm --user root --entrypoint "" -v $(pwd)/keys:/opt/pyaleph/keys alephim/pyaleph-node:|pyaleph_version| chown -R aleph:aleph /opt/pyaleph/keys

Download the latest image
-------------------------

Upgrade the docker-compose file:

.. parsed-literal::

    mv docker-compose.yml docker-compose-old.yml
    wget "https://raw.githubusercontent.com/aleph-im/pyaleph/|pyaleph_version|/deployment/samples/docker-compose/docker-compose.yml"

.. code-block:: bash

    docker-compose [-f <docker-compose-file>] pull

Copy your customizations to the new docker-compose if relevant:

.. parsed-literal::

    diff --color --side-by-side docker-compose-old.yml docker-compose.yml

Upgrade your node
-----------------

.. code-block:: bash

    docker-compose [-f <docker-compose-file>] down

    docker-compose [-f <docker-compose-file>] \
            run \
            --entrypoint /opt/pyaleph/migrations/config_updater.py \
            pyaleph \
            --key-dir /opt/pyaleph/keys \
            --key-file /opt/pyaleph/keys/node-secret.key \
            --config /opt/pyaleph/config.yml \
            upgrade

    docker-compose [-f <docker-compose-file>] down

Check that your node's secret key has been migrated

.. code-block:: bash

    ls ./keys

This should output:

    node-pub.key  node-secret.key  serialized-node-secret.key

Finally, restart your node:

.. code-block:: bash

    docker-compose [-f <docker-compose-file>] up -d
