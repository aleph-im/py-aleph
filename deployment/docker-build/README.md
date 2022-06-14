# Aleph Core Channel Node (CCN) Docker (Beta)

This directory contains the `Dockerfile` to build and run the CCN in production,
as well as a Docker Compose file for use during development.

## Build the Docker image

You can build the Docker image simply using:
```shell script
./deployment/docker-build/build.sh
```

or by running the Docker build command from the root of the repository:
```shell script
docker build -t alephim/pyaleph-node -f deployment/docker/pyaleph.dockerfile .
```

## Configure the CCN

We provide a template configuration in the file `samples/docker-compose/config.yml`,
which you will want to customize for your system.

Change the Ethereum API URL to the endpoint you want the CCN to use.

To run the local dev environment, you will need to set the P2P daemon, IPFS and MongoDB hosts to `127.0.0.1`.

### Generate your node's private key

An Aleph node needs an asymmetric key pair to communicate with other nodes on the network.

You can generate this key using the following commands after building the Docker image:
```shell script
docker run --rm --user root --entrypoint "" -v $(pwd)/node-secret.key:/opt/pyaleph/node-secret.key alephim/pyaleph-node:latest pyaleph --gen-keys
```

## Start the dev environment

Run the Docker Compose file to start all the required services:

```
docker-compose -f deployment/docker-build/docker-compose.yml up -d
```

This will instantiate the services for MongoDB, IPFS and the P2P daemon.

You can now start the Core Channel Node locally using the `pyaleph` command or by running the `aleph.commands` module,
for example from PyCharm.
