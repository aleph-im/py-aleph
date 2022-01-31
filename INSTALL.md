
# Local Installation

This document contains the instruction to install PyAleph locally without Docker.
Note that it may not be entirely up to date, since the Docker deployment is the most tested procedure.


## Dependencies

To install PyAleph, you must first install its dependencies.
Here are the requirements on Ubuntu:

`$ sudo apt install python3-dev build-essential libsnappy-dev zlib1g-dev libbz2-dev libgflags-dev liblz4-dev libgmp-dev libsecp256k1-dev`

You need to install the requirements, ideally in an empty virtualenv (I let
that part to you):

```bash
pip install git+https://github.com/aleph-im/py-libp2p.git
pip install git+https://github.com/aleph-im/nuls2-python.git
pip install git+https://github.com/aleph-im/aleph-client.git

pip install -U aioipfs

python setup.py develop
```

Then, once it's installed, you need to copy the sample-config.yaml file elsewhere,
and edit it to your liking (see configuration section).

To run PyAleph, run this command:

`$ pyaleph -c config.yaml` (where config.yaml is your configuration file you
edited earlier)


## Running tests

Install in develop with all extras:

`$ pip install -e ".[pokadot,cosmos,testing]"`

Then run the tests:

`$ pytest`



## Running services required

### IPFS

You can have a running go IPFS instance running and linked in the configuration file (TODO: write details), if you don't you need to set ipfsd.enabled to false in configuration.

PubSub should be active and configured to use GossipSub.
More info there: https://github.com/ipfs/go-ipfs/blob/master/docs/experimental-features.md#ipfs-pubsub

You can add our bootstrap node and connect to it on your ipfs node to be connected to the aleph network faster:

```
$ ipfs bootstrap add /dnsaddr/bootstrap.aleph.im/ipfs/QmPR8m8WCmYKuuxg5Qnadd4LbnTCD2L93cV2zPW5XGVHTG
$ ipfs swarm connect /dnsaddr/bootstrap.aleph.im/ipfs/QmPR8m8WCmYKuuxg5Qnadd4LbnTCD2L93cV2zPW5XGVHTG
```

### Mongodb

A local running mongodb instance is required, by default it's connected to localhost port 27017, you can change
the configuration file if needed.
