from pymongo import ASCENDING, DESCENDING, IndexModel

from aleph.model.base import BaseClass


class UnconfirmedMessage(BaseClass):
    """
    Synchronization messages sent by other nodes. Each document contains the hashes of
    unconfirmed messages for one peer on the network.

    Refer to the unconfirmed messages synchronization job for more details.
    """

    COLLECTION = "unconfirmed_messages"
    INDEXES = [IndexModel("peer_id", unique=True)]
