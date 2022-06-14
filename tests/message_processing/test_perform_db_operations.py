import pytest
from pymongo import DeleteOne
from pymongo import InsertOne

from aleph.jobs.job_utils import perform_db_operations
from aleph.model.db_bulk_operation import DbBulkOperation
from aleph.model.pending import PendingMessage, PendingTX

PENDING_TX = {
    "content": {
        "protocol": "aleph-offchain",
        "version": 1,
        "content": "test-data-pending-tx-messages",
    },
    "context": {
        "chain_name": "ETH",
        "tx_hash": "0xf49cb176c1ce4f6eb7b9721303994b05074f8fadc37b5f41ac6f78bdf4b14b6c",
        "time": 1632835747,
        "height": 13314512,
        "publisher": "0x23eC28598DCeB2f7082Cc3a9D670592DfEd6e0dC",
    },
}


@pytest.mark.asyncio
async def test_db_operations_insert_one(test_db):
    start_count = await PendingTX.count({})

    db_operations = [
        DbBulkOperation(collection=PendingTX, operation=InsertOne(PENDING_TX))
    ]
    await perform_db_operations(db_operations)

    end_count = await PendingTX.count({})
    stored_pending_tx = await PendingTX.collection.find_one(
        filter={"context.tx_hash": PENDING_TX["context"]["tx_hash"]}
    )

    assert stored_pending_tx["content"] == PENDING_TX["content"]
    assert stored_pending_tx["context"] == PENDING_TX["context"]
    assert end_count - start_count == 1


@pytest.mark.asyncio
async def test_db_operations_delete_one(test_db):
    await PendingTX.collection.insert_one(PENDING_TX)
    start_count = await PendingTX.count({})

    db_operations = [
        DbBulkOperation(
            collection=PendingTX,
            operation=DeleteOne(
                filter={"context.tx_hash": PENDING_TX["context"]["tx_hash"]}
            ),
        )
    ]
    await perform_db_operations(db_operations)

    end_count = await PendingTX.count({})
    assert end_count - start_count == -1


@pytest.mark.asyncio
async def test_db_operations_insert_and_delete(test_db, fixture_chain_data):
    """
    Test a typical case where we insert several messages and delete a pending TX.
    """

    await PendingTX.collection.insert_one(PENDING_TX)
    tx_start_count = await PendingTX.count({})
    msg_start_count = await PendingMessage.count({})

    db_operations = [
        DbBulkOperation(collection=PendingMessage, operation=InsertOne(msg))
        for msg in fixture_chain_data
    ]

    db_operations.append(
        DbBulkOperation(
            collection=PendingTX,
            operation=DeleteOne(
                filter={"context.tx_hash": PENDING_TX["context"]["tx_hash"]}
            ),
        )
    )

    await perform_db_operations(db_operations)

    tx_end_count = await PendingTX.count({})
    msg_end_count = await PendingMessage.count({})
    assert tx_end_count - tx_start_count == -1
    assert msg_end_count - msg_start_count == len(fixture_chain_data)

    # Check each message
    fixture_messages_by_hash = {msg["item_hash"]: msg for msg in fixture_chain_data}

    async for pending_msg in PendingMessage.collection.find(
        {"message.item_hash": {"$in": [msg["item_hash"] for msg in fixture_chain_data]}}
    ):
        pending_message = pending_msg["message"]
        expected_message = fixture_messages_by_hash[pending_message["item_hash"]]
        assert set(expected_message.items()).issubset(set(pending_message.items()))
