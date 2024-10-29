import datetime as dt
import json
from decimal import Decimal
from typing import List, Union

import pytest
import pytz
from aleph_message.models import (
    Chain,
    ExecutableContent,
    InstanceContent,
    ItemType,
    MessageType,
    ProgramContent,
)
from aleph_message.models.execution.program import (
    CodeContent,
    DataContent,
    FunctionRuntime,
)
from aleph_message.models.execution.volume import ImmutableVolume, ParentVolume

from aleph.db.accessors.cost import get_total_cost_for_address
from aleph.db.accessors.files import insert_message_file_pin, upsert_file_tag
from aleph.db.models import (
    AlephBalanceDb,
    MessageStatusDb,
    PendingMessageDb,
    StoredFileDb,
)
from aleph.toolkit.timestamp import timestamp_to_datetime
from aleph.types.db_session import DbSession, DbSessionFactory
from aleph.types.files import FileTag, FileType
from aleph.types.message_status import MessageStatus


def get_volume_refs(
    content: ExecutableContent,
) -> List[
    Union[ImmutableVolume, DataContent, ParentVolume, CodeContent, FunctionRuntime]
]:
    volumes: List[
        Union[ImmutableVolume, DataContent, ParentVolume, CodeContent, FunctionRuntime]
    ] = []

    for volume in content.volumes:
        if isinstance(volume, ImmutableVolume):
            volumes.append(volume)

    if isinstance(content, ProgramContent):
        volumes += [content.code, content.runtime]
        if content.data:
            volumes.append(content.data)

    elif isinstance(content, InstanceContent):
        if parent := content.rootfs.parent:
            volumes.append(parent)

    return volumes


def insert_volume_refs(session: DbSession, message: PendingMessageDb):
    """
    Insert volume references in the DB to make the program processable.
    """

    if message.item_content:
        content = InstanceContent.model_validate_json(message.item_content)
        volumes = get_volume_refs(content)

        created = pytz.utc.localize(dt.datetime(2023, 1, 1))

        for volume in volumes:
            # Note: we use the reversed ref to generate the file hash for style points,
            # but it could be set to any valid hash.
            file_hash = volume.ref[::-1]

            session.add(
                StoredFileDb(hash=file_hash, size=1024 * 1024, type=FileType.FILE)
            )
            session.flush()
            insert_message_file_pin(
                session=session,
                file_hash=volume.ref[::-1],
                owner=content.address,
                item_hash=volume.ref,
                ref=None,
                created=created,
            )
            upsert_file_tag(
                session=session,
                tag=FileTag(volume.ref),
                owner=content.address,
                file_hash=volume.ref[::-1],
                last_updated=created,
            )


@pytest.fixture
def fixture_instance_message(session_factory: DbSessionFactory) -> PendingMessageDb:
    content = {
        "address": "0x9319Ad3B7A8E0eE24f2E639c40D8eD124C5520Ba",
        "allow_amend": False,
        "variables": {
            "VM_CUSTOM_VARIABLE": "SOMETHING",
            "VM_CUSTOM_VARIABLE_2": "32",
        },
        "environment": {
            "reproducible": True,
            "internet": False,
            "aleph_api": False,
            "shared_cache": False,
        },
        "resources": {"vcpus": 1, "memory": 128, "seconds": 30},
        "requirements": {"cpu": {"architecture": "x86_64"}},
        "rootfs": {
            "parent": {
                "ref": "549ec451d9b099cad112d4aaa2c00ac40fb6729a92ff252ff22eef0b5c3cb613",
                "use_latest": True,
            },
            "persistence": "host",
            "name": "test-rootfs",
            "size_mib": 20 * 1024,
        },
        "authorized_keys": [
            "ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAAIGULT6A41Msmw2KEu0R9MvUjhuWNAsbdeZ0DOwYbt4Qt user@example",
            "ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAAIH0jqdc5dmt75QhTrWqeHDV9xN8vxbgFyOYs2fuQl7CI",
        ],
        "volumes": [
            {
                "comment": "Python libraries. Read-only since a 'ref' is specified.",
                "mount": "/opt/venv",
                "ref": "5f31b0706f59404fad3d0bff97ef89ddf24da4761608ea0646329362c662ba51",
                "use_latest": False,
            },
            {
                "comment": "Ephemeral storage, read-write but will not persist after the VM stops",
                "mount": "/var/cache",
                "ephemeral": True,
                "size_mib": 5,
            },
            {
                "comment": "Working data persisted on the VM supervisor, not available on other nodes",
                "mount": "/var/lib/sqlite",
                "name": "sqlite-data",
                "persistence": "host",
                "size_mib": 10,
            },
            {
                "comment": "Working data persisted on the Aleph network. "
                "New VMs will try to use the latest version of this volume, "
                "with no guarantee against conflicts",
                "mount": "/var/lib/statistics",
                "name": "statistics",
                "persistence": "store",
                "size_mib": 10,
            },
            {
                "comment": "Raw drive to use by a process, do not mount it",
                "name": "raw-data",
                "persistence": "host",
                "size_mib": 10,
            },
        ],
        "time": 1619017773.8950517,
    }

    pending_message = PendingMessageDb(
        item_hash="734a1287a2b7b5be060312ff5b05ad1bcf838950492e3428f2ac6437a1acad26",
        type=MessageType.instance,
        chain=Chain.ETH,
        sender="0x9319Ad3B7A8E0eE24f2E639c40D8eD124C5520Ba",
        signature=None,
        item_type=ItemType.inline,
        item_content=json.dumps(content),
        time=timestamp_to_datetime(1619017773.8950577),
        channel=None,
        reception_time=timestamp_to_datetime(1619017774),
        fetched=True,
        check_message=False,
        retries=0,
        next_attempt=dt.datetime(2023, 1, 1),
    )
    with session_factory() as session:
        session.add(pending_message)
        session.add(
            MessageStatusDb(
                item_hash=pending_message.item_hash,
                status=MessageStatus.PENDING,
                reception_time=pending_message.reception_time,
            )
        )
        session.commit()

    return pending_message


def test_get_total_cost_for_address(
    session_factory: DbSessionFactory, fixture_instance_message
):
    with session_factory() as session:
        session.add(
            AlephBalanceDb(
                address="0xB68B9D4f3771c246233823ed1D3Add451055F9Ef",
                chain=Chain.ETH,
                dapp=None,
                balance=Decimal(100_000),
                eth_height=0,
            )
        )
        insert_volume_refs(session, fixture_instance_message)
        session.commit()

        total_cost: Decimal = get_total_cost_for_address(
            session=session, address=fixture_instance_message.sender
        )
        assert total_cost == Decimal(
            0.66666666666666662965923251249478198587894439697265625
        )
