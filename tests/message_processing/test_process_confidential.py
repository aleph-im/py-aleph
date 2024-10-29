import datetime as dt
import itertools
import json
from decimal import Decimal
from typing import List, Protocol, cast

import pytest
import pytz
from aleph_message.models import (
    Chain,
    ExecutableContent,
    InstanceContent,
    ItemType,
    MessageType,
)
from aleph_message.models.execution.program import ProgramContent
from aleph_message.models.execution.volume import ImmutableVolume
from more_itertools import one

from aleph.db.accessors.files import insert_message_file_pin, upsert_file_tag
from aleph.db.accessors.vms import get_instance, get_vm_version
from aleph.db.models import (
    AlephBalanceDb,
    EphemeralVolumeDb,
    ImmutableVolumeDb,
    MessageStatusDb,
    PendingMessageDb,
    PersistentVolumeDb,
    StoredFileDb,
)
from aleph.jobs.process_pending_messages import PendingMessageProcessor
from aleph.toolkit.timestamp import timestamp_to_datetime
from aleph.types.channel import Channel
from aleph.types.db_session import DbSession, DbSessionFactory
from aleph.types.files import FileTag, FileType
from aleph.types.message_status import MessageStatus


class Volume(Protocol):
    ref: str
    use_latest: bool


@pytest.fixture
def fixture_confidential_vm_message(
    session_factory: DbSessionFactory,
) -> PendingMessageDb:
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
            "hypervisor": "qemu",
            "trusted_execution": {
                "policy": 1,
                "firmware": "e258d248fda94c63753607f7c4494ee0fcbe92f1a76bfdac795c9d84101eb317",
            },
        },
        "payment": {
            "chain": "AVAX",
            "type": "superfluid",
            "receiver": "0x2319Ad3B7A8E0eE24f2E639c40D8eD124C5520Bb",
        },
        "resources": {"vcpus": 1, "memory": 128, "seconds": 30},
        "requirements": {
            "cpu": {"architecture": "x86_64"},
            "node": {
                "node_hash": "149ec451d9b099cad112d4aaa2c00ac40fb6729a92ff252ff22eef0b5c3cb6PD"
            },
        },
        "rootfs": {
            "parent": {
                "ref": "549ec451d9b099cad112d4aaa2c00ac40fb6729a92ff252ff22eef0b5c3cb613",
                "use_latest": True,
            },
            "persistence": "host",
            "size_mib": 20000,
        },
        "authorized_keys": [
            "ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAAIGULT6A41Msmw2KEu0R9MvUjhuWNAsbdeZ0DOwYbt4Qt user@example",
            "ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAAIH0jqdc5dmt75QhTrWqeHDV9xN8vxbgFyOYs2fuQl7CI",
            "ssh-rsa AAAAB3NzaC1yc2EAAAADAQABAAABgQDRsrQV1HVrcnskNhyH0may8TG9fHCPawpAi3ZgAWU6V/R7ezvZOHnZdaFeIsOpFbPbt/l67Fur3qniSXllI2kvuh2D4BBJ9PwwlB2sgWzFDF34ADsfLQf+C/vpwrWemEEE91Tpj0dWbnf219i3mZLxy/+5Sv6kUy9YJlzWnDEUbaMAZK2CXrlK90b9Ns7mT82h6h3x9dLF/oCjBAKOSxbH2X+KgsDEZT0soxyluDqKNgKflkav+pvKFyD4J9IWM4j36r80yW+OPGsHqWoWleEhprfNb60RJPwKAYCDiBiSg6wCq5P+kS15O79Ko45wPaYDUwhRoNTcrWeadvTaCZgz9X3KDHgrX6wzdKqzQwtQeabhCaIGLFRMNl1Oy/BR8VozPbIe/mY28IN84An50UYkbve7nOGJucKc4hKxZKEVPpnVpRtIoWGwBJY2fi6C6wy2pBa8UX4C4t9NLJjNQSwFBzYOrphLu3ZW9A+267nogQHGnsJ5xnQ/MXximP3BlwM= user@example",
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
        signature="0x372da8230552b8c3e65c05b31a0ff3a24666d66c575f8e11019f62579bf48c2b7fe2f0bbe907a2a5bf8050989cdaf8a59ff8a1cbcafcdef0656c54279b4aa0c71b",
        item_type=ItemType.inline,
        item_content=json.dumps(content),
        time=timestamp_to_datetime(1619017773.8950577),
        channel=Channel("Fun-dApps"),
        reception_time=timestamp_to_datetime(1619017774),
        fetched=True,
        check_message=False,
        retries=1,
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


@pytest.fixture
def user_balance(session_factory: DbSessionFactory) -> AlephBalanceDb:
    balance = AlephBalanceDb(
        address="0x9319Ad3B7A8E0eE24f2E639c40D8eD124C5520Ba",
        chain=Chain.ETH,
        balance=Decimal(22_192),
        eth_height=0,
    )

    with session_factory() as session:
        session.add(balance)
        session.commit()
    return balance


def get_volume_refs(content: ExecutableContent) -> List[ImmutableVolume]:
    volumes: List[ImmutableVolume] = []

    for volume in content.volumes:
        if isinstance(volume, ImmutableVolume):
            volumes.append(volume)

    if isinstance(content, ProgramContent):
        volumes.append(cast(ImmutableVolume, content.code))
        volumes.append(cast(ImmutableVolume, content.runtime))
        if content.data:
            volumes.append(cast(ImmutableVolume, content.data))

    elif isinstance(content, InstanceContent):
        if parent := content.rootfs.parent:
            volumes.append(cast(ImmutableVolume, parent))

    return volumes


def insert_volume_refs(session: DbSession, message: PendingMessageDb):
    item_content = message.item_content if message.item_content is not None else ""
    content = InstanceContent.model_validate_json(item_content)
    volumes = get_volume_refs(content)
    created = pytz.utc.localize(dt.datetime(2023, 1, 1))

    for volume in volumes:
        file_hash = volume.ref[::-1]
        existing_file = session.query(StoredFileDb).filter_by(hash=file_hash).first()
        if not existing_file:
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


@pytest.mark.asyncio
async def test_process_confidential_vm(
    session_factory: DbSessionFactory,
    message_processor: PendingMessageProcessor,
    fixture_confidential_vm_message: PendingMessageDb,
    user_balance: AlephBalanceDb,
):
    with session_factory() as session:
        insert_volume_refs(session, fixture_confidential_vm_message)
        session.commit()

    pipeline = message_processor.make_pipeline()
    _ = [message async for message in pipeline]

    assert fixture_confidential_vm_message.item_content
    content_dict = json.loads(fixture_confidential_vm_message.item_content)

    with session_factory() as session:
        instance = get_instance(
            session=session, item_hash=fixture_confidential_vm_message.item_hash
        )
        assert instance is not None

        assert instance.owner == fixture_confidential_vm_message.sender
        assert not instance.allow_amend

        assert instance.resources_vcpus == content_dict["resources"]["vcpus"]
        assert instance.resources_memory == content_dict["resources"]["memory"]
        assert instance.resources_seconds == content_dict["resources"]["seconds"]

        assert instance.environment_internet == content_dict["environment"]["internet"]
        assert (
            instance.environment_aleph_api == content_dict["environment"]["aleph_api"]
        )
        assert (
            instance.environment_reproducible
            == content_dict["environment"]["reproducible"]
        )
        assert (
            instance.environment_shared_cache
            == content_dict["environment"]["shared_cache"]
        )

        assert instance.variables
        assert instance.variables == content_dict["variables"]

        rootfs = instance.rootfs
        assert rootfs.parent_ref == content_dict["rootfs"]["parent"]["ref"]
        assert (
            rootfs.parent_use_latest == content_dict["rootfs"]["parent"]["use_latest"]
        )
        assert rootfs.size_mib == content_dict["rootfs"]["size_mib"]
        assert rootfs.persistence == content_dict["rootfs"]["persistence"]

        assert len(instance.volumes) == 5

        volumes_by_type = {
            type: list(volumes_iter)
            for type, volumes_iter in itertools.groupby(
                sorted(instance.volumes, key=lambda v: str(v.__class__)),
                key=lambda v: v.__class__,
            )
        }
        assert len(volumes_by_type[EphemeralVolumeDb]) == 1
        assert len(volumes_by_type[PersistentVolumeDb]) == 3
        assert len(volumes_by_type[ImmutableVolumeDb]) == 1

        ephemeral_volume: EphemeralVolumeDb = cast(
            EphemeralVolumeDb, one(volumes_by_type[EphemeralVolumeDb])
        )
        assert ephemeral_volume.mount == "/var/cache"
        assert ephemeral_volume.size_mib == 5

        instance_version = get_vm_version(
            session=session, vm_hash=fixture_confidential_vm_message.item_hash
        )
        assert instance_version

        assert (
            instance_version.current_version
            == fixture_confidential_vm_message.item_hash
        )
        assert instance_version.owner == content_dict["address"]

        # Check the trusted execution details
        trusted_execution = content_dict["environment"]["trusted_execution"]
        assert (
            instance.environment_trusted_execution_policy == trusted_execution["policy"]
        )
        assert (
            instance.environment_trusted_execution_firmware
            == trusted_execution["firmware"]
        )
        # Check that node_hash is store in db (wasn't the case before)
        assert instance.node_hash == content_dict["requirements"]["node"]["node_hash"]
