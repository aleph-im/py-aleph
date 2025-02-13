import datetime as dt
import itertools
import json
from decimal import Decimal
from typing import List, Union

import pytest
import pytz
from aleph_message.models import (
    Chain,
    ExecutableContent,
    ForgetContent,
    InstanceContent,
    ItemHash,
    ItemType,
    MessageType,
    PaymentType,
)
from aleph_message.models.execution.program import (
    CodeContent,
    DataContent,
    FunctionRuntime,
    ProgramContent,
)
from aleph_message.models.execution.volume import ImmutableVolume, ParentVolume
from more_itertools import one

from aleph.db.accessors.files import insert_message_file_pin, upsert_file_tag
from aleph.db.accessors.messages import get_message_status, get_rejected_message
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
from aleph.services.cost import (
    _get_additional_storage_price,
    _get_product_price,
    get_total_and_detailed_costs,
    get_total_and_detailed_costs_from_db,
)
from aleph.toolkit.timestamp import timestamp_to_datetime
from aleph.types.db_session import DbSession, DbSessionFactory
from aleph.types.files import FileTag, FileType
from aleph.types.message_status import ErrorCode, MessageStatus


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
        "resources": {"vcpus": 1, "memory": 2048, "seconds": 30},
        "requirements": {"cpu": {"architecture": "x86_64"}},
        "rootfs": {
            "parent": {
                "ref": "549ec451d9b099cad112d4aaa2c00ac40fb6729a92ff252ff22eef0b5c3cb613",
                "use_latest": True,
            },
            "persistence": "host",
            "name": "test-rootfs",
            "size_mib": 20480,
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
                "mount": "/var/raw",
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


@pytest.fixture
def fixture_instance_message_payg(
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
        },
        "payment": {
            "chain": "AVAX",
            "receiver": "0xA07B1214bAe0D5ccAA25449C3149c0aC83658874",
            "type": "superfluid",
        },
        "resources": {"vcpus": 1, "memory": 2048, "seconds": 30},
        "requirements": {"cpu": {"architecture": "x86_64"}},
        "rootfs": {
            "parent": {
                "ref": "549ec451d9b099cad112d4aaa2c00ac40fb6729a92ff252ff22eef0b5c3cb613",
                "use_latest": True,
            },
            "persistence": "host",
            "name": "test-rootfs",
            "size_mib": 20480,
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
                "mount": "/var/raw",
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


@pytest.fixture
def fixture_forget_instance_message(
    fixture_instance_message: PendingMessageDb,
    user_balance: AlephBalanceDb,
) -> PendingMessageDb:
    content = ForgetContent(
        address=fixture_instance_message.sender,
        time=(fixture_instance_message.time + dt.timedelta(seconds=1)).timestamp(),
        hashes=[ItemHash(fixture_instance_message.item_hash)],
        reason="Bye Felicia",
    )

    pending_message = PendingMessageDb(
        item_hash="8a1497002b2fd19b6036f1ef9a652ad47f1700b3f0d380761dbd347be9178702",
        type=MessageType.forget,
        chain=Chain.ETH,
        sender=fixture_instance_message.sender,
        signature=None,
        item_type=ItemType.inline,
        item_content=content.json(),
        time=fixture_instance_message.time + dt.timedelta(seconds=1),
        channel=None,
        reception_time=fixture_instance_message.reception_time
        + dt.timedelta(seconds=1),
        fetched=True,
        check_message=False,
        retries=0,
        next_attempt=dt.datetime(2023, 1, 1),
    )
    return pending_message


def get_volume_refs(
    content: ExecutableContent,
) -> List[
    Union[CodeContent, DataContent, FunctionRuntime, ImmutableVolume, ParentVolume]
]:
    volumes: List[
        Union[CodeContent, DataContent, FunctionRuntime, ImmutableVolume, ParentVolume]
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

    assert message.item_content
    content = InstanceContent.parse_raw(message.item_content)
    volumes = get_volume_refs(content)

    created = pytz.utc.localize(dt.datetime(2023, 1, 1))

    for volume in volumes:
        # Note: we use the reversed ref to generate the file hash for style points,
        # but it could be set to any valid hash.
        file_hash = volume.ref[::-1]

        session.add(StoredFileDb(hash=file_hash, size=1024 * 1024, type=FileType.FILE))
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
async def test_process_instance(
    session_factory: DbSessionFactory,
    message_processor: PendingMessageProcessor,
    fixture_instance_message: PendingMessageDb,
    user_balance: AlephBalanceDb,
    fixture_product_prices_aggregate_in_db,
):
    with session_factory() as session:
        insert_volume_refs(session, fixture_instance_message)
        session.commit()

    pipeline = message_processor.make_pipeline()
    # Exhaust the iterator
    _ = [message async for message in pipeline]

    assert fixture_instance_message.item_content
    content_dict = json.loads(fixture_instance_message.item_content)

    with session_factory() as session:
        instance = get_instance(
            session=session, item_hash=fixture_instance_message.item_hash
        )
        assert instance is not None

        assert instance.owner == fixture_instance_message.sender
        assert not instance.allow_amend
        assert instance.replaces is None

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
        assert (
            rootfs.parent_use_latest == content_dict["rootfs"]["parent"]["use_latest"]
        )
        assert rootfs.size_mib == content_dict["rootfs"]["size_mib"]
        assert rootfs.persistence == content_dict["rootfs"]["persistence"]
        assert len(instance.volumes) == 5
        assert instance.node_hash is None

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

        ephemeral_volume: EphemeralVolumeDb = one(volumes_by_type[EphemeralVolumeDb])
        assert ephemeral_volume.mount == "/var/cache"
        assert ephemeral_volume.size_mib == 5

        instance_version = get_vm_version(
            session=session, vm_hash=fixture_instance_message.item_hash
        )
        assert instance_version

        assert instance_version.current_version == fixture_instance_message.item_hash
        assert instance_version.owner == content_dict["address"]


@pytest.mark.asyncio
async def test_process_instance_missing_volumes(
    session_factory: DbSessionFactory,
    message_processor: PendingMessageProcessor,
    fixture_instance_message: PendingMessageDb,
    user_balance: AlephBalanceDb,
):
    """
    Check that an instance message with volumes not references in file_tags/file_pins
    is rejected.
    """

    vm_hash = fixture_instance_message.item_hash
    pipeline = message_processor.make_pipeline()
    # Exhaust the iterator
    _ = [message async for message in pipeline]

    with session_factory() as session:
        instance = get_instance(session=session, item_hash=vm_hash)
        assert instance is None

        message_status = get_message_status(
            session=session, item_hash=ItemHash(vm_hash)
        )
        assert message_status is not None
        assert message_status.status == MessageStatus.REJECTED

        rejected_message = get_rejected_message(
            session=session, item_hash=ItemHash(vm_hash)
        )
        assert rejected_message is not None
        assert rejected_message.error_code == ErrorCode.VM_VOLUME_NOT_FOUND

        if fixture_instance_message.item_content:
            content = InstanceContent.parse_raw(fixture_instance_message.item_content)
            volume_refs = set(volume.ref for volume in get_volume_refs(content))
            assert isinstance(rejected_message.details, dict)
            assert set(rejected_message.details["errors"]) == volume_refs
            assert rejected_message.traceback is None


@pytest.mark.asyncio
async def test_forget_instance_message(
    session_factory: DbSessionFactory,
    message_processor: PendingMessageProcessor,
    fixture_instance_message: PendingMessageDb,
    user_balance: AlephBalanceDb,
    fixture_forget_instance_message: PendingMessageDb,
    fixture_product_prices_aggregate_in_db,
):
    vm_hash = fixture_instance_message.item_hash

    # Process the instance message
    with session_factory() as session:
        insert_volume_refs(session, fixture_instance_message)
        session.commit()

    pipeline = message_processor.make_pipeline()
    # Exhaust the iterator
    _ = [message async for message in pipeline]

    # Sanity check
    with session_factory() as session:
        instance = get_instance(session=session, item_hash=vm_hash)
        assert instance is not None

        # Insert the FORGET message and process it
        session.add(fixture_forget_instance_message)
        session.commit()

    pipeline = message_processor.make_pipeline()
    # Exhaust the iterator
    _ = [message async for message in pipeline]

    with session_factory() as session:
        instance = get_instance(session=session, item_hash=vm_hash)
        assert instance is None, "The instance is still present despite being forgotten"

        instance_version = get_vm_version(session=session, vm_hash=vm_hash)
        assert instance_version is None


@pytest.mark.asyncio
async def test_process_instance_balance(
    session_factory: DbSessionFactory,
    message_processor: PendingMessageProcessor,
    fixture_instance_message: PendingMessageDb,
):
    with session_factory() as session:
        insert_volume_refs(session, fixture_instance_message)
        session.commit()

    pipeline = message_processor.make_pipeline()
    # Exhaust the iterator
    _ = [message async for message in pipeline]

    with session_factory() as session:
        rejected_message = get_rejected_message(
            session=session, item_hash=fixture_instance_message.item_hash
        )
        assert rejected_message is not None


@pytest.mark.asyncio
async def test_get_additional_storage_price(
    session_factory: DbSessionFactory,
    fixture_instance_message: PendingMessageDb,
    fixture_product_prices_aggregate_in_db,
):
    with session_factory() as session:
        insert_volume_refs(session, fixture_instance_message)
        session.commit()

    if fixture_instance_message.item_content:
        content = InstanceContent.parse_raw(fixture_instance_message.item_content)
        with session_factory() as session:
            pricing = _get_product_price(session, content)

            additional_price = _get_additional_storage_price(
                content=content,
                pricing=pricing,
                session=session,
                item_hash=fixture_instance_message.item_hash,
                payment_type=PaymentType.hold,
            )

            cost = sum(c.cost_hold for c in additional_price)

            assert cost == Decimal("1.8")


@pytest.mark.asyncio
async def test_get_total_and_detailed_costs_from_db(
    session_factory: DbSessionFactory,
    message_processor: PendingMessageProcessor,
    fixture_instance_message: PendingMessageDb,
    fixture_product_prices_aggregate_in_db,
):
    with session_factory() as session:
        insert_volume_refs(session, fixture_instance_message)
        session.commit()

    pipeline = message_processor.make_pipeline()
    # Exhaust the iterator
    _ = [message async for message in pipeline]

    if fixture_instance_message.item_content:
        content = InstanceContent.parse_raw(fixture_instance_message.item_content)
        with session_factory() as session:
            cost, _ = get_total_and_detailed_costs(
                session=session,
                content=content,
                item_hash=fixture_instance_message.item_hash,
            )

            assert cost == Decimal("1001.8")


@pytest.mark.asyncio
async def test_compare_account_cost_with_cost_function_hold(
    session_factory: DbSessionFactory,
    message_processor: PendingMessageProcessor,
    fixture_instance_message: PendingMessageDb,
    user_balance: AlephBalanceDb,
    fixture_product_prices_aggregate_in_db,
):
    with session_factory() as session:
        insert_volume_refs(session, fixture_instance_message)
        session.commit()

    pipeline = message_processor.make_pipeline()
    # Exhaust the iterator
    _ = [message async for message in pipeline]

    assert fixture_instance_message.item_content
    content = InstanceContent.parse_raw(fixture_instance_message.item_content)
    with session_factory() as session:
        db_cost, _ = get_total_and_detailed_costs_from_db(
            session=session,
            content=content,
            item_hash=fixture_instance_message.item_hash,
        )

        cost, _ = get_total_and_detailed_costs(
            session=session,
            content=content,
            item_hash=fixture_instance_message.item_hash,
        )

    assert db_cost == cost


@pytest.mark.asyncio
async def test_compare_account_cost_with_cost_payg_funct(
    session_factory: DbSessionFactory,
    message_processor: PendingMessageProcessor,
    fixture_instance_message_payg: PendingMessageDb,
    user_balance: AlephBalanceDb,
):
    with session_factory() as session:
        insert_volume_refs(session, fixture_instance_message_payg)
        session.commit()

    pipeline = message_processor.make_pipeline()
    # Exhaust the iterator
    _ = [message async for message in pipeline]

    assert fixture_instance_message_payg.item_content

    content = InstanceContent.parse_raw(
        fixture_instance_message_payg.item_content
    )  # Parse again

    with session_factory() as session:
        assert content.payment.type == PaymentType.superfluid
        cost, details = get_total_and_detailed_costs(
            session=session,
            content=content,
            item_hash=fixture_instance_message_payg.item_hash,
        )

        db_cost, details = get_total_and_detailed_costs_from_db(
            session=session,
            content=content,
            item_hash=fixture_instance_message_payg.item_hash,
        )

    assert cost == 0
    assert cost == db_cost


@pytest.fixture
def fixture_instance_message_only_rootfs(
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
        },
        "resources": {"vcpus": 1, "memory": 2048, "seconds": 30},
        "requirements": {"cpu": {"architecture": "x86_64"}},
        "rootfs": {
            "parent": {
                "ref": "549ec451d9b099cad112d4aaa2c00ac40fb6729a92ff252ff22eef0b5c3cb613",
                "use_latest": True,
            },
            "persistence": "host",
            "name": "test-rootfs",
            "size_mib": 20480,
        },
        "authorized_keys": [
            "ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAAIGULT6A41Msmw2KEu0R9MvUjhuWNAsbdeZ0DOwYbt4Qt user@example",
            "ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAAIH0jqdc5dmt75QhTrWqeHDV9xN8vxbgFyOYs2fuQl7CI",
        ],
        "volumes": [],
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


@pytest.mark.asyncio
async def test_compare_account_cost_with_cost_function_without_volume(
    session_factory: DbSessionFactory,
    message_processor: PendingMessageProcessor,
    fixture_instance_message_only_rootfs: PendingMessageDb,
    user_balance: AlephBalanceDb,
    fixture_product_prices_aggregate_in_db,
):
    with session_factory() as session:
        insert_volume_refs(session, fixture_instance_message_only_rootfs)
        session.commit()

    pipeline = message_processor.make_pipeline()
    # Exhaust the iterator
    _ = [message async for message in pipeline]

    assert fixture_instance_message_only_rootfs.item_content
    content = InstanceContent.parse_raw(
        fixture_instance_message_only_rootfs.item_content
    )
    with session_factory() as session:
        cost, details = get_total_and_detailed_costs(
            session=session, content=content, item_hash="abab"
        )

        db_cost, details = get_total_and_detailed_costs_from_db(
            session=session,
            content=content,
            item_hash=fixture_instance_message_only_rootfs.item_hash,
        )
    assert db_cost == cost
