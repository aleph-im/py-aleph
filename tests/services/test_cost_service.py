from decimal import Decimal
from unittest.mock import Mock

import pytest
from aleph_message.models import ExecutableContent, InstanceContent

from aleph.services.cost import (
    _get_product_price,
    compute_cost,
    compute_flow_cost,
    get_additional_storage_price,
)
from aleph.toolkit.constants import HOUR
from aleph.types.db_session import DbSessionFactory


class StoredFileDb:
    pass


@pytest.fixture
def fixture_hold_instance_message() -> ExecutableContent:
    content = {
        "time": 1701099523.849,
        "rootfs": {
            "parent": {
                "ref": "6e30de68c6cedfa6b45240c2b51e52495ac6fb1bd4b36457b3d5ca307594d595",
                "use_latest": True,
            },
            "size_mib": 20480,
            "persistence": "host",
        },
        "address": "0xA07B1214bAe0D5ccAA25449C3149c0aC83658874",
        "volumes": [],
        "metadata": {"name": "Test Debian 12"},
        "resources": {"vcpus": 1, "memory": 2048, "seconds": 30},
        "allow_amend": False,
        "environment": {
            "internet": True,
            "aleph_api": True,
            "reproducible": False,
            "shared_cache": False,
        },
        "authorized_keys": [
            "ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAAIHlGJRaIv/EzNT0eNqNB5DiGEbii28Fb2zCjuO/bMu7y nesitor@gmail.com"
        ],
    }

    return InstanceContent.parse_obj(content)


@pytest.fixture
def fixture_hold_instance_message_complete() -> ExecutableContent:
    content = {
        "time": 1701099523.849,
        "rootfs": {
            "parent": {
                "ref": "6e30de68c6cedfa6b45240c2b51e52495ac6fb1bd4b36457b3d5ca307594d595",
                "use_latest": True,
            },
            "size_mib": 20480,
            "persistence": "host",
        },
        "address": "0xA07B1214bAe0D5ccAA25449C3149c0aC83658874",
        "volumes": [
            {
                "comment": "Ephemeral storage, read-write but will not persist after the VM stops",
                "mount": "/var/cache",
                "ephemeral": True,
                "size_mib": 50,
            },
            {
                "comment": "Working data persisted on the VM supervisor, not available on other nodes",
                "mount": "/var/lib/sqlite",
                "name": "sqlite-data",
                "persistence": "host",
                "size_mib": 100,
            },
            {
                "comment": "Working data persisted on the Aleph network. "
                "New VMs will try to use the latest version of this volume, "
                "with no guarantee against conflicts",
                "mount": "/var/lib/statistics",
                "name": "statistics",
                "persistence": "store",
                "size_mib": 100,
            },
            {
                "comment": "Raw drive to use by a process, do not mount it",
                "name": "raw-data",
                "persistence": "host",
                "size_mib": 100,
            },
        ],
        "metadata": {"name": "Test Debian 12"},
        "resources": {"vcpus": 1, "memory": 2048, "seconds": 30},
        "allow_amend": False,
        "environment": {
            "internet": True,
            "aleph_api": True,
            "reproducible": False,
            "shared_cache": False,
        },
        "authorized_keys": [
            "ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAAIHlGJRaIv/EzNT0eNqNB5DiGEbii28Fb2zCjuO/bMu7y nesitor@gmail.com"
        ],
    }

    return InstanceContent.parse_obj(content)


@pytest.fixture
def fixture_flow_instance_message() -> ExecutableContent:
    content = {
        "time": 1701099523.849,
        "rootfs": {
            "parent": {
                "ref": "6e30de68c6cedfa6b45240c2b51e52495ac6fb1bd4b36457b3d5ca307594d595",
                "use_latest": True,
            },
            "size_mib": 20480,
            "persistence": "host",
        },
        "address": "0xA07B1214bAe0D5ccAA25449C3149c0aC83658874",
        "volumes": [],
        "metadata": {"name": "Test Debian 12"},
        "resources": {"vcpus": 1, "memory": 2048, "seconds": 30},
        "allow_amend": False,
        "environment": {
            "internet": True,
            "aleph_api": True,
            "reproducible": False,
            "shared_cache": False,
        },
        "payment": {
            "chain": "AVAX",
            "receiver": "0xA07B1214bAe0D5ccAA25449C3149c0aC83658874",
            "type": "superfluid",
        },
        "authorized_keys": [
            "ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAAIHlGJRaIv/EzNT0eNqNB5DiGEbii28Fb2zCjuO/bMu7y nesitor@gmail.com"
        ],
    }

    return InstanceContent.parse_obj(content)


@pytest.fixture
def fixture_flow_instance_message_complete() -> ExecutableContent:
    content = {
        "time": 1701099523.849,
        "rootfs": {
            "parent": {
                "ref": "6e30de68c6cedfa6b45240c2b51e52495ac6fb1bd4b36457b3d5ca307594d595",
                "use_latest": True,
            },
            "size_mib": 20480,
            "persistence": "host",
        },
        "address": "0xA07B1214bAe0D5ccAA25449C3149c0aC83658874",
        "volumes": [
            {
                "comment": "Ephemeral storage, read-write but will not persist after the VM stops",
                "mount": "/var/cache",
                "ephemeral": True,
                "size_mib": 50,
            },
            {
                "comment": "Working data persisted on the VM supervisor, not available on other nodes",
                "mount": "/var/lib/sqlite",
                "name": "sqlite-data",
                "persistence": "host",
                "size_mib": 1024,
            },
            {
                "comment": "Working data persisted on the Aleph network. "
                "New VMs will try to use the latest version of this volume, "
                "with no guarantee against conflicts",
                "mount": "/var/lib/statistics",
                "name": "statistics",
                "persistence": "store",
                "size_mib": 10240,
            },
            {
                "comment": "Raw drive to use by a process, do not mount it",
                "name": "raw-data",
                "persistence": "host",
                "size_mib": 51200,
            },
        ],
        "metadata": {"name": "Test Debian 12"},
        "resources": {"vcpus": 1, "memory": 2048, "seconds": 30},
        "allow_amend": False,
        "environment": {
            "internet": True,
            "aleph_api": True,
            "reproducible": False,
            "shared_cache": False,
        },
        "payment": {
            "chain": "AVAX",
            "receiver": "0xA07B1214bAe0D5ccAA25449C3149c0aC83658874",
            "type": "superfluid",
        },
        "authorized_keys": [
            "ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAAIHlGJRaIv/EzNT0eNqNB5DiGEbii28Fb2zCjuO/bMu7y nesitor@gmail.com"
        ],
    }

    return InstanceContent.parse_obj(content)


def test_compute_cost(
    session_factory: DbSessionFactory,
    fixture_product_prices_aggregate_in_db,
    fixture_hold_instance_message,
):
    file_db = StoredFileDb()
    mock = Mock()
    mock.patch("_get_file_from_ref", return_value=file_db)

    with session_factory() as session:
        cost = compute_cost(content=fixture_hold_instance_message, session=session)
        assert cost == 1000


def test_compute_cost_conf(
    session_factory: DbSessionFactory,
    fixture_product_prices_aggregate_in_db,
    fixture_hold_instance_message,
):
    message_dict = fixture_hold_instance_message.dict()

    # Convert the message to conf
    message_dict["environment"].update(
        {
            "hypervisor": "qemu",  # Add qemu to the environment
            "trusted_execution": {
                "policy": 1,
                "firmware": "e258d248fda94c63753607f7c4494ee0fcbe92f1a76bfdac795c9d84101eb317",
            },
        }
    )

    rebuilt_message = InstanceContent.parse_obj(message_dict)

    file_db = StoredFileDb()
    mock = Mock()
    mock.patch("_get_file_from_ref", return_value=file_db)

    with session_factory() as session:
        cost = compute_cost(content=rebuilt_message, session=session)
        assert cost == 2000


def test_get_additional_storage_price(
    session_factory: DbSessionFactory,
    fixture_product_prices_aggregate_in_db,
    fixture_hold_instance_message,
):
    file_db = StoredFileDb()
    mock = Mock()
    mock.patch("_get_file_from_ref", return_value=file_db)

    with session_factory() as session:
        content = fixture_hold_instance_message

        pricing = _get_product_price(session, content=content)

        cost = get_additional_storage_price(
            content=content, pricing=pricing, session=session
        )
        assert cost == 0


def test_compute_cost_complete(
    session_factory: DbSessionFactory,
    fixture_product_prices_aggregate_in_db,
    fixture_hold_instance_message_complete,
):
    file_db = StoredFileDb()
    mock = Mock()
    mock.patch("_get_file_from_ref", return_value=file_db)

    with session_factory() as session:
        cost = compute_cost(
            content=fixture_hold_instance_message_complete, session=session
        )
        assert cost == 1017.50


def test_compute_flow_cost(
    session_factory: DbSessionFactory,
    fixture_product_prices_aggregate_in_db,
    fixture_flow_instance_message,
):
    file_db = StoredFileDb()
    mock = Mock()
    mock.patch("_get_file_from_ref", return_value=file_db)

    with session_factory() as session:
        cost = compute_flow_cost(content=fixture_flow_instance_message, session=session)
        assert cost == Decimal("0.00001527777777777777777777777778")
        cost_per_hour = cost * HOUR
        assert cost_per_hour == Decimal("0.05500000000000000000000000001")


def test_compute_flow_cost_conf(
    session_factory: DbSessionFactory,
    fixture_product_prices_aggregate_in_db,
    fixture_flow_instance_message,
):
    message_dict = fixture_flow_instance_message.dict()

    # Convert the message to conf
    message_dict["environment"].update(
        {
            "hypervisor": "qemu",  # Add qemu to the environment
            "trusted_execution": {
                "policy": 1,
                "firmware": "e258d248fda94c63753607f7c4494ee0fcbe92f1a76bfdac795c9d84101eb317",
            },
        }
    )

    rebuilt_message = InstanceContent.parse_obj(message_dict)

    # Proceed with the test
    file_db = StoredFileDb()
    mock = Mock()
    mock.patch("_get_file_from_ref", return_value=file_db)

    with session_factory() as session:
        cost = compute_flow_cost(content=rebuilt_message, session=session)
        assert cost == Decimal("0.00003055555555555555555555555556")
        cost_per_hour = cost * HOUR
        assert cost_per_hour == Decimal("0.11")


def test_compute_flow_cost_complete(
    session_factory: DbSessionFactory,
    fixture_product_prices_aggregate_in_db,
    fixture_flow_instance_message_complete,
):
    file_db = StoredFileDb()
    mock = Mock()
    mock.patch("_get_file_from_ref", return_value=file_db)

    with session_factory() as session:
        cost = compute_flow_cost(
            content=fixture_flow_instance_message_complete, session=session
        )
        assert cost == Decimal("0.00003224338277777777777777777778")
        cost_per_hour = cost * HOUR
        assert cost_per_hour == Decimal("0.116076178")
