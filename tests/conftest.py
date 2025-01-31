import asyncio
import contextlib
import datetime as dt
import json
import logging
import os
import shutil
import sys
from decimal import Decimal
from pathlib import Path
from typing import List, Protocol

import alembic.command
import alembic.config
import pytest
import pytest_asyncio
import pytz
from aleph_message.models import (
    Chain,
    ExecutableContent,
    InstanceContent,
    ItemType,
    MessageType,
    ProgramContent,
)
from aleph_message.models.execution.volume import ImmutableVolume
from configmanager import Config

import aleph.config
from aleph.db.accessors.files import insert_message_file_pin, upsert_file_tag
from aleph.db.connection import make_db_url, make_engine, make_session_factory
from aleph.db.models import (
    AlephBalanceDb,
    MessageStatusDb,
    PendingMessageDb,
    StoredFileDb,
)
from aleph.db.models.aggregates import AggregateDb, AggregateElementDb
from aleph.services.cache.node_cache import NodeCache
from aleph.services.ipfs import IpfsService
from aleph.services.storage.fileystem_engine import FileSystemStorageEngine
from aleph.storage import StorageService
from aleph.toolkit.constants import PRICE_AGGREGATE_KEY, PRICE_AGGREGATE_OWNER
from aleph.toolkit.timestamp import timestamp_to_datetime
from aleph.types.db_session import DbSession, DbSessionFactory
from aleph.types.files import FileTag, FileType
from aleph.types.message_status import MessageStatus
from aleph.web import create_aiohttp_app
from aleph.web.controllers.app_state_getters import (
    APP_STATE_CONFIG,
    APP_STATE_P2P_CLIENT,
    APP_STATE_SESSION_FACTORY,
    APP_STATE_STORAGE_SERVICE,
)

# Add the helpers to the PYTHONPATH.
# Note: mark the "helpers" directory as a source directory to tell PyCharm
# about this trick and avoid IDE errors.
sys.path.append(os.path.join(os.path.dirname(__file__), "helpers"))


@contextlib.contextmanager
def change_dir(directory: Path):
    current_directory = Path.cwd()
    try:
        os.chdir(directory)
        yield
    finally:
        os.chdir(current_directory)


def run_db_migrations(config: Config):
    logging.basicConfig(level=logging.DEBUG)

    project_dir = Path(__file__).parent.parent

    db_url = make_db_url(driver="psycopg2", config=config)
    alembic_cfg = alembic.config.Config("alembic.ini")
    alembic_cfg.attributes["configure_logger"] = False
    logging.getLogger("alembic").setLevel(logging.CRITICAL)

    with change_dir(project_dir):
        alembic.command.upgrade(alembic_cfg, "head", tag=db_url)


@pytest.fixture
def session_factory(mock_config):
    engine = make_engine(config=mock_config, echo=False, application_name="aleph-tests")

    with engine.begin() as conn:
        conn.execute("drop schema public cascade")
        conn.execute("create schema public")

    run_db_migrations(config=mock_config)
    return make_session_factory(engine)


@pytest.fixture
def mock_config() -> Config:
    config: Config = Config(aleph.config.get_defaults())

    config_file_path: Path = Path.cwd() / "config.yml"

    # The postgres/redis hosts use Docker network names in the default config.
    # We always use localhost for tests.
    config.postgres.host.value = "127.0.0.1"
    config.redis.host.value = "127.0.0.1"

    if config_file_path.exists():
        user_config_raw: str = config_file_path.read_text()

        # Little trick to allow empty config files
        if user_config_raw:
            config.yaml.loads(user_config_raw)

    # To test handle_new_storage
    config.storage.store_files.value = True

    # We set the global variable directly instead of patching it because of an issue
    # with mocker.patch. mocker.patch uses hasattr to determine the properties of
    # the mock, which does not work well with configmanager Config objects.
    aleph.config.app_config = config
    return config


@pytest_asyncio.fixture
async def node_cache(mock_config: Config):
    async with NodeCache(
        redis_host=mock_config.redis.host.value, redis_port=mock_config.redis.port.value
    ) as node_cache:
        yield node_cache


@pytest_asyncio.fixture
async def test_storage_service(mock_config: Config, mocker) -> StorageService:
    data_folder = Path("./data")

    # Delete files from previous runs
    if data_folder.is_dir():
        shutil.rmtree(data_folder)
    data_folder.mkdir(parents=True)

    storage_engine = FileSystemStorageEngine(folder=data_folder)
    async with IpfsService.new(mock_config) as ipfs_service:
        storage_service = StorageService(
            storage_engine=storage_engine,
            ipfs_service=ipfs_service,
            node_cache=mocker.AsyncMock(),
        )

        yield storage_service


@pytest.fixture
def ccn_test_aiohttp_app(mocker, mock_config, session_factory):
    # Make aiohttp return the stack trace on 500 errors
    event_loop = asyncio.get_event_loop()
    event_loop.set_debug(True)

    app = create_aiohttp_app()
    app[APP_STATE_CONFIG] = mock_config
    app[APP_STATE_P2P_CLIENT] = mocker.AsyncMock()
    app[APP_STATE_STORAGE_SERVICE] = mocker.AsyncMock()
    app[APP_STATE_SESSION_FACTORY] = session_factory

    return app


@pytest_asyncio.fixture
async def ccn_api_client(
    aiohttp_client,
    ccn_test_aiohttp_app,
):
    client = await aiohttp_client(ccn_test_aiohttp_app)
    return client


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
        type=MessageType.instance.value,
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
def instance_message_with_volumes_in_db(
    session_factory: DbSessionFactory, fixture_instance_message: PendingMessageDb
) -> None:
    with session_factory() as session:
        insert_volume_refs(session, fixture_instance_message)
        session.commit()


class Volume(Protocol):
    ref: str
    use_latest: bool


def get_volume_refs(content: ExecutableContent) -> List[Volume]:
    volumes = []

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
def user_balance_eth_avax(session_factory: DbSessionFactory) -> AlephBalanceDb:
    balance_eth = AlephBalanceDb(
        address="0x9319Ad3B7A8E0eE24f2E639c40D8eD124C5520Ba",
        chain=Chain.ETH,
        balance=Decimal(22_192),
        eth_height=0,
    )

    balance_avax = AlephBalanceDb(
        address="0x9319Ad3B7A8E0eE24f2E639c40D8eD124C5520Ba",
        chain=Chain.AVAX,
        balance=Decimal(22_192),
        eth_height=0,
    )

    with session_factory() as session:
        session.add(balance_eth)
        session.add(balance_avax)

        session.commit()
    return balance_avax


@pytest.fixture
def fixture_product_prices_aggregate_in_db(session_factory: DbSessionFactory) -> None:
    with session_factory() as session:
        item_hash = "7b74b9c5f73e7a0713dbe83a377b1d321ffb4a5411ea3df49790a9720b93a5bF"
        content = {
            "program": {
                "price": {
                    "storage": {"payg": "0.000000977", "holding": "0.05"},
                    "compute_unit": {"payg": "0.011", "holding": "200"},
                },
                "tiers": [
                    {"id": "tier-1", "compute_units": 1},
                    {"id": "tier-2", "compute_units": 2},
                    {"id": "tier-3", "compute_units": 4},
                    {"id": "tier-4", "compute_units": 6},
                    {"id": "tier-5", "compute_units": 8},
                    {"id": "tier-6", "compute_units": 12},
                ],
                "compute_unit": {
                    "vcpus": 1,
                    "disk_mib": 2048,
                    "memory_mib": 2048,
                },
            },
            "storage": {"price": {"storage": {"holding": "0.333333333"}}},
            "instance": {
                "price": {
                    "storage": {"payg": "0.000000977", "holding": "0.05"},
                    "compute_unit": {"payg": "0.055", "holding": "1000"},
                },
                "tiers": [
                    {"id": "tier-1", "compute_units": 1},
                    {"id": "tier-2", "compute_units": 2},
                    {"id": "tier-3", "compute_units": 4},
                    {"id": "tier-4", "compute_units": 6},
                    {"id": "tier-5", "compute_units": 8},
                    {"id": "tier-6", "compute_units": 12},
                ],
                "compute_unit": {
                    "vcpus": 1,
                    "disk_mib": 20480,
                    "memory_mib": 2048,
                },
            },
            "web3_hosting": {
                "price": {"fixed": 50, "storage": {"holding": "0.333333333"}}
            },
            "program_persistent": {
                "price": {
                    "storage": {"payg": "0.000000977", "holding": "0.05"},
                    "compute_unit": {"payg": "0.055", "holding": "1000"},
                },
                "tiers": [
                    {"id": "tier-1", "compute_units": 1},
                    {"id": "tier-2", "compute_units": 2},
                    {"id": "tier-3", "compute_units": 4},
                    {"id": "tier-4", "compute_units": 6},
                    {"id": "tier-5", "compute_units": 8},
                    {"id": "tier-6", "compute_units": 12},
                ],
                "compute_unit": {
                    "vcpus": 1,
                    "disk_mib": 20480,
                    "memory_mib": 2048,
                },
            },
            "instance_gpu_premium": {
                "price": {
                    "storage": {"payg": "0.000000977"},
                    "compute_unit": {"payg": "0.56"},
                },
                "tiers": [
                    {
                        "id": "tier-1",
                        "vram": 81920,
                        "model": "A100",
                        "compute_units": 16,
                    },
                    {
                        "id": "tier-2",
                        "vram": 81920,
                        "model": "H100",
                        "compute_units": 24,
                    },
                ],
                "compute_unit": {
                    "vcpus": 1,
                    "disk_mib": 61440,
                    "memory_mib": 6144,
                },
            },
            "instance_confidential": {
                "price": {
                    "storage": {"payg": "0.000000977", "holding": "0.05"},
                    "compute_unit": {"payg": "0.11", "holding": "2000"},
                },
                "tiers": [
                    {"id": "tier-1", "compute_units": 1},
                    {"id": "tier-2", "compute_units": 2},
                    {"id": "tier-3", "compute_units": 4},
                    {"id": "tier-4", "compute_units": 6},
                    {"id": "tier-5", "compute_units": 8},
                    {"id": "tier-6", "compute_units": 12},
                ],
                "compute_unit": {
                    "vcpus": 1,
                    "disk_mib": 20480,
                    "memory_mib": 2048,
                },
            },
            "instance_gpu_standard": {
                "price": {
                    "storage": {"payg": "0.000000977"},
                    "compute_unit": {"payg": "0.28"},
                },
                "tiers": [
                    {
                        "id": "tier-1",
                        "vram": 20480,
                        "model": "RTX 4000 ADA",
                        "compute_units": 3,
                    },
                    {
                        "id": "tier-2",
                        "vram": 24576,
                        "model": "RTX 3090",
                        "compute_units": 4,
                    },
                    {
                        "id": "tier-3",
                        "vram": 24576,
                        "model": "RTX 4090",
                        "compute_units": 6,
                    },
                    {
                        "id": "tier-4",
                        "vram": 49152,
                        "model": "L40S",
                        "compute_units": 12,
                    },
                ],
                "compute_unit": {
                    "vcpus": 1,
                    "disk_mib": 61440,
                    "memory_mib": 6144,
                },
            },
        }

        session.add(
            AggregateElementDb(
                item_hash=item_hash,
                key=PRICE_AGGREGATE_KEY,
                owner=PRICE_AGGREGATE_OWNER,
                content=content,
                creation_datetime=dt.datetime(2025, 1, 31),
            )
        )

        session.add(
            AggregateDb(
                key=PRICE_AGGREGATE_KEY,
                owner=PRICE_AGGREGATE_OWNER,
                content=content,
                creation_datetime=dt.datetime(2025, 1, 31),
                last_revision_hash=item_hash,
                dirty=False,
            )
        )

        session.commit()
