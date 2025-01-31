import datetime as dt
import itertools
import json
from decimal import Decimal
from typing import List

import pytest
import pytz
from aleph_message.models import Chain, ItemHash, ItemType, MessageType
from aleph_message.models.execution import MachineType
from aleph_message.models.execution.program import ProgramContent
from aleph_message.models.execution.volume import ImmutableVolume, VolumePersistence
from more_itertools import one
from sqlalchemy import select

from aleph.db.accessors.files import insert_message_file_pin, upsert_file_tag
from aleph.db.accessors.messages import get_message_status, get_rejected_message
from aleph.db.accessors.vms import get_program
from aleph.db.models import (
    AlephBalanceDb,
    EphemeralVolumeDb,
    ImmutableVolumeDb,
    MessageStatusDb,
    PendingMessageDb,
    PersistentVolumeDb,
    StoredFileDb,
    VmBaseDb,
)
from aleph.jobs.process_pending_messages import PendingMessageProcessor
from aleph.toolkit.timestamp import timestamp_to_datetime
from aleph.types.db_session import DbSession, DbSessionFactory
from aleph.types.files import FileTag, FileType
from aleph.types.message_status import ErrorCode, MessageStatus


@pytest.fixture
def fixture_program_message(session_factory: DbSessionFactory) -> PendingMessageDb:
    pending_message = PendingMessageDb(
        item_hash="734a1287a2b7b5be060312ff5b05ad1bcf838950492e3428f2ac6437a1acad26",
        type=MessageType.program,
        chain=Chain.ETH,
        sender="0x7083b90eBA420832A03C6ac7e6328d37c72e0260",
        signature="0x5c5c757f35403e9b6d6b1c5dc0be349284d76ded4cfa9edc6ad8522212f4235448bc80432ea7f8c5f4da315d6ce6902072d4eb7bbbebd969841f08a3df61c2971c",
        item_type=ItemType.inline,
        item_content='{"address":"0x7083b90eBA420832A03C6ac7e6328d37c72e0260","time":1655123939.12433,"type":"vm-function","allow_amend":false,"code":{"encoding":"squashfs","entrypoint":"python run.py","ref":"53ee77caeb7d6e0e982abf010b3d6ea2dbc1225e157e09283e3a9d7da757e193","use_latest":true},"variables":{"LD_LIBRARY_PATH":"/opt/extra_lib","DB_FOLDER":"/data","RPC_ENDPOINT":"https://rpc.tzkt.io/ithacanet","TRUSTED_RPC_ENDPOINT":"https://rpc.tzkt.io/ithacanet","WELL_CONTRACT":"KT1ReVgfaUqHzWWiNRfPXQxf7TaBLVbxrztw","PORT":"8080","CONCURRENT_JOB":"5","BATCH_SIZE":"10","UNTIL_BLOCK":"201396","PUBSUB":"{\\"namespace\\": \\"tznms\\",\\"uuid\\": \\"tz_uid_1\\",\\"hook_url\\": \\"_domain_or_ip_addess\\",\\"pubsub_server\\": \\"domain_or_ip_address\\",\\"secret_shared_key\\": \\"112secret_key\\",\\"channel\\": \\"storage\\",\\"running_mode\\": \\"readonly\\"}"},"on":{"http":false, "persistent": true},"environment":{"reproducible":false,"internet":true,"aleph_api":true,"shared_cache":false},"resources":{"vcpus":4,"memory":4095,"seconds":300},"runtime":{"ref":"bd79839bf96e595a06da5ac0b6ba51dea6f7e2591bb913deccded04d831d29f4","use_latest":true,"comment":"Aleph Alpine Linux with Python 3.8"},"volumes":[{"comment":"Extra lib","mount":"/opt/extra_lib","ref":"5f31b0706f59404fad3d0bff97ef89ddf24da4761608ea0646329362c662ba51","use_latest":true},{"comment":"Python Virtual Environment","mount":"/opt/packages","ref":"1000ebe0b61e41d5e23c10f6eb140e837188158598049829f2820f830139fc7d","use_latest":true},{"comment":"Data storage","mount":"/data","persistence":"host","name":"data","size_mib":128}]}',
        time=timestamp_to_datetime(1671637391),
        channel=None,
        reception_time=timestamp_to_datetime(1671637391),
        fetched=True,
        check_message=True,
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
def fixture_program_message_with_subscriptions(
    session_factory: DbSessionFactory,
) -> PendingMessageDb:
    pending_message = PendingMessageDb(
        item_hash="cad11970efe9b7478300fd04d7cc91c646ca0a792b9cc718650f86e1ccfac73e",
        type=MessageType.program,
        chain=Chain.ETH,
        sender="0xb5F010860b0964090d5414406273E6b3A8726E96",
        signature="0x93a4bff97ceb935091ac1daa57b4c0470256b945bde7ead5bd04e2a7139fe74343941e4a84ff563fd2c22da9599a0250ddda3f3217931d64f25de79b80bd2da11c",
        item_type=ItemType.inline,
        time=timestamp_to_datetime(1671637391),
        item_content='{"address":"0xb5F010860b0964090d5414406273E6b3A8726E96","time":1632489197.833036,"type":"vm-function","allow_amend":false,"code":{"encoding":"zip","entrypoint":"main:app","ref":"200af5241b583796441b249889500d8d9ee98cac5cbcc41076a4584c355a9ca5","use_latest":true},"on":{"http":true,"message":[{"sender":"0xE221373557Cc8e6094dB6cC3E8EFeb90003dE9ea","channel":"TEST"}]},"environment":{"reproducible":false,"internet":true,"aleph_api":true,"shared_cache":false},"resources":{"vcpus":1,"memory":128,"seconds":30},"runtime":{"ref":"c6dd36dbc94620159ffacde84cba102ede6cef7381e2e360c0c3b04423ba3eaa","use_latest":true,"comment":"Aleph Alpine Linux with Python 3.8"},"volumes":[]}',
        channel=None,
        reception_time=timestamp_to_datetime(1671637391),
        fetched=True,
        check_message=True,
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


def get_volumes_with_ref(content: ProgramContent) -> List:
    volumes = [content.code, content.runtime]
    if content.data:
        volumes.append(content.data)

    for volume in content.volumes:
        if isinstance(volume, ImmutableVolume):
            volumes.append(volume)

    return volumes


def insert_volume_refs(session: DbSession, message: PendingMessageDb):
    """
    Insert volume references in the DB to make the program processable.
    """

    assert message.item_content
    content = ProgramContent.parse_raw(message.item_content)
    volumes = get_volumes_with_ref(content)

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
        if volume.use_latest:
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
        address="0x7083b90eBA420832A03C6ac7e6328d37c72e0260",
        chain=Chain.ETH,
        balance=Decimal(22_192),
        eth_height=0,
    )

    with session_factory() as session:
        session.add(balance)
        session.commit()
    return balance


@pytest.mark.asyncio
async def test_process_program(
    user_balance: AlephBalanceDb,
    session_factory: DbSessionFactory,
    message_processor: PendingMessageProcessor,
    fixture_program_message: PendingMessageDb,
    fixture_product_prices_aggregate_in_db,
):
    with session_factory() as session:
        insert_volume_refs(session, fixture_program_message)
        session.commit()

    pipeline = message_processor.make_pipeline()
    # Exhaust the iterator
    _ = [message async for message in pipeline]

    assert fixture_program_message.item_content
    content_dict = json.loads(fixture_program_message.item_content)

    with session_factory() as session:
        program = get_program(
            session=session, item_hash=fixture_program_message.item_hash
        )
        assert program is not None

        assert program.owner == fixture_program_message.sender
        assert program.program_type == MachineType.vm_function
        assert not program.allow_amend
        assert program.replaces is None
        assert program.persistent

        assert program.resources_vcpus == content_dict["resources"]["vcpus"]
        assert program.resources_memory == content_dict["resources"]["memory"]
        assert program.resources_seconds == content_dict["resources"]["seconds"]

        assert program.environment_internet
        assert program.environment_aleph_api
        assert not program.environment_reproducible
        assert not program.environment_shared_cache

        assert program.variables
        assert len(program.variables) == 10

        runtime = program.runtime
        assert runtime.ref == content_dict["runtime"]["ref"]
        assert runtime.use_latest == content_dict["runtime"]["use_latest"]
        assert runtime.comment == content_dict["runtime"]["comment"]

        code_volume = program.code_volume
        assert code_volume.ref == content_dict["code"]["ref"]
        assert code_volume.encoding == content_dict["code"]["encoding"]
        assert code_volume.entrypoint == content_dict["code"]["entrypoint"]
        assert code_volume.use_latest == content_dict["code"]["use_latest"]

        assert len(program.volumes) == 3

        volumes_by_type = {
            type: list(volumes_iter)
            for type, volumes_iter in itertools.groupby(
                sorted(program.volumes, key=lambda v: str(v.__class__)),
                key=lambda v: v.__class__,
            )
        }
        assert EphemeralVolumeDb not in volumes_by_type
        assert len(volumes_by_type[PersistentVolumeDb]) == 1
        assert len(volumes_by_type[ImmutableVolumeDb]) == 2

        persistent_volume: PersistentVolumeDb = one(volumes_by_type[PersistentVolumeDb])
        assert persistent_volume.name == "data"
        assert persistent_volume.mount == "/data"
        assert persistent_volume.size_mib == 128
        assert persistent_volume.persistence == VolumePersistence.host


@pytest.mark.asyncio
async def test_program_with_subscriptions(
    session_factory: DbSessionFactory,
    message_processor: PendingMessageProcessor,
    fixture_program_message_with_subscriptions: PendingMessageDb,
):
    program_message = fixture_program_message_with_subscriptions
    with session_factory() as session:
        insert_volume_refs(session, program_message)
        session.commit()

    pipeline = message_processor.make_pipeline()
    # Exhaust the iterator
    _ = [message async for message in pipeline]

    assert program_message.item_content
    json.loads(program_message.item_content)

    with session_factory() as session:
        program: VmBaseDb = session.execute(select(VmBaseDb)).scalar_one()
        message_triggers = program.message_triggers
        assert message_triggers
        assert len(message_triggers) == 1
        message_trigger = message_triggers[0]

        assert message_trigger["channel"] == "TEST"
        assert message_trigger["sender"] == "0xE221373557Cc8e6094dB6cC3E8EFeb90003dE9ea"


@pytest.mark.asyncio
async def test_process_program_missing_volumes(
    session_factory: DbSessionFactory,
    message_processor: PendingMessageProcessor,
    fixture_program_message_with_subscriptions: PendingMessageDb,
):
    """
    Check that a program message with volumes not references in file_tags/file_pins
    is rejected.
    """

    program_message = fixture_program_message_with_subscriptions
    program_hash = program_message.item_hash
    pipeline = message_processor.make_pipeline()
    # Exhaust the iterator
    _ = [message async for message in pipeline]

    with session_factory() as session:
        program_db = get_program(session=session, item_hash=ItemHash(program_hash))
        assert program_db is None

        message_status = get_message_status(
            session=session, item_hash=ItemHash(program_hash)
        )
        assert message_status is not None
        assert message_status.status == MessageStatus.REJECTED

        rejected_message = get_rejected_message(
            session=session, item_hash=ItemHash(program_hash)
        )
        assert rejected_message is not None
        assert rejected_message.error_code == ErrorCode.VM_VOLUME_NOT_FOUND

        assert program_message.item_content
        content = ProgramContent.parse_raw(program_message.item_content)
        volume_refs = set(volume.ref for volume in get_volumes_with_ref(content))
        assert isinstance(rejected_message.details, dict)
        assert set(rejected_message.details["errors"]) == volume_refs
        assert rejected_message.traceback is None
