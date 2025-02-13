import logging
from decimal import Decimal
from typing import List, Protocol, Set, Union, overload

from aleph_message.models import (
    ExecutableContent,
    InstanceContent,
    Payment,
    PaymentType,
    ProgramContent,
)
from aleph_message.models.execution.instance import RootfsVolume
from aleph_message.models.execution.volume import (
    AbstractVolume,
    EphemeralVolume,
    ImmutableVolume,
    ParentVolume,
    PersistentVolume,
)

from aleph.db.accessors.balances import get_total_balance
from aleph.db.accessors.cost import get_total_cost_for_address
from aleph.db.accessors.files import (
    find_file_pins,
    find_file_tags,
    get_file_tag,
    get_message_file_pin,
)
from aleph.db.accessors.vms import (
    delete_vm,
    delete_vm_updates,
    get_program,
    is_vm_amend_allowed,
    refresh_vm_version,
    upsert_vm_version,
)
from aleph.db.models import (
    CodeVolumeDb,
    DataVolumeDb,
    EphemeralVolumeDb,
    ExportVolumeDb,
    ImmutableVolumeDb,
    MachineVolumeBaseDb,
    MessageDb,
    PersistentVolumeDb,
    ProgramDb,
    RootfsVolumeDb,
    RuntimeDb,
    StoredFileDb,
    VmBaseDb,
    VmInstanceDb,
)
from aleph.db.models.account_costs import AccountCostsDb
from aleph.handlers.content.content_handler import ContentHandler
from aleph.services.cost import get_total_and_detailed_costs
from aleph.toolkit.timestamp import timestamp_to_datetime
from aleph.types.db_session import DbSession
from aleph.types.files import FileTag
from aleph.types.message_status import (
    InsufficientBalanceException,
    InternalError,
    InvalidMessageFormat,
    VmCannotUpdateUpdate,
    VmRefNotFound,
    VmUpdateNotAllowed,
    VmVolumeNotFound,
    VmVolumeTooSmall,
)
from aleph.types.vms import VmVersion
from aleph.utils import safe_getattr

LOGGER = logging.getLogger(__name__)


def _get_vm_content(message: MessageDb) -> ExecutableContent:
    content = message.parsed_content
    if not isinstance(content, (InstanceContent, ProgramContent)):
        raise InvalidMessageFormat(
            f"Unexpected content type for program message: {message.item_hash}"
        )
    return content


@overload
def _map_content_to_db_model(
    item_hash: str, content: InstanceContent
) -> VmInstanceDb: ...


# For some reason, mypy is not happy with the overload resolution here.
# This seems linked to multiple inheritance of Pydantic base models, a deeper investigation
# is required.
@overload
def _map_content_to_db_model(item_hash: str, content: ProgramContent) -> ProgramDb:  # type: ignore
    ...


def _map_content_to_db_model(item_hash, content):
    db_cls = ProgramDb if isinstance(content, ProgramContent) else VmInstanceDb
    payment_type = PaymentType.hold

    volumes = [map_volume(volume) for volume in content.volumes]

    cpu_architecture = None
    cpu_vendor = None
    node_owner = None
    node_address_regex = None

    if content.requirements:
        if cpu := content.requirements.cpu:
            cpu_architecture = cpu.architecture
            cpu_vendor = cpu.vendor

        if node := content.requirements.node:
            node_owner = node.owner
            node_address_regex = node.address_regex

    trusted_execution_policy = None
    trusted_execution_firmware = None
    node_hash = None
    if not isinstance(content, ProgramContent):
        if content.environment.trusted_execution is not None:
            trusted_execution_policy = content.environment.trusted_execution.policy
            trusted_execution_firmware = content.environment.trusted_execution.firmware
        if safe_getattr(content, "requirements.node.node_hash") is not None:
            node_hash = content.requirements.node.node_hash

    if isinstance(content.payment, Payment):
        if content.payment.type is not None:
            payment_type = content.payment.type

    return db_cls(
        owner=content.address,
        item_hash=item_hash,
        allow_amend=content.allow_amend,
        metadata_=content.metadata,
        variables=content.variables,
        environment_reproducible=content.environment.reproducible,
        environment_internet=content.environment.internet,
        environment_aleph_api=content.environment.aleph_api,
        environment_shared_cache=content.environment.shared_cache,
        environment_trusted_execution_policy=trusted_execution_policy,
        environment_trusted_execution_firmware=trusted_execution_firmware,
        resources_vcpus=content.resources.vcpus,
        resources_memory=content.resources.memory,
        resources_seconds=content.resources.seconds,
        cpu_architecture=cpu_architecture,
        cpu_vendor=cpu_vendor,
        node_owner=node_owner,
        node_address_regex=node_address_regex,
        volumes=volumes,
        created=timestamp_to_datetime(content.time),
        node_hash=node_hash,
        payment_type=payment_type,
    )


def map_volume(volume: AbstractVolume) -> MachineVolumeBaseDb:
    comment = volume.comment
    mount = volume.mount

    if isinstance(volume, ImmutableVolume):
        return ImmutableVolumeDb(
            comment=comment, mount=mount, ref=volume.ref, use_latest=volume.use_latest
        )
    elif isinstance(volume, EphemeralVolume):
        return EphemeralVolumeDb(comment=comment, mount=mount, size_mib=volume.size_mib)
    elif isinstance(volume, PersistentVolume):
        if parent := volume.parent:
            parent_ref, parent_use_latest = parent.ref, parent.use_latest
        else:
            parent_ref, parent_use_latest = None, None

        return PersistentVolumeDb(
            comment=comment,
            mount=mount,
            persistence=volume.persistence,
            name=volume.name,
            size_mib=volume.size_mib,
            parent_ref=parent_ref,
            parent_use_latest=parent_use_latest,
        )
    else:
        raise InternalError(f"Unsupported volume type: {volume.__class__.__name__}")


def vm_message_to_db(message: MessageDb) -> VmBaseDb:
    content = _get_vm_content(message)
    vm = _map_content_to_db_model(message.item_hash, content)

    if isinstance(vm, ProgramDb) and isinstance(content, ProgramContent):
        vm.program_type = content.type
        vm.persistent = bool(content.on.persistent)
        vm.http_trigger = content.on.http

        if content.on.message:
            vm.message_triggers = [
                subscription.dict() for subscription in content.on.message
            ]

        vm.code_volume = CodeVolumeDb(
            encoding=content.code.encoding,
            entrypoint=content.code.entrypoint,
            ref=content.code.ref,
            use_latest=content.code.use_latest,
        )

        vm.runtime = RuntimeDb(
            ref=content.runtime.ref,
            use_latest=content.runtime.use_latest,
            comment=content.runtime.comment,
        )

        if content.data:
            vm.data_volume = DataVolumeDb(
                encoding=content.data.encoding,
                mount=content.data.mount,
                ref=content.data.ref,
                use_latest=content.data.use_latest,
            )

        if content.export:
            vm.export_volume = ExportVolumeDb(encoding=content.export.encoding)

    elif isinstance(content, InstanceContent):
        parent = content.rootfs.parent
        if isinstance(vm, VmInstanceDb):
            vm.rootfs = RootfsVolumeDb(
                parent_ref=parent.ref,
                parent_use_latest=parent.use_latest,
                size_mib=content.rootfs.size_mib,
                persistence=content.rootfs.persistence,
            )
            vm.authorized_keys = content.authorized_keys
        else:
            raise TypeError(f"Unexpected VM message content type: {type(vm)}")

    else:
        raise TypeError(f"Unexpected VM message content type: {type(content)}")

    return vm


def find_missing_volumes(
    session: DbSession, content: ExecutableContent
) -> Set[FileTag]:
    tags_to_check = set()
    pins_to_check = set()

    def add_ref_to_check(_volume):
        if _volume.use_latest:
            tags_to_check.add(_volume.ref)
        else:
            pins_to_check.add(_volume.ref)

    if isinstance(content, ProgramContent):
        add_ref_to_check(content.code)
        add_ref_to_check(content.runtime)
        if content.data:
            add_ref_to_check(content.data)

    elif isinstance(content, InstanceContent):
        add_ref_to_check(content.rootfs.parent)

    for volume in content.volumes:
        if isinstance(volume, ImmutableVolume):
            add_ref_to_check(volume)

        if isinstance(volume, PersistentVolume):
            if parent := volume.parent:
                add_ref_to_check(parent)

    # For each volume, if use_latest is set check the tags and otherwise check
    # the file pins.

    file_tags_db = set(find_file_tags(session=session, tags=tags_to_check))
    file_pins_db = set(find_file_pins(session=session, item_hashes=pins_to_check))

    return (pins_to_check - file_pins_db) | (tags_to_check - file_tags_db)


def check_parent_volumes_size_requirements(
    session: DbSession, content: ExecutableContent
) -> None:
    def _get_parent_volume_file(_parent: ParentVolume) -> StoredFileDb:
        if _parent.use_latest:
            file_tag = get_file_tag(session=session, tag=FileTag(_parent.ref))
            if file_tag is None:
                raise InternalError(
                    f"Could not find latest version of parent volume {_parent.ref}"
                )

            return file_tag.file

        file_pin = get_message_file_pin(session=session, item_hash=_parent.ref)
        if file_pin is None:
            raise InternalError(
                f"Could not find original version of parent volume {_parent.ref}"
            )

        return file_pin.file

    class HasParent(Protocol):
        parent: ParentVolume
        size_mib: int

    volumes_with_parent: List[Union[PersistentVolume, RootfsVolume]] = [
        volume
        for volume in content.volumes
        if isinstance(volume, PersistentVolume) and volume.parent is not None
    ]

    if isinstance(content, InstanceContent):
        volumes_with_parent.append(content.rootfs)

    for volume in volumes_with_parent:
        if volume.parent:
            volume_metadata = _get_parent_volume_file(volume.parent)
            volume_size = volume.size_mib * 1024 * 1024
            if volume_size < volume_metadata.size:
                raise VmVolumeTooSmall(
                    parent_size=volume_metadata.size,
                    parent_ref=volume.parent.ref,
                    parent_file=volume_metadata.hash,
                    volume_name=getattr(volume, "name", "rootfs"),
                    volume_size=volume_size,
                )


class VmMessageHandler(ContentHandler):
    """
    Handles both PROGRAM and INSTANCE messages.

    The implementation for both is very similar, making it simpler to implement both
    in the same handler.

    """

    async def check_balance(
        self, session: DbSession, message: MessageDb
    ) -> List[AccountCostsDb]:
        content = _get_vm_content(message)

        required_tokens, costs = get_total_and_detailed_costs(
            session, content, message.item_hash
        )

        # NOTE: For now allow to create ephimeral programs for free, but generate a cost depending on the content.payment prop (HOLD / STREAM)
        if isinstance(content, ProgramContent):
            if not content.on.persistent:
                return costs

        # NOTE: For now allow to create anything that is being paid with STREAM for free, but generate a cost depending on the content.payment prop (HOLD / STREAM)
        if content.payment and content.payment.is_stream:
            return costs

        # NOTE: Instances and persistent Programs being paid by HOLD are the only ones being checked for now
        current_balance = get_total_balance(address=content.address, session=session)
        current_instance_costs = get_total_cost_for_address(
            session=session, address=content.address
        )

        if current_balance < current_instance_costs + required_tokens:
            raise InsufficientBalanceException(
                balance=Decimal(current_balance),
                required_balance=current_instance_costs + required_tokens,
            )

        return costs

    async def check_dependencies(self, session: DbSession, message: MessageDb) -> None:
        content = _get_vm_content(message)

        missing_volumes = find_missing_volumes(session=session, content=content)
        if missing_volumes:
            raise VmVolumeNotFound([volume for volume in missing_volumes])

        check_parent_volumes_size_requirements(session=session, content=content)

        # Check dependencies if the message updates an existing instance/program
        if (ref := content.replaces) is not None:
            original_program = get_program(session=session, item_hash=ref)
            if original_program is None:
                raise VmRefNotFound(ref)

            if original_program.replaces is not None:
                raise VmCannotUpdateUpdate()

            is_amend_allowed = is_vm_amend_allowed(session=session, vm_hash=ref)
            if is_amend_allowed is None:
                raise InternalError(f"Could not find current version of program {ref}")

            if not is_amend_allowed:
                raise VmUpdateNotAllowed()

    @staticmethod
    async def process_vm_message(session: DbSession, message: MessageDb):
        vm = vm_message_to_db(message)
        session.add(vm)

        program_ref = vm.replaces or vm.item_hash
        upsert_vm_version(
            session=session,
            vm_hash=vm.item_hash,
            owner=vm.owner,
            current_version=VmVersion(program_ref),
            last_updated=vm.created,
        )

    async def process(self, session: DbSession, messages: List[MessageDb]) -> None:
        for message in messages:
            await self.process_vm_message(session=session, message=message)

    async def forget_message(self, session: DbSession, message: MessageDb) -> Set[str]:
        content = _get_vm_content(message)

        LOGGER.debug("Deleting program %s...", message.item_hash)

        delete_vm(session=session, vm_hash=message.item_hash)

        if content.replaces:
            update_hashes = set()
        else:
            update_hashes = set(
                delete_vm_updates(session=session, vm_hash=message.item_hash)
            )

        refresh_vm_version(session=session, vm_hash=message.item_hash)

        return update_hashes
