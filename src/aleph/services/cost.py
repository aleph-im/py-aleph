import math
from decimal import Decimal
from typing import Optional, Union

from aleph_message.models import ExecutableContent, InstanceContent, ProgramContent
from aleph_message.models.execution.volume import ImmutableVolume

from aleph.db.accessors.files import get_file_tag, get_message_file_pin
from aleph.db.models import StoredFileDb, FileTagDb, MessageFilePinDb
from aleph.toolkit.constants import GiB, MiB
from aleph.types.db_session import DbSession
from aleph.types.files import FileTag


def _get_file_from_ref(
    session: DbSession, ref: str, use_latest: bool
) -> Optional[StoredFileDb]:
    tag_or_pin: Optional[Union[MessageFilePinDb, FileTagDb]]

    if use_latest:
        tag_or_pin = get_file_tag(session=session, tag=FileTag(ref))
    else:
        tag_or_pin = get_message_file_pin(session=session, item_hash=ref)

    if tag_or_pin:
        return tag_or_pin.file

    return None


def get_volume_size(session: DbSession, content: ExecutableContent) -> int:
    ref_volumes = []
    sized_volumes = []

    if isinstance(content, InstanceContent):
        sized_volumes.append(content.rootfs)
    elif isinstance(content, ProgramContent):
        ref_volumes += [content.code, content.runtime]
        if content.data:
            ref_volumes.append(content.data)

    for volume in content.volumes:
        if isinstance(volume, ImmutableVolume):
            ref_volumes.append(volume)
        else:
            sized_volumes.append(volume)

    total_volume_size: int = 0

    for volume in ref_volumes:
        file = _get_file_from_ref(
            session=session, ref=volume.ref, use_latest=volume.use_latest
        )
        if file is None:
            raise RuntimeError(f"Could not find entry in file tags for {volume.ref}.")
        total_volume_size += file.size

    for volume in sized_volumes:
        total_volume_size += volume.size_mib * MiB

    return total_volume_size


def get_additional_storage_price(
    content: ExecutableContent, session: DbSession
) -> Decimal:
    is_microvm = isinstance(content, ProgramContent) and not content.on.persistent
    nb_compute_units = content.resources.vcpus
    free_storage_per_compute_unit = 2 * GiB if is_microvm else 20 * GiB

    total_volume_size = get_volume_size(session, content)
    additional_storage = max(
        total_volume_size - (free_storage_per_compute_unit * nb_compute_units), 0
    )
    price = Decimal(additional_storage) / 20 / MiB
    return price


def _get_nb_compute_units(content: ExecutableContent) -> int:
    cpu = content.resources.vcpus
    memory = math.ceil(content.resources.memory / 2048)
    nb_compute_units = cpu if cpu >= memory else memory
    return nb_compute_units


def _get_compute_unit_multiplier(content: ExecutableContent) -> int:
    compute_unit_multiplier = 1
    if isinstance(content, ProgramContent) and not content.on.persistent and content.environment.internet:
        compute_unit_multiplier += 1
    return compute_unit_multiplier


def compute_cost(session: DbSession, content: ExecutableContent) -> Decimal:
    is_microvm = isinstance(content, ProgramContent) and not content.on.persistent

    compute_unit_cost = 200 if is_microvm else 2000

    compute_units_required = _get_nb_compute_units(content)
    compute_unit_multiplier = _get_compute_unit_multiplier(content)

    compute_unit_price = (
        Decimal(compute_units_required) * compute_unit_multiplier * compute_unit_cost
    )
    price = compute_unit_price + get_additional_storage_price(content, session)
    return Decimal(price)
