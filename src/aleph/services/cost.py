import math
from decimal import Decimal
from typing import List, Optional, Union

from aleph_message.models import ExecutableContent, InstanceContent, ProgramContent
from aleph_message.models.execution.environment import InstanceEnvironment
from aleph_message.models.execution.instance import RootfsVolume
from aleph_message.models.execution.volume import (
    EphemeralVolume,
    ImmutableVolume,
    PersistentVolume,
)

from aleph.db.accessors.aggregates import get_aggregate_by_key
from aleph.db.accessors.files import get_file_tag, get_message_file_pin
from aleph.db.models import FileTagDb, MessageFilePinDb, StoredFileDb
from aleph.db.models.aggregates import AggregateDb
from aleph.toolkit.constants import (
    HOUR,
    PRICE_AGGREGATE_KEY,
    PRICE_AGGREGATE_OWNER,
    MiB,
)
from aleph.types.cost import ProductPriceType, ProductPricing
from aleph.types.db_session import DbSession
from aleph.types.files import FileTag


def _is_on_demand(content: ExecutableContent) -> bool:
    return isinstance(content, ProgramContent) and not content.on.persistent


def _is_confidential_vm(
    content: ExecutableContent, is_on_demand: Optional[bool]
) -> bool:
    is_on_demand = is_on_demand or _is_on_demand(content=content)

    return (
        not is_on_demand
        and isinstance(getattr(content, "environment", None), InstanceEnvironment)
        and getattr(content.environment, "trusted_execution", False)
    )


def _get_product_price_type(content: ExecutableContent) -> ProductPriceType:
    is_on_demand = _is_on_demand(content)
    is_confidential_vm = _is_confidential_vm(content, is_on_demand)

    return (
        ProductPriceType.PROGRAM
        if is_on_demand
        else (
            ProductPriceType.INSTANCE_CONFIDENTIAL
            if is_confidential_vm
            else ProductPriceType.INSTANCE
        )
    )


# TODO: Cache aggregate for 5 min
def _get_price_aggregate(session: DbSession) -> AggregateDb:
    aggregate = get_aggregate_by_key(
        session=session, owner=PRICE_AGGREGATE_OWNER, key=PRICE_AGGREGATE_KEY
    )

    if not aggregate:
        raise Exception()

    return aggregate


def _get_product_price(
    session: DbSession, content: ExecutableContent
) -> ProductPricing:
    type = _get_product_price_type(content)
    aggregate = _get_price_aggregate(session)

    return ProductPricing.from_aggregate(type, aggregate)


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


def _get_nb_compute_units(content: ExecutableContent) -> int:
    cpu = content.resources.vcpus
    memory = math.ceil(content.resources.memory / 2048)
    nb_compute_units = cpu if cpu >= memory else memory
    return nb_compute_units


# TODO: Include this in the aggregate
def _get_compute_unit_multiplier(content: ExecutableContent) -> int:
    compute_unit_multiplier = 1
    if (
        isinstance(content, ProgramContent)
        and not content.on.persistent
        and content.environment.internet
    ):
        compute_unit_multiplier += 1
    return compute_unit_multiplier


def get_volume_size(session: DbSession, content: ExecutableContent) -> int:
    ref_volumes = []
    sized_volumes: List[Union[EphemeralVolume, PersistentVolume, RootfsVolume]] = []

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
        if hasattr(volume, "ref") and hasattr(volume, "use_latest"):
            file = _get_file_from_ref(
                session=session, ref=volume.ref, use_latest=volume.use_latest
            )
            if file is None:
                raise RuntimeError(
                    f"Could not find entry in file tags for {volume.ref}."
                )
            total_volume_size += file.size
        else:
            raise RuntimeError(f"Could not find reference hash for {volume}.")

    for volume in sized_volumes:
        total_volume_size += volume.size_mib * MiB

    return total_volume_size


def get_additional_storage_bytes(
    content: ExecutableContent, pricing: ProductPricing, session: DbSession
) -> Decimal:
    nb_compute_units = _get_nb_compute_units(content)
    included_storage_per_compute_unit = (
        pricing.compute_unit.disk_mib if pricing.compute_unit else 0
    ) * MiB
    total_storage_for_free = included_storage_per_compute_unit * nb_compute_units

    total_volume_size = get_volume_size(session, content)

    additional_storage = max(
        total_volume_size - total_storage_for_free,
        0,
    )

    return Decimal(additional_storage)


def get_additional_storage_price(
    content: ExecutableContent, pricing: ProductPricing, session: DbSession
) -> Decimal:
    additional_storage_bytes = get_additional_storage_bytes(content, pricing, session)

    additional_storage_mib = additional_storage_bytes / MiB
    price_per_mib = pricing.price.storage.holding

    return additional_storage_mib * price_per_mib


def _get_additional_storage_flow_price(
    content: ExecutableContent, pricing: ProductPricing, session: DbSession
) -> Decimal:
    additional_storage_bytes = get_additional_storage_bytes(content, pricing, session)

    additional_storage_mib = additional_storage_bytes / MiB
    price_per_mib_hour = pricing.price.storage.payg
    price_per_mib_second = price_per_mib_hour / HOUR

    return additional_storage_mib * price_per_mib_second


def compute_cost(session: DbSession, content: ExecutableContent) -> Decimal:
    pricing = _get_product_price(session, content)

    compute_unit_cost = pricing.price.compute_unit.holding

    compute_units_required = _get_nb_compute_units(content)
    compute_unit_multiplier = _get_compute_unit_multiplier(content)

    compute_unit_price = (
        compute_units_required * compute_unit_multiplier * compute_unit_cost
    )

    additional_storage_price = get_additional_storage_price(content, pricing, session)
    return Decimal(compute_unit_price + additional_storage_price)


def compute_flow_cost(session: DbSession, content: ExecutableContent) -> Decimal:
    pricing = _get_product_price(session, content)

    compute_unit_cost_hour = pricing.price.compute_unit.payg
    compute_unit_cost_second = compute_unit_cost_hour / HOUR

    compute_units_required = _get_nb_compute_units(content)
    compute_unit_multiplier = _get_compute_unit_multiplier(content)

    compute_unit_price = (
        compute_units_required * compute_unit_multiplier * compute_unit_cost_second
    )

    additional_storage_flow_price = _get_additional_storage_flow_price(
        content, pricing, session
    )
    return Decimal(compute_unit_price + additional_storage_flow_price)
