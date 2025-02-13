import math
from decimal import Decimal
from functools import reduce
from typing import List, Optional, Tuple, Union

from aleph_message.models import (
    ExecutableContent,
    InstanceContent,
    PaymentType,
    ProgramContent,
    StoreContent,
)
from aleph_message.models.execution.environment import InstanceEnvironment
from aleph_message.models.execution.volume import ImmutableVolume

from aleph.db.accessors.aggregates import get_aggregate_by_key
from aleph.db.accessors.cost import get_message_costs
from aleph.db.accessors.files import get_file_tag, get_message_file_pin
from aleph.db.models import FileTagDb, MessageFilePinDb, StoredFileDb
from aleph.db.models.account_costs import AccountCostsDb
from aleph.db.models.aggregates import AggregateDb
from aleph.toolkit.constants import (
    HOUR,
    PRICE_AGGREGATE_KEY,
    PRICE_AGGREGATE_OWNER,
    MiB,
)
from aleph.toolkit.costs import format_cost
from aleph.types.cost import (
    CostType,
    ProductPriceType,
    ProductPricing,
    RefVolume,
    SizedVolume,
)
from aleph.types.db_session import DbSession
from aleph.types.files import FileTag

type CostComputableContent = ExecutableContent | StoreContent


def get_payment_type(content: CostComputableContent) -> PaymentType:
    return (
        PaymentType.superfluid
        if (
            hasattr(content, "payment")
            and content.payment
            and content.payment.is_stream
        )
        else PaymentType.hold
    )


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


def _get_product_price_type(content: CostComputableContent) -> ProductPriceType:
    if isinstance(content, StoreContent):
        return ProductPriceType.STORAGE

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
    session: DbSession, content: CostComputableContent
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


def _get_volumes_costs(
    session: DbSession,
    volumes: List[RefVolume | SizedVolume],
    payment_type: PaymentType,
    price_per_mib: Decimal,
    price_per_mib_second: Decimal,
    owner: str,
    item_hash: str,
) -> List[AccountCostsDb]:
    costs: List[AccountCostsDb] = []

    for volume in volumes:
        if isinstance(volume, SizedVolume):
            storage_mib = Decimal(volume.size_mib)

            cost_hold = format_cost(storage_mib * price_per_mib)
            cost_stream = format_cost(
                storage_mib * price_per_mib_second,
            )

            costs.append(
                AccountCostsDb(
                    owner=owner,
                    item_hash=item_hash,
                    type=volume.cost_type,
                    name=volume.name,
                    payment_type=payment_type,
                    cost_hold=cost_hold,
                    cost_stream=cost_stream,
                )
            )

        elif isinstance(volume, RefVolume):
            file = _get_file_from_ref(
                session=session, ref=volume.ref, use_latest=volume.use_latest
            )
            if file is None:
                raise RuntimeError(
                    f"Could not find entry in file tags for {volume.ref}."
                )

            storage_mib = Decimal(file.size / MiB)

            cost_hold = format_cost(storage_mib * price_per_mib)
            cost_stream = format_cost(
                storage_mib * price_per_mib_second,
            )

            costs.append(
                AccountCostsDb(
                    owner=owner,
                    item_hash=item_hash,
                    type=volume.cost_type,
                    name=volume.name,
                    ref=volume.ref,
                    payment_type=payment_type,
                    cost_hold=cost_hold,
                    cost_stream=cost_stream,
                )
            )

    return costs


def _get_execution_volumes_costs(
    session: DbSession,
    content: ExecutableContent,
    pricing: ProductPricing,
    payment_type: PaymentType,
    item_hash: str,
) -> List[AccountCostsDb]:
    volumes: List[RefVolume | SizedVolume] = []

    if isinstance(content, InstanceContent):
        volumes.append(
            SizedVolume(
                CostType.EXECUTION_INSTANCE_VOLUME_ROOTFS,
                content.rootfs.size_mib,
                content.rootfs.parent.ref,
            )
        )

    elif isinstance(content, ProgramContent):
        volumes += [
            RefVolume(
                CostType.EXECUTION_PROGRAM_VOLUME_CODE,
                content.code.ref,
                content.code.use_latest,
            ),
            RefVolume(
                CostType.EXECUTION_PROGRAM_VOLUME_RUNTIME,
                content.runtime.ref,
                content.runtime.use_latest,
            ),
        ]

        if content.data:
            volumes.append(
                RefVolume(
                    CostType.EXECUTION_PROGRAM_VOLUME_DATA,
                    content.data.ref,
                    content.data.use_latest,
                ),
            )

    for volume in content.volumes:
        if isinstance(volume, ImmutableVolume):
            volumes.append(
                RefVolume(
                    CostType.EXECUTION_VOLUME_INMUTABLE,
                    volume.ref,
                    volume.use_latest,
                    volume.mount,
                ),
            )
        else:
            volumes.append(
                SizedVolume(
                    CostType.EXECUTION_VOLUME_PERSISTENT,
                    volume.size_mib,
                    volume.mount,
                ),
            )

    price_per_mib = pricing.price.storage.holding
    price_per_mib_second = pricing.price.storage.payg / HOUR

    return _get_volumes_costs(
        session,
        volumes,
        payment_type,
        price_per_mib,
        price_per_mib_second,
        content.address,
        item_hash,
    )


def _get_additional_storage_price(
    session: DbSession,
    content: ExecutableContent,
    pricing: ProductPricing,
    payment_type: PaymentType,
    item_hash: str,
) -> List[AccountCostsDb]:
    # EXECUTION VOLUMES COSTS
    costs = _get_execution_volumes_costs(
        session, content, pricing, payment_type, item_hash
    )

    # EXECUTION STORAGE DISCOUNT
    nb_compute_units = _get_nb_compute_units(content)
    execution_volume_discount_mib = (
        pricing.compute_unit.disk_mib if pricing.compute_unit else 0
    ) * nb_compute_units

    price_per_mib = pricing.price.storage.holding
    price_per_mib_second = pricing.price.storage.payg / HOUR

    max_discount_hold = execution_volume_discount_mib * price_per_mib
    max_discount_stream = execution_volume_discount_mib * price_per_mib_second

    discount_holding = min(
        Decimal(reduce(lambda x, y: x + Decimal(y.cost_hold), costs, Decimal(0))),
        max_discount_hold,
    )
    discount_stream = min(
        Decimal(reduce(lambda x, y: x + Decimal(y.cost_stream), costs, Decimal(0))),
        max_discount_stream,
    )

    cost_hold = format_cost(-discount_holding)
    cost_stream = format_cost(-discount_stream)

    costs.append(
        AccountCostsDb(
            owner=content.address,
            item_hash=item_hash,
            type=CostType.EXECUTION_VOLUME_DISCOUNT,
            name=CostType.EXECUTION_VOLUME_DISCOUNT,
            payment_type=payment_type,
            cost_hold=cost_hold,
            cost_stream=cost_stream,
        )
    )

    return costs


def _calculate_executable_costs(
    session: DbSession,
    content: ExecutableContent,
    pricing: ProductPricing,
    item_hash: str,
) -> List[AccountCostsDb]:
    payment_type = get_payment_type(content)

    # EXECUTION COST
    compute_units_required = _get_nb_compute_units(content)
    compute_unit_multiplier = _get_compute_unit_multiplier(content)

    if not pricing.price.compute_unit:
        raise ValueError(
            "compute_unit not defined for type '{}' in pricing aggregate".format(
                pricing.type.value
            )
        )

    compute_unit_cost = pricing.price.compute_unit.holding
    compute_unit_cost_second = pricing.price.compute_unit.payg / HOUR

    compute_unit_price = (
        compute_units_required * compute_unit_multiplier * compute_unit_cost
    )
    compute_unit_price_stream = (
        compute_units_required * compute_unit_multiplier * compute_unit_cost_second
    )

    cost_hold = format_cost(compute_unit_price)
    cost_stream = format_cost(compute_unit_price_stream)

    execution_cost = AccountCostsDb(
        owner=content.address,
        item_hash=item_hash,
        type=CostType.EXECUTION,
        name=pricing.type,
        payment_type=payment_type,
        cost_hold=cost_hold,
        cost_stream=cost_stream,
    )

    costs: List[AccountCostsDb] = [execution_cost]
    costs += _get_additional_storage_price(
        session, content, pricing, payment_type, item_hash
    )

    return costs


def _calculate_storage_costs(
    session: DbSession,
    content: StoreContent,
    pricing: ProductPricing,
    item_hash: str,
) -> List[AccountCostsDb]:
    payment_type = get_payment_type(content)
    volume = RefVolume(CostType.STORAGE, item_hash, False)

    price_per_mib = pricing.price.storage.holding
    price_per_mib_second = pricing.price.storage.payg / HOUR

    return _get_volumes_costs(
        session,
        [volume],
        payment_type,
        price_per_mib,
        price_per_mib_second,
        content.address,
        item_hash,
    )


def get_detailed_costs(
    session: DbSession,
    content: CostComputableContent,
    item_hash: str,
    pricing: Optional[ProductPricing] = None,
) -> List[AccountCostsDb]:
    pricing = pricing or _get_product_price(session, content)

    if isinstance(content, StoreContent):
        return _calculate_storage_costs(session, content, pricing, item_hash)
    else:
        return _calculate_executable_costs(session, content, pricing, item_hash)


def get_total_and_detailed_costs(
    session: DbSession,
    content: CostComputableContent,
    item_hash: str,
) -> Tuple[Decimal, List[AccountCostsDb]]:
    payment_type = get_payment_type(content)

    costs = get_detailed_costs(session, content, item_hash)
    cost = format_cost(
        reduce(lambda x, y: x + y.cost_stream, costs, Decimal(0))
        if payment_type == PaymentType.superfluid
        else reduce(lambda x, y: x + y.cost_hold, costs, Decimal(0))
    )

    return Decimal(cost), list(costs)


def get_get_total_and_detailed_costs_from_db(
    session: DbSession,
    content: ExecutableContent,
    item_hash: str,
) -> Tuple[Decimal, List[AccountCostsDb]]:
    payment_type = get_payment_type(content)

    costs = get_message_costs(session, item_hash)
    cost = format_cost(
        reduce(lambda x, y: x + y.cost_stream, costs, Decimal(0))
        if payment_type == PaymentType.superfluid
        else reduce(lambda x, y: x + y.cost_hold, costs, Decimal(0))
    )

    return Decimal(cost), list(costs)
