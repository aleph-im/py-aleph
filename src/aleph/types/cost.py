from decimal import Decimal
from enum import Enum
from typing import Optional

from aleph.db.models.aggregates import AggregateDb
from aleph.toolkit.constants import PRICE_MAX_PRICE


class ProductPriceType(str, Enum):
    STORAGE = "storage"
    WEB3_HOSTING = "web3_hosting"
    PROGRAM = "program"
    PROGRAM_PERSISTENT = "program_persistent"
    INSTANCE = "instance"
    INSTANCE_GPU_PREMIUM = "instance_gpu_premium"
    INSTANCE_CONFIDENTIAL = "instance_confidential"
    INSTANCE_GPU_STANDARD = "instance_gpu_standard"


class ProductPriceOptions:
    holding: Decimal
    payg: Decimal

    def __init__(self, holding: Optional[str | Decimal], payg: Optional[str | Decimal]):
        self.holding = Decimal(holding or PRICE_MAX_PRICE)
        self.payg = Decimal(payg or PRICE_MAX_PRICE)


class ProductComputeUnit:
    vcpus: int
    disk_mib: int
    memory_mib: int

    def __init__(self, vcpus: int, disk_mib: int, memory_mib: int):
        self.vcpus = vcpus
        self.disk_mib = disk_mib
        self.memory_mib = memory_mib


class ProductPrice:
    storage: ProductPriceOptions
    compute_unit: ProductPriceOptions

    def __init__(
        self,
        storage: ProductPriceOptions,
        compute_unit: ProductPriceOptions,
    ):
        self.storage = storage
        self.compute_unit = compute_unit


class ProductPricing:
    type: ProductPriceType
    price: ProductPrice
    compute_unit: Optional[ProductComputeUnit]

    def __init__(
        self,
        type: ProductPriceType,
        price: ProductPrice,
        compute_unit: Optional[ProductComputeUnit] = None,
    ):
        self.type = type
        self.price = price
        self.compute_unit = compute_unit

    @staticmethod
    def from_aggregate(type: ProductPriceType, aggregate: AggregateDb):
        content = aggregate.content[type.value]

        price = content["price"]
        compute_unit = content["compute_unit"]

        pricing = ProductPricing(
            type,
            ProductPrice(
                ProductPriceOptions(
                    price["storage"]["holding"],
                    price["storage"]["payg"],
                ),
                ProductPriceOptions(
                    price["compute_unit"]["holding"],
                    price["compute_unit"]["payg"],
                ),
            ),
        )

        if compute_unit:
            pricing.compute_unit = ProductComputeUnit(
                compute_unit["vcpus"],
                compute_unit["disk_mib"],
                compute_unit["memory_mib"],
            )

        return pricing
