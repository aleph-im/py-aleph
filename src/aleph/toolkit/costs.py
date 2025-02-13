from decimal import Decimal, ROUND_CEILING

from aleph.toolkit.constants import PRICE_PRECISION


def format_cost(v: Decimal | str, p: int = PRICE_PRECISION) -> Decimal:
    return Decimal(v).quantize(Decimal(1) / Decimal(10**p), ROUND_CEILING)


def format_cost_str(v: Decimal | str, p: int = PRICE_PRECISION) -> str:
    n = format_cost(v, p)
    return "{:.{p}f}".format(n, p=p)
