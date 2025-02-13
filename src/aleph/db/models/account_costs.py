from decimal import Decimal
from typing import Optional

from aleph_message.models import PaymentType
from sqlalchemy import DECIMAL, BigInteger, Column, ForeignKey, String, UniqueConstraint
from sqlalchemy_utils.types.choice import ChoiceType

from aleph.types.cost import CostType

from .base import Base


class AccountCostsDb(Base):
    __tablename__ = "account_costs"

    id: int = Column(BigInteger, primary_key=True)
    owner: str = Column(String, nullable=False, index=True)
    # item_hash: str = Column(String, nullable=False)
    item_hash: str = Column(
        ForeignKey("messages.item_hash", ondelete="CASCADE"), nullable=False
    )
    type: CostType = Column(ChoiceType(CostType), nullable=False)
    name: str = Column(String, nullable=False)
    ref: Optional[str] = Column(String, nullable=True)
    payment_type: PaymentType = Column(ChoiceType(PaymentType), nullable=False)
    cost_hold: Decimal = Column(DECIMAL, nullable=False)
    cost_stream: Decimal = Column(DECIMAL, nullable=False)

    __table_args__ = (UniqueConstraint("owner", "item_hash", "type", "name"),)
