from typing import List

from pydantic import BaseModel, validator

from aleph.toolkit.costs import format_cost_str


class EstimatedCostDetailResponse(BaseModel):
    class Config:
        orm_mode = True

    type: str
    name: str
    cost_hold: str
    cost_stream: str

    @validator("cost_hold", "cost_stream")
    def check_format_price(cls, v):
        return format_cost_str(v)


class EstimatedCostsResponse(BaseModel):
    class Config:
        orm_mode = True

    required_tokens: float
    payment_type: str
    cost: str
    detail: List[EstimatedCostDetailResponse]
