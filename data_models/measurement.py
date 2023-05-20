import attr
from pydantic import BaseModel


@attr.s
class ACMeasurement(BaseModel):
    voltage: float
    current: float
    power: float
    temperature: float
    yieldTotal: float
    yieldDay: float


@attr.s
class DCMeasurement(BaseModel):
    input_number: int
    current: float
    voltage: float
    power: float
    yieldDay: float
    yieldTotal: float
