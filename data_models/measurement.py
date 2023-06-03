import attr
from pydantic import BaseModel


class MetaInfo(BaseModel):
    channel: int
    serial: str


@attr.s
class VoltageMeasurement(BaseModel):
    meta: MetaInfo
    voltage: float


@attr.s
class CurrentMeasurement(BaseModel):
    meta: MetaInfo
    current: float


@attr.s
class YieldMeasurement(BaseModel):
    meta: MetaInfo
    yieldToday: float


@attr.s
class TemperatureMeasurement(BaseModel):
    meta: MetaInfo
    temperature: float
