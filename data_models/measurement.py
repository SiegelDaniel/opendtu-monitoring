from pydantic import BaseModel


class MetaInfo(BaseModel):
    channel: int
    serial: str


class VoltageMeasurement(BaseModel):
    meta: MetaInfo
    voltage: float


class CurrentMeasurement(BaseModel):
    meta: MetaInfo
    current: float


class YieldMeasurement(BaseModel):
    meta: MetaInfo
    yieldToday: float


class TemperatureMeasurement(BaseModel):
    meta: MetaInfo
    temperature: float
